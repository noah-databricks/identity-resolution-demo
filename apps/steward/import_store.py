"""Atomic release import and release replacement. Never silently drops steward work.

``import_prepared`` is the first load into an empty queue. ``replace_release``
archives every row of the active release's review state (cases, proposals,
decisions, outbox, golden fields, agent runs and events) into
``release_archive_rows`` under one ``release_archives`` batch, proves the copy by
count and content digest, marks the prior ledger row inactive, and imports the
new release, all in one transaction. The new cases carry ``baseline_payload`` and
the new ledger row is the only active one, so ``reset_demo.py`` restores the new
release's baseline.
"""
from dataclasses import asdict
from psycopg.types.json import Jsonb
from domain import InvalidDecision, digest

# Archive order is parent first; deletion is the reverse (foreign keys).
ARCHIVE_TABLES = (
    ('review_cases', "case_id"),
    ('proposals', "proposal_id"),
    ('decisions', "decision_id"),
    ('outbox', "event_id"),
    ('reviewed_golden_fields', "decision_id || '/' || guest_identity || '/' || field_name"),
    ('agent_runs', "run_id"),
    ('agent_events', "lpad(sequence::text, 20, '0')"),
)
IMPORT_LOCK = "SELECT pg_advisory_xact_lock(hashtextextended('identity-steward-import',0))"
PUBLICATION_LOCK = "SELECT pg_advisory_xact_lock(hashtextextended('steward-publication',0))"


def content_digest_for(prepared, release_manifest):
    return digest({'prepared': prepared.fingerprint, 'manifest': release_manifest})


def insert_cases(conn, prepared):
    with conn.cursor() as cursor:
        cursor.executemany('INSERT INTO steward.review_cases '
            '(case_id,source_run_id,version,status,payload,baseline_payload,search_text,pipeline_confidence) '
            "VALUES (%s,%s,%s,'REVIEW',%s,%s,%s,%s)",
            [(case.case_id, case.source_run_id, case.version, Jsonb(asdict(case)), Jsonb(asdict(case)),
              ' '.join(r.name or '' for r in case.records),
              min((p.evidence_score for p in case.pair_evidence if p.decision == 'REVIEW'), default=None))
             for case in prepared.cases])


def import_prepared(pool, prepared, *, release_manifest):
    if release_manifest.get('source_run_id') != prepared.source_run_id:
        raise InvalidDecision('Import manifest run mismatch')
    content_digest = content_digest_for(prepared, release_manifest)
    summary = prepared.summary()
    with pool.connection() as conn, conn.transaction():
        conn.execute(IMPORT_LOCK)
        prior = conn.execute('SELECT content_digest,summary,active FROM steward.import_batches '
                             'WHERE source_run_id=%s', (prepared.source_run_id,)).fetchone()
        if prior:
            if prior[0] != content_digest:
                raise InvalidDecision('This release was already imported with different content')
            if not prior[2]:
                raise InvalidDecision('This release was archived; re-activating an archived release is not supported')
            # Never re-INSERT original memberships: applied decisions survive retry.
            return {**prior[1], 'replayed': True}
        if conn.execute('SELECT 1 FROM steward.review_cases LIMIT 1').fetchone():
            raise InvalidDecision('Existing review cases require explicit release reconciliation '
                                  '(import_release.py --replace-release)')
        insert_cases(conn, prepared)
        conn.execute('INSERT INTO steward.import_batches '
            '(source_run_id,content_digest,release_manifest,summary) VALUES (%s,%s,%s,%s)',
            (prepared.source_run_id, content_digest, Jsonb(release_manifest), Jsonb(summary)))
    return {**summary, 'replayed': False}


def require_reconciliation_schema(conn):
    present = {row[0] for row in conn.execute(
        "SELECT table_name||'.'||column_name FROM information_schema.columns "
        "WHERE table_schema='steward' AND table_name IN "
        "('import_batches','release_archives','release_archive_rows')").fetchall()}
    needed = {'import_batches.active', 'import_batches.archive_id', 'release_archives.archive_id',
              'release_archive_rows.row_data'}
    if not needed <= present:
        raise InvalidDecision('Reconciliation tables are missing: deploy the app first so schema.sql '
                              'adds them (databricks bundle deploy/run -t demo)')


def table_snapshot(conn, table, key):
    """Row count and content digest of a live table, in archive-key order."""
    return tuple(conn.execute(
        f'SELECT count(*), coalesce(md5(string_agg(to_jsonb(t)::text, chr(10) ORDER BY {key})), \'\') '
        f'FROM steward.{table} t').fetchone())


def archive_snapshot(conn, archive_id, table):
    return tuple(conn.execute(
        "SELECT count(*), coalesce(md5(string_agg(row_data::text, chr(10) ORDER BY row_key)), '') "
        'FROM steward.release_archive_rows WHERE archive_id=%s AND table_name=%s',
        (archive_id, table)).fetchone())


def archive_plan(conn, prior_run):
    counts, digests = {}, {}
    for table, key in ARCHIVE_TABLES:
        counts[table], digests[table] = table_snapshot(conn, table, key)
    by_status = dict(conn.execute('SELECT status, count(*) FROM steward.review_cases '
                                  'GROUP BY 1 ORDER BY 1').fetchall())
    unpublished = conn.execute('SELECT count(*) FROM steward.outbox '
                               'WHERE published_at IS NULL').fetchone()[0]
    return {'release': prior_run, 'row_counts': counts, 'row_digests': digests,
            'cases_by_status': by_status, 'unpublished_outbox_events': unpublished}


def replace_release(pool, prepared, *, release_manifest, apply=False):
    """Archive the active release and import ``prepared`` in its place. Idempotent.

    Outcomes: ``already_active`` (same run and content: nothing changes),
    ``would_replace`` / ``replaced``, ``would_import`` / ``imported`` (empty
    ledger). Refuses changed content for a known run, re-activation of an
    archived run, a queue holding cases from a non-active run, and in-flight
    agent runs (the worker would write events against archived cases).
    """
    if release_manifest.get('source_run_id') != prepared.source_run_id:
        raise InvalidDecision('Import manifest run mismatch')
    new_run = prepared.source_run_id
    content_digest = content_digest_for(prepared, release_manifest)
    summary = prepared.summary()
    with pool.connection() as conn, conn.transaction():
        # Same lock order as import, then publication/reset; nothing else takes both.
        conn.execute(IMPORT_LOCK)
        conn.execute(PUBLICATION_LOCK)
        require_reconciliation_schema(conn)
        if apply:
            # The app keeps serving reads, but no proposal, decision or agent write
            # can land between the archive copy and the delete below.
            conn.execute('LOCK TABLE ' + ', '.join('steward.' + t for t, _ in ARCHIVE_TABLES)
                         + ' IN EXCLUSIVE MODE')
        active = conn.execute('SELECT source_run_id,content_digest,release_manifest,summary,created_at '
                              'FROM steward.import_batches WHERE active').fetchall()
        if len(active) > 1:
            raise InvalidDecision('More than one active release in the import ledger')
        prior = active[0] if active else None
        known = conn.execute('SELECT content_digest,summary,active,archive_id FROM steward.import_batches '
                             'WHERE source_run_id=%s', (new_run,)).fetchone()
        runs = dict(conn.execute('SELECT source_run_id,count(*) FROM steward.review_cases '
                                 'GROUP BY 1').fetchall())
        plan = archive_plan(conn, prior[0]) if prior else None
        base = {'new_release': new_run, 'new_cases': len(prepared.cases),
                'active_release': prior[0] if prior else None, 'archive_plan': plan}
        if known:
            if known[0] != content_digest:
                raise InvalidDecision('This release was already imported with different content')
            if not known[2]:
                raise InvalidDecision(f'This release was archived ({known[3]}); re-activating an '
                                      'archived release is not supported')
            if set(runs) - {new_run}:
                raise InvalidDecision('Queue holds cases from a release that is not active')
            return {**base, 'outcome': 'already_active', 'replayed': True,
                    'message': 'This release is already the active release with identical content; '
                               'nothing to do.', 'summary': known[1]}
        if prior is None:
            if runs:
                raise InvalidDecision('Queue holds cases but the ledger has no active release')
            if not apply:
                return {**base, 'outcome': 'would_import', 'replayed': False, 'summary': summary}
            insert_cases(conn, prepared)
            conn.execute('INSERT INTO steward.import_batches '
                         '(source_run_id,content_digest,release_manifest,summary) VALUES (%s,%s,%s,%s)',
                         (new_run, content_digest, Jsonb(release_manifest), Jsonb(summary)))
            return {**base, 'outcome': 'imported', 'replayed': False, 'summary': summary}
        prior_run = prior[0]
        if set(runs) - {prior_run}:
            raise InvalidDecision('Queue holds cases from a release that is not active')
        in_flight = conn.execute("SELECT count(*) FROM steward.agent_runs "
                                 "WHERE status IN ('QUEUED','RUNNING')").fetchone()[0]
        if in_flight:
            raise InvalidDecision(f'{in_flight} agent reviews are queued or running; wait for them to finish')
        archive_id = 'archive-' + digest({'archived': prior_run, 'ledger_digest': prior[1],
                                          'replaced_by': new_run, 'content': content_digest})[:24]
        if not apply:
            return {**base, 'outcome': 'would_replace', 'replayed': False, 'archive_id': archive_id,
                    'summary': summary}
        conn.execute('INSERT INTO steward.release_archives (archive_id,source_run_id,'
                     'superseded_by_run_id,prior_ledger,row_counts,row_digests) VALUES (%s,%s,%s,%s,%s,%s)',
                     (archive_id, prior_run, new_run,
                      Jsonb({'content_digest': prior[1], 'release_manifest': prior[2], 'summary': prior[3],
                             'imported_at': prior[4].isoformat() if hasattr(prior[4], 'isoformat') else prior[4]}),
                      Jsonb(plan['row_counts']), Jsonb(plan['row_digests'])))
        for table, key in ARCHIVE_TABLES:
            conn.execute('INSERT INTO steward.release_archive_rows (archive_id,table_name,row_key,row_data) '
                         f'SELECT %s, %s, {key}, to_jsonb(t) FROM steward.{table} t',
                         (archive_id, table))
            if archive_snapshot(conn, archive_id, table) != (plan['row_counts'][table], plan['row_digests'][table]):
                raise InvalidDecision(f'Archived {table} differ from the live rows; nothing changed')
        for table, _ in reversed(ARCHIVE_TABLES):
            deleted = conn.execute(f'DELETE FROM steward.{table}').rowcount
            if deleted != plan['row_counts'][table]:
                raise InvalidDecision(f'{table} changed during reconciliation; nothing changed')
        conn.execute('UPDATE steward.import_batches SET active=false, superseded_at=now(), '
                     'superseded_by_run_id=%s, archive_id=%s WHERE source_run_id=%s',
                     (new_run, archive_id, prior_run))
        insert_cases(conn, prepared)
        conn.execute('INSERT INTO steward.import_batches '
                     '(source_run_id,content_digest,release_manifest,summary,replaces_run_id) '
                     'VALUES (%s,%s,%s,%s,%s)',
                     (new_run, content_digest, Jsonb(release_manifest), Jsonb(summary), prior_run))
        after = dict(conn.execute('SELECT source_run_id,count(*) FROM steward.review_cases '
                                  'GROUP BY 1').fetchall())
        if after != ({new_run: len(prepared.cases)} if prepared.cases else {}):
            raise InvalidDecision('Imported queue does not match the prepared release; rolled back')
    return {**base, 'outcome': 'replaced', 'replayed': False, 'archive_id': archive_id,
            'archived_rows': plan['row_counts'], 'summary': summary}
