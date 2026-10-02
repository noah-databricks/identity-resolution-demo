"""Deterministic demo reset: restore the active release's baseline. Dry-run by default.

The baseline is the release marked active in the import ledger. After
``import_release.py --replace-release --apply`` that is the new release, so a
reset returns to the new release's imported cases; archived releases are never
restored or touched.

Lakebase: every review case returns to its imported payload (version 1, resolver
groups, no golden fields, status REVIEW); proposals, decisions, outbox events,
golden-field choices, agent runs and agent events are deleted. The restored cases
are proven identical to the import by recomputing the import fingerprint stored
in the import ledger. The import ledger itself is never modified.

Gold: the release's steward decision events are deleted and the reviewed links
and reviewed profiles are republished from the now empty outbox, so no
STEWARD_RESOLVED links and no reviewed profiles remain. Resolver Gold tables and
truth or evaluation tables are never touched.

Usage (from the repository root):
  uv run --project apps/steward python apps/steward/reset_demo.py \\
    --profile <profile> --warehouse-id <warehouse-id> [--apply]
The release manifest defaults to the active ledger row; --manifest must match it.
"""
import argparse
import json
from pathlib import Path

from psycopg.types.json import Jsonb

from domain import digest
from store import load_case

TABLES = ('proposals', 'decisions', 'outbox', 'reviewed_golden_fields', 'agent_runs', 'agent_events')
ACTIVE_LEDGER = ('SELECT source_run_id, summary, release_manifest FROM steward.import_batches '
                 'WHERE active')


def active_release(conn):
    rows = conn.execute(ACTIVE_LEDGER).fetchall()
    if len(rows) != 1:
        raise ValueError('Reset expects exactly one active imported release')
    return rows[0]


def lakebase_state(conn):
    cases = conn.execute('SELECT status, count(*) FROM steward.review_cases '
                         'GROUP BY status ORDER BY status').fetchall()
    versions = conn.execute('SELECT count(*) FROM steward.review_cases '
                            "WHERE version <> 1 OR (payload->>'version')::int <> 1").fetchone()[0]
    runs = [row[0] for row in conn.execute('SELECT DISTINCT source_run_id FROM steward.review_cases '
                                           'ORDER BY 1').fetchall()]
    state = {'review_cases_by_status': dict(cases), 'cases_not_at_version_1': versions,
             'case_release_ids': runs}
    for table in TABLES:
        state[table] = conn.execute(f'SELECT count(*) FROM steward.{table}').fetchone()[0]
    return state


def baselines(conn):
    """Imported payload per case: stored baseline, else reconstructed from first decision."""
    first = {row[0]: (row[1], row[2]) for row in conn.execute(
        "SELECT DISTINCT ON (case_id) case_id, previous_groups, (payload->>'base_case_version')::int "
        'FROM steward.decisions ORDER BY case_id, created_at, applied_case_version').fetchall()}
    result = {}
    for case_id, payload, baseline in conn.execute(
            'SELECT case_id, payload, baseline_payload FROM steward.review_cases').fetchall():
        if baseline is not None:
            result[case_id] = baseline
        elif case_id in first:
            groups, base_version = first[case_id]
            if base_version != 1:
                raise ValueError(f'{case_id}: first decision does not start from the import')
            result[case_id] = {**payload, 'version': 1, 'current_groups': groups, 'golden_fields': []}
        elif payload['version'] == 1 and not payload.get('golden_fields'):
            result[case_id] = payload
        else:
            raise ValueError(f'{case_id}: changed without a decision; baseline unknown')
    return result


def import_fingerprint(conn, payloads):
    """Recompute PreparedImport.fingerprint for these payloads against the ledger."""
    from integration import PreparedImport
    run, summary = active_release(conn)[:2]
    if any(p['source_run_id'] != run for p in payloads.values()):
        raise ValueError('Queue holds cases from a release that is not active; nothing reset')
    cases = sorted((load_case(p) for p in payloads.values()), key=lambda c: c.records[0].record_key)
    prepared = PreparedImport(run, tuple(cases), (), summary['source_records'],
        summary['pipeline_customer_groups'], summary['candidate_pairs'], summary['review_pairs'],
        summary.get('cross_component_review_pairs', 0), summary.get('unscoped_review_pairs', 0))
    return prepared.fingerprint, summary['fingerprint']


def reset_lakebase(pool, *, apply):
    with pool.connection() as conn, conn.transaction():
        # Serialize with publication and with any concurrent reset.
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended('steward-publication',0))")
        before = lakebase_state(conn)
        payloads = baselines(conn)
        computed, recorded = import_fingerprint(conn, payloads)
        if computed != recorded:
            raise ValueError('Reconstructed baseline differs from the imported release; nothing reset')
        if apply:
            for table in ('agent_events', 'agent_runs', 'reviewed_golden_fields', 'outbox',
                          'decisions', 'proposals'):
                conn.execute(f'DELETE FROM steward.{table}')
            with conn.cursor() as cursor:
                cursor.executemany(
                    "UPDATE steward.review_cases SET version=1, status='REVIEW', payload=%s, "
                    'baseline_payload=%s, updated_at=now() WHERE case_id=%s',
                    [(Jsonb(p), Jsonb(p), case_id) for case_id, p in payloads.items()])
            after = lakebase_state(conn)
            restored = {row[0]: row[1] for row in conn.execute(
                'SELECT case_id, payload FROM steward.review_cases').fetchall()}
            if digest(restored) != digest(payloads):
                raise ValueError('Restored payloads differ from baseline; rolled back')
        else:
            after = None
    return before, after, {'computed': computed, 'import_ledger': recorded}


def gold_state(sql):
    from publish_reviews import EVENTS, GOLD, SURVIVORSHIP
    links = dict(sql.run(f'SELECT mapping_status, count(*) FROM {GOLD} GROUP BY 1 ORDER BY 1'))
    events = sql.run(f'SELECT count(*) FROM {EVENTS}')[0][0]
    try:
        profiles = sql.run(f'SELECT count(*) FROM {SURVIVORSHIP}')[0][0]
    except RuntimeError:
        profiles = 'table absent'
    return {'reviewed_links_by_status': links, 'steward_decision_events': events,
            'reviewed_profile_fields': profiles}


def active_manifest(pool, requested=None):
    """The active release manifest; a requested manifest must be that release."""
    from import_release import ReleaseManifest
    with pool.connection() as conn:
        run, _, raw = active_release(conn)
    manifest = ReleaseManifest.parse(raw)
    if requested is not None and (requested.source_run_id != run or requested.snapshots != manifest.snapshots):
        raise ValueError(f'--manifest is not the active release ({run}); omit it to use the active release')
    return manifest


def reset_demo(pool, sql, manifest, *, apply=False):
    summary = {'applied': apply, 'active_release': manifest.source_run_id, 'gold_before': gold_state(sql)}
    before, after, fingerprint = reset_lakebase(pool, apply=apply)
    summary.update(lakebase_before=before, baseline_fingerprint=fingerprint)
    if not apply:
        summary['message'] = 'Dry-run: nothing changed. Re-run with --apply to reset.'
        return summary
    summary['lakebase_after'] = after
    from publish_reviews import EVENTS, publish
    sql.run(f'DELETE FROM {EVENTS} WHERE source_run_id = :run', run=manifest.source_run_id)
    summary['publication'] = publish(pool, sql, manifest, apply=True)
    summary['gold_after'] = gold_state(sql)
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--profile', help='Databricks CLI profile (default: ambient auth)')
    parser.add_argument('--warehouse-id', required=True, help='SQL warehouse for the Gold tables')
    parser.add_argument('--manifest', help='Optional; must match the active release (default: the active ledger row)')
    parser.add_argument('--apply', action='store_true', help='Apply the reset; default is dry-run')
    args = parser.parse_args(argv)
    from databricks.sdk import WorkspaceClient
    from import_release import ReleaseManifest
    from lakebase import create_operator_pool
    from publish_reviews import Sql
    requested = ReleaseManifest.parse(json.loads(Path(args.manifest).read_text())) if args.manifest else None
    pool = create_operator_pool(args.profile)
    pool.open(wait=True)
    try:
        manifest = active_manifest(pool, requested)
        result = reset_demo(pool, Sql(WorkspaceClient(profile=args.profile), args.warehouse_id),
                            manifest, apply=args.apply)
        print(json.dumps(result, indent=2, default=str))
    finally:
        pool.close()


if __name__ == '__main__':
    main()
