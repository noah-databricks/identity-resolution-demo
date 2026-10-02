"""Operator-only, retryable Lakebase outbox -> reviewed Gold publication.

The App cannot call this command. Resolver Gold remains unchanged. Dry-run by default.
"""
import os
import argparse
import json
import re
import time

from publication import export_event, latest_overrides

CATALOG = os.environ.get('DEMO_CATALOG', 'identity_resolution_demo')
EVENTS = f'{CATALOG}.gold.steward_decision_events'
GOLD = f'{CATALOG}.gold.reviewed_customer_identity_links'
SURVIVORSHIP = f'{CATALOG}.gold.reviewed_customer_profiles'
EVENT_SCHEMA = ('event_id STRING,case_id STRING,source_run_id STRING,'
    'base_case_version BIGINT,applied_case_version BIGINT,actor STRING,mode STRING,'
    'action STRING,proposal_id STRING,trace_id STRING,content_digest STRING,'
    'memberships ARRAY<STRUCT<record_key:STRING,customer_id:STRING>>')
# One row per resulting steward guest and golden field: the chosen value plus
# provenance (which source record, which decision, who and how it was reviewed).
SURVIVORSHIP_FIELDS = (('source_run_id', 'STRING'), ('guest_identity', 'STRING'),
    ('case_id', 'STRING'), ('field_name', 'STRING'), ('field_value', 'STRING'),
    ('chosen_record_key', 'STRING'), ('source_system', 'STRING'),
    ('source_record_id', 'STRING'), ('decision_id', 'STRING'),
    ('applied_case_version', 'BIGINT'), ('reviewed_by', 'STRING'), ('review_mode', 'STRING'))
SURVIVORSHIP_SCHEMA = ','.join(f'{name} {kind}' for name, kind in SURVIVORSHIP_FIELDS)
ICEBERG_PROPERTIES = """TBLPROPERTIES ('delta.columnMapping.mode'='name',
            'delta.enableDeletionVectors'='false',
            'delta.enableIcebergCompatV2'='true','delta.universalFormat.enabledFormats'='iceberg')"""


class Sql:
    def __init__(self, workspace, warehouse):
        self.api, self.warehouse = workspace.statement_execution, warehouse

    def run(self, statement, **parameters):
        from databricks.sdk.service.sql import StatementParameterListItem
        response = self.api.execute_statement(statement=statement, warehouse_id=self.warehouse,
            parameters=[StatementParameterListItem(name=k,value=str(v),type='STRING')
                        for k,v in parameters.items()], wait_timeout='10s')
        deadline = time.monotonic() + 180
        while response.status.state.value in ('PENDING', 'RUNNING'):
            if time.monotonic() > deadline:
                self.api.cancel_execution(response.statement_id)
                raise RuntimeError('Publication query timed out; retry is safe')
            time.sleep(1)
            response = self.api.get_statement(response.statement_id)
        if response.status.state.value != 'SUCCEEDED':
            raise RuntimeError(f'Publication statement failed: {response.statement_id}')
        if response.manifest and response.manifest.truncated:
            raise RuntimeError('Publication response truncated')
        return response.result.data_array if response.result else []


DEFERRED_TABLE = 'operational_deferred_review_cases'


def deferred_expectation(manifest):
    """(records, cases) the release says it held back above the importer bound.

    Splink releases list those steward cases in ``operational_deferred_review_cases``
    (component level) and count them in the manifest metadata. Their mapping rows
    carry cluster_requires_review = false because they were never imported, yet
    they are unresolved review work, so reviewed Gold must not call them resolved.
    """
    metadata = getattr(manifest, 'metadata', None) or {}
    records = metadata.get('deferred_oversized_records', 0)
    cases = metadata.get('deferred_oversized_cases', 0)
    if any(type(v) is not int or v < 0 for v in (records, cases)) or bool(records) != bool(cases):
        raise ValueError('Manifest deferred-case metadata is invalid')
    return records, cases


def projection(manifest):
    # Parsed ReleaseManifest restricts names and integer snapshot versions.
    mappings, sources = (manifest.snapshots[k] for k in ('mappings','sources'))
    records, _ = deferred_expectation(manifest)
    if not re.fullmatch(r'[A-Za-z0-9_.-]{1,128}', manifest.source_run_id):
        raise ValueError('Release id is not a plain identifier')
    schema = mappings['table'].rsplit('.', 1)[0]
    # Deferred components map to the snapshot's provisional_customer_id exactly as
    # the resolver derives it (release.py: 'cmp_' + sha2(component_key)[0:28]).
    # The table is not version-pinned by the manifest, so publish() checks the
    # joined record and case counts against the manifest metadata.
    deferred = (f"""SELECT DISTINCT concat('cmp_',substring(sha2(component_key,256),1,28)) AS provisional_customer_id,
             case_key FROM {schema}.{DEFERRED_TABLE} WHERE release_id='{manifest.source_run_id}'"""
                if records else
                'SELECT CAST(NULL AS STRING) AS provisional_customer_id, CAST(NULL AS STRING) AS case_key WHERE false')
    return f"""WITH ranked AS (
      SELECT *, row_number() OVER (
        PARTITION BY source_run_id,case_id ORDER BY applied_case_version DESC) AS rn
      FROM {EVENTS} WHERE action <> 'DEFER'
    ), overrides AS (
      SELECT source_run_id,case_id,event_id,applied_case_version,actor,mode,
             member.record_key,member.customer_id
      FROM ranked LATERAL VIEW explode(memberships) e AS member WHERE rn=1
    ), deferred AS ({deferred}
    ) SELECT m.record_key,s.source_system,s.source_record_id,
      coalesce(o.customer_id,m.customer_id) AS customer_id,
      m.customer_id AS pipeline_customer_id,m.resolution_run_id,m.resolver_version,
      CASE WHEN o.event_id IS NOT NULL THEN 'STEWARD_RESOLVED'
           WHEN m.cluster_requires_review OR d.case_key IS NOT NULL THEN 'REQUIRES_REVIEW'
           ELSE 'PIPELINE_RESOLVED' END AS mapping_status,
      CASE WHEN d.case_key IS NOT NULL THEN 'DEFERRED_OVERSIZED'
           WHEN m.cluster_requires_review THEN 'STEWARD_QUEUE' END AS review_scope,
      d.case_key AS deferred_case_key,
      o.event_id AS steward_decision_id,o.case_id AS steward_case_id,
      o.applied_case_version AS steward_case_version,o.actor AS reviewed_by,
      o.mode AS review_mode
    FROM {mappings['table']} VERSION AS OF {mappings['version']} m
    JOIN {sources['table']} VERSION AS OF {sources['version']} s USING(record_key)
    LEFT JOIN overrides o ON m.record_key=o.record_key AND m.resolution_run_id=o.source_run_id
    LEFT JOIN deferred d ON m.provisional_customer_id=d.provisional_customer_id
    """


CARDINALITY = ('SELECT count(*),count(DISTINCT record_key),count(steward_decision_id),'
               "count(deferred_case_key),count(DISTINCT deferred_case_key),"
               "count(CASE WHEN deferred_case_key IS NOT NULL AND (steward_decision_id IS NOT NULL "
               "OR review_scope <> 'DEFERRED_OVERSIZED') THEN 1 END) FROM ")


def survivorship_rows(conn, source_run_id, overrides):
    """Golden-field choices of each case's latest published decision only.

    Superseded decisions and deferrals never survive. Every chosen source record
    must belong to the guest identity it is attributed to in the reviewed links.
    """
    current = {value['event_id'] for value in overrides.values()}
    rows = conn.execute(
        'SELECT g.guest_identity,g.case_id,g.field_name,g.field_value,g.chosen_record_key,'
        'g.decision_id,g.applied_case_version,d.actor,d.mode,c.payload '
        'FROM steward.reviewed_golden_fields g '
        'JOIN steward.decisions d USING (decision_id) '
        'JOIN steward.review_cases c ON c.case_id=g.case_id '
        'WHERE g.source_run_id=%s ORDER BY g.guest_identity,g.field_name',
        (source_run_id,)).fetchall()
    result = []
    for (identity, case_id, field, value, record_key, decision_id, version,
         actor, mode, payload) in rows:
        if decision_id not in current:
            continue
        member = overrides.get((source_run_id, record_key))
        if not member or member['customer_id'] != identity or member['event_id'] != decision_id:
            raise ValueError('Golden field provenance does not match reviewed membership')
        source = next((r for r in payload['records'] if r['record_key'] == record_key), None)
        if source is None or source.get(field) != value:
            raise ValueError('Golden field value does not match its source record')
        result.append(dict(source_run_id=source_run_id, guest_identity=identity, case_id=case_id,
            field_name=field, field_value=value, chosen_record_key=record_key,
            source_system=source['source_system'], source_record_id=source['source_record_id'],
            decision_id=decision_id, applied_case_version=version, reviewed_by=actor,
            review_mode=mode))
    return result


def publish(pool, sql, manifest, *, apply=False):
    with pool.connection() as conn, conn.transaction():
        # A single publisher serializes acknowledgements and the materialized product.
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended('steward-publication',0))")
        ledger = conn.execute('SELECT release_manifest, active FROM steward.import_batches '
                              'WHERE source_run_id=%s', (manifest.source_run_id,)).fetchone()
        if not ledger or ledger[0]['snapshots'] != manifest.snapshots:
            raise ValueError('Publication manifest must match the imported release')
        if not ledger[1]:
            # Reviewed Gold is rebuilt whole from one release's mapping; publishing
            # an archived release would overwrite Gold rebased on the active one.
            raise ValueError('This release is archived; publish the active release instead')
        raw = conn.execute('SELECT payload FROM steward.outbox '
                           "WHERE payload->>'source_run_id'=%s ORDER BY created_at,event_id",
                           (manifest.source_run_id,)).fetchall()
        events = [r[0] for r in raw]
        overrides = latest_overrides(events)  # fail closed on overlap or corrupt history
        exported = [export_event(e) for e in events]
        profiles = survivorship_rows(conn, manifest.source_run_id, overrides)
        summary = {'events':len(exported),'overridden_records':len(overrides),
                   'golden_fields':len(profiles),
                   'golden_guests':len({row['guest_identity'] for row in profiles}),
                   'applied':apply}
        if not apply:
            return summary
        # Iceberg-readable like the other Gold tables (enabled on the live table 26 Sep 2026).
        sql.run(f'CREATE TABLE IF NOT EXISTS {EVENTS} ({EVENT_SCHEMA}) USING DELTA {ICEBERG_PROPERTIES}')
        for event in exported:
            existing = sql.run(f'SELECT content_digest FROM {EVENTS} WHERE event_id=:event_id',
                               event_id=event['event_id'])
            if existing and existing != [[event['content_digest']]]:
                raise ValueError('Published event content differs; publication stopped')
            sql.run(f"""MERGE INTO {EVENTS} t USING
              (SELECT parsed.* FROM
                (SELECT from_json(:payload,'{EVENT_SCHEMA}') AS parsed)) s
              ON t.event_id=s.event_id
              WHEN NOT MATCHED THEN INSERT *""",
                payload=json.dumps(event))
        query = projection(manifest)
        counts = sql.run(f'{CARDINALITY}({query})')
        expected = manifest.snapshots['mappings']['row_count']
        deferred_records, deferred_cases = deferred_expectation(manifest)
        if counts != [[str(expected),str(expected),str(len(overrides)),
                       str(deferred_records),str(deferred_cases),'0']]:
            raise ValueError('Reviewed mapping cardinality differs from the pinned release '
                             '(deferred review records must match the manifest and never be steward-resolved)')
        sql.run(f"""CREATE OR REPLACE TABLE {GOLD} USING DELTA
          {ICEBERG_PROPERTIES}
          AS {query}""")
        # Rebuilt from the current decisions, like the links: superseded or
        # reset choices disappear. Iceberg-readable for the Snowflake linked DB.
        sql.run(f"""CREATE OR REPLACE TABLE {SURVIVORSHIP} ({SURVIVORSHIP_SCHEMA}) USING DELTA
          {ICEBERG_PROPERTIES}""")
        if profiles:
            sql.run(f"""INSERT INTO {SURVIVORSHIP}
              SELECT inline(from_json(:payload,'ARRAY<STRUCT<{SURVIVORSHIP_SCHEMA.replace(' ', ':')}>>'))""",
                payload=json.dumps(profiles))
        written = sql.run(f'SELECT count(*) FROM {SURVIVORSHIP}')
        if written != [[str(len(profiles))]]:
            raise ValueError('Reviewed profile cardinality differs from committed choices')

        for event in exported:
            conn.execute('UPDATE steward.outbox SET published_at=coalesce(published_at,now()),'
                         'attempts=attempts+1,last_error=NULL WHERE event_id=%s', (event['event_id'],))
        return summary


def gold_plan(sql, manifest):
    """Read-only: what the next publication of this release would write.

    Publication rebuilds reviewed_customer_identity_links with CREATE OR REPLACE
    from this release's pinned mapping and source snapshots (never appends), so the
    first publish after a replacement rebases Gold onto the new release mapping;
    reviewed_customer_profiles is rebuilt from the new release's decisions only.
    """
    rows = sql.run(f'{CARDINALITY}({projection(manifest)})')
    try:
        current = {f'{run} {status}': int(n) for run, status, n in sql.run(
            f'SELECT resolution_run_id, mapping_status, count(*) FROM {GOLD} GROUP BY 1,2 ORDER BY 1,2')}
    except RuntimeError:
        current = 'table absent'
    total, distinct, overridden, deferred, deferred_cases, _ = (int(v) for v in rows[0])
    return {'release': manifest.source_run_id,
            'mapping_snapshot': manifest.snapshots['mappings']['table'] + '@v'
                                + str(manifest.snapshots['mappings']['version']),
            'projected_rows': total, 'projected_distinct_records': distinct,
            'projected_steward_overrides': overridden,
            'projected_deferred_review_records': deferred,
            'projected_deferred_review_cases': deferred_cases,
            'expected_rows': manifest.snapshots['mappings']['row_count'],
            'current_reviewed_links_by_run_and_status': current}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', help='Optional; must match the active release (default: the active ledger row)')
    parser.add_argument('--profile', required=True)
    parser.add_argument('--warehouse-id', required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    from pathlib import Path
    from databricks.sdk import WorkspaceClient
    from import_release import ReleaseManifest
    from lakebase import create_operator_pool
    from reset_demo import active_manifest
    requested = ReleaseManifest.parse(json.loads(Path(args.manifest).read_text())) if args.manifest else None
    pool = create_operator_pool(args.profile); pool.open(wait=True)
    try:
        manifest = active_manifest(pool, requested)
        print(json.dumps(publish(pool,Sql(WorkspaceClient(profile=args.profile),args.warehouse_id),
                                 manifest,apply=args.apply)))
    finally:
        pool.close()


if __name__ == '__main__':
    main()
