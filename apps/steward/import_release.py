"""Operator-only release importer; no import endpoint is exposed to the App.

Defaults to dry-run. Reads only operational snapshots using explicit columns.
``--replace-release`` archives the active release's review state and imports a
new release in its place.
"""
import argparse
import os
from dataclasses import dataclass, field
import json
import re
import time
import gzip
from pathlib import Path
from tempfile import TemporaryDirectory

from domain import InvalidDecision
from source_adapter import DOB_FLAGS, FLAGS, NATIVE_BF, SIMILARITIES
from integration import prepare_import


SOURCE_COLUMNS = ('record_key', 'source_system', 'source_record_id', 'given_name',
    'family_name', 'full_name', 'personal_email', 'work_email', 'personal_phone',
    'work_phone', 'date_of_birth', 'home_address_line1', 'home_suburb', 'home_state',
    'home_postcode', 'home_country', 'work_address_line1', 'work_suburb', 'work_state',
    'work_postcode', 'work_country', 'company_name', 'source_updated_at')
MAPPING_COLUMNS = ('resolution_run_id', 'resolver_version', 'record_key', 'customer_id',
    'provisional_customer_id', 'cluster_requires_review')
DECISION_COLUMNS = ('resolution_run_id', 'resolver_version', 'left_record_key',
    'right_record_key', 'decision', 'evidence_score', 'candidate_routes',
    'evidence_reasons', 'safeguards_triggered', 'decision_explanation') + FLAGS + SIMILARITIES
SPECS = {
    'sources': ('operational_source_snapshot', SOURCE_COLUMNS, ('record_key',)),
    'mappings': ('operational_mapping_snapshot', MAPPING_COLUMNS, ('record_key',)),
    'decisions': ('operational_review_evidence_snapshot', DECISION_COLUMNS,
                  ('left_record_key', 'right_record_key')),
}


# Resolver releases live in <catalog>.resolution or a sibling
# resolution_<name> schema (e.g. resolution_splink_v4). Never truth/evaluation.
CATALOG = os.environ.get('DEMO_CATALOG', 'identity_resolution_demo')
TABLE_NAME = re.compile(re.escape(CATALOG) + r'\.(resolution(?:_[a-z0-9]+)*)\.([a-z_]+)')
OPTIONAL_MANIFEST_FIELDS = frozenset(('metadata',))
# Column names that suggest hidden evaluation or generator truth. Any match in
# any pinned snapshot refuses the release, even if the importer never selects it.
TRUTH_NAMES = frozenset(('truth_id', 'person_id', 'true_person_id', 'scenario_id', 'scenario',
    'expected_outcome', 'expected_decision', 'corruption_label', 'corruption_type',
    'is_true_match', 'true_match', 'ground_truth', 'gold_label', 'match_label', 'label',
    'is_duplicate', 'true_customer_id', 'entity_id', 'clerical_match_score'))
TRUTH_PARTS = ('truth', 'expected_', 'corruption', 'scenario', 'ground_', 'clerical', 'hidden_eval')


def truth_like_columns(columns):
    return sorted(c for c in columns if c.lower() in TRUTH_NAMES
                  or any(part in c.lower() for part in TRUTH_PARTS))


@dataclass(frozen=True)
class ReleaseManifest:
    source_run_id: str
    release_job_run_id: int
    snapshots: dict
    metadata: dict = field(default_factory=dict)

    @property
    def schema(self):
        return TABLE_NAME.fullmatch(self.snapshots['sources']['table']).group(1)

    @classmethod
    def parse(cls, value):
        required = {'source_run_id', 'release_job_run_id', 'snapshots'}
        if not isinstance(value, dict) or not required <= set(value) \
                or set(value) - required - OPTIONAL_MANIFEST_FIELDS:
            extra = sorted(set(value) - required - OPTIONAL_MANIFEST_FIELDS) if isinstance(value, dict) else []
            raise InvalidDecision('Unexpected release manifest fields'
                                  + (': ' + ', '.join(map(str, extra)) if extra else ''))
        if not isinstance(value['source_run_id'], str) or not value['source_run_id'].strip():
            raise InvalidDecision('Source run ID required')
        if type(value['release_job_run_id']) is not int or value['release_job_run_id'] <= 0:
            raise InvalidDecision('Release job run ID required')
        metadata = value.get('metadata', {})
        if not isinstance(metadata, dict) or len(metadata) > 20 or any(
                not isinstance(k, str) or isinstance(v, bool) or not isinstance(v, (str, int, float))
                for k, v in metadata.items()):
            raise InvalidDecision('Manifest metadata must be a small map of text or numbers')
        if not isinstance(value['snapshots'], dict) or set(value['snapshots']) != set(SPECS):
            raise InvalidDecision('All three operational snapshots are required')
        schemas = set()
        for key, (table, _, _) in SPECS.items():
            spec = value['snapshots'][key]
            if not isinstance(spec, dict) or set(spec) != {'table', 'version', 'row_count'}:
                raise InvalidDecision('Snapshot requires table, version and row count')
            match = TABLE_NAME.fullmatch(spec['table']) if isinstance(spec['table'], str) else None
            if not match or match.group(2) != table:
                raise InvalidDecision('Only approved operational snapshot tables are allowed')
            schemas.add(match.group(1))
            if any(type(spec[k]) is not int or spec[k] < 0 for k in ('version', 'row_count')):
                raise InvalidDecision('Snapshot version and row count must be nonnegative integers')
        if len(schemas) != 1:
            raise InvalidDecision('All snapshots must come from one resolver schema')
        return cls(value['source_run_id'], value['release_job_run_id'], value['snapshots'], metadata)


class Statements:
    """Minimal statement runner returning (column names, rows) for preflight."""
    def __init__(self, workspace, warehouse_id):
        self.api, self.warehouse_id = workspace.statement_execution, warehouse_id

    def run(self, sql, **parameters):
        from databricks.sdk.service.sql import StatementParameterListItem
        response = self.api.execute_statement(statement=sql, warehouse_id=self.warehouse_id,
            parameters=[StatementParameterListItem(name=k, value=str(v), type='STRING')
                        for k, v in parameters.items()], wait_timeout='10s')
        deadline = time.monotonic() + 180
        while response.status.state.value in ('PENDING', 'RUNNING'):
            if time.monotonic() >= deadline:
                self.api.cancel_execution(response.statement_id)
                raise InvalidDecision('Snapshot preflight query timed out')
            time.sleep(1)
            response = self.api.get_statement(response.statement_id)
        if response.status.state.value != 'SUCCEEDED':
            message = getattr(getattr(response.status, 'error', None), 'message', '') or ''
            raise InvalidDecision('Snapshot preflight failed: ' + message[:300])
        if response.manifest and response.manifest.truncated:
            raise InvalidDecision('Preflight response truncated')
        columns = [c.name for c in (response.manifest.schema.columns or [])] \
            if response.manifest and response.manifest.schema else []
        return columns, (response.result.data_array or []) if response.result else []


def preflight(statements, manifest):
    """Prove each pinned snapshot before any row is read.

    Per snapshot: a regular Delta table (DESCRIBE DETAIL), the declared version is
    in its history, its schema *at that version* has no truth-like column and has
    every required column, and its row count at that version (for mappings and
    decisions, for this run) equals the manifest. Returns the per-table report
    plus the evidence columns the reader must select for this resolver.
    """
    report, problems, columns_at = {}, [], {}
    for key, (_, required, _) in SPECS.items():
        spec = manifest.snapshots[key]
        table, version = spec['table'], spec['version']
        names, rows = statements.run(f'DESCRIBE DETAIL {table}')
        detail = dict(zip(names, rows[0])) if rows else {}
        if detail.get('format') != 'delta':
            problems.append(f'{key}: {table} is not a regular Delta table')
            continue
        names, history = statements.run(f'DESCRIBE HISTORY {table}')
        versions = {int(row[names.index('version')]) for row in history}
        if version not in versions:
            problems.append(f'{key}: version {version} is not in the table history')
            continue
        # LIMIT 0 reads the pinned schema only; no row values are returned.
        columns, _ = statements.run(f'SELECT * FROM {table} VERSION AS OF {version} LIMIT 0')
        columns_at[key] = columns
        truthy = truth_like_columns(columns)
        if truthy:
            problems.append(f'{key}: truth-like columns present: {", ".join(truthy)}')
        where = ' WHERE resolution_run_id = :run' if key != 'sources' else ''
        params = {'run': manifest.source_run_id} if key != 'sources' else {}
        _, counted = statements.run(f'SELECT count(*) FROM {table} VERSION AS OF {version}{where}', **params)
        count = int(counted[0][0])
        if count != spec['row_count']:
            problems.append(f'{key}: {count} rows at version {version}, manifest declares {spec["row_count"]}')
        report[key] = {'table': table, 'version': version, 'row_count': count,
                       'latest_version': max(versions), 'columns': len(columns)}
    evidence = evidence_columns(columns_at.get('decisions'), problems)
    for key, (_, required, _) in SPECS.items():
        if key not in columns_at:
            continue
        optional = set(evidence['projected_null']) if key == 'decisions' else set()
        missing = [c for c in required if c not in columns_at[key] and c not in optional]
        if missing:
            problems.append(f'{key}: required columns missing: {", ".join(missing)}')
    if problems:
        raise InvalidDecision('Release preflight refused: ' + '; '.join(problems))
    return {'snapshots': report, 'evidence_profile': evidence['profile'],
            'model_columns': list(evidence['extra']), 'projected_null_columns': list(evidence['projected_null'])}


def evidence_columns(columns, problems):
    """Evidence columns to select beyond DECISION_COLUMNS, by resolver profile.

    A snapshot with match_probability is a probabilistic (Splink) release. It
    may omit the hybrid resolver's similarity and exact-match flags (projected
    as NULL, shown as "Not evaluated", and reported), but never the DOB
    provenance flags.
    """
    if columns is None:
        return {'profile': None, 'extra': (), 'projected_null': ()}
    if 'match_probability' not in columns:
        return {'profile': 'rule_features', 'extra': (), 'projected_null': ()}
    extra = [c for c in ('match_probability', 'match_weight', 'comparison_match_weights',
                         'decision_rule', 'decision_reason') if c in columns]
    if 'comparison_match_weights' not in columns:
        for name in columns:
            match = NATIVE_BF.fullmatch(name)
            if match:
                extra.append(name)
                extra += [p + match.group(1) for p in ('bf_tf_adj_', 'gamma_') if p + match.group(1) in columns]
    optional = [c for c in FLAGS + SIMILARITIES if c not in DOB_FLAGS and c not in columns]
    return {'profile': 'splink', 'extra': tuple(extra), 'projected_null': tuple(optional)}


class SnapshotReader:
    def __init__(self, workspace, warehouse_id, manifest, *, page_size=1000, evidence=None):
        if not 1 <= page_size <= 2000:
            raise InvalidDecision('Bounded SQL page size required')
        self.workspace, self.warehouse_id, self.manifest = workspace, warehouse_id, manifest
        self.page_size = page_size
        # From preflight(): extra model columns and absent optional features.
        self.evidence = evidence or {'model_columns': [], 'projected_null_columns': []}

    def select_list(self, key):
        """Explicit allowlisted projection; names were validated by preflight."""
        columns = SPECS[key][1]
        if key != 'decisions':
            return ', '.join(columns)
        absent = set(self.evidence['projected_null_columns'])
        parts = [f'CAST(NULL AS DOUBLE) AS {c}' if c in absent else c for c in columns]
        return ', '.join(parts + list(self.evidence['model_columns']))

    def query(self, sql, parameters):
        from databricks.sdk.service.sql import StatementParameterListItem
        api = self.workspace.statement_execution
        response = api.execute_statement(statement=sql, warehouse_id=self.warehouse_id,
            parameters=[StatementParameterListItem(name=k, value=v, type='STRING')
                        for k,v in parameters.items()], wait_timeout='10s')
        deadline = time.monotonic() + 180
        while response.status.state.value in ('PENDING', 'RUNNING'):
            if time.monotonic() >= deadline:
                api.cancel_execution(response.statement_id)
                raise InvalidDecision('Operational snapshot query timed out')
            time.sleep(1)
            response = api.get_statement(response.statement_id)
        if response.status.state.value != 'SUCCEEDED':
            raise InvalidDecision('Operational snapshot query failed; inspect statement history')
        if response.manifest.truncated:
            raise InvalidDecision('Snapshot response truncated; reduce SQL page size')
        chunk = response.result
        while chunk is not None:
            for row in chunk.data_array or []:
                yield json.loads(row[0])
            if chunk.next_chunk_index is None:
                break
            chunk = api.get_statement_result_chunk_n(response.statement_id, chunk.next_chunk_index)

    def rows(self, key):
        spec = self.manifest.snapshots[key]
        _, columns, keys = SPECS[key]
        count, last = 0, None
        while True:
            params = {}
            predicates = []
            if key != 'sources':
                params['run'] = self.manifest.source_run_id
                predicates.append('resolution_run_id = :run')
            if last is not None:
                params.update({f'key{i}':v for i,v in enumerate(last)})
                # Tuple comparison is lexicographic; canonical pair ordering is preserved.
                lhs = ', '.join(keys)
                rhs = ', '.join(f':key{i}' for i in range(len(keys)))
                predicates.append(f'({lhs}) > ({rhs})')
            where = ' WHERE ' + ' AND '.join(predicates) if predicates else ''
            sql = ('SELECT to_json(struct(' + self.select_list(key) + "), map('ignoreNullFields', 'false')) AS payload FROM "
                   + spec['table'] + f' VERSION AS OF {spec["version"]}' + where
                   + ' ORDER BY ' + ', '.join(keys) + f' LIMIT {self.page_size}')
            page_count = 0
            for row in self.query(sql, params):
                current = tuple(row.get(k) for k in keys)
                if any(not isinstance(v, str) or not v for v in current) or (last is not None and current <= last):
                    raise InvalidDecision('Snapshot keys are missing, duplicated or unordered')
                last = current
                count += 1
                page_count += 1
                if count > spec['row_count']:
                    raise InvalidDecision('Snapshot has more rows than the manifest')
                yield row
            if page_count < self.page_size:
                break
        if count != spec['row_count']:
            raise InvalidDecision('Snapshot row count does not match release manifest')


def verify_release(workspace, manifest):
    run = workspace.jobs.get_run(run_id=manifest.release_job_run_id)
    if not run.state or not run.state.result_state or run.state.result_state.value != 'SUCCESS':
        raise InvalidDecision('Release job has not succeeded; apply is disabled')


class BulkSnapshotReader(SnapshotReader):
    """One ordered query per pinned table; bounded result chunks, private replay spool.

    Signed download URLs never receive workspace credentials and are never logged.
    The temporary spool is removed on success or failure. No truth columns queried.
    """
    def __enter__(self):
        self.spool = TemporaryDirectory(prefix='identity-operational-import-')
        self.complete = set()
        return self

    def __exit__(self, *args):
        self.spool.cleanup()

    def download_rows(self, key):
        import requests
        from databricks.sdk.service.sql import Disposition, Format, StatementParameterListItem
        spec = self.manifest.snapshots[key]
        _, columns, keys = SPECS[key]
        sql = ('SELECT to_json(struct(' + self.select_list(key)
            + "), map('ignoreNullFields', 'false')) AS payload FROM " + spec['table']
            + f' VERSION AS OF {spec["version"]}'
            + (' WHERE resolution_run_id = :run' if key != 'sources' else '')
            + ' ORDER BY ' + ', '.join(keys))
        api = self.workspace.statement_execution
        response = api.execute_statement(warehouse_id=self.warehouse_id, statement=sql,
            parameters=([StatementParameterListItem(name='run', value=self.manifest.source_run_id)]
                        if key != 'sources' else []),
            disposition=Disposition.EXTERNAL_LINKS, format=Format.JSON_ARRAY, wait_timeout='10s')
        deadline = time.monotonic() + 600
        while response.status.state.value in ('PENDING', 'RUNNING'):
            if time.monotonic() >= deadline:
                api.cancel_execution(response.statement_id)
                raise InvalidDecision('Bulk snapshot query timed out')
            time.sleep(1)
            response = api.get_statement(response.statement_id)
        if response.status.state.value != 'SUCCEEDED':
            raise InvalidDecision('Bulk snapshot query failed; inspect statement history')
        if response.manifest.truncated or response.manifest.total_row_count != spec['row_count']:
            raise InvalidDecision('Bulk snapshot manifest row count mismatch or truncation')
        count, last = 0, None
        for chunk_info in response.manifest.chunks or []:
            chunk = api.get_statement_result_chunk_n(response.statement_id, chunk_info.chunk_index)
            for link in chunk.external_links or []:
                if link.row_offset != count or not link.external_link.startswith('https://'):
                    raise InvalidDecision('Invalid external result chunk')
                # Never attach Databricks OAuth to a storage URL. Do not expose signed URLs on errors.
                try:
                    with requests.get(link.external_link, headers=link.http_headers or {},
                                      timeout=(15, 120), allow_redirects=False) as result:
                        if result.status_code != 200:
                            raise InvalidDecision('Snapshot chunk download failed')
                        rows = result.json()
                except requests.RequestException:
                    raise InvalidDecision('Snapshot chunk download failed') from None
                if len(rows) != link.row_count:
                    raise InvalidDecision('Incomplete snapshot chunk')
                for values in rows:
                    row = json.loads(values[0])
                    current = tuple(row.get(k) for k in keys)
                    if any(not isinstance(v, str) or not v for v in current) or (last is not None and current <= last):
                        raise InvalidDecision('Snapshot keys are missing, duplicated or unordered')
                    last = current
                    count += 1
                    yield row
        if count != spec['row_count']:
            raise InvalidDecision('Bulk snapshot row count does not match release manifest')

    def rows(self, key):
        path = Path(self.spool.name) / (key + '.jsonl.gz')
        if key in self.complete:
            with gzip.open(path, 'rt') as handle:
                for line in handle:
                    yield json.loads(line)
            return
        with gzip.open(path, 'wt', compresslevel=1) as handle:
            for row in self.download_rows(key):
                handle.write(json.dumps(row, ensure_ascii=False, separators=(',', ':')) + '\n')
                yield row
        self.complete.add(key)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--profile', help='Databricks CLI profile (default: ambient auth)')
    parser.add_argument('--warehouse-id', required=True)
    parser.add_argument('--replace-release', action='store_true',
                        help='Archive the active release and import this one in its place')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args(argv)
    with open(args.manifest) as handle:
        raw = json.load(handle)
    manifest = ReleaseManifest.parse(raw)
    from databricks.sdk import WorkspaceClient
    workspace = WorkspaceClient(profile=args.profile)
    checks = preflight(Statements(workspace, args.warehouse_id), manifest)
    if args.apply:
        verify_release(workspace, manifest)
    with BulkSnapshotReader(workspace, args.warehouse_id, manifest, evidence=checks) as reader:
        prepared = prepare_import(source_run_id=manifest.source_run_id,
            sources=reader.rows('sources'), mappings=reader.rows('mappings'),
            decisions=lambda: reader.rows('decisions'))
    if args.apply and prepared.oversized_components:
        # Fail closed rather than silently omit large cases.
        raise InvalidDecision('Oversized review components need explicit handling before apply')
    if not args.replace_release:
        if not args.apply:
            print(json.dumps({'dry_run': True, 'preflight': checks, **prepared.summary()}))
            return
        from lakebase import create_operator_pool
        from import_store import import_prepared
        pool = create_operator_pool(args.profile)
        with pool:
            print(json.dumps(import_prepared(pool, prepared, release_manifest=raw)))
        return
    from lakebase import create_operator_pool
    from import_store import replace_release
    from publish_reviews import Sql, gold_plan, publish
    sql = Sql(workspace, args.warehouse_id)
    pool = create_operator_pool(args.profile)
    with pool:
        result = {'dry_run': not args.apply, 'preflight': checks,
                  'lakebase': replace_release(pool, prepared, release_manifest=raw, apply=args.apply)}
        if args.apply:
            # Separate system, after the Lakebase commit. Safe to repeat: re-running
            # this command replays the Lakebase step and republishes.
            result['gold'] = publish(pool, sql, manifest, apply=True)
        result['gold_plan'] = gold_plan(sql, manifest)
    print(json.dumps(result, indent=2, default=str))


if __name__ == '__main__':
    main()
