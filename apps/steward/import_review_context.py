"""Derive why each case of the active release is in review. Dry-run by default.

Operator-only and read-only against Databricks. The release does not carry a
per-case review reason, so this reads the resolver's own frozen release tables:

* ``frozen_component_quarantine_<run>``: AUTO_MATCH links the resolver's clustering
  withheld (COMPONENT_DOB_CONFLICT, COMPONENT_CANNOT_LINK, COMPONENT_SIZE_LIMIT).
  Such a link crosses into another component, so it is never pair evidence of
  either case; without it a case with no REVIEW pair has no visible reason.
* ``frozen_match_decisions_<run>``: the Splink match probability of each link.
* ``operational_deferred_review_cases`` + the pinned mapping snapshot: which link
  ends sit in a deferred (oversized, not imported) case.

Each Lakebase case of the release gets one ``case_review_context`` row with its
graph links and the reason codes the queue shows as tags (review_reasons.py).
Case payloads, fingerprints and review state are not touched; reset leaves the
rows alone. Re-running replaces the release's rows in one transaction.

Usage (from the repository root):
  uv run --project apps/steward python apps/steward/import_review_context.py \\
    --profile <profile> --warehouse-id <warehouse-id> [--apply]
"""
import argparse
import json
import re
from collections import Counter, defaultdict

from psycopg.types.json import Jsonb

from review_reasons import GRAPH, reason_codes

RUN = re.compile(r'[A-Za-z0-9_-]{1,100}')


def frozen(schema, table, run_id):
    if not RUN.fullmatch(run_id):
        raise ValueError('Release id is not a plain identifier')
    return f"{schema}.frozen_{table}_{run_id.replace('-', '_')}"


def read_release(sql, manifest):
    schema = manifest.snapshots['mappings']['table'].rsplit('.', 1)[0]
    quarantine, decisions = (frozen(schema, t, manifest.source_run_id)
                             for t in ('component_quarantine', 'match_decisions'))
    versions = {t: int(sql.run(f'DESCRIBE HISTORY {t} LIMIT 1')[0][0]) for t in (quarantine, decisions)}
    links = sql.run(f"""SELECT q.left_record_key, q.right_record_key, q.reason, d.match_probability
      FROM {quarantine} VERSION AS OF {versions[quarantine]} q
      LEFT JOIN {decisions} VERSION AS OF {versions[decisions]} d
        ON d.left_record_key=q.left_record_key AND d.right_record_key=q.right_record_key
      ORDER BY 1, 2""")
    mappings = manifest.snapshots['mappings']
    deferred = sql.run(f"""SELECT m.record_key, d.case_key
      FROM {mappings['table']} VERSION AS OF {mappings['version']} m
      JOIN (SELECT DISTINCT concat('cmp_',substring(sha2(component_key,256),1,28)) pid, case_key
            FROM {schema}.operational_deferred_review_cases WHERE release_id='{manifest.source_run_id}') d
        ON d.pid=m.provisional_customer_id""")
    provenance = {'quarantine_table': quarantine, 'quarantine_version': versions[quarantine],
                  'decisions_table': decisions, 'decisions_version': versions[decisions],
                  'deferred_table': f'{schema}.operational_deferred_review_cases',
                  'mapping_snapshot': f"{mappings['table']}@v{mappings['version']}",
                  'derived_by': 'import_review_context.py'}
    return links, dict(deferred), provenance


def plan(cases, links, deferred):
    """Graph links per case. ``cases`` maps case_id -> baseline payload."""
    case_of = {r['record_key']: case_id for case_id, payload in cases.items() for r in payload['records']}
    per_case, unplaced, reasons = defaultdict(list), 0, Counter()
    for left, right, reason, probability in links:
        if reason not in GRAPH:
            raise ValueError(f'Unknown graph check reason {reason!r}')
        reasons[reason] += 1
        placed = False
        for key, other in ((left, right), (right, left)):
            if key not in case_of:
                continue
            placed = True
            per_case[case_of[key]].append({
                'record_key': key, 'other_record_key': other, 'reason': reason,
                'match_probability': None if probability is None else float(probability),
                'other_case_id': case_of.get(other),
                'other_deferred': other in deferred})
        if not placed and left not in deferred and right not in deferred:
            unplaced += 1
    rows = {case_id: (per_case.get(case_id, []), reason_codes(payload, per_case.get(case_id, [])))
            for case_id, payload in cases.items()}
    within = sum(1 for links in per_case.values() for g in links if g['other_case_id'] and
                 g['other_case_id'] == case_of.get(g['record_key']))
    summary = {'graph_links': len(links), 'graph_links_by_reason': dict(reasons),
               'cases_with_graph_links': len(per_case),
               'links_between_deferred_cases_only': sum(
                   1 for l, r, *_ in links if l not in case_of and r not in case_of and (l in deferred or r in deferred)),
               'unplaced_links': unplaced, 'links_within_one_case': within,
               'cases_by_first_reason': dict(Counter(codes[0] for _, codes in rows.values()))}
    if unplaced:
        raise ValueError(f'{unplaced} withheld links touch neither an imported nor a deferred case')
    return rows, summary


def run(pool, sql, manifest, *, apply=False):
    links, deferred, provenance = read_release(sql, manifest)
    with pool.connection() as conn, conn.transaction():
        cases = {case_id: baseline or payload for case_id, baseline, payload in conn.execute(
            'SELECT case_id, baseline_payload, payload FROM steward.review_cases '
            'WHERE source_run_id=%s', (manifest.source_run_id,)).fetchall()}
        rows, summary = plan(cases, links, deferred)
        summary.update(release=manifest.source_run_id, cases=len(cases), provenance=provenance, applied=apply)
        if not apply:
            return summary
        conn.execute('DELETE FROM steward.case_review_context WHERE source_run_id=%s',
                     (manifest.source_run_id,))
        with conn.cursor() as cursor:
            cursor.executemany(
                'INSERT INTO steward.case_review_context '
                '(source_run_id, case_id, graph_links, reason_codes, provenance) VALUES (%s,%s,%s,%s,%s)',
                [(manifest.source_run_id, case_id, Jsonb(graph), Jsonb(codes), Jsonb(provenance))
                 for case_id, (graph, codes) in sorted(rows.items())])
        written = conn.execute('SELECT count(*) FROM steward.case_review_context '
                               'WHERE source_run_id=%s', (manifest.source_run_id,)).fetchone()[0]
        if written != len(cases):
            raise ValueError('Review context rows differ from the release cases; rolled back')
        summary['written'] = written
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--profile', help='Databricks CLI profile (default: ambient auth)')
    parser.add_argument('--warehouse-id', required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args(argv)
    from databricks.sdk import WorkspaceClient
    from lakebase import create_operator_pool
    from publish_reviews import Sql
    from reset_demo import active_manifest
    pool = create_operator_pool(args.profile)
    pool.open(wait=True)
    try:
        manifest = active_manifest(pool)
        result = run(pool, Sql(WorkspaceClient(profile=args.profile), args.warehouse_id), manifest, apply=args.apply)
        print(json.dumps(result, indent=2, default=str))
    finally:
        pool.close()


if __name__ == '__main__':
    main()
