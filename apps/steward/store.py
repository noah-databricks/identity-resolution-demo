"""Lakebase transaction boundary. No model has direct SQL or write access."""
from dataclasses import asdict, replace
from uuid import uuid4
import json
import hashlib

from psycopg.types.json import Jsonb

from case_lean import lean as case_lean
from domain import IdentityRecord, PairEvidence, ReviewCase, Proposal, InvalidDecision, authorize_apply, validate_proposal


def load_case(payload: dict) -> ReviewCase:
    return ReviewCase(
        case_id=payload['case_id'], source_run_id=payload['source_run_id'], version=payload['version'],
        records=tuple(IdentityRecord(**record) for record in payload['records']),
        current_groups=tuple(tuple(group) for group in payload['current_groups']),
        golden_fields=tuple(payload.get('golden_fields', [])),
        cannot_link=tuple(tuple(pair) for pair in payload.get('cannot_link', [])),
        pair_evidence=tuple(PairEvidence(**{**pair,
            'features': tuple(tuple(feature) for feature in pair.get('features', [])),
            **{key: tuple(pair.get(key, [])) for key in
               ('candidate_routes', 'evidence_reasons', 'safeguards_triggered')}})
            for pair in payload.get('pair_evidence', [])),
    )


def load_proposal(payload: dict) -> Proposal:
    return Proposal(**{**payload, 'groups':tuple(tuple(group) for group in payload['groups']),
                       'evidence_refs':tuple(payload['evidence_refs'])})


class ReviewStore:
    def __init__(self, pool):
        self.pool = pool

    def summary(self):
        with self.pool.connection() as conn:
            counts = conn.execute('SELECT status,count(*) FROM steward.review_cases GROUP BY status').fetchall()
            latest = conn.execute('SELECT source_run_id,summary,created_at,release_manifest FROM '
                'steward.import_batches WHERE active ORDER BY created_at DESC LIMIT 1').fetchone()
            archived = conn.execute('SELECT count(*) FROM steward.import_batches '
                                    'WHERE NOT active').fetchone()[0]
            resolver = conn.execute("SELECT payload->'pair_evidence'->0->>'resolver_version' "
                'FROM steward.review_cases WHERE source_run_id=%s '
                "AND jsonb_array_length(payload->'pair_evidence')>0 LIMIT 1",
                (latest[0],)).fetchone() if latest else None
        release = None
        if latest:
            metadata = (latest[3] or {}).get('metadata') or {}
            release = {'source_run_id': latest[0], 'summary': latest[1], 'imported_at': latest[2],
                       'resolver_version': resolver[0] if resolver else None,
                       'metadata': {k: v for k, v in metadata.items() if isinstance(v, (str, int, float))}}
        return {'case_counts': dict(counts), 'source': 'Lakebase review queue',
                'release': release, 'archived_releases': archived}

    # Queue rows need only the case header and records; pair evidence can be
    # large (hundreds of pairs for probabilistic resolvers), so it is not read.
    QUEUE_PAYLOAD = ("jsonb_build_object('case_id',case_id,'source_run_id',source_run_id,"
                     "'version',version,'records',payload->'records',"
                     "'current_groups',payload->'current_groups',"
                     "'cannot_link',COALESCE(payload->'cannot_link','[]'::jsonb))")
    # Stored per case by import_review_context.py (release metadata, not case state).
    REVIEW_CODES = ('(SELECT x.reason_codes FROM steward.case_review_context x '
                    'WHERE x.source_run_id=review_cases.source_run_id AND x.case_id=review_cases.case_id)')
    PROBABILISTIC_CASE = ("jsonb_path_exists(payload, '$.pair_evidence[*] ? "
                          "(@.decision == \"REVIEW\" && @.model_evidence.match_probability != null)')")

    LEANS = ('all', 'same', 'different', 'unclear')

    def _queue_rows(self, conn, *, status, sort, search, page):
        orders = {'oldest': 'imported_at ASC, case_id ASC',
                  'confidence': 'pipeline_confidence DESC NULLS LAST, case_id ASC'}
        return conn.execute(
            'SELECT ' + self.QUEUE_PAYLOAD + ',status,pipeline_confidence,imported_at,updated_at,'
            # Splink cases: pipeline_confidence is the lowest REVIEW-pair match probability.
            + self.PROBABILISTIC_CASE + ',' + self.REVIEW_CODES +
            ' FROM steward.review_cases WHERE status=%s '
            'AND position(lower(%s) in lower(search_text)) > 0 ORDER BY '
            + orders[sort] + (' LIMIT %s OFFSET %s' if page else ''),
            (status, search.strip(), *page)).fetchall()

    @staticmethod
    def _queue_row(row):
        case = load_case(row[0])
        return {'case': case, 'status': row[1], 'pipeline_confidence': row[2],
                'imported_at': row[3], 'updated_at': row[4],
                'confidence_kind': 'match_probability' if row[5] else 'evidence_score',
                'review_codes': row[6] or [],
                'lean': case_lean(case.records, case.cannot_link, row[6] or [], row[2] if row[5] else None)}

    def list_cases(self, *, status: str, sort: str, limit: int, offset: int, search: str = '',
                   lean: str = 'all') -> list[dict]:
        return self.queue_page(status=status, sort=sort, limit=limit, offset=offset,
                               search=search, lean=lean)['rows']

    def queue_page(self, *, status: str, sort: str, limit: int, offset: int, search: str = '',
                   lean: str = 'all') -> dict:
        """One queue page plus per-lean counts. A lean filter is computed in the
        app over the filtered status (hundreds of cases, records only)."""
        if status not in ('REVIEW', 'RESOLVED', 'DEFERRED') or sort not in ('oldest', 'confidence') \
                or lean not in self.LEANS:
            raise InvalidDecision('Unknown queue filter or ordering')
        if not 1 <= limit <= 100 or not 0 <= offset <= 100000:
            raise InvalidDecision('Invalid queue page')
        if not isinstance(search, str) or len(search) > 100:
            raise InvalidDecision('Search must be at most 100 characters')
        with self.pool.connection() as conn:
            rows = [self._queue_row(r) for r in self._queue_rows(conn, status=status, sort=sort,
                                                                 search=search, page=())]
        counts = {'all': len(rows)} | {k: sum(r['lean']['kind'] == k for r in rows) for k in self.LEANS[1:]}
        if lean != 'all':
            rows = [r for r in rows if r['lean']['kind'] == lean]
            if lean == 'different':
                # Strongest conflicts first; ties keep the chosen order.
                rows.sort(key=lambda r: -r['lean']['strength'])
        return {'rows': rows[offset:offset + limit], 'total': len(rows), 'lean_counts': counts}

    def count_cases(self, *, status: str, search: str = '') -> int:
        with self.pool.connection() as conn:
            return conn.execute(
                'SELECT count(*) FROM steward.review_cases WHERE status=%s '
                'AND position(lower(%s) in lower(search_text)) > 0',
                (status, search.strip())).fetchone()[0]

    def get_case(self, case_id: str) -> ReviewCase:
        with self.pool.connection() as conn:
            row = conn.execute('SELECT payload FROM steward.review_cases WHERE case_id=%s', (case_id,)).fetchone()
            if not row:
                raise KeyError(case_id)
            return load_case(row[0])

    def review_context(self, case: ReviewCase) -> dict:
        """Release metadata that explains a case: the model version (for comparison
        level labels) and graph-withheld links imported by import_review_context.py."""
        with self.pool.connection() as conn:
            release = conn.execute("SELECT release_manifest->'metadata'->>'model_version' "
                                   'FROM steward.import_batches WHERE source_run_id=%s',
                                   (case.source_run_id,)).fetchone()
            context = conn.execute('SELECT graph_links, provenance FROM steward.case_review_context '
                                   'WHERE source_run_id=%s AND case_id=%s',
                                   (case.source_run_id, case.case_id)).fetchone()
        return {'model_version': release[0] if release else None,
                'graph_links': context[0] if context else [],
                'graph_checks_imported': context is not None}

    def confirm(self, proposal_id: str, *, request_id: str, actor: str,
                confirmed_digest: str) -> dict:
        """Choose audit mode from stored provenance, never browser input."""
        with self.pool.connection() as conn:
            row = conn.execute('SELECT payload FROM steward.proposals WHERE proposal_id=%s',
                               (proposal_id,)).fetchone()
            if not row:
                raise KeyError(proposal_id)
            proposal = load_proposal(row[0])
        return self.apply(proposal_id, request_id=request_id, actor=actor,
                          mode='manual' if proposal.origin == 'human' else 'confirm',
                          confirmed_digest=confirmed_digest)

    def propose(self, proposal: Proposal) -> None:
        with self.pool.connection() as conn, conn.transaction():
            row = conn.execute('SELECT payload FROM steward.review_cases WHERE case_id=%s FOR UPDATE', (proposal.case_id,)).fetchone()
            if not row:
                raise KeyError(proposal.case_id)
            validate_proposal(load_case(row[0]), proposal)
            conn.execute('INSERT INTO steward.proposals (proposal_id,case_id,payload,confirmation_digest) VALUES (%s,%s,%s,%s)',
                         (proposal.proposal_id, proposal.case_id, Jsonb(asdict(proposal)), proposal.confirmation_digest))

    def apply(self, proposal_id: str, *, request_id: str, actor: str, mode: str,
              confirmed_digest: str | None = None) -> dict:
        if not request_id.strip() or len(request_id) > 128:
            raise InvalidDecision('A bounded idempotency key is required')
        with self.pool.connection() as conn, conn.transaction():
            # Serializes retries even when they race before a decision exists.
            conn.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))', (request_id,))
            stored = conn.execute('SELECT payload FROM steward.proposals WHERE proposal_id=%s', (proposal_id,)).fetchone()
            if not stored:
                raise KeyError(proposal_id)
            proposal = load_proposal(stored[0])
            # Retries must still prove possession of the exact confirmation,
            # even though their original case version has already advanced.
            if mode != 'auto' and confirmed_digest != proposal.confirmation_digest:
                raise InvalidDecision('Confirmation must match the exact displayed proposal')
            prior = conn.execute('SELECT decision_id,proposal_id,actor,mode FROM steward.decisions WHERE request_id=%s', (request_id,)).fetchone()
            if prior:
                if (prior[1], prior[2], prior[3]) != (proposal_id, actor, mode):
                    raise InvalidDecision('Idempotency key already used for a different operation')
                return {'decision_id':prior[0], 'replayed':True}
            row = conn.execute('SELECT payload FROM steward.review_cases WHERE case_id=%s FOR UPDATE', (proposal.case_id,)).fetchone()
            case = load_case(row[0])
            authorize_apply(case, proposal, mode=mode, actor=actor, confirmed_digest=confirmed_digest)
            decision_id = str(uuid4())
            updated = replace(case, version=case.version+1, current_groups=proposal.groups,
                golden_fields=case.golden_fields if proposal.action == 'DEFER' else proposal.golden_fields)
            event = {'event_id':decision_id, 'case_id':case.case_id, 'source_run_id':case.source_run_id,
                     'base_case_version':case.version, 'applied_case_version':updated.version,
                     'proposal':asdict(proposal), 'actor':actor, 'mode':mode}
            conn.execute('INSERT INTO steward.decisions (decision_id,request_id,proposal_id,case_id,actor,mode,payload,previous_groups,applied_case_version) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                         (decision_id,request_id,proposal_id,case.case_id,actor,mode,Jsonb(event),Jsonb(case.current_groups),updated.version))
            conn.execute('UPDATE steward.review_cases SET version=%s,status=%s,payload=%s,updated_at=now() WHERE case_id=%s',
                         (updated.version,'DEFERRED' if proposal.action=='DEFER' else 'RESOLVED',Jsonb(asdict(updated)),case.case_id))
            conn.execute('INSERT INTO steward.outbox(event_id,payload) VALUES (%s,%s)', (decision_id,Jsonb(event)))

            # Save golden-field selections to survivorship table
            if proposal.golden_fields and proposal.action != 'DEFER':
                records = {r.record_key: r for r in case.records}
                for group, fields in zip(proposal.groups, proposal.golden_fields):
                    # Generate the same stable steward-identity as publication.py
                    identity_digest = hashlib.sha256(
                        json.dumps([case.source_run_id, sorted(group)],
                                   separators=(',', ':'), ensure_ascii=False).encode()
                    ).hexdigest()
                    guest_identity = 'steward-' + identity_digest

                    for field_name, chosen_record_key in fields.items():
                        record = records[chosen_record_key]
                        field_value = getattr(record, field_name)
                        conn.execute(
                            '''INSERT INTO steward.reviewed_golden_fields
                               (decision_id, case_id, source_run_id, applied_case_version,
                                guest_identity, field_name, chosen_record_key, field_value)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)''',
                            (decision_id, case.case_id, case.source_run_id, updated.version,
                             guest_identity, field_name, chosen_record_key, field_value)
                        )

            return {'decision_id':decision_id, 'case_version':updated.version, 'replayed':False}
