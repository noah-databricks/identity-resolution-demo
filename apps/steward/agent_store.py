"""Durable review requests. Browser input cannot supply evidence or predictions."""
from dataclasses import asdict
from contextlib import contextmanager
from uuid import uuid4
from psycopg.types.json import Jsonb
from domain import InvalidDecision, StaleDecision
from store import load_case, ReviewStore


class AgentStore:
    def __init__(self, pool):
        self.pool = pool

    def list_runs(self, case_id, *, actor, masked=True):
        with self.pool.connection() as conn:
            rows = conn.execute('SELECT run_id,mode,status,message,created_at FROM '
                'steward.agent_runs WHERE case_id=%s AND actor=%s '
                'ORDER BY created_at DESC,run_id DESC LIMIT 20', (case_id, actor)).fetchall()
        return [{'run_id': r[0], 'mode': r[1], 'status': r[2],
                 'message': '[masked]' if masked else r[3], 'created_at': r[4]} for r in rows]

    def read_run(self, run_id: str, *, actor: str, after: int = 0, masked: bool = True) -> dict:
        from presentation import public_event, public_proposal
        if after < 0:
            raise InvalidDecision('Event cursor cannot be negative')
        with self.pool.connection() as conn:
            row = conn.execute('SELECT case_id,mode,status,trace_id,error_code,proposal_id FROM '
                'steward.agent_runs WHERE run_id=%s AND actor=%s', (run_id, actor)).fetchone()
            if not row:
                raise KeyError(run_id)
            events = conn.execute('SELECT sequence,event_type,payload,created_at FROM '
                'steward.agent_events WHERE run_id=%s AND sequence>%s '
                'ORDER BY sequence LIMIT 100', (run_id, after)).fetchall()
            proposal = None
            if row[5]:
                stored = conn.execute('SELECT payload,confirmation_digest FROM steward.proposals '
                                      'WHERE proposal_id=%s', (row[5],)).fetchone()
                if stored:
                    proposal = public_proposal(stored[0], masked=masked)
                    proposal['confirmation_digest'] = stored[1]
        return {'run_id': run_id, 'case_id': row[0], 'mode': row[1], 'status': row[2],
                'trace_id': row[3], 'error_code': row[4], 'proposal': proposal, 'masked': masked,
                'next_cursor': events[-1][0] if events else after,
                'events': [{'sequence': e[0], 'type': e[1],
                            'payload': public_event(e[1], e[2], masked=masked),
                            'created_at': e[3]} for e in events]}

    def event(self, run_id: str, kind: str, payload: dict) -> None:
        with self.pool.connection() as conn:
            conn.execute('INSERT INTO steward.agent_events(run_id,event_type,payload) '
                         'VALUES (%s,%s,%s)', (run_id, kind, Jsonb(payload)))

    def recover_interrupted(self) -> int:
        """Expire abandoned work without repeating inference or auto application.

        Ten minutes exceeds the bounded 90-second inference timeout. finish()
        locks and checks the same row, so a late worker cannot apply after expiry.
        """
        with self.pool.connection() as conn, conn.transaction():
            rows = conn.execute("UPDATE steward.agent_runs SET status='FAILED',"
                "error_code='INTERRUPTED',updated_at=now() WHERE status='RUNNING' "
                "AND updated_at < now() - interval '10 minutes' RETURNING run_id").fetchall()
            for row in rows:
                conn.execute('INSERT INTO steward.agent_events(run_id,event_type,payload) '
                    "VALUES (%s,'failed',%s)", (row[0], Jsonb({'code': 'INTERRUPTED'})))
            return len(rows)

    def finish(self, run_id: str, proposal) -> dict:
        """Proposal, optional auto application and completion commit together."""
        with self.pool.connection() as conn, conn.transaction():
            row = conn.execute('SELECT actor,mode,status,case_snapshot FROM '
                'steward.agent_runs WHERE run_id=%s FOR UPDATE', (run_id,)).fetchone()
            if not row or row[2] != 'RUNNING':
                raise InvalidDecision('Review request is not running')
            from domain import validate_proposal
            validate_proposal(load_case(row[3]), proposal)
            if proposal.origin != 'agent' or proposal.created_by != row[0] or not proposal.trace_id:
                raise InvalidDecision('Worker requires a traced proposal for the requesting reviewer')

            class TransactionPool:
                @contextmanager
                def connection(self):
                    yield conn

            reviews = ReviewStore(TransactionPool())
            reviews.propose(proposal)  # Revalidates against current case state.
            applied = None
            if row[1] == 'auto':
                applied = reviews.apply(proposal.proposal_id, request_id='agent:' + run_id,
                                        actor=row[0], mode='auto')
            result = {'proposal_id': proposal.proposal_id,
                      'confirmation_digest': proposal.confirmation_digest,
                      'mode': row[1], 'applied': applied}
            conn.execute("UPDATE steward.agent_runs SET status='COMPLETED',proposal_id=%s,"
                         'trace_id=%s,updated_at=now() WHERE run_id=%s',
                         (proposal.proposal_id, proposal.trace_id, run_id))
            conn.execute('INSERT INTO steward.agent_events(run_id,event_type,payload) '
                         "VALUES (%s,'completed',%s)", (run_id, Jsonb(result)))
            return result

    def fail(self, run_id: str, code: str) -> None:
        # Persist a bounded code, never raw provider errors that may include PII.
        if code not in ('TIMEOUT', 'INVALID_RESULT', 'INTERRUPTED', 'RATE_LIMITED', 'REVIEW_FAILED'):
            code = 'REVIEW_FAILED'
        with self.pool.connection() as conn, conn.transaction():
            changed = conn.execute("UPDATE steward.agent_runs SET status='FAILED',error_code=%s,"
                "updated_at=now() WHERE run_id=%s AND status='RUNNING' RETURNING run_id", (code, run_id)).fetchone()
            if changed:
                conn.execute('INSERT INTO steward.agent_events(run_id,event_type,payload) '
                             "VALUES (%s,'failed',%s)", (run_id, Jsonb({'code': code})))

    def enqueue(self, *, case_id: str, case_version: int, actor: str, mode: str,
                message: str, request_id: str) -> dict:
        if mode not in ('confirm', 'auto') or not actor.strip():
            raise InvalidDecision('Authenticated reviewer and valid agent mode required')
        if not request_id.strip() or len(request_id) > 128 or not 1 <= len(message.strip()) <= 4000:
            raise InvalidDecision('Bounded request ID and message required')
        with self.pool.connection() as conn, conn.transaction():
            conn.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',
                         (actor + ':' + request_id,))
            prior = conn.execute('SELECT run_id,case_id,mode,message,case_snapshot FROM '
                'steward.agent_runs WHERE actor=%s AND request_id=%s',
                (actor, request_id)).fetchone()
            if prior:
                if (prior[1], prior[2], prior[3], prior[4]['version']) != (case_id, mode, message, case_version):
                    raise InvalidDecision('Request ID already used for a different review')
                return {'run_id': prior[0], 'replayed': True}
            row = conn.execute('SELECT payload,status FROM steward.review_cases '
                               'WHERE case_id=%s FOR UPDATE', (case_id,)).fetchone()
            if not row:
                raise KeyError(case_id)
            case = load_case(row[0])
            if case.version != case_version or row[1] == 'RESOLVED':
                raise StaleDecision('Case changed or already resolved; refresh before reviewing')
            run_id = str(uuid4())
            conn.execute('INSERT INTO steward.agent_runs '
                '(run_id,case_id,actor,mode,status,request_id,case_snapshot,message) '
                "VALUES (%s,%s,%s,%s,'QUEUED',%s,%s,%s)",
                (run_id, case_id, actor, mode, request_id, Jsonb(asdict(case)), message))
            conn.execute('INSERT INTO steward.agent_events(run_id,event_type,payload) '
                         "VALUES (%s,'queued',%s)", (run_id, Jsonb({'mode': mode})))
            return {'run_id': run_id, 'replayed': False}

    def claim(self) -> dict | None:
        """One claimant per request; no connection is held during model inference.

        Interrupted requests need explicit recovery; never silently re-run auto
        decisions whose application status may be unknown.
        """
        with self.pool.connection() as conn, conn.transaction():
            row = conn.execute("SELECT run_id,actor,mode,message,case_snapshot FROM "
                "steward.agent_runs WHERE status='QUEUED' AND case_snapshot IS NOT NULL "
                'ORDER BY created_at,run_id FOR UPDATE SKIP LOCKED LIMIT 1').fetchone()
            if row is None:
                return None
            conn.execute("UPDATE steward.agent_runs SET status='RUNNING',updated_at=now() WHERE run_id=%s",
                         (row[0],))
            conn.execute('INSERT INTO steward.agent_events(run_id,event_type,payload) '
                         "VALUES (%s,'started','{}'::jsonb)", (row[0],))
            return dict(zip(('run_id', 'actor', 'mode', 'message', 'case_snapshot'), row))
