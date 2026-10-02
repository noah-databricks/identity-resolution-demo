"""Operational HTTP boundary; never accepts an actor or truth labels from clients.

Production must run behind the Databricks Apps authenticated reverse proxy.
Tests inject the store and override authenticated_actor explicitly.
"""
from contextlib import asynccontextmanager
import asyncio
import os
import json
from dataclasses import asdict
from pathlib import Path
from typing import Literal
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field

from domain import InvalidDecision, Proposal, StaleDecision, public_record, public_pair
from review_reasons import derive, label_pairs, tags


def authenticated_actor(request: Request) -> str:
    # This header is supplied by the authenticated platform proxy. Do not expose
    # this service directly to an untrusted network outside Databricks Apps.
    actor = request.headers.get('x-forwarded-email', '').strip()
    if not actor:
        raise HTTPException(401, 'Authentication required')
    if request.method not in ('GET', 'HEAD', 'OPTIONS'):
        # JSON-only mutation endpoints plus a non-simple header prevent ambient
        # cookie authentication from being used by cross-origin HTML forms.
        if request.headers.get('x-steward-request') != '1':
            raise HTTPException(403, 'Application request header required')
        if request.headers.get('sec-fetch-site') == 'cross-site':
            raise HTTPException(403, 'Cross-site writes are not allowed')
    return actor


class ManualProposal(BaseModel):
    model_config = ConfigDict(extra='forbid')
    case_version: int = Field(gt=0)
    evidence_fingerprint: str = Field(min_length=64, max_length=64)
    action: str
    groups: list[list[str]]
    rationale: str = Field(min_length=1, max_length=4000)
    evidence_refs: list[str] = Field(default_factory=list, max_length=100)
    golden_fields: list[dict[str, str]] = Field(default_factory=list, max_length=100)


class Confirmation(BaseModel):
    model_config = ConfigDict(extra='forbid')
    request_id: str = Field(min_length=1, max_length=128)
    confirmed_digest: str = Field(min_length=64, max_length=64)


class AgentRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    case_version: int = Field(gt=0)
    request_id: str = Field(min_length=1, max_length=128)
    mode: Literal['confirm', 'auto'] = 'confirm'
    message: str = Field(min_length=1, max_length=4000)


def create_app(store=None, agent_store=None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app):
        if store is not None:
            app.state.store = store
            app.state.agents = agent_store
            yield
            return
        from lakebase import create_pool
        from store import ReviewStore
        pool = create_pool()
        pool.open(wait=True)
        worker_task = None
        stop = asyncio.Event()
        try:
            # Deploy first: the app principal becomes the schema owner.
            with pool.connection() as conn:
                conn.execute(Path(__file__).with_name('schema.sql').read_text())
            app.state.store = ReviewStore(pool)
            from agent_store import AgentStore
            from reviewer import configure_tracing
            from worker import run_forever
            configure_tracing(os.environ['MLFLOW_EXPERIMENT_ID'])
            app.state.agents = AgentStore(pool)
            worker_task = asyncio.create_task(run_forever(app.state.agents, stop))
            yield
        finally:
            stop.set()
            if worker_task:
                worker_task.cancel()
                try:
                    await worker_task
                except asyncio.CancelledError:
                    pass
            pool.close()

    app = FastAPI(title='Steward', lifespan=lifespan)
    assets = Path(__file__).with_name('static')
    if assets.is_dir():
        app.mount('/assets', StaticFiles(directory=assets), name='assets')

    @app.get('/')
    def frontend(actor: str = Depends(authenticated_actor)):
        return FileResponse(assets / 'index.html')

    @app.get('/api/whoami')
    def whoami(actor: str = Depends(authenticated_actor)):
        return {'email': actor, 'execution_identity': 'app_service_principal'}

    @app.get('/api/summary')
    def summary(request: Request, actor: str = Depends(authenticated_actor)):
        return request.app.state.store.summary()

    @app.get('/api/cases/{case_id}/agent-runs')
    def case_runs(case_id: str, request: Request, masked: bool = False,
                  actor: str = Depends(authenticated_actor)):
        if request.app.state.agents is None:
            raise HTTPException(503, 'Agent worker unavailable')
        return {'items': request.app.state.agents.list_runs(case_id, actor=actor, masked=masked)}

    @app.middleware('http')
    async def private_responses(request, call_next):
        response = await call_next(request)
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response

    @app.exception_handler(InvalidDecision)
    async def invalid(request, exc):
        return JSONResponse(status_code=409 if isinstance(exc, StaleDecision) else 422,
                            content={'detail': str(exc)})

    @app.exception_handler(KeyError)
    async def missing(request, exc):
        return JSONResponse(status_code=404, content={'detail': 'Not found'})

    @app.get('/api/cases/{case_id}')
    def detail(case_id: str, request: Request, masked: bool = Query(False),
               actor: str = Depends(authenticated_actor)):
        case = request.app.state.store.get_case(case_id)
        result = asdict(case)
        result['records'] = [public_record(record, masked=masked) for record in case.records]
        result['pair_evidence'] = [public_pair(pair, masked=masked) for pair in case.pair_evidence]
        result['evidence_fingerprint'] = case.fingerprint
        result['masked'] = masked
        # Explanations use a fixed vocabulary over codes and flags, never record
        # values, so they are the same masked or unmasked.
        explain = getattr(request.app.state.store, 'review_context', None)
        context = explain(case) if explain else {'model_version': None, 'graph_links': [],
                                                 'graph_checks_imported': False}
        label_pairs(result['pair_evidence'], context['model_version'])
        # Unmasked pair codes: derive() only matches them against its own vocabulary.
        unmasked = {'case_id': case.case_id, 'current_groups': result['current_groups'],
                    'pair_evidence': [asdict(pair) for pair in case.pair_evidence]}
        result['review_reasons'] = derive(unmasked, context['graph_links']) | {
            'graph_checks_imported': context['graph_checks_imported']}
        result['model_version'] = context['model_version']
        return result

    @app.post('/api/cases/{case_id}/agent-runs', status_code=202)
    def start_review(case_id: str, body: AgentRequest, request: Request,
                     actor: str = Depends(authenticated_actor)):
        agents = request.app.state.agents
        if agents is None:
            raise HTTPException(503, 'Agent worker unavailable')
        return agents.enqueue(case_id=case_id, actor=actor, **body.model_dump())

    @app.get('/api/agent-runs/{run_id}')
    def read_review(run_id: str, request: Request, after: int = Query(0, ge=0),
                    masked: bool = False, actor: str = Depends(authenticated_actor)):
        agents = request.app.state.agents
        if agents is None:
            raise HTTPException(503, 'Agent worker unavailable')
        return agents.read_run(run_id, actor=actor, after=after, masked=masked)

    @app.get('/api/agent-runs/{run_id}/stream')
    async def stream_review(run_id: str, request: Request, after: int = Query(0, ge=0),
                            masked: bool = False, actor: str = Depends(authenticated_actor)):
        agents = request.app.state.agents
        if agents is None:
            raise HTTPException(503, 'Agent worker unavailable')
        # Authorize before sending SSE headers, and on every subsequent read.
        first = await asyncio.to_thread(agents.read_run, run_id, actor=actor, after=after, masked=masked)

        async def events():
            view = first
            previous = None
            while not await request.is_disconnected():
                encoded = json.dumps(view, default=str)
                if encoded != previous:
                    yield f'event: update\ndata: {encoded}\n\n'
                    previous = encoded
                else:
                    yield ': heartbeat\n\n'
                if view['status'] in ('COMPLETED', 'FAILED') and len(view['events']) < 100:
                    return
                await asyncio.sleep(.25)
                view = await asyncio.to_thread(agents.read_run, run_id, actor=actor,
                    after=view['next_cursor'], masked=masked)
        return StreamingResponse(events(), media_type='text/event-stream',
                                 headers={'X-Accel-Buffering': 'no', 'Cache-Control': 'no-store'})

    @app.get('/api/cases')
    def queue(request: Request, status: Literal['REVIEW', 'RESOLVED', 'DEFERRED'] = 'REVIEW',
              sort: Literal['oldest', 'confidence'] = 'oldest',
              limit: int = Query(30, ge=1, le=100), offset: int = Query(0, ge=0, le=100000),
              search: str = Query('', max_length=100),
              lean: Literal['all', 'same', 'different', 'unclear'] = 'all',
              masked: bool = False, actor: str = Depends(authenticated_actor)):
        page = request.app.state.store.queue_page(status=status, sort=sort, limit=limit, offset=offset,
                                                  search=search, lean=lean)
        rows = page['rows']
        items = []
        for row in rows:
            case = row['case']
            items.append({key: value for key, value in row.items() if key not in ('case', 'review_codes')} | {
                'case_id': case.case_id, 'case_version': case.version,
                'source_run_id': case.source_run_id,
                'review_tags': tags(row.get('review_codes')),
                'records': [public_record(record, masked=masked) for record in case.records]})
        return {'items': items, 'offset': offset, 'limit': limit, 'masked': masked, 'total': page['total'],
                'lean_counts': page['lean_counts']}

    @app.post('/api/cases/{case_id}/proposals', status_code=201)
    def propose(case_id: str, body: ManualProposal, request: Request,
                actor: str = Depends(authenticated_actor)):
        proposal = Proposal(str(uuid4()), case_id, body.case_version,
                            body.evidence_fingerprint, body.action,
                            tuple(tuple(group) for group in body.groups),
                            body.rationale, tuple(body.evidence_refs), 'human', actor,
                            golden_fields=tuple(body.golden_fields))
        request.app.state.store.propose(proposal)
        return {'proposal_id': proposal.proposal_id,
                'confirmation_digest': proposal.confirmation_digest}

    @app.post('/api/proposals/{proposal_id}/confirm')
    def confirm(proposal_id: str, body: Confirmation, request: Request,
                actor: str = Depends(authenticated_actor)):
        # Auto mode is deliberately absent from this public API. Only the
        # server-side agent worker may perform automatic application.
        return request.app.state.store.confirm(proposal_id, request_id=body.request_id,
                    actor=actor, confirmed_digest=body.confirmed_digest)

    return app


app = create_app()

if __name__ == '__main__':
    import os
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=int(os.environ.get('DATABRICKS_APP_PORT', 8000)))
