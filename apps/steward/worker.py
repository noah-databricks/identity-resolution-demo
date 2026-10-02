"""Bounded agent worker. API activation follows recovery and masking validation."""
import asyncio
import logging

from domain import InvalidDecision
from store import load_case


async def run_forever(store, stop: asyncio.Event, *, review_fn=None):
    """One bounded worker; requests and events remain in Lakebase across restarts."""
    while not stop.is_set():
        try:
            await asyncio.to_thread(store.recover_interrupted)
            worked = await process_one(store, review_fn=review_fn)
            if worked:
                continue
            delay = 2
        except asyncio.CancelledError:
            raise
        except Exception:
            # Raw DB/provider errors can include source data. Log a safe marker.
            logging.getLogger(__name__).error('Agent worker storage operation failed; retrying')
            delay = 5
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except TimeoutError:
            pass


def rate_limited(error: BaseException) -> bool:
    """Model endpoint HTTP 429, possibly wrapped by the agents runtime."""
    seen = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        if getattr(error, 'status_code', None) == 429 or type(error).__name__ == 'RateLimitError':
            return True
        error = error.__cause__ or error.__context__
    return False


async def process_one(store, *, review_fn=None) -> bool:
    if review_fn is None:
        from reviewer import review
        review_fn = review
    request = await asyncio.to_thread(store.claim)
    if request is None:
        return False
    run_id = request['run_id']

    async def emit(kind, payload):
        # Only publish completion after its transaction commits. The reviewer
        # proposal event is a preview, not an acknowledgement of persistence.
        if kind != 'proposal':
            await asyncio.to_thread(store.event, run_id, kind, payload)

    try:
        proposal = await review_fn(load_case(request['case_snapshot']), actor=request['actor'],
                                   message=request['message'], emit=emit)
        await asyncio.to_thread(store.finish, run_id, proposal)
    except asyncio.CancelledError:
        await asyncio.shield(asyncio.to_thread(store.fail, run_id, 'INTERRUPTED'))
        raise
    except TimeoutError:
        await asyncio.to_thread(store.fail, run_id, 'TIMEOUT')
    except InvalidDecision:
        await asyncio.to_thread(store.fail, run_id, 'INVALID_RESULT')
    except Exception as error:
        code = 'RATE_LIMITED' if rate_limited(error) else 'REVIEW_FAILED'
        await asyncio.to_thread(store.fail, run_id, code)
    return True
