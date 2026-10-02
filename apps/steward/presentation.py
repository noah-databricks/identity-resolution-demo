"""Server-side masking for free-form agent content.

Do not attempt name-substring redaction: model text can paraphrase or transform
PII. Masked mode withholds free-form content and preserves only known metadata.
"""
from copy import deepcopy

TOOLS = frozenset(('get_case_context', 'get_safeguards', 'get_pipeline_observations', 'preview_resolution'))
CODES = frozenset(('TIMEOUT', 'INVALID_RESULT', 'INTERRUPTED', 'REVIEW_FAILED'))


def public_event(kind: str, payload: dict, *, masked: bool) -> dict:
    if not masked:
        return deepcopy(payload)
    if kind in ('tool_result', 'tool_started'):
        tool = payload.get('tool')
        return {'tool': tool if tool in TOOLS else 'tool', 'content_masked': True}
    if kind == 'failed':
        code = payload.get('code')
        return {'code': code if code in CODES else 'REVIEW_FAILED'}
    if kind == 'queued':
        return {'mode': payload.get('mode') if payload.get('mode') in ('auto', 'confirm') else None}
    if kind == 'completed':
        # This event is constructed by AgentStore.finish, not by the model.
        return {key: deepcopy(payload[key]) for key in
                ('proposal_id', 'confirmation_digest', 'mode', 'applied') if key in payload}
    return {'content_masked': True}


def public_proposal(proposal: dict, *, masked: bool) -> dict:
    result = deepcopy(proposal)
    if masked:
        result['rationale'] = '[masked]'
        result['evidence_refs'] = []
    return result
