"""Truth-blind review contracts shared by manual and agent decision paths."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from typing import Literal


Action = Literal['MERGE', 'SEPARATE', 'PARTITION', 'DEFER']
Mode = Literal['manual', 'confirm', 'auto']


class InvalidDecision(ValueError):
    pass


class StaleDecision(InvalidDecision):
    pass


@dataclass(frozen=True)
class IdentityRecord:
    record_key: str
    source_system: str
    source_record_id: str
    name: str | None = None
    email: str | None = None
    phone: str | None = None
    date_of_birth: str | None = None
    address: str | None = None
    contact_context: str | None = None
    given_name: str | None = None
    family_name: str | None = None
    work_email: str | None = None
    work_phone: str | None = None
    work_address: str | None = None
    company_name: str | None = None
    source_updated_at: str | None = None


@dataclass(frozen=True)
class PairEvidence:
    left_record_key: str
    right_record_key: str
    source_run_id: str
    resolver_version: str
    decision: str
    evidence_score: float
    features: tuple[tuple[str, int | float | None], ...] = ()
    candidate_routes: tuple[str, ...] = ()
    evidence_reasons: tuple[str, ...] = ()
    safeguards_triggered: tuple[str, ...] = ()
    decision_explanation: str | None = None
    # Probabilistic resolver evidence (e.g. Splink): match probability, total and
    # per-comparison match weights and the routing rule. None for resolvers that
    # do not supply it; omitted from fingerprints then, so older releases keep
    # their imported fingerprints.
    model_evidence: dict | None = None


@dataclass(frozen=True)
class ReviewCase:
    case_id: str
    source_run_id: str
    version: int
    records: tuple[IdentityRecord, ...]
    current_groups: tuple[tuple[str, ...], ...]
    # All contradictions in the affected component, not only the candidate edge.
    cannot_link: tuple[tuple[str, str], ...] = ()
    pair_evidence: tuple[PairEvidence, ...] = ()
    golden_fields: tuple[dict[str, str], ...] = ()

    @property
    def fingerprint(self) -> str:
        return digest(fingerprint_payload(asdict(self)))


def fingerprint_payload(case: dict) -> dict:
    """Drop optional fields that older imports never carried, when empty."""
    if not case.get('golden_fields'):
        case.pop('golden_fields', None)
    for pair in case.get('pair_evidence', ()):
        if pair.get('model_evidence') is None:
            pair.pop('model_evidence', None)
    return case


@dataclass(frozen=True)
class Proposal:
    proposal_id: str
    case_id: str
    case_version: int
    evidence_fingerprint: str
    action: Action
    groups: tuple[tuple[str, ...], ...]
    rationale: str
    evidence_refs: tuple[str, ...]
    origin: Literal['human', 'agent']
    created_by: str
    trace_id: str | None = None
    golden_fields: tuple[dict[str, str], ...] = ()

    @property
    def confirmation_digest(self) -> str:
        payload = asdict(self)
        if not self.golden_fields:
            payload.pop('golden_fields')
        return digest(payload)


GOLDEN_FIELDS = frozenset(('name', 'given_name', 'family_name', 'email', 'phone',
    'date_of_birth', 'address', 'work_email', 'work_phone', 'work_address',
    'company_name', 'contact_context'))


def validate_golden_fields(case: ReviewCase, proposal: Proposal) -> None:
    if not proposal.golden_fields:
        return
    if proposal.action == 'DEFER':
        raise InvalidDecision('Deferral cannot change golden fields')
    if len(proposal.golden_fields) != len(proposal.groups):
        raise InvalidDecision('Golden fields must align with every resulting guest')
    records = {r.record_key: r for r in case.records}
    for group, choices in zip(proposal.groups, proposal.golden_fields):
        for field, source in choices.items():
            if field not in GOLDEN_FIELDS:
                raise InvalidDecision('Unknown golden field')
            if source not in group or source not in records:
                raise InvalidDecision('Golden field source must belong to its guest')
            if getattr(records[source], field) in (None, ''):
                raise InvalidDecision('Golden field source has no value')


def digest(value: object) -> str:
    return sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


def validate_partition(case: ReviewCase, groups: tuple[tuple[str, ...], ...]) -> None:
    keys = [r.record_key for r in case.records]
    if not keys or len(keys) != len(set(keys)):
        raise InvalidDecision('Case must contain unique record keys')
    flattened = [key for group in groups for key in group]
    if not groups or any(not group for group in groups):
        raise InvalidDecision('Identity groups cannot be empty')
    if len(flattened) != len(set(flattened)) or set(flattened) != set(keys):
        raise InvalidDecision('Every affected record must occur exactly once')
    membership = {key: i for i, group in enumerate(groups) for key in group}
    constraints = list(case.cannot_link)
    for pair in case.pair_evidence:
        if pair.source_run_id != case.source_run_id:
            raise InvalidDecision('Pair evidence belongs to a different source run')
        if pair.left_record_key not in membership or pair.right_record_key not in membership:
            raise InvalidDecision('Pair evidence references a record outside the case')
        if dict(pair.features).get('valid_dob_contradiction') == 1:
            constraints.append((pair.left_record_key, pair.right_record_key))
    for left, right in constraints:
        if left not in membership or right not in membership:
            raise InvalidDecision('Contradiction references a record outside the case')
        # Pipeline identity observations are advisory in stewardship. Keep
        # reference integrity checks, but do not veto a reasoned reviewer decision.


def validate_proposal(case: ReviewCase, proposal: Proposal) -> None:
    if proposal.case_id != case.case_id or proposal.case_version != case.version:
        raise StaleDecision('Case changed; request a fresh proposal')
    if proposal.evidence_fingerprint != case.fingerprint:
        raise StaleDecision('Source evidence changed; request a fresh proposal')
    if not proposal.rationale.strip() or not proposal.created_by.strip():
        raise InvalidDecision('Rationale and actor are required')
    if proposal.origin not in ('human', 'agent'):
        raise InvalidDecision('Unknown proposal origin')
    if proposal.action not in ('MERGE', 'SEPARATE', 'PARTITION', 'DEFER'):
        raise InvalidDecision('Unknown decision action')
    validate_golden_fields(case, proposal)
    if proposal.action == 'DEFER':
        if proposal.groups != case.current_groups:
            raise InvalidDecision('Deferral cannot change identity membership')
        return
    validate_partition(case, proposal.groups)
    if proposal.action == 'MERGE' and len(proposal.groups) != 1:
        raise InvalidDecision('MERGE requires one resulting identity')
    if proposal.action == 'SEPARATE' and any(len(group) != 1 for group in proposal.groups):
        raise InvalidDecision('SEPARATE requires one identity per record; use PARTITION otherwise')
    if proposal.action == 'PARTITION' and not 1 < len(proposal.groups) < len(case.records):
        raise InvalidDecision('PARTITION requires multiple nontrivial identity groups')


def authorize_apply(case: ReviewCase, proposal: Proposal, *, mode: Mode,
                    actor: str, confirmed_digest: str | None = None) -> None:
    validate_proposal(case, proposal)
    if not actor.strip():
        raise InvalidDecision('Authenticated actor is required')
    if mode not in ('manual', 'confirm', 'auto'):
        raise InvalidDecision('Unknown review mode')
    if mode == 'auto':
        if proposal.origin != 'agent' or not proposal.trace_id:
            raise InvalidDecision('Automatic application requires a traced agent proposal')
    elif confirmed_digest != proposal.confirmation_digest:
        raise InvalidDecision('Confirmation must match the exact displayed proposal')


def public_record(record: IdentityRecord, *, masked: bool) -> dict:
    """Mask on the server, before serialization; not a browser-only obscuring effect."""
    result = asdict(record)
    if masked:
        for field in ('name', 'email', 'phone', 'date_of_birth', 'address',
                      'contact_context', 'given_name', 'family_name', 'work_email',
                      'work_phone', 'work_address', 'company_name'):
            if result[field] is not None:
                result[field] = '[masked]'
    return result


def public_pair(pair: PairEvidence, *, masked: bool) -> dict:
    result = asdict(pair)
    if masked:
        # Free-form explanations and upstream reason strings may include PII.
        result['decision_explanation'] = '[masked]' if pair.decision_explanation else None
        result['evidence_reasons'] = []
        result['safeguards_triggered'] = []
        result['candidate_routes'] = []
    return result
