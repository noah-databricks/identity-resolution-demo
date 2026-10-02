"""Allowlisted conversion of operational source rows; no database access.

This is not an importer: release attestation and snapshot consistency must be
established before any resulting record is persisted in a review case.
"""
from datetime import date, datetime
import math
import re

from domain import IdentityRecord, InvalidDecision, PairEvidence

FLAGS = ('rare_email_exact', 'rare_phone_exact', 'shared_email_exact',
    'shared_phone_exact', 'home_address_exact', 'work_address_exact',
    'name_exact', 'given_name_exact', 'family_name_exact', 'home_postcode_exact',
    'company_exact', 'multilingual_name_pair', 'valid_dob_exact',
    'valid_dob_contradiction', 'left_dob_is_default', 'right_dob_is_default',
    'personal_email_disagrees', 'personal_phone_disagrees')
SIMILARITIES = ('name_similarity', 'address_similarity', 'name_ann_similarity',
    'address_ann_similarity', 'name_ai_similarity', 'address_ai_similarity')
# Required for every resolver: default-DOB provenance must never be inferred.
DOB_FLAGS = ('valid_dob_exact', 'valid_dob_contradiction',
             'left_dob_is_default', 'right_dob_is_default')
# Optional probabilistic-model evidence (Splink). A release that has
# match_probability is a probabilistic release; the other columns are optional.
MODEL_COLUMNS = ('match_probability', 'match_weight', 'comparison_match_weights', 'decision_rule')
COMPARISON = re.compile(r'[a-z][a-z0-9_]{0,63}')
RULE = re.compile(r'[A-Z][A-Z0-9_]{0,79}')
# Splink comparison-level labels are model metadata ("Exact", "Normalised edit
# similarity >= 0.9"), never record values; restrict them to that shape.
LEVEL = re.compile(r'[A-Za-z0-9 .,:;<>=_()%+/-]{1,80}')
NATIVE_BF = re.compile(r'bf_(?!tf_adj_)([a-z][a-z0-9_]{0,63})')


def finite(value, what, *, low=None, high=None):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise InvalidDecision(f'{what} must be a finite number')
    if (low is not None and value < low) or (high is not None and value > high):
        raise InvalidDecision(f'{what} is out of range')
    return float(value)


def model_evidence(row: dict) -> dict | None:
    """Allowlisted Splink-style evidence, or None when the resolver supplies none.

    Per-comparison weights come from ``comparison_match_weights`` (an array of
    {comparison, level, match_weight}) or from native Splink ``bf_<comparison>``
    Bayes factors with optional ``bf_tf_adj_<comparison>`` and ``gamma_<comparison>``.
    The routing rule is ``decision_rule``, else Splink's ``decision_reason`` code.
    """
    if row.get('match_probability') is None:
        if any(row.get(key) is not None for key in MODEL_COLUMNS[1:]):
            raise InvalidDecision('Model evidence requires a match probability')
        return None
    probability = finite(row['match_probability'], 'Match probability', low=0, high=1)
    weight = row.get('match_weight')
    weight = None if weight is None else finite(weight, 'Match weight')
    comparisons = []
    explicit = row.get('comparison_match_weights')
    if explicit is not None:
        if not isinstance(explicit, list) or len(explicit) > 40:
            raise InvalidDecision('Comparison match weights must be a bounded array')
        for item in explicit:
            if not isinstance(item, dict) or not set(item) <= {'comparison', 'level', 'match_weight'}:
                raise InvalidDecision('Unexpected comparison match weight fields')
            name, level = item.get('comparison'), item.get('level')
            if not isinstance(name, str) or not COMPARISON.fullmatch(name):
                raise InvalidDecision('Invalid comparison name')
            if level is not None and (not isinstance(level, str) or not LEVEL.fullmatch(level)):
                raise InvalidDecision('Invalid comparison level label')
            comparisons.append({'comparison': name, 'level': level,
                                'match_weight': finite(item.get('match_weight'), 'Comparison match weight')})
    else:
        for key in sorted(row):
            match = NATIVE_BF.fullmatch(key)
            if not match or row[key] is None:
                continue
            name = match.group(1)
            factor = finite(row[key], 'Bayes factor', low=0)
            adjust = row.get('bf_tf_adj_' + name)
            factor *= 1.0 if adjust is None else finite(adjust, 'Term-frequency adjustment', low=0)
            if factor <= 0:
                raise InvalidDecision('Bayes factor must be positive')
            gamma = row.get('gamma_' + name)
            if gamma is not None and (isinstance(gamma, bool) or not isinstance(gamma, int)):
                raise InvalidDecision('Comparison level index must be an integer')
            level = None if gamma is None else 'Missing' if gamma == -1 else \
                'Disagree' if gamma == 0 else f'Level {gamma}'
            comparisons.append({'comparison': name, 'level': level,
                                'match_weight': round(math.log2(factor), 6)})
    if len({c['comparison'] for c in comparisons}) != len(comparisons):
        raise InvalidDecision('Duplicate comparison in match weights')
    rule = row.get('decision_rule', row.get('decision_reason'))
    if rule is not None and (not isinstance(rule, str) or not RULE.fullmatch(rule)):
        raise InvalidDecision('Routing rule must be an upper-case code')
    return {'model': 'splink', 'match_probability': probability, 'match_weight': weight,
            'comparisons': comparisons, 'routing_rule': rule}


def pair_evidence(row: dict, *, source_run_id: str) -> PairEvidence:
    """Convert a match_decisions row from the pinned release, never infer flags."""
    if row.get('resolution_run_id') != source_run_id or not source_run_id:
        raise InvalidDecision('Pair evidence belongs to a different source run')
    left, right = row.get('left_record_key'), row.get('right_record_key')
    if not isinstance(left, str) or not isinstance(right, str) or not left or not left < right:
        raise InvalidDecision('Candidate pair must have canonical distinct record keys')
    if row.get('decision') not in ('AUTO_MATCH', 'REVIEW', 'REJECT'):
        raise InvalidDecision('Unknown pipeline decision')
    if not isinstance(row.get('resolver_version'), str) or not row['resolver_version']:
        raise InvalidDecision('Resolver version is required')

    features = []
    for key in FLAGS + SIMILARITIES:
        if key not in row:
            raise InvalidDecision('Required pair evidence is missing')
        value = row[key]
        if value is not None:
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise InvalidDecision('Pair evidence must be finite numeric values')
            if key in FLAGS and value not in (0, 1):
                raise InvalidDecision('Pair evidence flag must be zero or one')
        features.append((key, value))
    # Unknown DOB provenance is not silently converted into a safe zero.
    if any(row[key] is None for key in DOB_FLAGS):
        raise InvalidDecision('DOB provenance flags are required')
    if row['valid_dob_contradiction'] and any(row[key] for key in
            ('left_dob_is_default', 'right_dob_is_default', 'valid_dob_exact')):
        raise InvalidDecision('Contradictory DOB evidence flags')
    score = row.get('evidence_score')
    if isinstance(score, bool) or not isinstance(score, (int, float)) or not math.isfinite(score):
        raise InvalidDecision('Finite pipeline evidence score is required')

    def texts(key):
        value = row.get(key)
        if not isinstance(value, (list, tuple)) or any(not isinstance(v, str) for v in value):
            raise InvalidDecision('Evidence reasons and routes must be text arrays')
        return tuple(value)

    return PairEvidence(left, right, source_run_id, row['resolver_version'],
        row['decision'], float(score), tuple(features), texts('candidate_routes'),
        texts('evidence_reasons'), texts('safeguards_triggered'),
        optional_text(row.get('decision_explanation')), model_evidence(row))


def optional_text(value):
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if not isinstance(value, str):
        raise InvalidDecision('Source identity field must be text or a date')
    return value if value.strip() else None


def source_record(row: dict) -> IdentityRecord:
    """Keep personal and workplace fields separate; do not infer identity."""
    forbidden = {'truth_id', 'person_id', 'scenario_id', 'expected_outcome',
                 'expected_decision', 'corruption_label'}
    if forbidden.intersection(row):
        raise InvalidDecision('Non-operational metadata is not allowed in source input')
    ids = {key: optional_text(row.get(key)) for key in
           ('record_key', 'source_system', 'source_record_id')}
    if not all(ids.values()):
        raise InvalidDecision('Source identifiers are required')

    def address(prefix):
        parts = [optional_text(row.get(prefix + field)) for field in
                 ('_address_line1', '_suburb', '_state', '_postcode', '_country')]
        return ', '.join(part for part in parts if part) or None

    return IdentityRecord(**ids,
        name=optional_text(row.get('full_name')),
        given_name=optional_text(row.get('given_name')),
        family_name=optional_text(row.get('family_name')),
        email=optional_text(row.get('personal_email')),
        phone=optional_text(row.get('personal_phone')),
        date_of_birth=optional_text(row.get('date_of_birth')),
        address=address('home'), work_address=address('work'),
        work_email=optional_text(row.get('work_email')),
        work_phone=optional_text(row.get('work_phone')),
        company_name=optional_text(row.get('company_name')),
        source_updated_at=optional_text(row.get('source_updated_at')))
