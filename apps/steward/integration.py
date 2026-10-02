"""Offline preparation of complete, truth-blind review cases from one release."""
from dataclasses import asdict, dataclass
from collections import defaultdict
import hashlib

from domain import ReviewCase, InvalidDecision, digest, fingerprint_payload
from source_adapter import source_record, pair_evidence


@dataclass(frozen=True)
class PreparedImport:
    source_run_id: str
    cases: tuple[ReviewCase, ...]
    oversized_components: tuple[tuple[str, ...], ...]
    source_count: int
    customer_count: int
    decision_count: int
    review_pair_count: int
    # REVIEW pairs whose endpoints started in different components; their
    # components were joined into one case. Unscoped pairs touch no quarantined
    # component and remain resolver candidates, reported rather than imported.
    cross_component_review_pairs: int = 0
    unscoped_review_pairs: int = 0

    @property
    def fingerprint(self):
        # Zero-valued linkage counters are omitted so releases without
        # cross-component pairs keep their original, already-imported fingerprint.
        payload = asdict(self)
        for key in ('cross_component_review_pairs', 'unscoped_review_pairs'):
            if not payload[key]:
                payload.pop(key)
        # ReviewCase gained golden_fields and PairEvidence gained model_evidence
        # after the first release was imported; omit empty ones like ReviewCase does.
        for case in payload['cases']:
            fingerprint_payload(case)
        return digest(payload)

    def summary(self):
        return dict(source_run_id=self.source_run_id, cases=len(self.cases),
            oversized_components=len(self.oversized_components),
            excluded_records=sum(map(len, self.oversized_components)),
            source_records=self.source_count, pipeline_customer_groups=self.customer_count,
            candidate_pairs=self.decision_count, review_pairs=self.review_pair_count,
            cross_component_review_pairs=self.cross_component_review_pairs,
            unscoped_review_pairs=self.unscoped_review_pairs,
            fingerprint=self.fingerprint)


def prepare_import(*, source_run_id, sources, mappings, decisions,
                   max_case_records=50, max_case_pairs=1225):
    """Build cases from resolver-quarantined provisional components.

    REVIEW is a pair decision, not a transitive membership relation. Joining all
    REVIEW edges creates giant, operationally meaningless components. The
    resolver's explicit ``cluster_requires_review`` flag selects work; current
    memberships and its bounded provisional component define complete case scope.
    A REVIEW pair from a quarantined component into another component joins
    both components into one case (one hop per pair, size limits still apply).
    """
    if not source_run_id or max_case_records < 2 or max_case_pairs < 1:
        raise InvalidDecision('Run and valid component limits required')
    records = {}
    for row in sources:
        record = source_record(row)
        if record.record_key in records:
            raise InvalidDecision('Duplicate source record')
        records[record.record_key] = record
    parent = {key:key for key in records}

    def find(key):
        while parent[key] != key:
            parent[key] = parent[parent[key]]
            key = parent[key]
        return key

    def union(a, b):
        a, b = find(a), find(b)
        if a != b:
            parent[max(a,b)] = min(a,b)

    memberships, provisional, customer_for, seeds = {}, {}, {}, set()
    for row in mappings:
        key = row.get('record_key')
        if row.get('resolution_run_id') != source_run_id:
            raise InvalidDecision('Mapping belongs to a different source run')
        if key not in records or key in customer_for:
            raise InvalidDecision('Mapping record missing or duplicated')
        customer = row.get('customer_id')
        if not isinstance(customer, str) or not customer:
            raise InvalidDecision('Customer membership is required')
        if not isinstance(row.get('cluster_requires_review'), bool):
            raise InvalidDecision('Quarantine status is required')
        customer_for[key] = customer
        union(key, memberships.setdefault(customer, key))
        component = row.get('provisional_customer_id')
        if component is not None:
            if not isinstance(component, str) or not component:
                raise InvalidDecision('Invalid provisional component ID')
            union(key, provisional.setdefault(component, key))
        if row['cluster_requires_review']:
            if component is None:
                raise InvalidDecision('Quarantined component ID is required')
            seeds.add(key)
    if set(customer_for) != set(records):
        raise InvalidDecision('Source and mapping coverage must agree exactly')

    # Mapping quarantine is the resolver's operational contract. A REVIEW pair
    # that crosses from a quarantined component into another component is an
    # ambiguous identity question spanning both, so both join one case. This is
    # one hop per pair, never a transitive closure over unquarantined REVIEW
    # edges; the size limits below still fail closed on any oversized result.
    seeds_roots = {find(key) for key in seeds}
    seen = set()
    crossing = []
    unscoped = 0
    # Hash only the operational evidence to detect changed repeat reads.
    scan_hash = hashlib.sha256()
    for row in decisions():
        pair = pair_evidence(row, source_run_id=source_run_id)
        keys = (pair.left_record_key, pair.right_record_key)
        if keys in seen or any(key not in records for key in keys):
            raise InvalidDecision('Duplicate pair or missing source context')
        seen.add(keys)
        scan_hash.update(digest(asdict(pair)).encode())
        if pair.decision != 'REVIEW':
            continue
        left, right = find(keys[0]), find(keys[1])
        if left == right:
            if left not in seeds_roots:
                unscoped += 1
        elif left in seeds_roots or right in seeds_roots:
            crossing.append(keys)
        else:
            unscoped += 1
    first_hash = scan_hash.hexdigest()
    count = len(seen)
    del seen
    for left, right in crossing:
        union(left, right)
    wanted = {find(key) for key in seeds}

    components = defaultdict(list)
    for key in sorted(records):
        root = find(key)
        if root in wanted:
            components[root].append(key)
    oversized = {root for root, keys in components.items() if len(keys) > max_case_records}
    evidence = defaultdict(list)
    scan_hash = hashlib.sha256()
    second_count = 0
    review_count = 0
    for row in decisions():
        pair = pair_evidence(row, source_run_id=source_run_id)
        scan_hash.update(digest(asdict(pair)).encode())
        second_count += 1
        left, right = find(pair.left_record_key), find(pair.right_record_key)
        if pair.decision == 'REVIEW' and left == right and left in wanted:
            review_count += 1
        if left == right and left in wanted and left not in oversized:
            evidence[left].append(pair)
            if len(evidence[left]) > max_case_pairs:
                oversized.add(left)
                del evidence[left]
    if second_count != count or scan_hash.hexdigest() != first_hash:
        raise InvalidDecision('Decision snapshot changed between reads')

    cases = []
    for root, keys in sorted(components.items()):
        if root in oversized:
            continue
        groups = defaultdict(list)
        for key in keys:
            groups[customer_for[key]].append(key)
        pairs = tuple(sorted(evidence[root], key=lambda p:(p.left_record_key,p.right_record_key)))
        conflicts = tuple((p.left_record_key,p.right_record_key) for p in pairs
                          if dict(p.features)['valid_dob_contradiction'] == 1)
        case_id = 'review-' + digest({'run':source_run_id, 'records':keys})[:32]
        cases.append(ReviewCase(case_id, source_run_id, 1,
            tuple(records[k] for k in keys), tuple(sorted(tuple(g) for g in groups.values())),
            conflicts, pairs))
    return PreparedImport(source_run_id, tuple(cases),
        tuple(tuple(components[root]) for root in sorted(oversized)),
        len(records), len(memberships), count, review_count,
        len(crossing), unscoped)
