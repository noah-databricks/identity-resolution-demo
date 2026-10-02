"""Deterministic constrained clustering and persistent identity reconciliation.

Bounded in-memory graph for the 150k-record demo, run on a serverless job driver.
The caller enforces explicit size limits; no claim of unbounded distributed scale.
"""

import hashlib
from collections import Counter, defaultdict


def components(records, edges, forbidden=(), max_size=50, dob_compatible=None):
    """Greedy highest-score-first union with whole-component vetoes.

    ``records`` maps key -> DOB string or None. With ``dob_compatible`` (a function of
    two DOB strings), a merge is vetoed only when some DOB pair across the two sides is
    incompatible; without it, any two distinct DOBs veto (the v3 behaviour).
    """
    parent = {key: key for key in records}
    members = {key: {key} for key in records}
    dobs = {key: ({dob} if dob else set()) for key, dob in records.items()}
    denied = defaultdict(set)
    for a, b in forbidden:
        denied[a].add(b)
        denied[b].add(a)
    rejected = []

    def root(key):
        while parent[key] != key:
            parent[key] = parent[parent[key]]
            key = parent[key]
        return key

    for a, b, score in sorted(edges, key=lambda e: (-e[2], e[0], e[1])):
        ra, rb = root(a), root(b)
        if ra == rb:
            continue
        reason = None
        if dob_compatible is None and len(dobs[ra] | dobs[rb]) > 1:
            reason = "COMPONENT_DOB_CONFLICT"
        elif dob_compatible is not None and any(not dob_compatible(x, y)
                                                for x in dobs[ra] for y in dobs[rb] if x != y):
            reason = "COMPONENT_DOB_CONFLICT"
        elif len(members[ra]) + len(members[rb]) > max_size:
            reason = "COMPONENT_SIZE_LIMIT"
        elif any(denied[key] & members[rb] for key in members[ra]):
            reason = "COMPONENT_CANNOT_LINK"
        if reason:
            rejected.append((a, b, reason))
            continue
        if ra > rb:
            ra, rb = rb, ra
        parent[rb] = ra
        members[ra].update(members.pop(rb))
        dobs[ra].update(dobs.pop(rb))
    return {key: root(key) for key in records}, rejected


def reconcile(cluster_by_record, previous, created, run_id):
    """An old ID claims only its largest continuing fragment; oldest wins merges.

`previous` maps records to active IDs. `created` maps IDs to sortable creation
times. Aliases are issued only for IDs whose entire surviving claim is merged;
an ID that survives a split elsewhere must never be aliased away.
"""
    overlaps = defaultdict(Counter)
    grouped = defaultdict(list)
    for record, component in cluster_by_record.items():
        grouped[component].append(record)
        if record in previous:
            overlaps[previous[record]][component] += 1
    claim = {old: min(counts, key=lambda c: (-counts[c], c))
             for old, counts in overlaps.items()}
    claimants = defaultdict(list)
    for old, component in claim.items():
        claimants[component].append(old)
    identity_by_component, aliases = {}, []
    for component, records in sorted(grouped.items()):
        ids = claimants[component]
        if ids:
            surviving = min(ids, key=lambda old: (created.get(old, ""), old))
            aliases.extend((old, surviving) for old in ids if old != surviving)
        else:
            material = run_id + "|" + "|".join(sorted(records))
            surviving = "cus_" + hashlib.sha256(material.encode()).hexdigest()[:28]
        identity_by_component[component] = surviving
    return ({record: identity_by_component[component]
             for record, component in cluster_by_record.items()}, aliases)
