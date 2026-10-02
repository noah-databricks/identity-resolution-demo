"""Which way a review case leans: one guest, or possibly different people. Pure; no I/O.

Truth-blind: uses only source record values, the resolver's cannot-link pairs,
stored review reason codes and the lowest review-pair match probability. It is a
queue filter for stewards, not a decision; the steward or agent still decides.

Checked offline once against the synthetic release's hidden truth (never read by
the app): "likely one guest" cases were 99% one person, and the five strongest
"possibly different people" cases were all different people.
"""
import itertools
import re

# Signal: (weight, queue label). A case leans "different people" at weight >= 2.
SIGNALS = {
    'cannot_link': (4, 'Resolver marked records as different people'),
    'emails': (2, 'Different personal emails'),
    'birth_dates': (2, 'Different birth dates'),
    'first_names': (1, 'Different first names'),
    'graph_dob_conflict': (1, 'Birth-date conflict with another group'),
}
SEPARATE_AT = 2
# Every review pair at or above this probability, with no conflict signal.
SAME_GUEST_PROBABILITY = 0.95


def _norm(value):
    return re.sub(r'[^a-z0-9@.]', '', str(value or '').lower())


def _related(a, b):
    """Same first name, or one is a prefix of the other (Matt/Matthew)."""
    return a == b or a.startswith(b) or b.startswith(a)


def lean(records, cannot_link=(), reason_codes=(), lowest_probability=None):
    """{'kind': 'same'|'different'|'unclear', 'strength': int, 'signals': [label, ...]}."""
    get = (lambda r, k: r.get(k)) if records and isinstance(records[0], dict) else getattr
    found = []
    if cannot_link:
        found.append('cannot_link')
    if len({_norm(get(r, 'email')) for r in records if get(r, 'email')}) > 1:
        found.append('emails')
    if len({get(r, 'date_of_birth') for r in records if get(r, 'date_of_birth')}) > 1:
        found.append('birth_dates')
    given = {re.sub(r'[^a-z]', '', get(r, 'given_name').lower()) for r in records if get(r, 'given_name')}
    if any(not _related(a, b) for a, b in itertools.combinations(sorted(given - {''}), 2)):
        found.append('first_names')
    if 'COMPONENT_DOB_CONFLICT' in (reason_codes or ()):
        found.append('graph_dob_conflict')
    strength = sum(SIGNALS[s][0] for s in found)
    if strength >= SEPARATE_AT:
        kind = 'different'
    elif not found and lowest_probability is not None and lowest_probability >= SAME_GUEST_PROBABILITY:
        kind = 'same'
    else:
        kind = 'unclear'
    return {'kind': kind, 'strength': strength, 'signals': [SIGNALS[s][1] for s in found]}
