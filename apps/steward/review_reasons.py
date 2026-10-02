"""Why a case is in review, and readable Splink comparison levels. Pure; no I/O.

Everything here is a fixed vocabulary over validated codes and numeric flags, so
it is safe to show while PII masking is on: no record value is ever quoted.

Two sources explain a case:
* REVIEW pairs inside the case: the resolver's routing rule plus the pair's flags.
* Graph checks (``graph_links``): an AUTO_MATCH link the resolver's clustering
  withheld (component_quarantine) because joining the two groups would break a
  constraint. Those links cross into another component, so they are never pair
  evidence of the case; ``import_review_context.py`` reads them read-only from
  the release's frozen quarantine table.
"""
import re

# Splink comparison levels per model version. Splink numbers the levels of a
# comparison from the top: the null level is -1, the first non-null level is
# n-1, ..., the ELSE level is 0. Labels follow resolution/splink_v4/model.py
# (label_for_charts) in the resolver release; ``detail`` keeps Splink's label.
_EXACT_ONLY = {1: ('Exact', 'Exact'), 0: ('Different', 'Disagree'), -1: ('Missing', 'Missing')}
_FUZZY = {3: ('Exact', 'Exact'),
          2: ('Close spelling (90%+ similar)', 'Normalised edit similarity >= 0.9'),
          1: ('Similar spelling (75%+ similar)', 'Normalised edit similarity >= 0.75'),
          0: ('Different', 'Disagree'), -1: ('Missing', 'Missing')}
COMPARISON_LEVELS = {
    'splink-v4.0': {'name_n': _FUZZY, 'home_address_n': _FUZZY, 'personal_email_n': _EXACT_ONLY,
                    'personal_phone_n': _EXACT_ONLY, 'dob_usable': _EXACT_ONLY},
}
_STORED_LEVEL = re.compile(r'Level (\d{1,2})')


def level_label(model_version, comparison, stored):
    """(label, splink_label) for a stored level, or None when the model is unknown.

    Imports store gamma levels as 'Missing' (-1), 'Disagree' (0) or 'Level n'.
    Explicit labels from ``comparison_match_weights`` are left as they are.
    """
    levels = COMPARISON_LEVELS.get(model_version, {}).get(comparison)
    if not levels or stored is None:
        return None
    match = _STORED_LEVEL.fullmatch(stored)
    gamma = int(match.group(1)) if match else {'Missing': -1, 'Disagree': 0}.get(stored)
    return levels.get(gamma)


def label_pairs(pairs, model_version):
    """Add level_label / level_detail to public pair dicts in place."""
    for pair in pairs:
        for comparison in (pair.get('model_evidence') or {}).get('comparisons') or ():
            found = level_label(model_version, comparison['comparison'], comparison.get('level'))
            if found:
                comparison['level_label'], comparison['level_detail'] = found
    return pairs


GRAPH = {
    'COMPONENT_DOB_CONFLICT': ('Birth dates conflict', 'A likely link was blocked: birth dates would conflict',
        'the combined guest would hold birth dates that cannot belong to one person'),
    'COMPONENT_CANNOT_LINK': ('Cannot-link conflict', 'A likely link was blocked by a cannot-link rule',
        'the combined guest would contain two records the resolver marked as different people '
        '(a genuine birth-date conflict, or a contact shared by different people)'),
    'COMPONENT_SIZE_LIMIT': ('Group size limit', 'A likely link was blocked by the group size limit',
        'the combined guest would exceed the resolver\'s record limit for one guest'),
}
REVIEW_RULES = {
    'INSUFFICIENT_EVIDENCE_REVIEW': ('No personal contact', 'Probable match without independent personal evidence'),
    'PERSONAL_CONTACT_NAME_CONFLICT': ('Contact shared, name differs', 'A personal contact is shared but the names conflict'),
    'CONFLICTING_PERSONAL_CONTACTS_REVIEW': ('Contacts conflict', 'Same name and address, conflicting personal contacts'),
    'AMBIGUOUS_COMMON_NAME_ADDRESS': ('Common name', 'Common name at a shared address, no personal contact'),
    'SEMANTIC_CANDIDATE_REVIEW': ('Semantic match only', 'Similar name and address found by semantic search only'),
}
NOTES = {
    'DOB_ENTRY_VARIANT': 'Birth dates differ in a way that looks like a data-entry slip (day/month swap or one digit).',
    'IGNORE_SOURCE_DEFAULT_DOB': 'A source default birth date was ignored.',
    'SHARED_CONTEXT_NOT_IDENTITY': 'Some records share only a household or work contact, which is not evidence of the same person.',
}
PRIORITY = ('COMPONENT_DOB_CONFLICT', 'COMPONENT_CANNOT_LINK', 'COMPONENT_SIZE_LIMIT')


def _percent(p):
    if p is None:
        return 'unscored'
    if 0.999 < p < 1:
        return '>99.9%'
    return f'{p * 100:.1f}%'.replace('.0%', '%')


def _range(probabilities):
    low, high = min(probabilities), max(probabilities)
    return _percent(high) if _percent(low) == _percent(high) else f'{_percent(low)} to {_percent(high)}'


def _flags(pair):
    return {k: v for k, v in pair.get('features') or () if v is not None}


def _comparisons(pair):
    return {c['comparison']: c.get('level') for c in (pair.get('model_evidence') or {}).get('comparisons') or ()}


GAPS = {
    'NAME_NOT_EXACT': ('Name not exact', 'names are not identical'),
    'NAME_DIFFERS': ('Name differs', 'full names differ'),
    'DOB_CONFLICT': ('Birth dates conflict', 'birth dates conflict'),
    'DOB_DEFAULT': ('Default birth date', 'a birth date is a source default'),
    'DOB_MISSING': ('Birth date missing', 'a birth date is missing'),
    'DOB_DIFFERS': ('Birth dates differ', 'birth dates differ, though not a clear conflict'),
    'DOB_UNCONFIRMED': ('Birth date unconfirmed', 'birth date not confirmed'),
    'CONTACT_NOT_PERSONAL': ('Contact used by others',
                             'the matching email or phone is also used by other people, so it is not personal'),
    'NO_PERSONAL_CONTACT': ('No personal contact', 'no shared personal email or phone'),
    'EMAIL_DIFFERS': ('Emails differ', 'personal emails differ'),
    'PHONE_DIFFERS': ('Phones differ', 'personal phones differ'),
}


def pair_facts(pair):
    """(agrees, gaps): plain-language agreements and gap codes for one pair, from flags only."""
    f, levels = _flags(pair), _comparisons(pair)
    agrees, gaps = [], []
    if f.get('name_exact'):
        agrees.append('exact name')
    elif (f.get('name_similarity') or 0) >= 0.9:
        agrees.append('close name spelling')
        gaps.append('NAME_NOT_EXACT')
    elif f.get('given_name_exact') or f.get('family_name_exact'):
        agrees.append('given name' if f.get('given_name_exact') else 'family name')
        gaps.append('NAME_DIFFERS')
    if f.get('valid_dob_exact'):
        agrees.append('birth date')
    elif f.get('valid_dob_contradiction'):
        gaps.append('DOB_CONFLICT')
    elif f.get('left_dob_is_default') or f.get('right_dob_is_default'):
        gaps.append('DOB_DEFAULT')
    elif levels.get('dob_usable') == 'Missing':
        gaps.append('DOB_MISSING')
    elif levels.get('dob_usable') is not None:
        gaps.append('DOB_DIFFERS')
    else:
        gaps.append('DOB_UNCONFIRMED')
    if f.get('home_address_exact'):
        agrees.append('home address')
    elif (f.get('address_similarity') or 0) >= 0.9:
        agrees.append('similar home address')
    elif f.get('home_postcode_exact'):
        agrees.append('postcode')
    if f.get('rare_email_exact') or f.get('rare_phone_exact'):
        agrees.append('personal email' if f.get('rare_email_exact') else 'personal phone')
    elif f.get('shared_email_exact') or f.get('shared_phone_exact'):
        gaps.append('CONTACT_NOT_PERSONAL')
    else:
        gaps.append('NO_PERSONAL_CONTACT')
    if f.get('personal_email_disagrees'):
        gaps.append('EMAIL_DIFFERS')
    if f.get('personal_phone_disagrees'):
        gaps.append('PHONE_DIFFERS')
    return agrees, gaps


def _join(items):
    return items[0] if len(items) == 1 else ', '.join(items[:-1]) + ' and ' + items[-1]


def derive(case, graph_links=()):
    """Reasons for one case, most decisive first.

    ``case`` is the public case dict (records, current_groups, pair_evidence);
    ``graph_links`` are rows of {record_key, other_record_key, other_case_id,
    other_deferred, reason, match_probability}.
    """
    reasons = []
    for code in PRIORITY:
        links = [g for g in graph_links if g.get('reason') == code]
        if not links:
            continue
        tag, title, why = GRAPH[code]
        own = case.get('case_id')
        others = sorted({g['other_case_id'] for g in links if g.get('other_case_id') not in (None, own)})
        deferred = any(g.get('other_deferred') for g in links)
        inside = any(g.get('other_case_id') == own for g in links) and own is not None
        where = ('another guest group in this case' if inside and not others and not deferred else
                 'another case in this queue' if len(others) == 1 and not deferred else
                 f'{len(others)} other cases in this queue' if others and not deferred else
                 'a case held outside the queue (too large to import)' if not others else
                 'other cases, one held outside the queue')
        n = len(links)
        detail = (f'The resolver scored {n} link{"s" if n != 1 else ""} from this case to {where} as a match '
                  f'({_range([g.get("match_probability") or 0 for g in links])}), but kept '
                  f'{"them" if n != 1 else "it"} apart because {why}. Decide which records belong together'
                  + ('.' if inside and not others and not deferred else '; the other side is reviewed separately.'))
        reasons.append({'code': code, 'tag': tag, 'title': title, 'detail': detail,
                        'records': sorted({g['record_key'] for g in links}),
                        'other_cases': others, 'other_deferred': deferred})
    review = [p for p in case.get('pair_evidence') or () if p.get('decision') == 'REVIEW']
    by_rule = {}
    for pair in review:
        rule = (pair.get('model_evidence') or {}).get('routing_rule') or 'REVIEW'
        by_rule.setdefault(rule, []).append(pair)
    for rule, pairs in sorted(by_rule.items(), key=lambda item: -len(item[1])):
        probabilities = [(p.get('model_evidence') or {}).get('match_probability', p.get('evidence_score')) for p in pairs]
        strongest = pairs[max(range(len(pairs)), key=lambda i: probabilities[i] or 0)]
        tag, title = REVIEW_RULES.get(rule, (None, None))
        title = title or rule.replace('_', ' ').capitalize()
        agrees, gaps = pair_facts(strongest)
        n = len(pairs)
        lead = (f'{n} pair{"s" if n != 1 else ""} scored {_range([p or 0 for p in probabilities])} '
                'match probability')
        if rule == 'INSUFFICIENT_EVIDENCE_REVIEW':
            detail = (f'{lead}, but a score alone does not merge guests. The resolver merges automatically only '
                      'with a shared personal email or phone, or an exact name with a compatible birth date.')
        else:
            detail = f'{lead}; the resolver routed {"them" if n != 1 else "it"} to a steward.'
        facts = []
        if agrees:
            facts.append(('Agrees' if n == 1 else 'Strongest pair agrees') + ' on ' + _join(agrees) + '.')
        if gaps:
            facts.append('Not established: ' + '; '.join(GAPS[g][1] for g in gaps) + '.')
        reasons.append({'code': rule, 'tag': tag or 'Resolver review', 'title': title,
                        'detail': detail, 'facts': facts, 'gaps': gaps,
                        'records': [strongest['left_record_key'], strongest['right_record_key']],
                        'pairs': n})
    notes = []
    safeguards = {s for p in review for s in p.get('safeguards_triggered') or ()}
    flags = [_flags(p) for p in review]
    if any(f.get('left_dob_is_default') or f.get('right_dob_is_default') for f in flags):
        safeguards.add('IGNORE_SOURCE_DEFAULT_DOB')
    notes += [NOTES[code] for code in NOTES if code in safeguards]
    groups = len(case.get('current_groups') or ())
    if groups > 1:
        notes.append(f'The resolver currently holds these records as {groups} separate guests.')
    if not reasons:
        reasons.append({'code': 'UNSPECIFIED', 'tag': 'Resolver review', 'title': 'Flagged by the resolver for review',
                        'detail': 'The release marks this group for review but supplies no review pair or graph check '
                                  'for it.', 'records': []})
    return {'reasons': reasons, 'notes': notes}


def reason_codes(case, graph_links=()):
    """Stored per case so the queue can tag cases without reading pair evidence:
    reason codes, most decisive first, then the strongest review pair's gaps."""
    reasons = derive(case, graph_links)['reasons']
    return [r['code'] for r in reasons] + [g for r in reasons for g in r.get('gaps', ())]


def tags(codes):
    """Short queue tags for stored codes: the main reason plus one specific gap."""
    out = []
    for code in codes or ():
        tag = (GRAPH.get(code, (None,))[0] or REVIEW_RULES.get(code, (None,))[0]
               or GAPS.get(code, (None,))[0] or 'Resolver review')
        if tag not in out:
            out.append(tag)
    return out[:2]
