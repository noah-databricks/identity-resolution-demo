// Probabilistic resolver evidence (Splink): per-comparison match weights and the
// safety rule that routed a pair. Pure string rendering so it can be unit tested.
const RULES = {
  CONFLICTING_PERSONAL_CONTACTS_REVIEW: 'Same name and address, conflicting personal contacts',
  AMBIGUOUS_COMMON_NAME_ADDRESS: 'Common name at a shared address, no personal contact',
  INSUFFICIENT_EVIDENCE_REVIEW: 'Probable match without independent personal evidence',
  SEMANTIC_CANDIDATE_REVIEW: 'Similar name and address found by semantic search only',
  VALID_DOB_CONFLICT: 'Valid birth dates differ',
  SHARED_CONTEXT_NOT_IDENTITY: 'Shared household or work contact only',
  LOW_MATCH_PROBABILITY: 'Match probability below the review threshold',
  PERSONAL_CONTACT_AND_DOB: 'Personal contact and birth date agree',
  PERSONAL_CONTACT_AND_ADDRESS: 'Personal contact and address agree',
  NAME_DOB_ADDRESS_CORROBORATION: 'Name, birth date and address agree',
  SOURCE_DEFAULT_DOB_IGNORED: 'Source default birth date ignored',
  MULTILINGUAL_ADDRESS_AND_DOB: 'Transliterated name with matching address and birth date',
  INDEPENDENT_PERSONAL_EVIDENCE: 'High probability with independent personal evidence',
  NAME_ADDRESS_CORROBORATION: 'High probability with matching name and address',
};
const COMPARISONS = {
  name_n: 'Name', full_name: 'Name', given_name_n: 'Given name', family_name_n: 'Family name',
  personal_email_n: 'Personal email', personal_phone_n: 'Personal phone', dob_usable: 'Date of birth',
  home_address_n: 'Home address', postcode_n: 'Postcode', work_email_n: 'Work email', work_phone_n: 'Work phone',
};
const VERBS = {REVIEW: 'Routed to review by', AUTO_MATCH: 'Auto-matched by', REJECT: 'Rejected by'};
const LOCALE = 'en-AU';

export function sentence(code) {
  const text = String(code ?? '').replace(/_n$/, '').replaceAll('_', ' ').toLowerCase().trim();
  return text ? text[0].toUpperCase() + text.slice(1) : '';
}
export const ruleTitle = code => RULES[code] || sentence(code);
export const comparisonTitle = name => COMPARISONS[name] || sentence(name);
export function signed(value) {
  const text = Math.abs(value).toLocaleString(LOCALE, {minimumFractionDigits: 2, maximumFractionDigits: 2});
  return value > 0 ? `+${text}` : value < 0 ? `−${text}` : text;
}
export function percent(probability) {
  if (probability > 0 && probability < 0.001) return '<0.1%';
  if (probability < 1 && probability > 0.999) return '>99.9%';
  return (probability * 100).toLocaleString(LOCALE, {maximumFractionDigits: 1}) + '%';
}
// One scale per case so bars are comparable across its pairs.
export function weightScale(pairs) {
  const weights = pairs.flatMap(p => p.model_evidence?.comparisons || []).map(c => Math.abs(c.match_weight));
  return Math.max(1, ...weights);
}
// Queue score: Splink cases carry the lowest REVIEW-pair match probability, shown as a
// percentage consistent with the evidence pane; rule-based scores keep their decimal form.
export function queueScore(item, decimal) {
  const value = item.pipeline_confidence;
  if (value === null || value === undefined) return {text: 'No score', title: 'No review pair score'};
  return item.confidence_kind === 'match_probability'
    ? {text: `Match ${percent(value)}`, title: 'Lowest match probability among this case’s review pairs'}
    : {text: `Score ${decimal(value)}`, title: 'Lowest evidence score among this case’s review pairs'};
}
export function pairBadge(pair) {
  return pair.model_evidence ? ` · ${percent(pair.model_evidence.match_probability)} match probability` : '';
}

export function modelEvidenceMarkup(pair, esc, scale = weightScale([pair])) {
  const model = pair.model_evidence;
  if (!model) return '';
  const comparisons = model.comparisons || [];
  const sum = comparisons.reduce((total, c) => total + c.match_weight, 0);
  const prior = model.match_weight == null ? null : model.match_weight - sum;
  const rows = comparisons.map(c => {
    const width = Math.min(50, Math.abs(c.match_weight) / scale * 50).toFixed(1);
    const sign = c.match_weight > 0 ? 'for' : c.match_weight < 0 ? 'against' : 'neutral';
    return `<tr class="${sign}"><th scope="row">${esc(comparisonTitle(c.comparison))}</th>`
      + (c.level_label
        ? `<td class="level" title="${esc(`Splink level: ${c.level_detail}`)}">${esc(c.level_label)}</td>`
        : `<td class="level">${esc(c.level ?? 'Level not supplied')}</td>`)
      + `<td class="bar" aria-hidden="true"><span style="--w:${width}%"></span></td>`
      + `<td class="num">${signed(c.match_weight)}</td></tr>`;
  }).join('');
  const rule = model.routing_rule
    ? `<p class="routing"><span>${esc(VERBS[pair.decision] || 'Decided by')}</span> <strong>${esc(ruleTitle(model.routing_rule))}</strong><code>${esc(model.routing_rule)}</code></p>`
    : '<p class="routing muted">Routing rule not supplied</p>';
  return `<div class="model-evidence">${rule}`
    + `<dl class="model-stats"><div><dt>Match probability</dt><dd>${percent(model.match_probability)}</dd></div>`
    + (model.match_weight == null ? '' : `<div><dt>Match weight</dt><dd>${signed(model.match_weight)}</dd></div>`)
    + `</dl>`
    + (rows ? `<table class="weights"><caption>Match weight by comparison <small>Positive supports the same guest, negative opposes it</small></caption>`
      + `<thead><tr><th scope="col">Comparison</th><th scope="col">Level</th><th scope="col"><span class="sr-only">Contribution</span></th><th scope="col" class="num">Weight</th></tr></thead>`
      + `<tbody>${rows}</tbody>`
      + (prior == null ? '' : `<tfoot><tr><th scope="row" colspan="3">Starting weight (prior)</th><td class="num">${signed(prior)}</td></tr></tfoot>`)
      + `</table>` : '<p class="muted">Per-comparison weights not supplied.</p>')
    + `</div>`;
}

// Rule-based pair features (the resolver's flags and similarities) in plain words.
export const FEATURES = {
  rare_email_exact: 'Personal email is the same', rare_phone_exact: 'Personal phone is the same',
  shared_email_exact: 'Same email, also used by other people', shared_phone_exact: 'Same phone, also used by other people',
  home_address_exact: 'Home address is the same', work_address_exact: 'Work address is the same',
  name_exact: 'Full name is the same', given_name_exact: 'Given name is the same', family_name_exact: 'Family name is the same',
  home_postcode_exact: 'Home postcode is the same', company_exact: 'Company is the same',
  multilingual_name_pair: 'Names are written in different scripts', valid_dob_exact: 'Birth date is the same',
  valid_dob_contradiction: 'Birth dates conflict', left_dob_is_default: 'Left birth date is a source default',
  right_dob_is_default: 'Right birth date is a source default', personal_email_disagrees: 'Personal emails differ',
  personal_phone_disagrees: 'Personal phones differ', name_similarity: 'Name spelling similarity',
  address_similarity: 'Address spelling similarity', name_ann_similarity: 'Name semantic similarity',
  address_ann_similarity: 'Address semantic similarity', name_ai_similarity: 'Name AI similarity',
  address_ai_similarity: 'Address AI similarity',
};
const SIMILARITY = /_similarity$/;
export const featureTitle = key => FEATURES[key] || sentence(key);
export function featureValue(key, value) {
  if (value === null || value === undefined) return 'Not evaluated';
  if (SIMILARITY.test(key)) return `${Math.round(value * 100)}%`;
  return value ? 'Yes' : 'No';
}

// The checks that decide a pair, one row per evidence area, only when they say
// something: agreements, contradictions, and the absence of a personal contact.
export function ruleChecks(pair, side = k => k) {
  const f = Object.fromEntries(pair.features.filter(([, v]) => v !== null && v !== undefined));
  const levels = Object.fromEntries((pair.model_evidence?.comparisons || []).map(c => [c.comparison, c.level_label || c.level]));
  const pct = v => `${Math.round(v * 100)}% similar`;
  const checks = [];
  const add = (check, result, tone) => checks.push({check, result, tone});
  for (const [kind, label] of [['email', 'Personal email'], ['phone', 'Personal phone']]) {
    if (f[`rare_${kind}_exact`]) add(label, 'Same, used only by these records', 'for');
    else if (f[`shared_${kind}_exact`]) add(label, 'Same, but also used by other people', 'info');
    else if (f[`personal_${kind}_disagrees`]) add(label, 'Different', 'against');
  }
  if (!checks.length && ('rare_email_exact' in f || 'rare_phone_exact' in f))
    add('Personal contact', 'No personal email or phone in common', 'against');
  if (f.name_exact) add('Name', 'Exact', 'for');
  else if (f.name_similarity != null) add('Name', [pct(f.name_similarity), f.given_name_exact && 'given name exact', f.family_name_exact && 'family name exact'].filter(Boolean).join(', '), f.name_similarity >= 0.9 ? 'for' : 'info');
  else if (f.given_name_exact || f.family_name_exact) add('Name', f.given_name_exact ? 'Given name exact' : 'Family name exact', 'info');
  if (f.multilingual_name_pair) add('Name script', 'Written in different scripts', 'info');
  if (f.valid_dob_exact) add('Birth date', 'Same', 'for');
  else if (f.valid_dob_contradiction) add('Birth date', 'Conflict', 'against');
  else if (f.left_dob_is_default || f.right_dob_is_default)
    add('Birth date', `Source default on ${[f.left_dob_is_default && side(pair.left_record_key), f.right_dob_is_default && side(pair.right_record_key)].filter(Boolean).join(' and ')}, ignored`, 'info');
  else if (levels.dob_usable === 'Missing') add('Birth date', 'Missing on at least one record', 'info');
  else if (levels.dob_usable) add('Birth date', 'Different', 'against');
  if (f.home_address_exact) add('Home address', 'Same', 'for');
  else if (f.address_similarity) add('Home address', pct(f.address_similarity) + (f.home_postcode_exact ? ', postcode same' : ''), f.address_similarity >= 0.9 ? 'for' : 'info');
  else if (f.home_postcode_exact) add('Home address', 'Postcode same', 'info');
  if (f.work_address_exact || f.company_exact)
    add('Workplace', [f.company_exact && 'Same company', f.work_address_exact && 'same work address'].filter(Boolean).join(', ') + ' (not personal evidence)', 'info');
  return checks;
}

export function ruleChecksMarkup(pair, esc, side = k => k) {
  const supplied = pair.features.filter(([, v]) => v !== null && v !== undefined);
  if (!supplied.length) return '';
  const checks = ruleChecks(pair, side);
  const list = checks.length
    ? `<table class="checks"><caption>Rule checks</caption><tbody>${checks.map(c => `<tr class="${c.tone}"><th scope="row">${esc(c.check)}</th><td>${esc(c.result)}</td></tr>`).join('')}</tbody></table>`
    : '<p class="caption">No rule check fired for this pair.</p>';
  const hidden = pair.features.length - supplied.length;
  return list + `<details class="all-features"><summary>All rule features · ${supplied.length}</summary><table class="features"><tbody>`
    + supplied.map(([key, value]) => `<tr><td>${esc(key === 'left_dob_is_default' || key === 'right_dob_is_default'
      ? `${side(key[0] === 'l' ? pair.left_record_key : pair.right_record_key)} birth date is a source default` : featureTitle(key))}</td><td>${esc(featureValue(key, value))}</td></tr>`).join('')
    + `</tbody></table>${hidden ? `<p class="caption">${hidden} rule-based feature${hidden === 1 ? '' : 's'} not supplied by this resolver.</p>` : ''}</details>`;
}

// "Why this needs review": server-derived reasons (review_reasons.py).
export function whyReviewMarkup(explain, esc, label) {
  if (!explain?.reasons?.length) return '';
  const chips = keys => (keys || []).length
    ? `<p class="why-records">${keys.length === 2 ? 'Pair' : 'Records'}: ${keys.map(k => esc(label(k))).join(keys.length === 2 ? ' and ' : ', ')}</p>` : '';
  const others = r => (r.other_cases || []).length
    ? `<p class="why-links">Linked case${r.other_cases.length === 1 ? '' : 's'}: ${r.other_cases.map(id => `<button type="button" class="link" data-open-case="${esc(id)}">${esc(id.replace(/^review-/, '').slice(0, 8))}</button>`).join(' ')}</p>` : '';
  const items = explain.reasons.map(r => `<li><div><strong>${esc(r.title)}</strong>`
    + `<p>${esc(r.detail)}</p>${(r.facts || []).map(f => `<p>${esc(f)}</p>`).join('')}${chips(r.records)}${others(r)}</div></li>`).join('');
  const notes = (explain.notes || []).map(n => `<li>${esc(n)}</li>`).join('');
  return `<section class="why-review" aria-labelledby="why-review-title"><h3 id="why-review-title">Why this is in review</h3><ul class="why-reasons">${items}</ul>`
    + (notes ? `<ul class="why-notes">${notes}</ul>` : '')
    + (explain.graph_checks_imported === false ? '<p class="caption">Graph checks for this release are not imported; only review pairs are explained.</p>' : '')
    + `</section>`;
}
