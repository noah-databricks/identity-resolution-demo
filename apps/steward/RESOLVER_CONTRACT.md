# Resolver integration boundary

Status: schemas verified read-only through Databricks MCP on 18 September 2026.
The pinned v2.3 release has since been imported: 211 quarantined components.
Release job run `306060156836139` reports SUCCESS (rechecked 18 September 2026).
This does not establish complete REVIEW-pair coverage or reviewed-Gold publication.

All names below are in `identity_resolution_demo`.

| Table | Operational use | Keys / context |
| --- | --- | --- |
| `resolution.federated_guest_records` | Original identity fields | `record_key`, source IDs, Unicode names, separate personal/work contacts and addresses, DOB, source timestamp |
| `resolution.candidate_pairs` | Candidate provenance | Canonical left/right record keys, routes, representations, scope, ANN ranks/distances |
| `resolution.pair_features` | Field-level evidence | Canonical pair, exact/fuzzy/AI similarities, shared contacts, default-DOB flags, valid-DOB conflict |
| `resolution.match_decisions` | Automated result and its evidence snapshot | `resolution_run_id`, resolver/policy versions, pair keys, flat field features, score, reasons, safeguards and explanation |
| `resolution.customer_mapping` | Existing membership and quarantine | Run/version, record key, customer and provisional customer IDs, review flag/reasons, mapping status |
| `gold.customer_master` | Published guest summary | Schema discovery pending |
| `gold.customer_source_link` | Published guest membership | Schema discovery pending |
| `gold.match_evidence` | Published evidence | Schema discovery pending |

## Import requirements

- Use explicit column allowlists, not `SELECT *` or arbitrary JSON passthrough.
- Never read `truth.*` or `evaluation.*` from the App, importer or agent.
- Import only an explicitly released run, not whichever materialized view is newest.
- Pin Delta versions/immutable references for sources and mapping alongside the run.
  Source records and pair_features currently have no run column. A run filter on
  decisions alone cannot guarantee a consistent source snapshot.
- Prefer the flat evidence already in match_decisions over a fresh pair_features
  join; preserve default-DOB flags and valid-DOB conflicts unchanged.
- Form cases from the full affected identity/component context, not a sampled pair
  alone. Include existing memberships and all relevant cannot-link constraints.
- Bound oversized components explicitly; do not silently truncate context and then
  permit a merge or auto decision.
- Preserve work and personal context in the domain contract. `source_adapter.py`
  now maps these separately, preserving Unicode, sparse values and timestamps.
  Pair features and default-DOB provenance are mapped by the same allowlisted
  adapter. `integration.py` now builds complete cases; `import_release.py` reads
  pinned operational snapshots and defaults to dry-run.
- Atomically version case snapshots in Lakebase. New evidence invalidates old
  proposals; it must not overwrite an applied stewardship decision silently.

## Probabilistic evidence columns (Splink releases)

A release whose `operational_review_evidence_snapshot` has a `match_probability`
column is imported with the `splink` evidence profile (see `import_release.preflight`).
Columns, all optional except `match_probability`:

| Column | Type | Use |
| --- | --- | --- |
| `match_probability` | DOUBLE in [0, 1] | Shown per pair and in the pair heading |
| `match_weight` | DOUBLE | Total match weight; the app shows `match_weight - sum(comparisons)` as the starting (prior) weight |
| `comparison_match_weights` | ARRAY<STRUCT<comparison STRING, level STRING, match_weight DOUBLE>> | Per-comparison contribution rows. `comparison` matches `[a-z][a-z0-9_]*`; `level` is the Splink level label (`Exact`, `Normalised edit similarity >= 0.9`), never a record value |
| `bf_<comparison>`, `bf_tf_adj_<comparison>`, `gamma_<comparison>` | DOUBLE, DOUBLE, INT | Used only when `comparison_match_weights` is absent: weight = log2(bf x tf_adj), level from gamma (-1 Missing, 0 Disagree, n Level n); the app shows a readable label per model version (`review_reasons.COMPARISON_LEVELS`, keyed by manifest `metadata.model_version`, currently `splink-v4.0`) |
| `decision_rule`, else `decision_reason` | STRING, upper-case code | The safety rule that routed the pair, for example `CONFLICTING_PERSONAL_CONTACTS_REVIEW` |

The four DOB provenance flags (`valid_dob_exact`, `valid_dob_contradiction`,
`left_dob_is_default`, `right_dob_is_default`) stay required. The hybrid resolver's
other exact-match flags and similarities may be absent; they are projected as NULL
and listed in the dry-run as `projected_null_columns`. `evidence_score` should be
the match probability so the queue's score sort is meaningful. The mapping snapshot
still needs `provisional_customer_id` and `cluster_requires_review` to scope cases.

## Publication boundary

The App never operates the resolver or explainer pipeline. Confirmed decisions are
persisted with an outbox event. A separate idempotent publisher must validate these
events against the source run, publish reviewed Gold and verify Snowflake read-back.
The existing backend Snowflake attestation covers automated Gold only; it is not
evidence that operational decisions flow through this path. This publisher remains
unimplemented and is required for operational end-to-end completion.

## Backend report (not an independent release attestation)

The backend thread reports run `714860427292674` froze v2.2 twice and published
Gold readable from Snowflake, but failed the hard-negative release gate. It reports
v2.3 smoke update `36d991a3-b857-4fd9-9c83-c3fda86e7054` protects 1,000/1,000 hard
negatives; a full v2.3 release is still pending. Do not treat the smoke as release.

## Review reasons and deferred cases (Splink v4)

The release carries no per-case review reason. The app explains a case from:

- its REVIEW pairs (routing rule plus the pair's flags), and
- graph-withheld links: `frozen_component_quarantine_<run>` rows (AUTO_MATCH links
  the clustering refused: `COMPONENT_DOB_CONFLICT`, `COMPONENT_CANNOT_LINK`,
  `COMPONENT_SIZE_LIMIT`) with probabilities from `frozen_match_decisions_<run>`.
  They cross components, so they are never pair evidence. `import_review_context.py`
  reads them read-only (Delta-version pinned) into Lakebase `case_review_context`.

`operational_deferred_review_cases` (component level) is not version-pinned by the
manifest; publication joins it to the mapping snapshot through
`provisional_customer_id = 'cmp_' || sha2(component_key)[0:28]` (the resolver's own
derivation) and refuses to publish unless the joined records and cases equal
`metadata.deferred_oversized_records` / `deferred_oversized_cases`.
