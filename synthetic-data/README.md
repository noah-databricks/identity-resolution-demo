# Synthetic Data Generator

This workstream creates a deterministic Australian hospitality population, expands
it into blinded operational identities, and produces connected activity. The
resolver is deliberately treated as an unknown downstream consumer.

## Run

The Asset Bundle job should invoke `synthetic-data/generate.py`. The runner accepts
Databricks job parameters (or environment variables) and executes the transformations
in `sql/01_generate.sql` using Spark SQL.

```text
--catalog identity_resolution_demo
--source-schema source
--truth-schema truth
--reference-schema reference
--seed identity-demo-v1
--generator-version 4.1.0
--people 61000
--stage generate   # or: validate
```

The `generate` stage first loads the committed public reference CSVs in
`reference/` into `<catalog>.<reference-schema>` and then runs `sql/01_generate.sql`.
The job's second task runs the `validate` stage (`sql/02_validate_realism.sql`),
which fails the job when name, locality, personal-email, work-identity,
records-per-person or contact-drift statistics leave their accepted bands. Every `SELECT assert_true(...)` in either file is
collected, so assertions really execute.

The catalog and schemas are parameters; they are never silently selected. The
approved demo defaults are supplied only by the deployment job. The job principal
needs `USE CATALOG`, `CREATE SCHEMA`, and table creation rights in both schemas.

## Scale

| Dataset | Rows |
|---|---:|
| Canonical people | 61,000 |
| Blinded source identity records | about 150,000 (drawn per guest, see below) |
| POS transactions | 500,000 |
| SevenRooms reservations | 120,000 |
| Me&U orders | 200,000 |
| Moshtix tickets | 80,000 |
| Momentus bookings | 30,000 |
| Tokenized payment events | 400,000 |

Counts can be reduced for tests through job parameters without changing rules.

## Visible Contract

`source.source_identity_records` has the exact resolver input contract:

```text
source_system STRING NOT NULL
source_record_id STRING NOT NULL
given_name STRING
family_name STRING
full_name STRING
personal_email STRING
work_email STRING
personal_phone STRING
work_phone STRING
date_of_birth DATE
home_address_line1 STRING
home_suburb STRING
home_state STRING
home_postcode STRING
home_country STRING
work_address_line1 STRING
work_suburb STRING
work_state STRING
work_postcode STRING
work_country STRING
company_name STRING
source_updated_at TIMESTAMP
```

System views expose the same contract. Activity tables reference opaque
`source_record_id` values. Visible outputs contain no canonical key, generator
seed, corruption label, scenario label, difficulty, or evaluation split.

## Restricted Truth Contract

- `truth.household_membership`, `truth.households`, `truth.canonical_people`,
  `truth.employment_history`, and `truth.guest_relationships` (records per guest,
  visit segment, preferred sources, record timeline and contact-drift history)
- `truth.identity_record_truth(identity_record_id, truth_person_id, source_system,
  evaluation_split, include_in_benchmark)`
- `truth.identity_record_cohorts(identity_record_id, cohort_name, cohort_value)`;
  `cohort_name` is `scenario_type`, `source_system`, `corruption` or `contact_drift`
- `truth.curated_scenario_cases(scenario_key, left_identity_record_id,
  right_identity_record_id, expected_same_person, expected_decision,
  expected_candidate_route, required_safeguard, presenter_narrative)`
- `truth.hard_negative_cases(case_id, scenario_type, left_identity_record_id,
  right_identity_record_id, presenter_narrative, expected_same_person)`
- `truth.generator_manifest`

Only the generator and evaluator principals may read the truth schema. Merely
placing these objects in a separate schema is not sufficient; infrastructure must
revoke resolver and Steward App access to the entire schema.

## Determinism and Blinding

Every probabilistic choice is derived from `xxhash64(seed, rule name, stable row
index)`. Re-running a version therefore produces the same values without `rand()`.
Opaque IDs are SHA-256 hashes of source-local coordinates; they do not embed the
person index or provide a cross-system join key. The manifest records the seed and
version only in the restricted schema.

The benchmark contains standard and challenge records. Dangerous non-matches are
first-class fixtures: household contact sharing, corporate organisers (a shared
`events@`/`bookings@` mailbox on the group's employer domain), twins with similar
given names, common-name collisions (pairs sharing a frequent name and locality),
recycled work role mailboxes, the venue placeholder `noemail@venue.example`, and
shared payment instruments. Hard-negative pairs are revealed only through the
restricted contract. A difficult true-match cohort also changes contact context
enough to remove direct exact identifiers.

One pinned contextual example is deliberately presenter-friendly: the same hidden
person can appear as `sam.taylor@workspace.com` (employer "Workspace", the one
pinned employer outside the fictional employer list) in a work context and
`samt2000@hotmail.com` in a personal context. Neither address reveals the hidden
person key, and the resolver is not told that this pair was constructed together.

## Curated Scenario Fixtures

Seven restricted fixtures exercise the candidate and decision paths used in the
demo: Hangul versus English transliteration; colleagues sharing corporate contact
details; a parent and adult child sharing household details; a repeated Momentus
default DOB versus a real DOB; a sparse typo-heavy booking; duplicate SevenRooms
profiles; and Emily Zhang versus Yutong Zhang / 张雨桐. The default DOB is repeated
across enough Momentus profiles to be identifiable as a source-level sentinel,
not as a one-off rule tailored to the hero record.

Scenario keys, expected decisions, expected routes, safeguards, narratives, and
record-to-person answers exist only in `truth`. The resolver-visible source table
contains ordinary opaque records and cannot distinguish a curated fixture from
the rest of the population. The Emily/Yutong fixture deliberately uses different
name and address representations: shared valid DOB, family name and personal
context provide corroboration; the generator does not declare the given names
equivalent.

The generator intentionally does not encode a matching policy. It creates plausible
source behaviour and adverse cases; it does not publish inverse corruption rules or
features tailored to any resolver.

## Household and identity coherence

Generator v3 builds deterministic solo, couple, family, shared and
multigenerational household archetypes before it creates people. Household roles
drive plausible age bands and surname sharing: multi-person households deliberately
include all-shared, partly-shared and unrelated surname patterns.

Canonical emails are generated only after the canonical name exists. Name-based and
initial-based addresses are mechanically coherent and unique; the small opaque legacy
cohort is explicitly labelled only in the restricted truth schema. Common-name and
twins challenges are coherent canonical identities rather than arbitrary people whose
visible names are overwritten after email generation.

## Population realism (generator v4)

Generator v4 draws the canonical population from public reference data (sources
and licences in `reference/README.md`) instead of short inline lists:

- Names: heritage-weighted given and family names with Zipf-like frequencies.
  English-origin given names follow NSW birth-decade frequencies, so a 1965 birth
  reads like a 1965 name. Heritage mix targets Greater Sydney (about 52% Anglo or
  Celtic, 10% Chinese, 5% Indian, 4% Italian, 3% each Vietnamese, Lebanese and
  Greek, 2% each Korean and Filipino, remainder other). Partners are sometimes of
  a different heritage, and many second-generation guests use English given names.
  Chinese names use Hanyu Pinyin, Taiwanese Wade-Giles or Cantonese spellings;
  Korean names use Revised Romanization. Native script appears only in the curated
  presenter scenarios.
- Addresses: every canonical Australian (suburb, state, postcode) triple comes from
  the gazetteer. About 75% of people live in Greater Sydney (weighted towards the
  eastern suburbs, inner city, inner west and north shore, with heritage
  concentrations such as Chinese in Ryde and Chatswood or Vietnamese in the south
  west), about 15% interstate or in regional NSW and about 10% are overseas visitors
  with country-appropriate address, postcode and phone formats. Units use `4/12
  Crown Street` in the canonical record.
- Contact details: personal email domains are age-skewed (ISP mailboxes such as
  bigpond and optusnet concentrate in guests aged 55 and over), local parts use
  varied name-derived styles, and numeric suffixes are birth years or small
  numbers. Mobiles are unique `+61 4xx xxx xxx` numbers; each source stores them in
  its own format. Canonical personal emails, work emails and mobiles are globally
  unique; sharing only happens in the explicit household and organiser fixtures.
- Work identity: 342 fictional employers with heavy-tailed sizes, real business
  district offices and per-employer email conventions. A work email exists only
  with its employer and domain, in the canonical record and in every source.
  Documented source gap: payments records can carry the company printed on a
  corporate card without any business email.
- Ages: household anchors drive coherent member ages (parents at least 18 years
  older than adult children, partners within a few years); guests are 18 to 85.

Source-visible corruption is explicit and low-rate: typos (transposition, dropped
or doubled letter, vowel slip), nicknames, email case or domain slips, phone digit
swaps, day and month swaps, street or suburb typos and neighbouring-postcode
mismatches. Each applied corruption is recorded in the restricted
`truth.identity_record_cohorts` as `cohort_name='corruption'`. Per-source capture
profiles decide which fields each system records (for example Moshtix nearly
always has DOB and postcode, Me&U rarely has a surname or address, payments
carries upper-case cardholder names).

## Relationship depth and contact drift (generator v4.1)

Up to v4 every canonical person had exactly three source records in three different
systems. Resolvers tuned against that learned the constant (for example capping how
many records may share a contact). v4.1 draws the number of records per guest from a
heavy-tailed distribution that reflects a multi-venue hospitality group over
2019-2025, stored in the restricted `truth.guest_relationships`:

| Records per guest | Target share |
|---|---:|
| 1 | 55-65% |
| 2-3 | 20-25% |
| 4-8 | 8-12% |
| 9-20 | 3-6% |
| 21-60 | 0.5-1.5% |

- Drivers: a restricted `visit_segment` per guest. Overseas visitors are almost
  always seen once or twice (never more than eight records); residents of the
  venue areas (Eastern Suburbs, City and Inner South, Inner West, Northern Beaches)
  and hospitality-industry workers (Greater Sydney, 18 to 40, no corporate employer)
  return far more often than other Sydney, interstate or regional guests.
- Source affinity: each guest has up to three preferred systems chosen by weighted
  sampling. Occasional guests lean to Moshtix and Momentus, regulars to POS,
  payments, SevenRooms and Me&U, and Moshtix skews young. A guest visits each
  preferred system once and then concentrates on the favourites, so regulars
  produce genuine within-source duplicates while staying in at most three systems.
  Each source holds roughly one sixth of the records.
- Timeline: a guest's records are spread chronologically across a relationship
  window (longer for deeper relationships) and `source_updated_at` follows slot
  order.
- Contact drift: guests seen more than once occasionally change mobile (3% of 2-3
  record guests, 7% of 4+), personal email (2% / 6%) or home address (4% / 9%);
  employers change on the per-person move date in `truth.employment_history`.
  Records captured before a change keep the previous value, and each such record is
  tagged `contact_drift` in the cohorts table. Previous mobiles use 04 prefixes that
  current numbers never use and previous mailboxes never equal another guest's
  current address.
- Curated heroes and challenge cohorts: the ten pinned presenter guests keep exactly
  their three v4 records (same sources, IDs and visible values, including
  `source_updated_at`). Other scenario cohorts (person index below 3300) keep their
  v4 slots 0-2 and gain extra records only in the same three systems, with no drift.
- Evaluation splits stay assigned by person, so every record of a guest lands in the
  same split.

The `records_per_person` gate fails the job when the histogram leaves those bands,
a guest with 9+ records spans more than three systems, fewer than 90% of overseas
visitors have one or two records, venue-area guests are not at least 1.2x as deep
as other guests, or any source is more than 20% away from an even share. The
`contact_drift` gate fails when any previous value was captured after the current
value or when visible drift leaves 1-15% of multi-record guests.
