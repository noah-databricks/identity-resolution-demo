# Splink v4 guest identity resolver

Truth-blind serverless resolver. It reads only the source table it is given and writes
only to `<catalog>.resolution_splink_v4`. Candidate pairs come from two routes,
deterministic blocking keys and AI Search nearest neighbours, and are scored by a trained
Splink 4 model.

## Stages

1. `source`: read-only projection of the federated source contract (never alters the
   Snowflake connection), immutable content history, normalisation, source-default DOB
   profile, `contact_base`.
2. `profile`: observed contact-sharing profile. A contact is shared only on direct
   evidence that different people use it (two valid, mutually incompatible DOBs), or a
   role mailbox. No record-count rule: regulars legitimately have many records.
3. `deterministic`: bounded exact/contact/name-context blocks; oversized blocks are
   refined (family name, then initial) instead of dropped.
4. `embeddings` + `ann`: Qwen3 512-d name/address vectors, AI Search indexes
   `splink_v4_*identity_{name,address}_idx`, top-20 neighbours for every record.
5. `train` / `score`: Splink 4.0.17 EM (no labels), external candidate scoring,
   pair features, policy decisions (`policy.py`).
6. `cluster`: constrained graph (DOB-compatibility veto, cannot-links, size bound),
   persistent IDs, survivorship, immutable frozen tables `frozen_*_<release>`.
7. `release`: Steward-importer operational snapshots, Gold-compatible projections and
   `release_manifests` row. The release stage itself publishes nothing; the setup
   job's `publish_gold` task (`tasks/publish_gold.py`) publishes Gold from the pinned projections.

## Run

The setup job (`resources/setup.job.yml`) runs every stage in order as serverless
notebook tasks of `run.py`, each with `stage`, `schema`, `source_table` and `release_id`
parameters. To rerun one stage, repair that task in the job run.
