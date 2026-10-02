"""Extract presenter evidence from the promoted Splink v4 resolver release.

Writes ``static/release-evidence.js`` (the evidence contract the presenter loads as
``window.IDENTITY_RELEASE_EVIDENCE``). The setup job's ``refresh_explainer`` task runs
this after every build; to run it by hand:

    python apps/identity-presenter/extract_release_evidence.py \
        --warehouse <warehouse-id> [--profile <profile>] [--release splink-v4-release]

Every value comes from a read-only query:

- records: ``source.source_identity_records`` (visible fields exactly as stored)
- candidate route, Splink comparison levels and Bayes factors, match weight,
  probability, decision and reason: the release's immutable
  ``resolution_splink_v4.frozen_match_decisions_<release>`` table
- comparison level labels and the prior: the fitted model artifact
  (``resolution_splink_v4.model_artifacts``) that scored the release
- contact profile flags: ``resolution_splink_v4.contact_records``
- headline counts: frozen release tables and ``release_manifests``

The restricted ``truth.curated_scenario_cases`` table is read only to locate
which record pairs the six presenter scenarios depict. No expected outcome or
other truth column is read or written, and nothing is ever written to the
workspace.

Per-comparison match weight = log2(bf_<col>) + log2(bf_tf_adj_<col>), the
native Splink decomposition; the prior weight plus the comparison weights sum
to the stored ``match_weight``. The script asserts that identity.
"""

from __future__ import annotations

import argparse
import json
import os
import math
import time
from datetime import datetime, timezone
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
OUTPUT = APP_DIR / "static" / "release-evidence.js"

CATALOG = os.environ.get("DEMO_CATALOG", "identity_resolution_demo")
RES = f"{CATALOG}.resolution_splink_v4"
SOURCE_TABLE = f"{CATALOG}.source.source_identity_records"
CURATED_TABLE = f"{CATALOG}.truth.curated_scenario_cases"

# Presenter scenario key -> curated scenario key, in presentation order.
SCENARIOS = [
    ("preferred-name", "english_chinese_name"),
    ("corporate", "corporate_shared_contact"),
    ("household", "household_parent_child"),
    ("default-dob", "default_dob"),
    ("sparse", "typo_sparse_booking"),
    ("duplicate", "within_source_duplicate"),
    ("review", None),
]

# Scenarios located by record keys instead of the curated table. The review pair and
# the hand-off component are one Steward case (Jessica Pham): two auto-matches join
# three records through a shared phone and a shared email, and a fourth record the
# model scores at 99% has no independent personal contact, so it goes to a steward.
EXPLICIT_PAIRS = {
    "review": ("1ac0fa4e5490cd2a91ace74daa743bb9a4eb3267cc962ec50daed9f6b9c43f25",
               "30e4325398cee75ee40b35712694f964ab572a7cf1df2d8b56194524d9676dfc"),
}
HANDOFF_COMPONENT = (
    "1ac0fa4e5490cd2a91ace74daa743bb9a4eb3267cc962ec50daed9f6b9c43f25",
    "30e4325398cee75ee40b35712694f964ab572a7cf1df2d8b56194524d9676dfc",
    "6c77109bc3a2ed42dcc9170d4aab0e7b546b83e1d36c0dc7277e946bd04a89cb",
    "8050ebdfd18df669e9514665949448b8bf36f4d3bcf5e3f788f0dcc9b15cdef1",
)

# Splink comparisons in model order (resolution/splink_v4/model.py).
COMPARISONS = ["name_n", "personal_email_n", "personal_phone_n", "dob_usable", "home_address_n"]
TF_COMPARISONS = {"name_n", "personal_email_n", "personal_phone_n", "home_address_n"}

VISIBLE_FIELDS = [
    "source_system", "source_record_id", "given_name", "family_name", "full_name",
    "personal_email", "work_email", "personal_phone", "work_phone", "date_of_birth",
    "home_address_line1", "home_suburb", "home_state", "home_postcode", "home_country",
    "work_address_line1", "work_suburb", "work_state", "work_postcode", "work_country",
    "company_name",
]
PROFILE_FIELDS = ["email_is_shared", "phone_is_shared", "dob_is_source_default",
                  "personal_email_n", "personal_phone_n", "dob_usable"]
FEATURE_FIELDS = ["dob_variant", "given_variant", "family_variant", "cross_script",
                  "shared_context", "source_default_dob", "personal_contact",
                  "personal_email_exact", "personal_phone_exact", "safeguards_triggered"]

RECORD_KEY = "sha2(concat_ws('|', lower(trim(s.source_system)), trim(s.source_record_id)), 256)"


class Warehouse:
    def __init__(self, profile: str | None, warehouse: str):
        from databricks.sdk import WorkspaceClient
        self.client, self.warehouse = WorkspaceClient(profile=profile).api_client, warehouse

    def query(self, sql: str) -> list[dict]:
        body = {"warehouse_id": self.warehouse, "statement": sql, "wait_timeout": "50s",
                "format": "JSON_ARRAY", "disposition": "INLINE"}
        r = self.client.do("POST", "/api/2.0/sql/statements", body=body)
        while r["status"]["state"] in ("PENDING", "RUNNING"):
            time.sleep(2)
            r = self.client.do("GET", f"/api/2.0/sql/statements/{r['statement_id']}")
        if r["status"]["state"] != "SUCCEEDED":
            raise RuntimeError(f"{r['status']}\n{sql}")
        cols = r["manifest"]["schema"]["columns"]
        rows = r["result"].get("data_array", []) or []
        return [{c["name"]: _typed(v, c["type_name"]) for c, v in zip(cols, row)} for row in rows]


def _typed(value, type_name: str):
    if value is None:
        return None
    if type_name in ("INT", "LONG", "SHORT", "BYTE"):
        return int(value)
    if type_name in ("DOUBLE", "FLOAT", "DECIMAL"):
        return float(value)
    if type_name == "BOOLEAN":
        return value == "true"
    if type_name == "ARRAY":
        return json.loads(value)
    return value


def lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def log2(x: float) -> float:
    return math.log2(x)


def comparison_levels(settings: dict) -> dict:
    """gamma -> label for each comparison, following Splink's level numbering."""
    levels = {}
    for comp in settings["comparisons"]:
        non_null = [lvl for lvl in comp["comparison_levels"] if not lvl.get("is_null_level")]
        top = len(non_null) - 1
        mapping = {-1: "Missing"}
        for index, lvl in enumerate(non_null):
            mapping[top - index] = lvl["label_for_charts"]
        levels[comp["output_column_name"]] = mapping
    return levels


def extract(wh: Warehouse, release: str) -> dict:
    suffix = release.replace("-", "_")
    decisions_table = f"{RES}.frozen_match_decisions_{suffix}"

    manifest_row = wh.query(f"SELECT * FROM {RES}.release_manifests WHERE release_id={lit(release)}")
    if not manifest_row:
        raise SystemExit(f"release {release} not found in {RES}.release_manifests")
    manifest_row = manifest_row[0]
    manifest = json.loads(manifest_row["manifest_json"])

    model_keys = wh.query(f"SELECT DISTINCT model_key, resolver_version FROM {decisions_table}")
    assert len(model_keys) == 1, model_keys
    model_key, resolver_version = model_keys[0]["model_key"], model_keys[0]["resolver_version"]
    artifact = wh.query(f"SELECT model_version, mlflow_run_id, settings_json FROM {RES}.model_artifacts "
                        f"WHERE model_key={lit(model_key)}")[0]
    settings = json.loads(artifact["settings_json"])
    prior = settings["probability_two_random_records_match"]
    prior_weight = log2(prior / (1 - prior))
    levels = comparison_levels(settings)

    curated = {r["scenario_key"]: r for r in wh.query(
        f"SELECT scenario_key, left_identity_record_id, right_identity_record_id FROM {CURATED_TABLE}")}

    def pair(presenter_key, curated_key):
        if curated_key is None:
            return EXPLICIT_PAIRS[presenter_key]
        return curated[curated_key]["left_identity_record_id"], curated[curated_key]["right_identity_record_id"]

    keys = sorted({k for p, c in SCENARIOS for k in pair(p, c)} | set(HANDOFF_COMPONENT))
    key_list = ",".join(lit(k) for k in keys)
    records = {r["record_key"]: r for r in wh.query(
        f"SELECT {RECORD_KEY} AS record_key, {', '.join('s.' + f for f in VISIBLE_FIELDS)} "
        f"FROM {SOURCE_TABLE} s WHERE {RECORD_KEY} IN ({key_list})")}
    profiles = {r["record_key"]: r for r in wh.query(
        f"SELECT record_key, {', '.join(PROFILE_FIELDS)} FROM {RES}.contact_records "
        f"WHERE record_key IN ({key_list})")}

    comparison_cols = []
    for col in COMPARISONS:
        comparison_cols += [f"gamma_{col}", f"bf_{col}", f"{col}_l", f"{col}_r"]
        if col in TF_COMPARISONS:
            comparison_cols.append(f"bf_tf_adj_{col}")
    pair_filter = " OR ".join(
        f"(record_key_l={lit(min(a, b))} AND record_key_r={lit(max(a, b))})"
        for a, b in [pair(p, c) for p, c in SCENARIOS]
        + [(a, b) for i, a in enumerate(HANDOFF_COMPONENT) for b in HANDOFF_COMPONENT[i + 1:]])
    decisions = {(r["record_key_l"], r["record_key_r"]): r for r in wh.query(
        f"SELECT record_key_l, record_key_r, match_weight, match_probability, decision, decision_reason, "
        f"resolution_run_id, candidate_routes, candidate_reasons, candidate_scopes, "
        f"ann_name_score, ann_name_rank, ann_address_score, ann_address_rank, "
        f"{', '.join(FEATURE_FIELDS)}, {', '.join(comparison_cols)} "
        f"FROM {decisions_table} WHERE {pair_filter}")}

    scenarios = []
    for presenter_key, curated_key in SCENARIOS:
        left_key, right_key = pair(presenter_key, curated_key)
        d = decisions[(min(left_key, right_key), max(left_key, right_key))]
        assert d["resolution_run_id"] == release, d["resolution_run_id"]
        swapped = d["record_key_l"] != left_key  # Splink orders keys; present curated left first
        comparisons, total = [], prior_weight
        for col in COMPARISONS:
            bf = d[f"bf_{col}"]
            tf = d.get(f"bf_tf_adj_{col}", 1.0) if col in TF_COMPARISONS else 1.0
            weight = log2(bf) + log2(tf)
            total += weight
            lv, rv = d[f"{col}_l"], d[f"{col}_r"]
            comparisons.append({
                "column": col, "gamma": d[f"gamma_{col}"], "level": levels[col][d[f"gamma_{col}"]],
                "bayes_factor": bf, "tf_adjustment": tf, "weight": weight,
                "left_normalised": rv if swapped else lv, "right_normalised": lv if swapped else rv,
            })
        assert abs(total - d["match_weight"]) < 1e-6, (presenter_key, total, d["match_weight"])
        scenarios.append({
            "key": presenter_key,
            "curated_scenario": curated_key,
            "left": {"record_key": left_key, **{f: records[left_key][f] for f in VISIBLE_FIELDS},
                     "profile": {f: profiles[left_key][f] for f in PROFILE_FIELDS}},
            "right": {"record_key": right_key, **{f: records[right_key][f] for f in VISIBLE_FIELDS},
                      "profile": {f: profiles[right_key][f] for f in PROFILE_FIELDS}},
            "candidate": {
                "routes": d["candidate_routes"], "reasons": d["candidate_reasons"], "scopes": d["candidate_scopes"],
                "ann_name_score": d["ann_name_score"], "ann_name_rank": d["ann_name_rank"],
                "ann_address_score": d["ann_address_score"], "ann_address_rank": d["ann_address_rank"],
            },
            "splink": {"prior_weight": prior_weight, "match_weight": d["match_weight"],
                       "match_probability": d["match_probability"], "comparisons": comparisons},
            "features": {f: d[f] for f in FEATURE_FIELDS},
            "decision": d["decision"],
            "decision_reason": d["decision_reason"],
        })

    # Hand-off component: every pair decision inside it and the customers the
    # constrained graph formed from them.
    customers = {r["record_key"]: r["customer_id"] for r in wh.query(
        f"SELECT record_key, customer_id FROM {RES}.frozen_customer_mapping_{suffix} "
        f"WHERE record_key IN ({','.join(lit(k) for k in HANDOFF_COMPONENT)})")}
    handoff = {
        "records": [{"record_key": k, **{f: records[k][f] for f in VISIBLE_FIELDS},
                     "customer_id": customers[k]} for k in HANDOFF_COMPONENT],
        "pairs": [{"left": a, "right": b, "decision": d["decision"], "decision_reason": d["decision_reason"],
                   "match_probability": d["match_probability"]}
                  for i, a in enumerate(HANDOFF_COMPONENT) for b in HANDOFF_COMPONENT[i + 1:]
                  for d in [decisions[(min(a, b), max(a, b))]]],
    }

    counts = wh.query(f"""SELECT
      (SELECT count(*) FROM {SOURCE_TABLE}) AS source_records,
      (SELECT count(*) FROM {RES}.frozen_source_snapshot_{suffix}) AS release_records,
      (SELECT count(*) FROM {RES}.frozen_candidate_pairs_{suffix}) AS candidate_pairs,
      (SELECT count(*) FROM {RES}.frozen_candidate_pairs_{suffix} WHERE array_contains(candidate_routes,'ANN')) AS ann_pairs,
      (SELECT count(*) FROM {RES}.frozen_candidate_pairs_{suffix} WHERE array_contains(candidate_routes,'DETERMINISTIC')) AS deterministic_pairs,
      (SELECT count(*) FROM {decisions_table} WHERE decision='AUTO_MATCH') AS auto_match_pairs,
      (SELECT count(*) FROM {decisions_table} WHERE decision='REVIEW') AS review_pairs,
      (SELECT count(*) FROM {decisions_table} WHERE decision='REJECT') AS reject_pairs,
      (SELECT count(DISTINCT customer_id) FROM {RES}.frozen_customer_mapping_{suffix}) AS customers""")[0]
    meta = manifest.get("metadata", {})
    headline = {**counts, "steward_cases": meta.get("steward_cases"),
                "deferred_review_cases": meta.get("deferred_oversized_cases")}

    return {
        "release": {
            "release_id": release,
            "resolver_version": resolver_version,
            "model_version": artifact["model_version"],
            "model_key": model_key,
            "mlflow_run_id": artifact["mlflow_run_id"],
            "policy": "resolution/splink_v4/policy.py decide_v4 (POLICY_VERSION p4)",
            "decision_step": "decide_v4",
            "published": manifest_row["published"],
            "release_created_at": manifest_row["created_at"],
            "release_job_run_id": manifest.get("release_job_run_id"),
            "decisions_table": decisions_table,
            "source_table": SOURCE_TABLE,
            "prior_probability": prior,
            "extracted_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        },
        "headline": headline,
        "comparison_levels": {col: {str(k): v for k, v in sorted(levels[col].items())} for col in COMPARISONS},
        "scenarios": scenarios,
        "handoff": handoff,
    }


def render(evidence: dict) -> str:
    body = json.dumps(evidence, ensure_ascii=False, indent=1)
    return ("// Generated by apps/identity-presenter/extract_release_evidence.py. Do not edit.\n"
            f"window.IDENTITY_RELEASE_EVIDENCE={body};\n")


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", help="Databricks CLI profile (default: ambient auth)")
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--release", default="splink-v4-release")
    args = parser.parse_args(argv)
    evidence = extract(Warehouse(args.profile, args.warehouse), args.release)
    OUTPUT.write_text(render(evidence), encoding="utf-8")
    print(f"Wrote {OUTPUT} ({len(evidence['scenarios'])} scenarios, release {args.release})")


if __name__ == "__main__":
    main()
