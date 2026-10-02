"""Bounded constrained graph, persistent IDs, provenance, immutable freeze and the
operational (Steward importer) + Gold-compatible release projections.

Nothing here publishes: Gold-compatible tables are written only inside the v4
resolver schema, and operational snapshots are pinned by version in a manifest.
"""

import json

from . import MODEL_VERSION
from . import normalization as norm
from .graph import components, reconcile
from .search import fingerprint
from .source import SOURCE_COLUMNS, replace

GRAPH_VERSION = "g1"
# g0-v3: exact-DOB component veto, 50-record bound, v3 cannot-link reasons.
# g1: DOB-compatibility veto (swap tolerated), 100-record bound, cannot-links only on
#     direct contradictions (genuine DOB conflict, contact shared by different people).
MAX_COMPONENT = {"g0-v3": 50}.get(GRAPH_VERSION, 100)
FORBIDDEN_REASONS = {"g0-v3": ("SHARED_CONTEXT_NOT_IDENTITY", "CONFLICTING_PERSONAL_CONTACTS_REVIEW",
                               "AMBIGUOUS_COMMON_NAME_ADDRESS")}.get(GRAPH_VERSION, ("SHARED_CONTACT_DIFFERENT_PEOPLE", "SAME_BIRTH_FAMILY_GIVEN_CONFLICT"))
FROZEN_TABLES = ("source_snapshot", "candidate_pairs", "pair_features", "match_decisions", "customer_mapping",
                 "gold_customer_profile", "gold_field_provenance", "component_quarantine")


def freeze(spark, ns, table, run_id):
    target = f"{ns}frozen_{table}_{run_id.replace('-', '_')}"
    source = f"{ns}{table}"
    if not spark.catalog.tableExists(target):
        spark.sql(f"CREATE TABLE {target} DEEP CLONE {source}")
    else:
        columns = spark.table(source).columns
        assert fingerprint(spark, source, columns) == fingerprint(spark, target, columns), \
            f"Refusing to overwrite immutable release: {target}"
    return target


def _dob_compatible(a, b):
    from datetime import date
    return norm.dob_compatible(date.fromisoformat(a), date.fromisoformat(b))


def cluster_and_freeze(spark, ns, run_id):
    contacts = spark.table(f"{ns}contact_records")
    assert contacts.count() <= 250_000, "Use distributed graph above the demo record bound"
    edges = spark.table(f"{ns}match_decisions").where("decision='AUTO_MATCH'")
    assert edges.count() <= 2_000_000, "Use distributed graph above the demo edge bound"
    records = {r.record_key: str(r.dob_usable) if r.dob_usable else None
               for r in contacts.select("record_key", "dob_usable").collect()}
    accepted = [(r.left_record_key, r.right_record_key, r.match_probability)
                for r in edges.select("left_record_key", "right_record_key", "match_probability").collect()]
    # A rejected direct comparison is also a graph-level cannot-link when it carries a
    # genuine identity contradiction, so a third record cannot bridge it transitively.
    reasons = ",".join(f"'{r}'" for r in FORBIDDEN_REASONS)
    contradiction = ("valid_dob_conflict" if GRAPH_VERSION == "g0-v3"
                     else "valid_dob_conflict AND dob_variant='CONFLICT'")
    denied = spark.table(f"{ns}match_decisions").where(f"({contradiction}) OR decision_reason IN ({reasons})")
    forbidden = [(r.left_record_key, r.right_record_key)
                 for r in denied.select("left_record_key", "right_record_key").collect()]
    groups, rejected = components(records, accepted, forbidden=forbidden, max_size=MAX_COMPONENT,
                                  dob_compatible=None if GRAPH_VERSION == "g0-v3" else _dob_compatible)
    previous, created = {}, {}
    if spark.catalog.tableExists(f"{ns}active_mapping"):
        previous = {r.record_key: r.customer_id for r in spark.table(f"{ns}active_mapping").collect()}
    if spark.catalog.tableExists(f"{ns}customer_id_registry"):
        created = {r.customer_id: str(r.created_at) for r in spark.table(f"{ns}customer_id_registry").collect()}
    mapping, aliases = reconcile(groups, previous, created, run_id)
    replace(spark, f"{ns}component_quarantine", spark.createDataFrame(rejected,
        "left_record_key string,right_record_key string,reason string"))
    frame = spark.createDataFrame([(key, customer, groups[key]) for key, customer in mapping.items()],
        "record_key string,customer_id string,component_key string")
    replace(spark, f"{ns}customer_mapping", frame)
    _survivorship(spark, ns)
    snapshots = {table: freeze(spark, ns, table, run_id) for table in FROZEN_TABLES}
    # Resolver-internal IDs only, not the public/App release pointer.
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {ns}customer_id_registry (
      customer_id STRING,created_at TIMESTAMP) USING DELTA""")
    spark.sql(f"""MERGE INTO {ns}customer_id_registry t USING
      (SELECT DISTINCT customer_id FROM {ns}customer_mapping) s ON t.customer_id=s.customer_id
      WHEN NOT MATCHED THEN INSERT (customer_id,created_at) VALUES(s.customer_id,current_timestamp())""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {ns}identity_history (
      record_key STRING, customer_id STRING,valid_from TIMESTAMP,valid_to TIMESTAMP,release_id STRING) USING DELTA""")
    spark.sql(f"""MERGE INTO {ns}identity_history t USING {ns}customer_mapping s ON t.record_key=s.record_key
      WHEN MATCHED AND t.valid_to IS NULL AND t.customer_id<>s.customer_id THEN UPDATE SET valid_to=current_timestamp()
      WHEN NOT MATCHED BY SOURCE AND t.valid_to IS NULL THEN UPDATE SET valid_to=current_timestamp()""")
    spark.sql(f"""INSERT INTO {ns}identity_history SELECT s.record_key,s.customer_id,current_timestamp(),
      cast(NULL AS TIMESTAMP),'{run_id}' FROM {ns}customer_mapping s LEFT ANTI JOIN {ns}identity_history t
      ON s.record_key=t.record_key AND s.customer_id=t.customer_id AND t.valid_to IS NULL""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {ns}customer_id_alias (
      retired_customer_id STRING,surviving_customer_id STRING,release_id STRING) USING DELTA""")
    spark.createDataFrame([(a, b, run_id) for a, b in aliases],
        "retired_customer_id string,surviving_customer_id string,release_id string").createOrReplaceTempView("v4_aliases")
    spark.sql(f"""MERGE INTO {ns}customer_id_alias t USING v4_aliases s
      ON t.retired_customer_id=s.retired_customer_id AND t.release_id=s.release_id WHEN NOT MATCHED THEN INSERT *""")
    replace(spark, f"{ns}active_mapping", frame.select("record_key", "customer_id"))
    return {"customers": len(set(mapping.values())), "quarantined_edges": len(rejected),
            "aliases": len(aliases), "graph_version": GRAPH_VERSION, "max_component": MAX_COMPONENT,
            "frozen_tables": snapshots, "publication": "NOT_PROMOTED"}


SURVIVORSHIP = (("full_name", "priority"), ("given_name", "priority"), ("family_name", "priority"),
                ("personal_email", "recent"), ("personal_phone", "recent"), ("work_email", "recent"),
                ("work_phone", "recent"), ("dob_usable", "priority"), ("home_address", "recent"),
                ("work_address", "recent"), ("company_name", "recent"))


def _survivorship(spark, ns):
    # Configurable field-level survivorship; addresses are selected atomically,
    # avoiding a synthetic address assembled from contradictory source records.
    spark.sql(f"CREATE TABLE IF NOT EXISTS {ns}survivorship_rules (field STRING, ordering STRING) USING DELTA")
    spark.createDataFrame(list(SURVIVORSHIP), "field string,ordering string").createOrReplaceTempView("v4_default_survivorship")
    spark.sql(f"""MERGE INTO {ns}survivorship_rules t USING v4_default_survivorship s
      ON t.field=s.field WHEN NOT MATCHED THEN INSERT *""")
    assert not spark.sql(f"SELECT field FROM {ns}survivorship_rules GROUP BY field HAVING count(*)>1").take(1)
    assert not spark.table(f"{ns}survivorship_rules").where("ordering NOT IN ('priority','recent') OR ordering IS NULL").take(1)

    def address(prefix):
        return (f"CASE WHEN {prefix}_address_n IS NOT NULL THEN to_json(named_struct('line1',{prefix}_address_line1,"
                f"'suburb',{prefix}_suburb,'state',{prefix}_state,'postcode',{prefix}_postcode,'country',{prefix}_country)) END")
    spark.sql(f"""CREATE OR REPLACE TABLE {ns}survivorship_candidates AS
      SELECT m.customer_id,c.record_key,c.source_system,c.source_updated_at,c.source_priority,
        v.field,v.value FROM {ns}contact_records c JOIN {ns}customer_mapping m USING(record_key),
      LATERAL inline(array(
        named_struct('field','full_name','value',coalesce(nullif(full_name,''),concat_ws(' ',given_name,family_name))),
        named_struct('field','given_name','value',nullif(trim(given_name),'')),
        named_struct('field','family_name','value',nullif(trim(family_name),'')),
        named_struct('field','personal_email','value',personal_email_n),
        named_struct('field','personal_phone','value',personal_phone_n),
        named_struct('field','work_email','value',work_email_n),
        named_struct('field','work_phone','value',work_phone_n),
        named_struct('field','dob_usable','value',cast(dob_usable AS STRING)),
        named_struct('field','home_address','value',{address('home')}),
        named_struct('field','work_address','value',{address('work')}),
        named_struct('field','company_name','value',nullif(trim(company_name),'')))) v""")
    spark.sql(f"""CREATE OR REPLACE TABLE {ns}gold_field_provenance AS SELECT
      c.* FROM {ns}survivorship_candidates c JOIN {ns}survivorship_rules r USING(field)
      WHERE c.value IS NOT NULL AND c.value<>''
      QUALIFY row_number() OVER(PARTITION BY customer_id,field ORDER BY
        CASE WHEN r.ordering='priority' THEN source_priority ELSE 0 END ASC,
        source_updated_at DESC NULLS LAST,source_priority ASC,record_key ASC)=1""")
    picks = ",\n        ".join(f"max(IF(field='{f}',value,NULL)) {f}" for f, _ in SURVIVORSHIP)
    spark.sql(f"""CREATE OR REPLACE TABLE {ns}gold_customer_profile AS SELECT
      m.customer_id,count(*) source_record_count,sort_array(collect_set(c.source_system)) source_systems,
      max(p.full_name) full_name,max(p.given_name) given_name,max(p.family_name) family_name,
      max(p.personal_email) personal_email,max(p.personal_phone) personal_phone,
      max(p.work_email) work_email,max(p.work_phone) work_phone,
      cast(max(p.dob_usable) AS DATE) date_of_birth,max(p.home_address) home_address_json,
      max(p.work_address) work_address_json,max(p.company_name) company_name
      FROM {ns}customer_mapping m JOIN {ns}contact_records c USING(record_key)
      LEFT JOIN (SELECT customer_id,
        {picks}
        FROM {ns}gold_field_provenance GROUP BY customer_id) p USING(customer_id)
      GROUP BY m.customer_id""")


def steward_cases(sizes, quarantined, review_edges, pair_counts, max_records=50, max_pairs=1225):
    """Mirror of the importer's case formation: quarantined components joined by
    cross-component REVIEW pairs (union-find). Returns (case count, deferred) where
    deferred maps component -> (case key, records, pairs) for cases above the bound.
    ``pair_counts`` maps (component_a, component_b) -> number of scored pairs."""
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    seeds = set(quarantined)
    for a, b in review_edges:
        if a in seeds or b in seeds:
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[max(ra, rb)] = min(ra, rb)
    wanted = {find(c) for c in seeds}
    members = {}
    for c in set(sizes) | seeds:
        if find(c) in wanted:
            members.setdefault(find(c), set()).add(c)
    records = {k: sum(sizes.get(c, 0) for c in v) for k, v in members.items()}
    pairs = dict.fromkeys(members, 0)
    for (a, b), n in pair_counts.items():
        ra = find(a)
        if ra in pairs and find(b) == ra:
            pairs[ra] += n
    deferred = {}
    for k, comps in members.items():
        if records[k] > max_records or pairs[k] > max_pairs:
            for c in comps:
                deferred[c] = (k, records[k], pairs[k])
    return len(members), deferred


def _version(spark, table):
    return int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first().version)


def operational_release(spark, ns, run_id, job_run_id):
    """Steward-importer snapshots + Gold-compatible projections from ONE frozen release."""
    s = run_id.replace("-", "_")
    f = {t: f"{ns}frozen_{t}_{s}" for t in FROZEN_TABLES}
    for t in f.values():
        assert spark.catalog.tableExists(t), f"Release must be frozen first: {t}"
    decisions, mapping = f["match_decisions"], f["customer_mapping"]
    versions = spark.table(decisions).select("resolver_version", "resolution_run_id").distinct().collect()
    assert len(versions) == 1, "One resolver/policy version per release"
    resolver_version = versions[0].resolver_version
    # Records needing a steward: any REVIEW pair or graph-withheld AUTO edge touches them.
    spark.sql(f"""CREATE OR REPLACE TEMP VIEW v4_review_touch AS
      SELECT left_record_key k FROM {decisions} WHERE decision='REVIEW'
      UNION SELECT right_record_key FROM {decisions} WHERE decision='REVIEW'
      UNION SELECT left_record_key FROM {f['component_quarantine']}
      UNION SELECT right_record_key FROM {f['component_quarantine']}""")
    quarantined = [r.component_key for r in spark.sql(f"""SELECT DISTINCT m.component_key FROM {mapping} m
      JOIN v4_review_touch t ON t.k=m.record_key""").collect()]
    cases, deferred = steward_cases(
        {r.component_key: r.n for r in spark.sql(f"SELECT component_key,count(*) n FROM {mapping} GROUP BY 1").collect()},
        quarantined,
        [(r.a, r.b) for r in spark.sql(f"""SELECT lm.component_key a,rm.component_key b FROM {decisions} d
          JOIN {mapping} lm ON lm.record_key=d.left_record_key JOIN {mapping} rm ON rm.record_key=d.right_record_key
          WHERE d.decision='REVIEW' AND lm.component_key<>rm.component_key""").collect()],
        {(r.a, r.b): r.n for r in spark.sql(f"""SELECT lm.component_key a,rm.component_key b,count(*) n FROM {decisions} d
          JOIN {mapping} lm ON lm.record_key=d.left_record_key JOIN {mapping} rm ON rm.record_key=d.right_record_key
          GROUP BY 1,2""").collect()})
    # Steward cases above the importer bound (50 records / 1,225 pairs) cannot be
    # imported. They are NOT hidden: they stay REVIEW in match_decisions, count in the
    # evaluator's review queue, and are listed in operational_deferred_review_cases
    # and the manifest metadata for explicit handling.
    spark.createDataFrame([(run_id, c, case, records, pairs) for c, (case, records, pairs) in deferred.items()]
                          or [(run_id, "", "", 0, 0)],
        "release_id string,component_key string,case_key string,case_records long,case_pairs long") \
        .where("component_key<>''").write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{ns}operational_deferred_review_cases")
    spark.createDataFrame([(c,) for c in quarantined if c not in deferred] or [("",)], "component_key string") \
        .createOrReplaceTempView("v4_quarantined_components")
    src_cols = ",".join(["record_key"] + SOURCE_COLUMNS)
    spark.sql(f"""CREATE OR REPLACE TABLE {ns}operational_source_snapshot AS
      SELECT {src_cols} FROM {f['source_snapshot']}""")
    spark.sql(f"""CREATE OR REPLACE TABLE {ns}operational_mapping_snapshot AS
      SELECT '{run_id}' resolution_run_id,'{resolver_version}' resolver_version,m.record_key,m.customer_id,
        concat('cmp_',substring(sha2(m.component_key,256),1,28)) provisional_customer_id,
        q.component_key IS NOT NULL cluster_requires_review
      FROM {mapping} m LEFT JOIN v4_quarantined_components q USING(component_key)""")
    evidence = f"""
        '{run_id}' resolution_run_id,d.resolver_version,d.left_record_key,d.right_record_key,d.decision,
        cast(d.match_probability AS DOUBLE) evidence_score,d.candidate_routes,
        array(d.decision_reason) evidence_reasons,d.safeguards_triggered,
        concat(d.decision,': ',d.decision_reason,' (Splink p=',format_number(d.match_probability,4),')') decision_explanation,
        cast(d.personal_email_exact AND NOT d.shared_email_exact AS INT) rare_email_exact,
        cast(d.personal_phone_exact AND NOT d.shared_phone_exact AS INT) rare_phone_exact,
        cast(d.shared_email_exact AS INT) shared_email_exact,cast(d.shared_phone_exact AS INT) shared_phone_exact,
        cast(d.address_agreement AS INT) home_address_exact,cast(d.work_address_exact AS INT) work_address_exact,
        cast(d.name_exact AS INT) name_exact,cast(d.given_name_exact AS INT) given_name_exact,
        cast(d.family_name_exact AS INT) family_name_exact,cast(d.postcode_agreement AS INT) home_postcode_exact,
        cast(d.company_exact AS INT) company_exact,cast(d.cross_script AS INT) multilingual_name_pair,
        cast(d.dob_agreement AS INT) valid_dob_exact,
        cast(coalesce(d.dob_variant='CONFLICT',false) AS INT) valid_dob_contradiction,
        cast(d.left_dob_is_default AS INT) left_dob_is_default,cast(d.right_dob_is_default AS INT) right_dob_is_default,
        cast(d.personal_email_conflict AS INT) personal_email_disagrees,
        cast(d.personal_phone_conflict AS INT) personal_phone_disagrees,
        cast(d.name_edit_similarity AS DOUBLE) name_similarity,cast(d.address_edit_similarity AS DOUBLE) address_similarity,
        cast(d.ann_name_score AS DOUBLE) name_ann_similarity,cast(d.ann_address_score AS DOUBLE) address_ann_similarity,
        cast(NULL AS DOUBLE) name_ai_similarity,cast(NULL AS DOUBLE) address_ai_similarity,
        cast(d.match_probability AS DOUBLE) match_probability,cast(d.match_weight AS DOUBLE) match_weight,
        d.decision_reason"""
    # Native Splink per-comparison evidence (Bayes factors, tf adjustments, levels).
    native = [c for c in spark.table(decisions).columns
              if c.startswith(("bf_", "gamma_")) and not c.endswith(("_l", "_r"))]
    evidence += "".join(f",\n        d.{c}" if c.startswith("gamma_") else f",\n        cast(d.{c} AS DOUBLE) {c}"
                        for c in sorted(native))
    spark.sql(f"""CREATE OR REPLACE TABLE {ns}operational_review_evidence_snapshot AS
      SELECT {evidence} FROM {decisions} d
      WHERE d.left_record_key IN (SELECT record_key FROM {ns}operational_mapping_snapshot WHERE cluster_requires_review)
         OR d.right_record_key IN (SELECT record_key FROM {ns}operational_mapping_snapshot WHERE cluster_requires_review)""")
    # Gold-compatible projections (same columns as identity_resolution_demo.gold.*). Not published.
    spark.sql(f"""CREATE OR REPLACE TABLE {ns}gold_customer_master AS SELECT
      '{run_id}' resolution_run_id,'{resolver_version}' resolver_version,p.customer_id,p.given_name,p.family_name,
      p.personal_email,p.work_email,p.personal_phone,p.work_phone,p.date_of_birth,
      p.home_address_json home_address,p.work_address_json work_address,p.company_name,
      p.source_record_count,p.source_systems,current_timestamp() gold_updated_at
      FROM {f['gold_customer_profile']} p""")
    spark.sql(f"""CREATE OR REPLACE TABLE {ns}gold_customer_identity_links AS
      WITH strongest AS (SELECT k record_key,max(p) s FROM (
        SELECT left_record_key k,match_probability p FROM {decisions} WHERE decision='AUTO_MATCH'
        UNION ALL SELECT right_record_key,match_probability FROM {decisions} WHERE decision='AUTO_MATCH') GROUP BY k)
      SELECT m.customer_id,s.source_system,s.source_record_id,'{run_id}' resolution_run_id,
        '{resolver_version}' resolver_version,current_timestamp() resolved_at,
        CASE WHEN o.cluster_requires_review THEN 'REQUIRES_REVIEW' ELSE 'AUTO_RESOLVED' END mapping_status,
        o.cluster_requires_review,cast(st.s AS DOUBLE) strongest_edge_score
      FROM {mapping} m JOIN {f['source_snapshot']} s USING(record_key)
      JOIN {ns}operational_mapping_snapshot o USING(record_key)
      LEFT JOIN strongest st USING(record_key)""")
    spark.sql(f"""CREATE OR REPLACE TABLE {ns}gold_match_evidence AS SELECT
      '{run_id}' resolution_run_id,d.resolver_version,current_timestamp() resolved_at,d.left_record_key,d.right_record_key,
      array_join(d.candidate_scopes,',') candidate_scope,d.candidate_routes,d.candidate_reasons,
      filter(transform(d.candidate_reasons,x -> CASE WHEN x LIKE 'ANN_%' THEN lower(substring(x,5)) END),x -> x IS NOT NULL)
        candidate_representations,
      cast(d.match_probability AS DOUBLE) evidence_score,d.decision,
      concat(d.decision,': ',d.decision_reason) decision_explanation,
      cast(d.personal_email_exact AND NOT d.shared_email_exact AS INT) rare_email_exact,
      cast(d.personal_phone_exact AND NOT d.shared_phone_exact AS INT) rare_phone_exact,
      cast(d.shared_email_exact AS INT) shared_email_exact,cast(d.shared_phone_exact AS INT) shared_phone_exact,
      cast(d.name_exact AS INT) name_exact,cast(d.name_edit_similarity AS DOUBLE) name_similarity,
      cast(d.dob_agreement AS INT) valid_dob_exact,cast(coalesce(d.dob_variant='CONFLICT',false) AS INT) valid_dob_contradiction,
      cast(d.left_dob_is_default AS INT) left_dob_is_default,cast(d.right_dob_is_default AS INT) right_dob_is_default,
      cast(d.address_agreement AS INT) home_address_exact,cast(d.address_edit_similarity AS DOUBLE) address_similarity,
      cast(d.ann_name_score AS DOUBLE) name_ann_similarity,cast(d.ann_address_score AS DOUBLE) address_ann_similarity,
      d.safeguards_triggered
      FROM {decisions} d WHERE d.decision IN ('AUTO_MATCH','REVIEW')""")
    snapshots = {}
    for key, table in (("sources", "operational_source_snapshot"), ("mappings", "operational_mapping_snapshot"),
                       ("decisions", "operational_review_evidence_snapshot")):
        name = f"{ns}{table}"
        rows = spark.table(name).count() if key == "sources" else \
            spark.table(name).where(f"resolution_run_id='{run_id}'").count()
        snapshots[key] = {"table": name, "version": _version(spark, name), "row_count": rows}
    assert snapshots["sources"]["row_count"] == snapshots["mappings"]["row_count"]
    manifest = {"source_run_id": run_id, "release_job_run_id": int(job_run_id) if job_run_id else 0,
                "snapshots": snapshots,
                "metadata": {"resolver_version": resolver_version, "model_version": MODEL_VERSION,
                             "schema": ns.rstrip("."), "steward_cases": cases - len({v[0] for v in deferred.values()}),
                             "deferred_oversized_cases": len({v[0] for v in deferred.values()}),
                             "deferred_oversized_records": sum({v[0]: v[1] for v in deferred.values()}.values())}}
    gold = {t: {"table": f"{ns}{t}", "version": _version(spark, f"{ns}{t}"), "row_count": spark.table(f"{ns}{t}").count()}
            for t in ("gold_customer_master", "gold_customer_identity_links", "gold_match_evidence")}
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {ns}release_manifests (release_id STRING,resolver_version STRING,
      model_version STRING,manifest_json STRING,gold_projections_json STRING,published BOOLEAN,created_at TIMESTAMP) USING DELTA""")
    spark.createDataFrame([(run_id, resolver_version, MODEL_VERSION, json.dumps(manifest), json.dumps(gold), False)],
        "release_id string,resolver_version string,model_version string,manifest_json string,"
        "gold_projections_json string,published boolean").selectExpr("*", "current_timestamp() created_at") \
        .write.mode("append").saveAsTable(f"{ns}release_manifests")
    return {"manifest": manifest, "gold_projections": gold, "publication": "NOT_PUBLISHED"}
