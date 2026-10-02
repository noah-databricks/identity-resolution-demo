"""Fit Splink on serverless, freeze its artifact, score all hybrid candidates."""

import hashlib
import json

from . import MODEL_VERSION, SPLINK_VERSION
from .backend import ServerlessSparkAPI, predict_candidate_pairs
from .model import settings, train
from .regularization import regularize
from .search import fingerprint
from .source import replace

FEATURE_COLUMNS = ["record_key", "name_n", "family_name_n", "personal_email_n",
                   "personal_phone_n", "dob_usable", "home_address_n", "postcode_n"]


def fit_model(spark, ns, run_id):
    import mlflow
    from pyspark.sql import functions as F
    from splink import Linker

    replace(spark, f"{ns}model_input", spark.table(f"{ns}contact_records").select(*FEATURE_COLUMNS))
    source_fingerprint = fingerprint(spark, f"{ns}model_input", FEATURE_COLUMNS)
    from pathlib import Path
    # The key covers the model specification itself, so changing comparisons or
    # training blocks can never silently reuse a previously fitted artifact.
    spec_hash = hashlib.sha256(Path(__file__).with_name("model.py").read_bytes()).hexdigest()
    key = hashlib.sha256((MODEL_VERSION + SPLINK_VERSION + spec_hash + source_fingerprint).encode()).hexdigest()
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {ns}model_artifacts (
      model_key STRING, model_version STRING, source_fingerprint STRING,
      settings_json STRING, mlflow_run_id STRING, trained_at TIMESTAMP) USING DELTA""")
    saved = spark.table(f"{ns}model_artifacts").where(F.col("model_key") == key).take(1)
    api = ServerlessSparkAPI(spark, *ns.split(".")[:2])
    if saved:
        artifact = json.loads(saved[0].settings_json)
        linker = Linker(f"{ns}model_input", artifact, db_api=api)
        mlflow_run_id = saved[0].mlflow_run_id
    else:
        linker = Linker(f"{ns}model_input", settings(), db_api=api)
        # Per-user experiment, so the pipeline runs unchanged in any workspace.
        user = spark.sql("SELECT current_user()").first()[0]
        # Directly under the user folder, which exists in every workspace.
        mlflow.set_experiment(f"/Users/{user}/identity-splink-v4-model-training")
        with mlflow.start_run(run_name=MODEL_VERSION + "-" + run_id) as run:
            # Estimate a global prior from high-precision observable rules.
            # Assumed rule recall is explicit (not claimed calibrated truth).
            prior_rules = [
                "l.personal_email_n=r.personal_email_n AND l.name_n=r.name_n AND l.dob_usable=r.dob_usable",
                "l.personal_phone_n=r.personal_phone_n AND l.name_n=r.name_n AND l.dob_usable=r.dob_usable",
            ]
            linker.training.estimate_probability_two_random_records_match(prior_rules, recall=0.7)
            train(linker)
            raw_artifact = linker.misc.save_model_to_json()
            artifact, smoothing = regularize(raw_artifact, spark.table(f"{ns}model_input").count())
            linker = Linker(f"{ns}model_input", artifact, db_api=api)
            mlflow.log_params({"model_version": MODEL_VERSION, "splink_version": SPLINK_VERSION,
                "source_fingerprint": source_fingerprint, "prior_rule_recall_assumption": 0.7,
                "auto_threshold": 0.995, "review_threshold": 0.5, "threshold_status": "PROVISIONAL_UNCALIBRATED",
                "backend": "Spark Connect / native SQL", "random_seed": 20260921})
            mlflow.log_dict(artifact, "splink-settings.json")
            mlflow.log_dict(raw_artifact, "splink-settings-before-smoothing.json")
            mlflow.log_dict(smoothing, "smoothing-audit.json")
            from pathlib import Path
            mlflow.log_dict({p.name:hashlib.sha256(p.read_bytes()).hexdigest()
                for p in Path(__file__).parent.iterdir() if p.suffix in {".py",".sql"}}, "code-content-hashes.json")
            mlflow.log_dict({"prior_rules": prior_rules, "features": FEATURE_COLUMNS,
                "training_labels": "none", "truth_access": "none"}, "training-contract.json")
            # These charts expose learned weights and estimates for DS inspection.
            for name, chart in (("match-weights", linker.visualisations.match_weights_chart()),
                                ("m-u-parameters", linker.visualisations.m_u_parameters_chart())):
                mlflow.log_dict(chart.to_dict(), f"diagnostics/{name}.vl.json")
            mlflow_run_id = run.info.run_id
        spark.createDataFrame([(key,MODEL_VERSION,source_fingerprint,json.dumps(artifact),mlflow_run_id)],
            "model_key string,model_version string,source_fingerprint string,settings_json string,mlflow_run_id string") \
            .withColumn("trained_at",F.current_timestamp()).write.mode("append").saveAsTable(f"{ns}model_artifacts")

    return linker, key, mlflow_run_id


def score(spark, ns, run_id):
    from pyspark.sql import functions as F
    from . import normalization as norm
    from . import policy
    linker, key, mlflow_run_id = fit_model(spark, ns, run_id)
    auto_threshold, review_threshold = policy.AUTO_THRESHOLD, policy.REVIEW_THRESHOLD
    policy_version = policy.POLICY_VERSION
    assert 0 <= review_threshold < auto_threshold <= 1
    predictions = predict_candidate_pairs(linker, spark.table(f"{ns}candidate_pairs")).as_spark_dataframe()
    replace(spark, f"{ns}splink_predictions", predictions)
    assert predictions.count() == spark.table(f"{ns}candidate_pairs").count()
    assert not predictions.where("match_probability IS NULL OR isnan(match_probability) OR match_probability<0 OR match_probability>1").take(1)
    spark.udf.register("v4_dob_variant", norm.dob_variant, "string")
    spark.udf.register("v4_given_variant", norm.given_variant, "string")
    spark.udf.register("v4_family_variant", norm.family_variant, "string")
    spark.sql(f"""CREATE OR REPLACE TABLE {ns}pair_features AS
      WITH ann AS (
        SELECT left_record_key,right_record_key,
          max(IF(representation='name',score,NULL)) ann_name_score,
          min(IF(representation='name',rank,NULL)) ann_name_rank,
          max(IF(representation='address',score,NULL)) ann_address_score,
          min(IF(representation='address',rank,NULL)) ann_address_rank
        FROM {ns}ann_candidate_hits GROUP BY left_record_key,right_record_key)
      SELECT p.*,
      c.candidate_routes,c.candidate_reasons,c.candidate_scopes,
      ann.ann_name_score,ann.ann_name_rank,ann.ann_address_score,ann.ann_address_rank,
      coalesce(a.dob_usable<>b.dob_usable,false) valid_dob_conflict,
      v4_dob_variant(a.dob_usable,b.dob_usable) dob_variant,
      coalesce(a.dob_usable=b.dob_usable,false) dob_agreement,
      a.dob_is_source_default OR b.dob_is_source_default source_default_dob,
      coalesce(a.dob_is_source_default,false) left_dob_is_default,
      coalesce(b.dob_is_source_default,false) right_dob_is_default,
      coalesce(a.personal_email_n=b.personal_email_n OR
        (a.personal_phone_n=b.personal_phone_n AND NOT
          (coalesce(pp.name_count,0)>1 AND coalesce(pp.dob_count,0)=0)),false) personal_contact,
      coalesce(a.personal_email_n=b.personal_email_n,false) personal_email_exact,
      coalesce(a.personal_phone_n=b.personal_phone_n,false) personal_phone_exact,
      coalesce(a.email_n=b.email_n,false) email_exact,
      coalesce(a.phone_n=b.phone_n,false) phone_exact,
      coalesce(a.email_n=b.email_n AND (a.email_is_shared OR b.email_is_shared),false) shared_email_exact,
      coalesce(a.phone_n=b.phone_n AND (a.phone_is_shared OR b.phone_is_shared),false) shared_phone_exact,
      IF(a.email_n=b.email_n,a.email_given_names,NULL) email_given_names,
      IF(a.email_n=b.email_n,a.email_record_count,NULL) email_record_count,
      IF(a.phone_n=b.phone_n,a.phone_given_names,NULL) phone_given_names,
      IF(a.phone_n=b.phone_n,a.phone_record_count,NULL) phone_record_count,
      coalesce(a.email_n<>b.email_n AND a.email_n IS NOT NULL AND b.email_n IS NOT NULL,false) personal_email_conflict,
      coalesce(a.phone_n<>b.phone_n AND a.phone_n IS NOT NULL AND b.phone_n IS NOT NULL,false) personal_phone_conflict,
      coalesce(a.name_n=b.name_n OR (1.0-levenshtein(a.name_n,b.name_n)/greatest(length(a.name_n),length(b.name_n),1))>=0.9,false) name_agreement,
      coalesce(a.name_n=b.name_n,false) name_exact,
      coalesce(a.given_key=b.given_key,false) given_name_exact,
      v4_given_variant(a.given_name,b.given_name) given_variant,
      v4_family_variant(a.family_name,b.family_name) family_variant,
      coalesce(a.family_name_n=b.family_name_n,false) family_name_exact,
      coalesce(a.given_key<>b.given_key,false) given_name_conflict,
      coalesce(a.family_name_n<>b.family_name_n,false) family_name_conflict,
      coalesce(size(array_intersect(split(a.name_n,' '),split(b.name_n,' '))) /
        least(size(split(a.name_n,' ')),size(split(b.name_n,' '))),0.0) name_token_overlap,
      coalesce(a.home_address_n=b.home_address_n,false) address_agreement,
      coalesce(a.work_address_n=b.work_address_n,false) work_address_exact,
      coalesce(a.postcode_n=b.postcode_n,false) postcode_agreement,
      coalesce(lower(trim(a.company_name))=lower(trim(b.company_name)),false) company_exact,
      coalesce(a.work_email_n=b.work_email_n OR a.work_phone_n=b.work_phone_n,false) work_contact_exact,
      coalesce((a.name_n RLIKE '[^\\\\x00-\\\\x7F]') <>
        (b.name_n RLIKE '[^\\\\x00-\\\\x7F]'),false) cross_script,
      coalesce(1.0-levenshtein(a.name_n,b.name_n)/
        greatest(length(a.name_n),length(b.name_n),1),0.0) name_edit_similarity,
      coalesce(1.0-levenshtein(a.home_address_n,b.home_address_n)/
        greatest(length(a.home_address_n),length(b.home_address_n),1),0.0) address_edit_similarity,
      coalesce((a.email_n=b.email_n AND (a.email_is_shared OR b.email_is_shared)) OR
        (a.phone_n=b.phone_n AND (a.phone_is_shared OR b.phone_is_shared)) OR
        a.work_email_n=b.work_email_n OR a.work_phone_n=b.work_phone_n,false) shared_context,
      '{key}' model_key,'{MODEL_VERSION}-{policy_version}' resolver_version,'{run_id}' resolution_run_id
      FROM {ns}splink_predictions p JOIN {ns}candidate_pairs c
        ON p.record_key_l=c.left_record_key AND p.record_key_r=c.right_record_key
      JOIN {ns}contact_records a ON a.record_key=p.record_key_l
      JOIN {ns}contact_records b ON b.record_key=p.record_key_r
      LEFT JOIN {ns}contact_prevalence pp ON pp.kind='phone'
        AND pp.value=a.personal_phone_n AND pp.value=b.personal_phone_n
      LEFT JOIN ann ON ann.left_record_key=p.record_key_l AND ann.right_record_key=p.record_key_r""")
    features = spark.table(f"{ns}pair_features")
    inputs = [c for c in policy.INPUTS]
    missing = set(inputs) - set(features.columns)
    assert not missing, f"Policy inputs missing from pair_features: {missing}"

    def decision(row):
        return policy.decide(row.asDict(), auto_threshold=auto_threshold, review_threshold=review_threshold)

    udf = F.udf(decision, "decision string, reason string")
    decisions = features.withColumn("policy", udf(F.struct(*inputs)))
    decisions = (decisions.withColumn("decision", F.col("policy.decision"))
        .withColumn("decision_reason", F.col("policy.reason")).drop("policy")
        .withColumn("left_record_key", F.col("record_key_l"))
        .withColumn("right_record_key", F.col("record_key_r"))
        .withColumn("safeguards_triggered", F.expr(policy.SAFEGUARDS_SQL)))
    replace(spark, f"{ns}match_decisions", decisions)
    return {"model_key":key,"mlflow_run_id":mlflow_run_id,"policy_version":policy_version,
        "decisions":{r.decision:r['count'] for r in spark.table(f"{ns}match_decisions").groupBy("decision").count().collect()},
        "auto_threshold":auto_threshold,"review_threshold":review_threshold}
