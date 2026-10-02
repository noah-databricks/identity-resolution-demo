# Databricks notebook source
# MAGIC %md
# MAGIC # Serverless identity resolution v4: trained Splink + hybrid candidates
# MAGIC Each task is rerunnable and truth-blind. Consumer publication is a separate, gated step.

# COMMAND ----------
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent))
dbutils.widgets.text("stage", "source")
dbutils.widgets.text("schema", "identity_resolution_demo.resolution_splink_v4")
dbutils.widgets.text("prefix", "")
dbutils.widgets.text("release_id", "splink-v4-dev-0")
dbutils.widgets.text("source_table", "snowflake_source.identity.source_identity_records")
dbutils.widgets.text("job_run_id", "")
stage = dbutils.widgets.get("stage")
schema = dbutils.widgets.get("schema")
prefix = dbutils.widgets.get("prefix")
run_id = dbutils.widgets.get("release_id")
source_table = dbutils.widgets.get("source_table")
job_run_id = dbutils.widgets.get("job_run_id")
assert re.fullmatch(r"[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*", schema)
assert re.fullmatch(r"([a-z][a-z0-9]*_)?", prefix)
assert re.fullmatch(r"[A-Za-z0-9_-]+", run_id)
assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", source_table)
# Resolver outputs may only land in the v4 resolver schema (truth-blind boundary).
assert schema.split(".")[1] == "resolution_splink_v4", schema
ns = f"{schema}.{prefix}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"USE CATALOG {schema.split('.')[0]}")
spark.sql(f"USE SCHEMA {schema.split('.')[1]}")

if stage == "source":
    from splink_v4.source import materialize
    result = materialize(spark, ns, run_id, source_table)
elif stage == "profile":
    from splink_v4.source import profile
    result = profile(spark, ns)
elif stage == "deterministic":
    from splink_v4.candidates import deterministic
    result = deterministic(spark, ns)
elif stage == "embeddings":
    from splink_v4.search import embeddings
    result = embeddings(spark, ns)
elif stage == "ann":
    from splink_v4.search import retrieve
    result = retrieve(spark, ns, run_id)
elif stage == "train":
    from splink_v4.scoring import fit_model
    _, model_key, mlflow_run_id = fit_model(spark, ns, run_id)
    result = {"model_key": model_key, "mlflow_run_id": mlflow_run_id}
elif stage == "score":
    from splink_v4.scoring import score
    result = score(spark, ns, run_id)
elif stage == "cluster":
    from splink_v4.release import cluster_and_freeze
    result = cluster_and_freeze(spark, ns, run_id)
elif stage == "release":
    from splink_v4.release import operational_release
    result = operational_release(spark, ns, run_id, job_run_id)
else:
    raise ValueError(f"Unknown stage {stage}")
dbutils.notebook.exit(json.dumps({"stage": stage, "release_id": run_id, "ns": ns, **result}, default=str))
