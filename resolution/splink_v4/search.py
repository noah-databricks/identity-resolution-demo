"""SQL embeddings and real, resumable, bounded-concurrency AI Search ANN."""

import hashlib
import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .source import replace

MODEL = "databricks-qwen3-embedding-0-6b"
VERSION = "qwen3-0.6b-matryoshka-512-l2-v1"
ENDPOINT = "identity-resolution-search"


def fingerprint(spark, table, columns):
    expr = ",".join(columns)
    rows = spark.sql(f"""WITH hashes AS (SELECT sha2(to_json(struct({expr})),256) h FROM {table})
      SELECT substring(h,1,2) bucket,sha2(concat_ws('',sort_array(collect_list(h))),256) h
      FROM hashes GROUP BY substring(h,1,2) ORDER BY bucket""").collect()
    return hashlib.sha256("".join(r.h for r in rows).encode()).hexdigest()


# Read-only, content-addressed vector caches from earlier resolver runs. Keys are
# hashes of resolver-visible name/address text only; nothing else is copied.
REUSABLE_SCHEMAS = ("resolution", "resolution_splink", "bakeoff_splink_v4")  # in the resolver's catalog


def index_name(ns, representation):
    schema, prefix = ns.rstrip(".").rsplit(".", 1) if not ns.endswith(".") else (ns[:-1], "")
    return f"{schema}.splink_v4_{prefix}identity_{representation}_idx"


def embeddings(spark, ns):
    template = Path(__file__).with_name("embedding_template.sql").read_text().replace("__NS__", ns)
    # These templates contain no string literals with semicolons.
    statements = [s.strip() for s in template.split(";") if s.strip() and not all(
        not line.strip() or line.lstrip().startswith("--") for line in s.splitlines())]
    # Create the two caches first, then safely reuse content-addressed vectors.
    for statement in statements[:2]:
        spark.sql(statement)
    for representation in ("name", "address"):
        cache = f"identity_{representation}_embedding_cache"
        catalog = ns.split(".")[0]
        for old in (f"{catalog}.{s}.{cache}" for s in REUSABLE_SCHEMAS):
            if spark.catalog.tableExists(old):
                spark.sql(f"""MERGE INTO {ns}{cache} t USING (
                  SELECT * FROM {old} WHERE model_name='{MODEL}' AND embedding_version='{VERSION}'
                    AND embedding_dimensions=512 AND size(embedding)=512
                    AND text_hash=sha2(search_text,256) AND embedding_error IS NULL
                    AND forall(embedding,x -> x IS NOT NULL AND NOT isnan(x) AND abs(x)<=1)
                    AND abs(sqrt(aggregate(embedding,cast(0 as double),(a,x)->a+x*x))-1)<0.001
                  QUALIFY row_number() OVER(PARTITION BY text_hash ORDER BY generated_at DESC)=1
                  ) s ON t.text_hash=s.text_hash WHEN NOT MATCHED THEN INSERT *""")
    for statement in statements[2:]:
        spark.sql(statement)
    stats = {}
    for representation in ("name", "address"):
        bad = spark.sql(f"""SELECT count(*) FROM {ns}identity_search_records
          WHERE {representation}_search_text IS NOT NULL AND (
            {representation}_embedding IS NULL OR size({representation}_embedding)<>512 OR
            NOT forall({representation}_embedding,x -> x IS NOT NULL AND NOT isnan(x) AND abs(x)<=1) OR
            abs(sqrt(aggregate({representation}_embedding,cast(0 as double),(a,x)->a+x*x))-1)>0.001)""").first()[0]
        assert bad == 0, f"{representation}: {bad} invalid/missing embeddings"
        stats[representation] = sync(spark, ns, representation)
    return stats


def sync(spark, ns, representation):
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import NotFound
    from databricks.sdk.service.vectorsearch import (DeltaSyncVectorIndexSpecRequest,
        EmbeddingVectorColumn, PipelineType, VectorIndexType)
    w = WorkspaceClient()
    name, source = index_name(ns, representation), f"{ns}identity_{representation}_index_source"
    count = spark.table(source).count()
    try:
        info = w.vector_search_indexes.get_index(name)
    except NotFound:
        w.vector_search_indexes.create_index(name=name, endpoint_name=ENDPOINT,
            primary_key="record_key", index_type=VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
                source_table=source, pipeline_type=PipelineType.TRIGGERED,
                embedding_vector_columns=[EmbeddingVectorColumn(name="embedding",embedding_dimension=512)]))
        info = None
    deadline = time.monotonic() + 1800
    while time.monotonic() < deadline:
        info = w.vector_search_indexes.get_index(name)
        if info.status and info.status.ready:
            break
        time.sleep(10)
    else:
        raise TimeoutError(f"Index creation timed out: {name}")
    pipeline = info.delta_sync_index_spec.pipeline_id
    # Query serving can become ready before the initial pipeline update has
    # terminated. A second sync is rejected while that update is still RUNNING.
    deadline = time.monotonic() + 1800
    while time.monotonic() < deadline:
        updates = w.pipelines.list_updates(pipeline_id=pipeline).updates or []
        if not updates or str(getattr(updates[0].state,"value",updates[0].state)) in {"COMPLETED","FAILED","CANCELED"}:
            break
        time.sleep(10)
    else:
        raise TimeoutError(f"Existing index update has not terminated: {name}")
    before = {u.update_id for u in (w.pipelines.list_updates(pipeline_id=pipeline).updates or [])}
    w.vector_search_indexes.sync_index(index_name=name)
    # 'ready' stays true while a triggered update runs: require a NEW completed
    # update, plus the expected row count, not merely the old ONLINE status.
    while time.monotonic() < deadline:
        updates = w.pipelines.list_updates(pipeline_id=pipeline).updates or []
        new = [u for u in updates if u.update_id not in before]
        failed = [u for u in new if str(getattr(u.state,"value",u.state)) in {"FAILED","CANCELED"}]
        if failed:
            raise RuntimeError(f"Index sync failed: {name}, update {failed[0].update_id}")
        if any(str(getattr(u.state,"value",u.state)) == "COMPLETED" for u in new):
            info = w.vector_search_indexes.get_index(name)
            if info.status.ready and info.status.indexed_row_count == count:
                return {"index": name, "indexed_records": count}
        time.sleep(10)
    raise TimeoutError(f"No completed fresh sync/count agreement: {name}")


def retrieve(spark, ns, run_id):
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import functions as F
    from .candidates import union
    population = fingerprint(spark, f"{ns}identity_search_records",
        ["record_key", "name_text_hash", "address_text_hash", "embedding_version"])
    hit_schema = "population string, query_key string, neighbour_key string, representation string, rank int, score double, attempt_id string"
    checkpoint_schema = "population string, query_key string, representation string, attempt_id string"
    for table, ddl in (("ann_vector_hit_cache", hit_schema), ("ann_vector_checkpoints", checkpoint_schema)):
        spark.sql(f"CREATE TABLE IF NOT EXISTS {ns}{table} ({ddl}) USING DELTA")
    w = WorkspaceClient()
    queried = 0
    started = time.monotonic()
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {ns}ann_retrieval_progress (
      release_id STRING,representation STRING,completed_queries BIGINT,elapsed_seconds DOUBLE,
      recorded_at TIMESTAMP) USING DELTA""")
    for representation in ("name", "address"):
        # Identical cached vectors have the same ANN query. Cache the raw top21
        # once per text hash, then remove each record's self-hit during expansion.
        # This is request deduplication, not pre-merging people with the same name.
        pending = spark.sql(f"""SELECT q.text_hash record_key,e.embedding FROM (
          SELECT DISTINCT s.{representation}_text_hash text_hash
          FROM {ns}identity_search_records s JOIN {ns}ann_query_records q USING(record_key)
          WHERE s.{representation}_embedding IS NOT NULL) q
          JOIN {ns}identity_{representation}_embedding_cache e USING(text_hash)
          LEFT ANTI JOIN {ns}ann_vector_checkpoints c ON c.query_key=q.text_hash
            AND c.representation='{representation}' AND c.population='{population}'
          """)
        index = index_name(ns, representation)

        def query(row):
            for attempt in range(4):
                try:
                    result = w.vector_search_indexes.query_index(index_name=index,
                        columns=["record_key"], query_vector=list(row.embedding),
                        query_type="ANN", num_results=21)
                    neighbours = result.result.data_array or []
                    assert len(neighbours) == 21 and len({x[0] for x in neighbours}) == 21, "Incomplete ANN response"
                    return row.record_key, [(population,row.record_key,x[0],representation,i+1,float(x[-1]),batch_attempt)
                                            for i,x in enumerate(neighbours)]
                except Exception:
                    if attempt == 3:
                        raise
                    time.sleep(2 ** attempt)

        # Do not keep a Spark Connect result iterator open while doing remote
        # ANN calls: buffered results can outlive the operation's inactivity TTL.
        # Each keyset page is a complete, bounded Spark action before API work.
        last_key = ""
        with ThreadPoolExecutor(max_workers=32) as pool:
            while batch := pending.where(F.col("record_key") > last_key).orderBy("record_key").limit(1000).collect():
                batch_attempt = str(uuid.uuid4())
                answers = list(pool.map(query, batch))
                hits = [hit for _,values in answers for hit in values]
                if hits:
                    spark.createDataFrame(hits,hit_schema).write.mode("append").saveAsTable(f"{ns}ann_vector_hit_cache")
                # Commit checkpoint only after hits. The attempt ID makes hits
                # from a crash before this commit ineligible for expansion.
                spark.createDataFrame([(population,key,representation,batch_attempt) for key,_ in answers],checkpoint_schema).write.mode("append").saveAsTable(f"{ns}ann_vector_checkpoints")
                queried += len(batch)
                last_key = batch[-1].record_key
                spark.createDataFrame([(run_id,representation,queried,time.monotonic()-started)],
                    "release_id string,representation string,completed_queries long,elapsed_seconds double") \
                    .withColumn("recorded_at",F.current_timestamp()).write.mode("append").saveAsTable(f"{ns}ann_retrieval_progress")
    spark.sql(f"""CREATE OR REPLACE TABLE {ns}ann_candidate_hits AS
      WITH query_hashes AS (
        SELECT s.record_key,'name' representation,s.name_text_hash text_hash
        FROM {ns}identity_search_records s JOIN {ns}ann_query_records q USING(record_key)
        UNION ALL
        SELECT s.record_key,'address' representation,s.address_text_hash text_hash
        FROM {ns}identity_search_records s JOIN {ns}ann_query_records q USING(record_key)
      ), raw_hits AS (
        SELECT DISTINCT h.* FROM {ns}ann_vector_hit_cache h
        JOIN {ns}ann_vector_checkpoints c USING(population,query_key,representation,attempt_id)
        WHERE h.population='{population}'
      ), expanded AS (
        SELECT q.record_key query_key,h.neighbour_key,h.representation,h.score,
          row_number() OVER(PARTITION BY q.record_key,h.representation ORDER BY h.rank,h.neighbour_key) rank
        FROM query_hashes q JOIN raw_hits h ON h.query_key=q.text_hash AND h.representation=q.representation
        WHERE q.record_key<>h.neighbour_key
      )
      SELECT DISTINCT least(query_key,neighbour_key) left_record_key,
        greatest(query_key,neighbour_key) right_record_key,h.representation,
        h.query_key,h.rank,h.score,
        IF(a.source_system=b.source_system,'WITHIN_SOURCE','CROSS_SOURCE') scope,
        '{population}' population
      FROM expanded h
      JOIN {ns}contact_base a ON a.record_key=h.query_key
      JOIN {ns}contact_base b ON b.record_key=h.neighbour_key
      WHERE h.rank<=20""")
    return {"new_ann_queries": queried, "population": population,
            "elapsed_seconds":time.monotonic()-started, **union(spark,ns)}
