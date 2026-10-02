"""Bounded deterministic proposals; never unconditional identity merges."""

# Pair-generation compute bound per block. Blocks above it are not dropped: they are
# refined by family name (then given-name initial) and re-bounded, so a regular's
# large personal-email block still yields candidates while a placeholder block with
# thousands of unrelated guests cannot explode quadratically.
MAX_BLOCK = 50

BLOCKS = [
    ("email_n", "PERSONAL_EMAIL"), ("phone_n", "PERSONAL_PHONE"),
    ("work_email_n", "WORK_EMAIL"), ("work_phone_n", "WORK_PHONE"),
    ("home_address_n", "HOME_ADDRESS"), ("work_address_n", "WORK_ADDRESS"),
    ("CASE WHEN family_name_n IS NOT NULL AND dob_usable IS NOT NULL THEN concat_ws('|',family_name_n,cast(dob_usable as string)) END", "FAMILY_DOB"),
    ("CASE WHEN family_name_n IS NOT NULL AND given_name_n IS NOT NULL AND postcode_n IS NOT NULL THEN concat_ws('|',family_name_n,substring(given_name_n,1,1),postcode_n) END", "FAMILY_INITIAL_POSTCODE"),
    ("CASE WHEN phone_n IS NOT NULL THEN right(phone_n,6) END", "PHONE_SUFFIX"),
]


def deterministic(spark, ns):
    sql = " UNION ALL ".join(
        f"SELECT record_key,source_system,'{reason}' reason,{expr} block,family_name_n,"
        f"substring(given_name_n,1,1) initial FROM {ns}contact_base" for expr, reason in BLOCKS)
    spark.sql(f"""CREATE OR REPLACE TABLE {ns}blocking_keys AS
      WITH k AS (SELECT *,count(*) OVER(PARTITION BY reason,block) block_size
        FROM ({sql}) WHERE block IS NOT NULL),
      r1 AS (SELECT record_key,source_system,reason,
          CASE WHEN block_size<={MAX_BLOCK} THEN block
               WHEN family_name_n IS NOT NULL THEN concat(block,'#',family_name_n) END block,
          family_name_n,initial,block_size original_block_size FROM k),
      r2 AS (SELECT *,count(*) OVER(PARTITION BY reason,block) refined_size FROM r1 WHERE block IS NOT NULL),
      r3 AS (SELECT record_key,source_system,reason,
        CASE WHEN refined_size<={MAX_BLOCK} THEN block
             WHEN initial IS NOT NULL THEN concat(block,'#',initial) END block,original_block_size
        FROM r2)
      SELECT *,count(*) OVER(PARTITION BY reason,block) block_size FROM r3 WHERE block IS NOT NULL""")
    spark.sql(f"""CREATE OR REPLACE TABLE {ns}deterministic_candidate_hits AS
      SELECT DISTINCT a.record_key left_record_key,b.record_key right_record_key,
        a.reason, 'DETERMINISTIC' route,
        IF(a.source_system=b.source_system,'WITHIN_SOURCE','CROSS_SOURCE') scope
      FROM {ns}blocking_keys a JOIN {ns}blocking_keys b
      ON a.reason=b.reason AND a.block=b.block AND a.record_key < b.record_key
      WHERE a.block_size BETWEEN 2 AND {MAX_BLOCK}""")
    # Every record queries ANN. Coverage no longer depends on contact-sharing
    # profiles, so profile changes never require retrieval to be re-run.
    spark.sql(f"""CREATE OR REPLACE TABLE {ns}ann_query_records AS
      SELECT record_key FROM {ns}contact_base""")
    return {"deterministic_hits": spark.table(f"{ns}deterministic_candidate_hits").count(),
            "ann_query_records": spark.table(f"{ns}ann_query_records").count()}


def union(spark, ns):
    spark.sql(f"""CREATE OR REPLACE TABLE {ns}candidate_pairs AS
      SELECT left_record_key,right_record_key,sort_array(collect_set(reason)) candidate_reasons,
        sort_array(collect_set(route)) candidate_routes,sort_array(collect_set(scope)) candidate_scopes
      FROM (SELECT * FROM {ns}deterministic_candidate_hits UNION ALL
        SELECT left_record_key,right_record_key,concat('ANN_',upper(representation)) reason,
          'ANN' route,scope FROM {ns}ann_candidate_hits)
      GROUP BY left_record_key,right_record_key""")
    pairs = spark.table(f"{ns}candidate_pairs")
    assert not pairs.where("left_record_key >= right_record_key").take(1)
    return {"candidate_pairs": pairs.count()}
