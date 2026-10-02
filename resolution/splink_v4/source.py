"""Source snapshot, normalisation and observed contact profiles. No generator metadata.

Every table is addressed as ``{ns}<name>`` where ``ns`` is ``catalog.schema.`` plus an
optional run prefix (for isolated out-of-distribution runs in the same schema).
"""

from . import normalization as norm

SOURCE_COLUMNS = (
    "source_system source_record_id given_name family_name full_name personal_email "
    "work_email personal_phone work_phone date_of_birth home_address_line1 home_suburb "
    "home_state home_postcode home_country work_address_line1 work_suburb work_state "
    "work_postcode work_country company_name source_updated_at"
).split()

# Read-only federated source. v4 never alters the Snowflake connection; an expired
# federation credential fails the run instead of being renewed from this job.
DEFAULT_SOURCE = "snowflake_source.identity.source_identity_records"

# Generic role mailbox local parts (real-world convention, unchanged from v3).
ROLE_MAILBOX = "^(events|info|bookings|reservations|admin|office|reception)@"


def replace(spark, table, frame):
    frame.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def materialize(spark, ns, run_id, source_table=DEFAULT_SOURCE):
    from pyspark.sql import functions as F
    frame = spark.table(source_table)
    # Explicit projection is a one-way leakage boundary, not SELECT *.
    frame = frame.select(*[F.col(c).alias(c) for c in SOURCE_COLUMNS])
    frame = (frame.withColumn("source_system", F.lower(F.trim("source_system")))
             .withColumn("source_record_id", F.trim("source_record_id"))
             .withColumn("source_updated_at", F.col("source_updated_at").cast("timestamp"))
             .withColumn("date_of_birth", F.col("date_of_birth").cast("date")))
    frame = frame.withColumn("record_key", F.sha2(F.concat_ws("|", "source_system", "source_record_id"), 256))
    frame = frame.withColumn("source_hash", F.sha2(F.to_json(F.struct(*SOURCE_COLUMNS), {"ignoreNullFields": "false"}), 256))
    replace(spark, f"{ns}source_snapshot", frame)
    source = spark.table(f"{ns}source_snapshot")
    n = source.count()
    assert 0 < n <= 250_000, f"Demo driver bound exceeded or source empty: {n}"
    assert source.select("record_key").distinct().count() == n, "Duplicate source keys"
    assert not source.where("source_system IS NULL OR source_system = '' OR source_record_id IS NULL OR source_record_id = ''").take(1)
    view = "v4_incoming_" + ns.replace(".", "_")
    source.withColumn("first_seen_run", F.lit(run_id)).withColumn("ingested_at", F.current_timestamp()).createOrReplaceTempView(view)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {ns}source_history USING DELTA AS SELECT * FROM {view} WHERE false")
    spark.sql(f"""MERGE INTO {ns}source_history t USING {view} s
      ON t.record_key=s.record_key AND t.source_hash=s.source_hash
      WHEN NOT MATCHED THEN INSERT *""")

    for name, fn in (("v4_text", norm.text), ("v4_email", norm.email),
                     ("v4_phone", norm.phone), ("v4_address", norm.address),
                     ("v4_given", norm.given_key)):
        spark.udf.register(name, fn, "string")
    spark.sql(f"""CREATE OR REPLACE TABLE {ns}contact_normalized AS SELECT *,
      v4_text(coalesce(nullif(full_name,''), concat_ws(' ',given_name,family_name))) name_n,
      v4_text(given_name) given_name_n, v4_text(family_name) family_name_n,
      v4_given(given_name) given_key,
      v4_email(personal_email) email_n, v4_email(work_email) work_email_n,
      v4_phone(personal_phone) phone_n, v4_phone(work_phone) work_phone_n,
      CASE WHEN nullif(trim(home_address_line1),'') IS NOT NULL THEN
        v4_address(concat_ws(' ',home_address_line1,home_suburb,home_state,home_postcode,home_country)) END home_address_n,
      CASE WHEN nullif(trim(work_address_line1),'') IS NOT NULL THEN
        v4_address(concat_ws(' ',work_address_line1,work_suburb,work_state,work_postcode,work_country)) END work_address_n,
      nullif(upper(trim(home_postcode)),'') postcode_n
      FROM {ns}source_snapshot""")
    spark.sql(f"""CREATE OR REPLACE TABLE {ns}default_dob_profile AS
      SELECT source_system,date_of_birth,count(*) value_count,
        count(*) / max(source_rows) prevalence,
        count(*) >= 25 AND count(*) / max(source_rows) >= 0.005 is_source_default
      FROM (SELECT *,count(*) OVER (PARTITION BY source_system) source_rows
        FROM {ns}contact_normalized) WHERE date_of_birth IS NOT NULL
      GROUP BY source_system,date_of_birth""")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {ns}source_priorities (source_system STRING, priority INT) USING DELTA")
    spark.createDataFrame([("sevenrooms",10),("momentus",20),("me_and_u",30),
        ("moshtix",40),("pos",40),("payments",40)],"source_system string,priority int").createOrReplaceTempView("v4_default_priorities")
    spark.sql(f"""MERGE INTO {ns}source_priorities t USING v4_default_priorities s
      ON t.source_system=s.source_system WHEN NOT MATCHED THEN INSERT *""")
    assert not spark.sql(f"SELECT source_system FROM {ns}source_priorities GROUP BY source_system HAVING count(*)>1").take(1)
    # Candidate generation, embeddings and ANN read this profile-independent base,
    # so contact-profile changes never invalidate retrieval.
    spark.sql(f"""CREATE OR REPLACE TABLE {ns}contact_base AS SELECT c.*,
      coalesce(d.is_source_default,false) dob_is_source_default,
      CASE WHEN NOT coalesce(d.is_source_default,false) AND c.date_of_birth BETWEEN DATE'1900-01-01' AND current_date()
        THEN c.date_of_birth END dob_usable,
      coalesce(pr.priority,100) source_priority
      FROM {ns}contact_normalized c
      LEFT JOIN {ns}default_dob_profile d USING(source_system,date_of_birth)
      LEFT JOIN {ns}source_priorities pr ON pr.source_system=c.source_system""")
    return {"source_records": n, "contacts": spark.table(f"{ns}contact_base").count()}


PROFILE_VERSION = "observed-dob-conflict"


def shared_rule(kind, alias):
    """SQL predicate: is this record's email/phone observed to be shared?"""
    if PROFILE_VERSION == "v3-equivalent":
        role = f" OR c.email_n RLIKE '{ROLE_MAILBOX}'" if kind == "email" else ""
        return (f"coalesce({alias}.dob_count > 1 OR {alias}.name_count > 8 OR "
                f"{alias}.record_count > 30{role},false)")
    if PROFILE_VERSION == "observed-dob-conflict":
        # Shared only on direct evidence that two different people use the contact:
        # two valid DOBs that are not entry variants of each other. No record-count rule.
        role = f" OR c.email_n RLIKE '{ROLE_MAILBOX}'" if kind == "email" else ""
        return f"coalesce({alias}.incompatible_dob_pairs > 0{role},false)"
    raise ValueError(PROFILE_VERSION)


def profile(spark, ns):
    """Observed contact-sharing evidence, derived from the data rather than record counts.

    A personal contact used by one person over time accumulates name spellings and
    DOB entry variants (dd/mm order, one-digit slips) but its DOBs stay *compatible*.
    A contact used by several people carries *incompatible* valid DOBs or several
    unrelated given names. Record volume alone is never evidence of sharing: real
    regulars legitimately have many records.
    """
    spark.udf.register("v4_dob_compatible", norm.dob_compatible, "boolean")
    spark.sql(f"""CREATE OR REPLACE TABLE {ns}contact_prevalence AS
      WITH expanded AS (
        SELECT record_key,name_n,given_key,family_name_n,dob_usable,
          explode(array(named_struct('kind','email','value',email_n),
            named_struct('kind','phone','value',phone_n))) contact
        FROM {ns}contact_base),
      e AS (SELECT * FROM expanded WHERE contact.value IS NOT NULL),
      dobs AS (SELECT DISTINCT contact.kind kind,contact.value value,dob_usable FROM e WHERE dob_usable IS NOT NULL),
      conflicts AS (
        SELECT a.kind,a.value,count_if(NOT v4_dob_compatible(a.dob_usable,b.dob_usable)) incompatible_dob_pairs
        FROM dobs a JOIN dobs b ON a.kind=b.kind AND a.value=b.value AND a.dob_usable<b.dob_usable
        GROUP BY a.kind,a.value)
      SELECT e.contact.kind kind,e.contact.value value,count(*) record_count,
        count(DISTINCT name_n) name_count,count(DISTINCT given_key) given_name_count,count(DISTINCT family_name_n) family_name_count,
        count(DISTINCT dob_usable) dob_count,coalesce(max(c.incompatible_dob_pairs),0) incompatible_dob_pairs
      FROM e LEFT JOIN conflicts c ON c.kind=e.contact.kind AND c.value=e.contact.value
      GROUP BY e.contact.kind,e.contact.value""")
    shared = {kind: shared_rule(kind, alias) for kind, alias in (("email", "e"), ("phone", "p"))}
    spark.sql(f"""CREATE OR REPLACE TABLE {ns}contact_records AS SELECT c.*,
      {shared['email']} email_is_shared,
      {shared['phone']} phone_is_shared,
      CASE WHEN NOT {shared['email']} THEN c.email_n END personal_email_n,
      CASE WHEN NOT {shared['phone']} THEN c.phone_n END personal_phone_n,
      e.record_count email_record_count,e.given_name_count email_given_names,
      p.record_count phone_record_count,p.given_name_count phone_given_names
      FROM {ns}contact_base c
      LEFT JOIN {ns}contact_prevalence e ON e.kind='email' AND e.value=c.email_n
      LEFT JOIN {ns}contact_prevalence p ON p.kind='phone' AND p.value=c.phone_n""")
    stats = spark.sql(f"""SELECT count(*) n,count_if(email_is_shared) shared_email,
      count_if(phone_is_shared) shared_phone FROM {ns}contact_records""").first().asDict()
    return stats
