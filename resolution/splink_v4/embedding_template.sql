-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Materialise identity search records
-- MAGIC
-- MAGIC This task runs after `contact_records` is refreshed and before AI
-- MAGIC Search synchronization. Embeddings are cached by a hash of their input text,
-- MAGIC so an unchanged field is never sent to the model again. Email, phone, DOB,
-- MAGIC payment data and opaque identifiers are deliberately excluded from prompts.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS __NS__identity_name_embedding_cache (
  text_hash STRING NOT NULL,
  search_text STRING NOT NULL,
  embedding ARRAY<FLOAT>,
  embedding_error STRING,
  raw_dimensions INT,
  model_name STRING NOT NULL,
  embedding_dimensions INT NOT NULL,
  embedding_version STRING NOT NULL,
  generated_at TIMESTAMP NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS __NS__identity_address_embedding_cache
LIKE __NS__identity_name_embedding_cache;

-- COMMAND ----------

MERGE INTO __NS__identity_name_embedding_cache AS current
USING (
  WITH source_text AS (
    SELECT DISTINCT
      sha2(name_search_text, 256) AS text_hash,
      name_search_text AS search_text
    FROM (
      SELECT nullif(trim(concat_ws(' | ',
        CASE WHEN full_name IS NOT NULL THEN concat('full name: ', full_name) END,
        CASE WHEN given_name IS NOT NULL THEN concat('given name: ', given_name) END,
        CASE WHEN family_name IS NOT NULL THEN concat('family name: ', family_name) END
      )), '') AS name_search_text
      FROM __NS__contact_base
    )
    WHERE name_search_text IS NOT NULL
  ), pending AS (
    SELECT s.*
    FROM source_text s
    LEFT JOIN __NS__identity_name_embedding_cache c USING (text_hash)
    WHERE c.text_hash IS NULL OR c.embedding IS NULL
  ), called AS (
    SELECT *, ai_query(
      'databricks-qwen3-embedding-0-6b', search_text, failOnError => false
    ) AS response
    FROM pending
  ), clipped AS (
    SELECT *, slice(response.result, 1, 512) AS clipped_vector
    FROM called
  ), normalised AS (
    SELECT *, sqrt(aggregate(
      clipped_vector, CAST(0 AS DOUBLE),
      (acc, x) -> acc + CAST(x AS DOUBLE) * CAST(x AS DOUBLE)
    )) AS l2_norm
    FROM clipped
  )
  SELECT
    text_hash, search_text,
    CASE WHEN response.errorMessage IS NULL AND size(response.result) >= 512 AND l2_norm > 0
      THEN transform(clipped_vector, x -> CAST(CAST(x AS DOUBLE) / l2_norm AS FLOAT))
    END AS embedding,
    CASE
      WHEN response.errorMessage IS NOT NULL THEN response.errorMessage
      WHEN size(response.result) < 512 THEN concat('model returned ', size(response.result), ' dimensions')
      WHEN l2_norm <= 0 THEN 'model returned a zero-norm vector'
    END AS embedding_error,
    size(response.result) AS raw_dimensions,
    'databricks-qwen3-embedding-0-6b' AS model_name,
    512 AS embedding_dimensions,
    'qwen3-0.6b-matryoshka-512-l2-v1' AS embedding_version,
    current_timestamp() AS generated_at
  FROM normalised
) AS incoming
ON current.text_hash = incoming.text_hash
WHEN MATCHED AND current.embedding IS NULL THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

MERGE INTO __NS__identity_address_embedding_cache AS current
USING (
  WITH source_text AS (
    SELECT DISTINCT
      sha2(address_search_text, 256) AS text_hash,
      address_search_text AS search_text
    FROM (
      SELECT nullif(trim(concat_ws(' | ',
        CASE WHEN home_address_line1 IS NOT NULL THEN concat('home address: ', home_address_line1) END,
        CASE WHEN home_suburb IS NOT NULL THEN concat('suburb: ', home_suburb) END,
        CASE WHEN home_state IS NOT NULL THEN concat('state: ', home_state) END,
        CASE WHEN home_postcode IS NOT NULL THEN concat('postcode: ', home_postcode) END,
        CASE WHEN home_country IS NOT NULL THEN concat('country: ', home_country) END
      )), '') AS address_search_text
      FROM __NS__contact_base
    )
    WHERE address_search_text IS NOT NULL
  ), pending AS (
    SELECT s.*
    FROM source_text s
    LEFT JOIN __NS__identity_address_embedding_cache c USING (text_hash)
    WHERE c.text_hash IS NULL OR c.embedding IS NULL
  ), called AS (
    SELECT *, ai_query(
      'databricks-qwen3-embedding-0-6b', search_text, failOnError => false
    ) AS response
    FROM pending
  ), clipped AS (
    SELECT *, slice(response.result, 1, 512) AS clipped_vector
    FROM called
  ), normalised AS (
    SELECT *, sqrt(aggregate(
      clipped_vector, CAST(0 AS DOUBLE),
      (acc, x) -> acc + CAST(x AS DOUBLE) * CAST(x AS DOUBLE)
    )) AS l2_norm
    FROM clipped
  )
  SELECT
    text_hash, search_text,
    CASE WHEN response.errorMessage IS NULL AND size(response.result) >= 512 AND l2_norm > 0
      THEN transform(clipped_vector, x -> CAST(CAST(x AS DOUBLE) / l2_norm AS FLOAT))
    END AS embedding,
    CASE
      WHEN response.errorMessage IS NOT NULL THEN response.errorMessage
      WHEN size(response.result) < 512 THEN concat('model returned ', size(response.result), ' dimensions')
      WHEN l2_norm <= 0 THEN 'model returned a zero-norm vector'
    END AS embedding_error,
    size(response.result) AS raw_dimensions,
    'databricks-qwen3-embedding-0-6b' AS model_name,
    512 AS embedding_dimensions,
    'qwen3-0.6b-matryoshka-512-l2-v1' AS embedding_version,
    current_timestamp() AS generated_at
  FROM normalised
) AS incoming
ON current.text_hash = incoming.text_hash
WHEN MATCHED AND current.embedding IS NULL THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS __NS__identity_search_records (
  record_key STRING NOT NULL,
  source_system STRING NOT NULL,
  source_record_id STRING NOT NULL,
  name_search_text STRING,
  address_search_text STRING,
  name_text_hash STRING,
  address_text_hash STRING,
  name_embedding ARRAY<FLOAT>,
  address_embedding ARRAY<FLOAT>,
  name_embedding_error STRING,
  address_embedding_error STRING,
  model_name STRING NOT NULL,
  embedding_dimensions INT NOT NULL,
  embedding_version STRING NOT NULL,
  name_embedding_updated_at TIMESTAMP,
  address_embedding_updated_at TIMESTAMP,
  source_updated_at TIMESTAMP,
  materialised_at TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking' = 'true'
);

MERGE INTO __NS__identity_search_records AS current
USING (
  WITH search_text AS (
    SELECT
      r.record_key, r.source_system, r.source_record_id,
      nullif(trim(concat_ws(' | ',
        CASE WHEN r.full_name IS NOT NULL THEN concat('full name: ', r.full_name) END,
        CASE WHEN r.given_name IS NOT NULL THEN concat('given name: ', r.given_name) END,
        CASE WHEN r.family_name IS NOT NULL THEN concat('family name: ', r.family_name) END
      )), '') AS name_search_text,
      nullif(trim(concat_ws(' | ',
        CASE WHEN r.home_address_line1 IS NOT NULL THEN concat('home address: ', r.home_address_line1) END,
        CASE WHEN r.home_suburb IS NOT NULL THEN concat('suburb: ', r.home_suburb) END,
        CASE WHEN r.home_state IS NOT NULL THEN concat('state: ', r.home_state) END,
        CASE WHEN r.home_postcode IS NOT NULL THEN concat('postcode: ', r.home_postcode) END,
        CASE WHEN r.home_country IS NOT NULL THEN concat('country: ', r.home_country) END
      )), '') AS address_search_text,
      r.source_updated_at
    FROM __NS__contact_base r
  ), hashed AS (
    SELECT *, sha2(name_search_text, 256) AS name_text_hash,
      sha2(address_search_text, 256) AS address_text_hash
    FROM search_text
  )
  SELECT
    h.record_key, h.source_system, h.source_record_id,
    h.name_search_text, h.address_search_text,
    h.name_text_hash, h.address_text_hash,
    n.embedding AS name_embedding,
    a.embedding AS address_embedding,
    n.embedding_error AS name_embedding_error,
    a.embedding_error AS address_embedding_error,
    'databricks-qwen3-embedding-0-6b' AS model_name,
    512 AS embedding_dimensions,
    'qwen3-0.6b-matryoshka-512-l2-v1' AS embedding_version,
    n.generated_at AS name_embedding_updated_at,
    a.generated_at AS address_embedding_updated_at,
    h.source_updated_at,
    current_timestamp() AS materialised_at
  FROM hashed h
  LEFT JOIN __NS__identity_name_embedding_cache n
    ON h.name_text_hash = n.text_hash
  LEFT JOIN __NS__identity_address_embedding_cache a
    ON h.address_text_hash = a.text_hash
) AS incoming
ON current.record_key = incoming.record_key
WHEN MATCHED AND NOT (
  current.name_text_hash <=> incoming.name_text_hash
  AND current.address_text_hash <=> incoming.address_text_hash
  AND current.source_system <=> incoming.source_system
  AND current.source_record_id <=> incoming.source_record_id
  AND current.name_embedding <=> incoming.name_embedding
  AND current.address_embedding <=> incoming.address_embedding
  AND current.name_embedding_error <=> incoming.name_embedding_error
  AND current.address_embedding_error <=> incoming.address_embedding_error
  AND current.source_updated_at <=> incoming.source_updated_at
) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS __NS__identity_name_index_source (
  record_key STRING NOT NULL,
  source_system STRING NOT NULL,
  source_record_id STRING NOT NULL,
  search_text STRING NOT NULL,
  text_hash STRING NOT NULL,
  embedding ARRAY<FLOAT> NOT NULL,
  embedding_version STRING NOT NULL,
  updated_at TIMESTAMP NOT NULL
) USING DELTA TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking' = 'true'
);

MERGE INTO __NS__identity_name_index_source AS current
USING (
  SELECT record_key, source_system, source_record_id,
    name_search_text AS search_text, name_text_hash AS text_hash,
    name_embedding AS embedding, embedding_version,
    coalesce(name_embedding_updated_at, materialised_at) AS updated_at
  FROM __NS__identity_search_records
  WHERE name_embedding IS NOT NULL
) AS incoming
ON current.record_key = incoming.record_key
WHEN MATCHED AND NOT (
  current.text_hash <=> incoming.text_hash
  AND current.embedding <=> incoming.embedding
  AND current.embedding_version <=> incoming.embedding_version
) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

CREATE TABLE IF NOT EXISTS __NS__identity_address_index_source
LIKE __NS__identity_name_index_source;

MERGE INTO __NS__identity_address_index_source AS current
USING (
  SELECT record_key, source_system, source_record_id,
    address_search_text AS search_text, address_text_hash AS text_hash,
    address_embedding AS embedding, embedding_version,
    coalesce(address_embedding_updated_at, materialised_at) AS updated_at
  FROM __NS__identity_search_records
  WHERE address_embedding IS NOT NULL
) AS incoming
ON current.record_key = incoming.record_key
WHEN MATCHED AND NOT (
  current.text_hash <=> incoming.text_hash
  AND current.embedding <=> incoming.embedding
  AND current.embedding_version <=> incoming.embedding_version
) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;
