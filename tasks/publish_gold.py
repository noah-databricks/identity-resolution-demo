"""Publish a frozen Splink v4 release into <catalog>.gold.

Reads the release's Gold projections at the Delta versions pinned in
resolution_splink_v4.release_manifests, so a later run cannot change what is
published. The Gold tables are managed Delta with Iceberg reads enabled (UniForm),
which is what Snowflake's Unity Catalog Iceberg REST catalog reads in place.
"""
import argparse
import json

from pyspark.sql import SparkSession

TABLES = {  # release projection -> Gold table
    "gold_customer_master": "customer_master",
    "gold_customer_identity_links": "customer_identity_links",
    "gold_match_evidence": "match_evidence",
}
ICEBERG = ("TBLPROPERTIES ('delta.columnMapping.mode'='name','delta.enableIcebergCompatV2'='true',"
           "'delta.universalFormat.enabledFormats'='iceberg')")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--catalog", required=True)
    args = parser.parse_args()
    release_id = args.release_id
    resolver, gold = f"{args.catalog}.resolution_splink_v4", f"{args.catalog}.gold"
    spark = SparkSession.builder.getOrCreate()
    rows = spark.sql(f"""SELECT gold_projections_json FROM {resolver}.release_manifests
      WHERE release_id = '{release_id}' ORDER BY created_at DESC LIMIT 1""").collect()
    assert rows, f"release {release_id} has no manifest; run the release stage first"
    projections = json.loads(rows[0][0])
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold}")
    for source, target in TABLES.items():
        pinned = projections[source]
        spark.sql(f"""CREATE OR REPLACE TABLE {gold}.{target} {ICEBERG}
          AS SELECT * FROM {pinned['table']} VERSION AS OF {pinned['version']}""")
        count = spark.table(f"{gold}.{target}").count()
        assert count == pinned["row_count"], (target, count, pinned["row_count"])
        print(f"{gold}.{target}: {count} rows from {pinned['table']} v{pinned['version']}")
    spark.sql(f"UPDATE {resolver}.release_manifests SET published = true WHERE release_id = '{release_id}'")


if __name__ == "__main__":
    main()
