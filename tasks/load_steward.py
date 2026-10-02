"""Load a published release into the Steward app's Lakebase review queue.

Runs as the last part of the setup job, as the job's run-as user:

1. Creates the Steward schema if the app has not created it yet.
2. Shares ownership of it with the app's service principal (regrant_app_principal.py).
3. Imports the release (import_release.py). Rerunning with the same release replays
   the earlier import; a rebuilt release replaces the active one.
4. Derives each case's review reasons (import_review_context.py).
"""
import argparse
import json
import os
import sys
import tempfile
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    for name in ("--catalog", "--release-id", "--warehouse-id", "--lakebase-endpoint",
                 "--steward-app", "--steward-dir"):
        parser.add_argument(name, required=True)
    args = parser.parse_args()

    os.environ.update(DEMO_CATALOG=args.catalog, LAKEBASE_ENDPOINT=args.lakebase_endpoint)
    sys.path.insert(0, args.steward_dir)
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession
    import import_release
    import import_review_context
    from lakebase import create_operator_pool
    from regrant_app_principal import share_ownership

    spark = SparkSession.builder.getOrCreate()
    rows = spark.sql(f"""SELECT manifest_json FROM {args.catalog}.resolution_splink_v4.release_manifests
      WHERE release_id = '{args.release_id}' ORDER BY created_at DESC LIMIT 1""").collect()
    assert rows, f"release {args.release_id} has no manifest; run the release stage first"
    manifest = json.loads(rows[0][0])
    path = Path(tempfile.mkdtemp()) / "manifest.json"
    path.write_text(json.dumps(manifest))

    app_principal = WorkspaceClient().apps.get(args.steward_app).service_principal_client_id
    pool = create_operator_pool()
    with pool, pool.connection() as conn:
        if conn.execute("SELECT to_regclass('steward.import_batches')").fetchone()[0] is None:
            with conn.transaction():
                conn.execute((Path(args.steward_dir) / "schema.sql").read_text())
        print("ownership", share_ownership(conn, os.environ["PGUSER"], app_principal))
        active = conn.execute("SELECT source_run_id FROM steward.import_batches WHERE active").fetchone()

    importer = ["--manifest", str(path), "--warehouse-id", args.warehouse_id, "--apply"]
    if active and active[0] != manifest["source_run_id"]:
        print(f"Replacing active release {active[0]} with {manifest['source_run_id']}")
        importer.append("--replace-release")
    import_release.main(importer)
    import_review_context.main(["--warehouse-id", args.warehouse_id, "--apply"])


if __name__ == "__main__":
    main()
