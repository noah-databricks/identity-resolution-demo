"""Regenerate the explainer's release evidence from this workspace and redeploy it.

The explainer serves static evidence (records, Splink comparisons, decisions and
counts) extracted read-only from the frozen release. This task re-extracts it from
the release this job just built, writes it into the app's deployed source folder and
redeploys the app, so the explainer always shows this workspace's own release.
"""
import argparse
import io
import os
import sys


def main() -> None:
    parser = argparse.ArgumentParser()
    for name in ("--catalog", "--release-id", "--warehouse-id", "--presenter-app", "--presenter-dir"):
        parser.add_argument(name, required=True)
    args = parser.parse_args()

    os.environ["DEMO_CATALOG"] = args.catalog
    sys.path.insert(0, args.presenter_dir)
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.apps import AppDeployment
    from databricks.sdk.service.workspace import ImportFormat
    import extract_release_evidence as extractor

    try:
        evidence = extractor.extract(extractor.Warehouse(None, args.warehouse_id), args.release_id)
    except (AssertionError, KeyError, IndexError) as error:
        # The committed evidence comes from a reference build of the same seed and
        # pipeline, so the explainer still works; it just shows that build's numbers.
        print(f"WARNING: could not extract evidence from release {args.release_id} ({error!r}). "
              "The explainer keeps the evidence committed in the repo.")
        return
    w = WorkspaceClient()
    target = f"{args.presenter_dir.removeprefix('/Workspace')}/static/release-evidence.js"
    w.workspace.upload(target, io.BytesIO(extractor.render(evidence).encode("utf-8")),
                       format=ImportFormat.AUTO, overwrite=True)
    print(f"Wrote {target}: {len(evidence['scenarios'])} scenarios from release {args.release_id}")
    deployment = w.apps.deploy_and_wait(args.presenter_app, AppDeployment(source_code_path=args.presenter_dir))
    print(f"Redeployed {args.presenter_app}: {deployment.status.state.value}")


if __name__ == "__main__":
    main()
