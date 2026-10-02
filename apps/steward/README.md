# Steward app

The operational tool for the cases the pipeline could not decide on its own. A data
steward, or the review agent with the steward's confirmation, reads the evidence for one
case and decides whether its records are one guest or several.

- **Queue:** every uncertain group from the release, with its review reasons, filterable
  by "Same guest" and "Different people" (derived from source values, cannot-link pairs,
  review reasons and match probability only).
- **Case view:** the records side by side, the pair evidence and the graph of links.
- **Review agent:** calls the Unity Catalog model service
  `<catalog>.steward_ai.review_agent` through Unity AI Gateway (Responses API). It routes
  to GPT-5.6 Sol and falls back to GPT-5.5, then Claude Sonnet 5, on a 429 or 5xx. Runs
  are traced to the bundle's MLflow experiment. The agent proposes; the steward confirms
  the exact proposal, unless auto-approve mode is chosen.
- **State:** review cases, proposals and decisions live in Lakebase Postgres (schema
  `steward`). Decisions also go to an outbox for publication to Gold.

The app never reads synthetic truth, difficulty cohorts or correctness labels.

## Operator scripts

The setup job runs the import for you (`tasks/load_steward.py`). Run these by hand with a
Databricks CLI profile and the bundle's warehouse ID (`databricks bundle summary`):

| Script | Purpose |
|---|---|
| `import_release.py --manifest m.json --warehouse-id W [--apply] [--replace-release]` | Import a frozen release into the review queue (dry-run by default) |
| `import_review_context.py --warehouse-id W [--apply]` | Derive each case's review reasons |
| `publish_reviews.py --warehouse-id W [--apply]` | Publish steward decisions to `gold.reviewed_customer_identity_links`, `gold.reviewed_customer_profiles` and `gold.steward_decision_events` |
| `reset_demo.py --warehouse-id W [--apply]` | Restore the release baseline: undo every decision and republish |
| `regrant_app_principal.py <app-sp-client-id>` | Share schema ownership with a recreated app's service principal |

All take `--profile <name>`. Install their dependencies with
`pip install -r apps/steward/requirements.txt` and run them from `apps/steward`.

See [RESOLVER_CONTRACT.md](RESOLVER_CONTRACT.md) for the truth-blind boundary between
the resolver and the app.
