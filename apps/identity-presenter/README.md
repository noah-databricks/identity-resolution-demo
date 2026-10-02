# Identity resolution explainer

A read-only Databricks App that walks an audience through how the pipeline resolves
guest identities, stage by stage, up to the point where a steward is needed. Use the
chevrons on either side of the viewer to step through it.

Every record, candidate route, Splink comparison, probability, decision and count comes
from the frozen release, through `extract_release_evidence.py`. The vector universe,
point coordinates and neighbourhood regions are an illustrative projection.

| Path | Role |
|---|---|
| `app.py` | FastAPI: `/` serves the explainer, `/health` is a readiness probe |
| `static/` | The explainer page, styles, scripts and official product marks |
| `static/release-evidence.js` | Generated evidence. The setup job rewrites it from your release |
| `extract_release_evidence.py` | Read-only extraction of that evidence from the release tables |

To refresh the evidence by hand after a new release:

```bash
pip install databricks-sdk
python apps/identity-presenter/extract_release_evidence.py --profile <p> --warehouse <id> --release splink-v4-release
databricks bundle deploy
```

Run locally with `pip install -r requirements.txt && python app.py` (port 8000).
