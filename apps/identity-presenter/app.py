"""FastAPI app that serves the identity-resolution presenter.

- ``/`` serves the presenter page and its assets from ``static/``.
- ``/health`` is a lightweight readiness probe.

The app is read-only. It serves evidence extracted from the Splink v4 resolver
release (``extract_release_evidence.py``); it does not trigger or query the
resolver pipeline at runtime. The vector universe remains an illustrative
projection.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "static"
PRESENTER = STATIC_DIR / "identity-resolution-presenter.html"


def _release_evidence() -> dict:
    text = (STATIC_DIR / "release-evidence.js").read_text(encoding="utf-8")
    start, end = text.index("=") + 1, text.rstrip().rindex(";")
    return json.loads(text[start:end])


RELEASE = _release_evidence()

app = FastAPI(
    title="Identity Resolution Presenter",
    description="Presenter explainer: candidate retrieval through governed identity decisions.",
    version="0.2.0",
)


@app.get("/health", tags=["health"])
def health() -> dict:
    return {
        "status": "healthy",
        "app": "identity-presenter",
        "presenter": PRESENTER.exists(),
        "release_id": RELEASE["release"]["release_id"],
        "scenarios": len(RELEASE["scenarios"]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/", include_in_schema=False)
def presenter() -> FileResponse:
    return FileResponse(PRESENTER, media_type="text/html")


app.mount("/", StaticFiles(directory=STATIC_DIR), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("DATABRICKS_APP_PORT", "8000")))
