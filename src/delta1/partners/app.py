"""FX-rates REST API of the mock partner."""

from __future__ import annotations

import os
import random
from datetime import UTC, date, datetime
from typing import Annotated

from fastapi import FastAPI, Header, HTTPException, Query

from delta1.partners.fx import history, hour_floor, rates_at

API_KEY = os.environ.get("PARTNER_API_KEY", "demo-key")
FAILURE_RATE = float(os.environ.get("FX_FAILURE_RATE", "0.2"))

app = FastAPI(title="Delta1 mock partner", version="1.0")
_rng = random.Random()


def _check(api_key: str | None) -> None:
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="invalid API key")
    if _rng.random() < FAILURE_RATE:
        # Real FX vendors throttle and time out; the DAG has to retry.
        raise HTTPException(status_code=503, detail="upstream temporarily unavailable")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/v1/fx/latest")
def latest(x_api_key: Annotated[str | None, Header()] = None) -> dict:
    _check(x_api_key)
    now = datetime.now(UTC)
    return {"base": "EUR", "as_of": hour_floor(now).isoformat(), "rates": rates_at(now)}


@app.get("/v1/fx/history")
def fx_history(
    day: Annotated[date, Query(alias="date")],
    x_api_key: Annotated[str | None, Header()] = None,
) -> dict:
    _check(x_api_key)
    return {"base": "EUR", "date": day.isoformat(), "quotes": history(day)}
