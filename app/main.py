from __future__ import annotations

import asyncio
import os
import random
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException

from .generator import (
    apply_filters,
    build_dataset,
    decode_page_token,
    encode_page_token,
)
from .models import ExtractRequest, ExtractResponse, SAP_MB25_FIELDS


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


class Settings:
    seed = _env_int("MOCK_SEED", 42)
    days_back = _env_int("MOCK_DAYS_BACK", 120)
    records_per_day = _env_int("MOCK_RECORDS_PER_DAY", 120)
    latency_ms_min = _env_int("MOCK_LATENCY_MS_MIN", 250)
    latency_ms_max = _env_int("MOCK_LATENCY_MS_MAX", 1200)
    timeout_rate = _env_float("MOCK_TIMEOUT_RATE", 0.01)
    error_rate = _env_float("MOCK_ERROR_RATE", 0.02)
    duplicate_rate = _env_float("MOCK_DUPLICATE_RATE", 0.03)
    timeout_seconds = _env_int("MOCK_TIMEOUT_SECONDS", 25)
    token = os.getenv("MOCK_TOKEN", "")


settings = Settings()
app = FastAPI(title="sap-mock", version="1.0.0")
dataset = build_dataset(settings.days_back, settings.records_per_day, settings.seed)
request_rng = random.Random(settings.seed + 999)


@app.get("/health")
async def health() -> dict[str, str | int]:
    return {
        "status": "ok",
        "dataset_size": len(dataset),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/schema")
async def schema() -> dict[str, list[str]]:
    return {"fields": SAP_MB25_FIELDS}


@app.get("/version")
async def version() -> dict[str, str]:
    return {"service": "sap-mock", "version": "1.0.0"}


@app.post("/rfc/Z_MB25_EXTRACT", response_model=ExtractResponse)
async def z_mb25_extract(
    req: ExtractRequest,
    x_mock_token: str | None = Header(default=None),
) -> ExtractResponse:
    if settings.token and x_mock_token != settings.token:
        raise HTTPException(status_code=401, detail="Invalid token")

    if req.from_date_obj > req.to_date_obj:
        raise HTTPException(status_code=400, detail="from_date cannot be greater than to_date")

    simulated_latency_ms = request_rng.randint(settings.latency_ms_min, settings.latency_ms_max)
    await asyncio.sleep(simulated_latency_ms / 1000)

    if request_rng.random() < settings.timeout_rate:
        await asyncio.sleep(settings.timeout_seconds)

    if request_rng.random() < settings.error_rate:
        raise HTTPException(status_code=500, detail="RFC temporary failure")

    filtered = apply_filters(dataset, req)
    start = decode_page_token(req.page_token)
    end = start + req.page_size

    page = filtered[start:end]
    if page and request_rng.random() < settings.duplicate_rate:
        page.append(page[-1].copy())

    next_page_token = encode_page_token(end) if end < len(filtered) else None

    return ExtractResponse(
        request_id=str(uuid4()),
        source_ts=datetime.now(timezone.utc).isoformat(),
        total_estimated=len(filtered),
        next_page_token=next_page_token,
        records=page,
    )
