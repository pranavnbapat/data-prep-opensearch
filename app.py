# app.py

from enum import Enum
import logging
import os
from pydantic import field_validator
from typing import Optional
from dotenv import load_dotenv

from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel

from download_mongodb_data import get_ko_metadata, select_environment
load_dotenv()
app = FastAPI(title="Data Prep")

MAX_PAGE_SIZE = 100

class EnvMode(str, Enum):
    DEV = "DEV"
    PRD = "PRD"

class RunParams(BaseModel):
    page_size: Optional[int] = None
    background: bool = True
    env_mode: EnvMode = EnvMode.DEV

    model_config = {"extra": "ignore"}

    @field_validator("page_size")
    @classmethod
    def clamp_page_size(cls, v: Optional[int]) -> Optional[int]:
        # Let None mean "use default from env (or 100)"
        if v is None:
            return None
        # If user sends 0 or negative, treat as None so we fall back to default 100
        if v <= 0:
            return None
        # Cap to 100
        return min(v, MAX_PAGE_SIZE)

def _effective_page_size(val: Optional[int]) -> int:
    # Default from env (or 100), then cap to 100, min 1 (defensive)
    env_default = int(os.getenv("DL_PAGE_SIZE", str(MAX_PAGE_SIZE)))
    ps = env_default if val is None else val
    if ps < 1:
        ps = 1
    if ps > MAX_PAGE_SIZE:
        ps = MAX_PAGE_SIZE
    return ps

@app.get("/healthz")
def healthz():
    return {"ok": True}

def _run_job(page_size: Optional[int], env_mode_value: str):
    # Switch creds/host first
    select_environment(env_mode_value)

    # Read static knobs from ENV
    mw = int(os.getenv("DL_MAX_WORKERS", "10"))
    sc = int(os.getenv("DL_SORT_CRITERIA", "1"))
    ps = _effective_page_size(page_size)

    logging.info(
        "Starting data-prep job (env=%s, page_size=%s, max_workers=%s, sort_criteria=%s)",
        env_mode_value, ps, mw, sc
    )
    try:
        get_ko_metadata(max_workers=mw, page_size=ps, sort_criteria=sc)
    finally:
        logging.info("Data-prep job finished")

@app.post("/run")
def trigger_run(params: RunParams, bg: BackgroundTasks):
    # Fail fast on bad/missing env vars before backgrounding
    try:
        select_environment(params.env_mode.value)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    ps = _effective_page_size(params.page_size)

    if params.background:
        bg.add_task(_run_job, ps, params.env_mode.value)
        return {"status": "scheduled", "params": {**params.model_dump(), "page_size": ps}}
    _run_job(ps, params.env_mode.value)
    return {"status": "completed", "params": {**params.model_dump(), "page_size": ps}}

