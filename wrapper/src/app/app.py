"""프레임워크 구동 설정 — 인스턴스·lifespan(공유 자원). (design D8)"""
import asyncio
import os
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI

from app import api

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://ollama:11434")
MODEL = os.environ.get("MODEL", "qwen3:8b")
MAX_INFLIGHT = int(os.environ.get("MAX_INFLIGHT", "2"))
REWRITE_TIMEOUT_S = float(os.environ.get("REWRITE_TIMEOUT_S", "5"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.client = httpx.AsyncClient(base_url=OLLAMA_URL)
    app.state.sem = asyncio.Semaphore(MAX_INFLIGHT)
    app.state.model = MODEL
    app.state.rewrite_timeout = REWRITE_TIMEOUT_S
    yield
    await app.state.client.aclose()


app = FastAPI(title="study-note llm wrapper", lifespan=lifespan)
app.include_router(api.router)
