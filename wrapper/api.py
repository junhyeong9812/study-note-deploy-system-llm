"""라우터 — HTTP 관심사만. 도메인 오류를 계약된 상태코드(503·422)로 번역한다. (design D2·D3)"""
import time

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from domain.prompt import DigestIn, RewriteResult
from logger import log
from usecase import rewrite
from validate import SchemaViolation

router = APIRouter()


class RewriteIn(BaseModel):
    model_config = ConfigDict(extra="forbid")   # 계약 밖 필드 거부 — D2 입력 계약 강제

    request_id: str = Field(min_length=1, max_length=64)   # backend가 발행 (로그 규약)
    query: str = Field(min_length=1, max_length=300)


@router.post("/rewrite", response_model=RewriteResult)
async def post_rewrite(body: RewriteIn, request: Request):
    state = request.app.state
    started = time.monotonic()

    def elapsed_ms() -> int:
        return int((time.monotonic() - started) * 1000)

    try:
        result = await rewrite.run(
            body.request_id, body.query,
            client=state.client, sem=state.sem,
            model=state.model, timeout=state.rewrite_timeout,
        )
        await log(body.request_id, f"rewrite ok {elapsed_ms()}ms")   # 성공도 기록 (규약)
        return result
    except rewrite.Busy:
        await log(body.request_id, "rewrite rejected: busy", "warning")
        return JSONResponse({"error": "busy", "retry_after": 2}, status_code=503,
                            headers={"Retry-After": "2"})
    except rewrite.Timeout:
        await log(body.request_id, f"rewrite timeout {elapsed_ms()}ms", "error")
        return JSONResponse({"error": "upstream_timeout"}, status_code=503)
    except rewrite.Upstream as upstream_error:
        await log(body.request_id, f"rewrite upstream error: {str(upstream_error)[:200]}", "error")
        return JSONResponse({"error": "upstream", "detail": str(upstream_error)[:200]},
                            status_code=503)
    except SchemaViolation as violation:
        await log(body.request_id, f"rewrite schema violation: {violation.last_error}", "error")
        return JSONResponse({"error": "schema_violation", "detail": violation.last_error},
                            status_code=422)


@router.post("/digest")
async def post_digest(body: DigestIn):
    # 계약(DigestIn·DigestResult)만 예약, 구현은 2차 (design D5)
    await log(body.request_id, "digest not implemented", "warning")
    return JSONResponse({"error": "not_implemented"}, status_code=501)


@router.get("/health")
async def health(request: Request):
    state = request.app.state
    try:
        response = await state.client.get("/api/tags", timeout=2)
        response.raise_for_status()
        model_names = [model["name"] for model in response.json().get("models", [])]
        model_present = any(
            name == state.model or name.startswith(state.model + ":")
            for name in model_names
        )
        if not model_present:
            await log("health", f"model missing: {state.model}", "warning")
            return JSONResponse({"status": "model_missing", "model": state.model},
                                status_code=503)
        return {"status": "ok", "model": state.model}
    except (httpx.HTTPError, ValueError, KeyError, TypeError):
        await log("health", "ollama unreachable or invalid response", "warning")
        return JSONResponse({"status": "ollama_unreachable", "model": state.model},
                            status_code=503)
