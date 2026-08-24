"""라우터 — HTTP 관심사만. 도메인 오류를 계약된 상태코드(503·422)로 번역한다. (design D2·D3)"""
import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from domain.prompt import DigestIn, RewriteResult
from usecase import rewrite
from validate import SchemaViolation

router = APIRouter()


class RewriteIn(BaseModel):
    model_config = ConfigDict(extra="forbid")   # 계약 밖 필드 거부 — D2 입력은 query뿐

    query: str = Field(min_length=1, max_length=300)


@router.post("/rewrite", response_model=RewriteResult)
async def post_rewrite(body: RewriteIn, request: Request):
    state = request.app.state
    try:
        return await rewrite.run(
            body.query,
            client=state.client, sem=state.sem,
            model=state.model, timeout=state.rewrite_timeout,
        )
    except rewrite.Busy:
        return JSONResponse({"error": "busy", "retry_after": 2}, status_code=503,
                            headers={"Retry-After": "2"})
    except rewrite.Timeout:
        return JSONResponse({"error": "upstream_timeout"}, status_code=503)
    except rewrite.Upstream as upstream_error:
        return JSONResponse({"error": "upstream", "detail": str(upstream_error)[:200]},
                            status_code=503)
    except SchemaViolation as violation:
        return JSONResponse({"error": "schema_violation", "detail": violation.last_error},
                            status_code=422)


@router.post("/digest")
async def post_digest(body: DigestIn):
    # 계약(DigestIn·DigestResult)만 예약, 구현은 2차 (design D5)
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
            return JSONResponse({"status": "model_missing", "model": state.model},
                                status_code=503)
        return {"status": "ok", "model": state.model}
    except (httpx.HTTPError, ValueError, KeyError, TypeError):
        return JSONResponse({"status": "ollama_unreachable", "model": state.model},
                            status_code=503)
