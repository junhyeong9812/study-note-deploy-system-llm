"""라우터 — HTTP 관심사만. 도메인 오류를 상태코드로 번역한다. (design D2)"""
import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from usecase import rewrite
from validate import SchemaViolation

router = APIRouter()


class RewriteIn(BaseModel):
    query: str
    topics: list[str] | None = None


@router.post("/rewrite")
async def post_rewrite(body: RewriteIn, request: Request):
    state = request.app.state
    try:
        result = await rewrite.run(
            body.query, body.topics,
            client=state.client, sem=state.sem,
            model=state.model, timeout=state.rewrite_timeout,
        )
        return result.model_dump()
    except rewrite.Busy:
        return JSONResponse({"error": "busy", "retry_after": 2}, status_code=503,
                            headers={"Retry-After": "2"})
    except rewrite.Upstream as upstream_error:
        return JSONResponse({"error": "upstream", "detail": str(upstream_error)[:200]},
                            status_code=502)
    except SchemaViolation as violation:
        return JSONResponse({"error": "schema_violation", "detail": violation.last_error},
                            status_code=422)


@router.post("/digest")
async def post_digest():
    # 계약만 예약 (design D5) — 2차 범위
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
        return {"status": "ok" if model_present else "model_missing", "model": state.model}
    except httpx.HTTPError:
        return JSONResponse({"status": "ollama_unreachable", "model": state.model},
                            status_code=503)
