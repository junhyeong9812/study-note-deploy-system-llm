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
    s = request.app.state
    try:
        result = await rewrite.run(
            body.query, body.topics,
            client=s.client, sem=s.sem, model=s.model, timeout=s.rewrite_timeout,
        )
        return result.model_dump()
    except rewrite.Busy:
        return JSONResponse({"error": "busy", "retry_after": 2}, status_code=503,
                            headers={"Retry-After": "2"})
    except rewrite.Upstream as e:
        return JSONResponse({"error": "upstream", "detail": str(e)[:200]}, status_code=502)
    except SchemaViolation as e:
        return JSONResponse({"error": "schema_violation", "detail": e.last_error}, status_code=422)


@router.post("/digest")
async def post_digest():
    # 계약만 예약 (design D5) — 2차 범위
    return JSONResponse({"error": "not_implemented"}, status_code=501)


@router.get("/health")
async def health(request: Request):
    s = request.app.state
    try:
        r = await s.client.get("/api/tags", timeout=2)
        r.raise_for_status()
        models = [m["name"] for m in r.json().get("models", [])]
        ok = any(m == s.model or m.startswith(s.model + ":") for m in models)
        return {"status": "ok" if ok else "model_missing", "model": s.model}
    except httpx.HTTPError:
        return JSONResponse({"status": "ollama_unreachable", "model": s.model}, status_code=503)
