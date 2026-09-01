"""라우터 — HTTP 관심사만. 도메인 오류를 계약된 상태코드(503·422)로 번역한다. (design D2·D3)"""
import time

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from fastapi.responses import StreamingResponse

from app.domain.envelope import SuccessEnvelope, fail, ok
from app.logger import log
from app.usecase import chat, rewrite
from app.validate import SchemaViolation

router = APIRouter()


class RewriteIn(BaseModel):
    model_config = ConfigDict(extra="forbid")   # 계약 밖 필드 거부 — D2 입력 계약 강제

    request_id: str = Field(min_length=1, max_length=64)   # backend가 발행 (로그 규약)
    query: str = Field(min_length=1, max_length=300)


@router.post("/rewrite", response_model=SuccessEnvelope)
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
        return ok(result.model_dump())
    except rewrite.Busy:
        await log(body.request_id, "rewrite rejected: busy", "warning")
        return JSONResponse(fail("busy", retry_after=2), status_code=503,
                            headers={"Retry-After": "2"})
    except rewrite.Timeout:
        await log(body.request_id, f"rewrite timeout {elapsed_ms()}ms", "error")
        return JSONResponse(fail("upstream_timeout"), status_code=503)
    except rewrite.Upstream as upstream_error:
        await log(body.request_id, f"rewrite upstream error: {str(upstream_error)[:200]}", "error")
        return JSONResponse(fail("upstream", detail=str(upstream_error)[:200]),
                            status_code=503)
    except SchemaViolation as violation:
        await log(body.request_id, f"rewrite schema violation: {violation.last_error}", "error")
        return JSONResponse(fail("schema_violation", detail=violation.last_error),
                            status_code=422)



class ChatMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role: str = Field(pattern="^(system|user|assistant)$")
    content: str = Field(min_length=1, max_length=20_000)


class ChatIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str = Field(min_length=1, max_length=64)
    messages: list[ChatMessage] = Field(min_length=1, max_length=40)


@router.post("/chat")
async def post_chat(body: ChatIn, request: Request):
    """채팅 스트리밍 — 성공 시 text/plain 청크(봉투 없음: 스트림이 계약).
    스트림 시작 전 오류(포화·검증)는 봉투 JSON."""
    state = request.app.state
    if state.sem.locked():
        await log(body.request_id, "chat rejected: busy", "warning")
        return JSONResponse(fail("busy", retry_after=2), status_code=503,
                            headers={"Retry-After": "2"})

    async def token_stream():
        count = 0
        try:
            async for token in chat.stream(
                body.request_id, [m.model_dump() for m in body.messages],
                client=state.client, sem=state.sem, model=state.model,
            ):
                count += 1
                yield token
            await log(body.request_id, f"chat ok tokens~{count}")
        except Exception as error:                       # 스트림 도중 오류 — 로그만 (연결은 끊김)
            await log(body.request_id, f"chat stream error: {type(error).__name__}", "error")

    return StreamingResponse(token_stream(), media_type="text/plain; charset=utf-8")


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
