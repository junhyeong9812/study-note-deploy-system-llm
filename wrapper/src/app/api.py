"""라우터 — HTTP 관심사만. 도메인 오류를 계약된 상태코드(503·422)로 번역한다. (design D2·D3)"""
import asyncio
import json
import time
from contextlib import aclosing, suppress

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


NDJSON = "application/x-ndjson"


DISCONNECT_POLL_S = 0.5


async def _client_disconnected(request: Request) -> None:
    """상대가 연결을 끊을 때까지 기다린다.
    uvicorn은 끊긴 연결에 쓰는 것을 조용히 무시하므로(쓰기 실패로는 알 수 없다) 직접 확인한다."""
    while not await request.is_disconnected():
        await asyncio.sleep(DISCONNECT_POLL_S)


async def chat_event_lines(body: ChatIn, state, request: Request):
    """이벤트 한 개 = JSON 한 줄. 대기·시작·오류를 로그로도 남긴다(대기 로그는 첫 순번만).
    상대(backend)가 연결을 끊으면 즉시 멈춘다 — 떠난 질문이 대기 자리·생성 슬롯을 쓰지 않게."""
    token_count = 0
    queued_logged = False
    disconnected = asyncio.ensure_future(_client_disconnected(request))
    try:
        # aclosing — 이 제너레이터가 닫히면 대기 자리·생성 슬롯 반납도 즉시 실행된다
        async with aclosing(chat.events(
            body.request_id, [m.model_dump() for m in body.messages],
            client=state.client, queue=state.chat_queue, model=state.model,
        )) as chat_events:
            next_event = None
            try:
                while True:
                    next_event = asyncio.ensure_future(anext(chat_events))
                    await asyncio.wait({next_event, disconnected},
                                       return_when=asyncio.FIRST_COMPLETED)
                    if disconnected.done():      # 이벤트가 동시에 도착했어도 떠난 상대에겐 보내지 않는다
                        await log(body.request_id, "chat client gone", "warning")
                        return
                    try:
                        event = next_event.result()
                    except StopAsyncIteration:
                        return
                    kind = event["type"]
                    if kind == "queued" and not queued_logged:
                        queued_logged = True
                        await log(body.request_id, f"chat queued position={event['position']}")
                    elif kind == "token":
                        token_count += 1
                    elif kind == "error":
                        await log(body.request_id, f"chat error: {event['code']}",
                                  "error" if event["code"] == "upstream" else "warning")
                    elif kind == "done":
                        await log(body.request_id, f"chat ok tokens~{token_count}")
                    yield json.dumps(event, ensure_ascii=False) + "\n"
            finally:
                # 어떤 경로로 나가든(이탈·이 태스크의 취소·예외) 기다리던 이벤트 태스크를 먼저 끝낸다 —
                # 살아 있는 채로 chat_events를 닫으면 닫기가 실패해 자리·슬롯 반납이 미뤄진다.
                # 취소가 대기·생성 지점에 들어가 정리(finally)가 돈다.
                if next_event is not None:
                    next_event.cancel()
                    with suppress(asyncio.CancelledError, StopAsyncIteration, Exception):
                        await next_event
    finally:
        disconnected.cancel()


@router.post("/chat")
async def post_chat(body: ChatIn, request: Request):
    """채팅 스트리밍 — 두 형식.
    Accept: application/x-ndjson → 이벤트 줄(queued·start·token·error·done), 포화면 줄을 세운다.
    그 외 → text/plain 청크(봉투 없음), 포화면 즉시 503 봉투(구 계약)."""
    state = request.app.state
    if NDJSON in request.headers.get("accept", ""):
        return StreamingResponse(
            chat_event_lines(body, state, request), media_type=NDJSON,
            headers={"X-Accel-Buffering": "no", "Cache-Control": "no-store"})
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
