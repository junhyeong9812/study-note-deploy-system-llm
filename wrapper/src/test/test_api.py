"""거절·검증·재시도·업스트림 이상 경로 우선 (spec ⑤). ollama는 respx로 mock.
LOG_REDIS_URL 미설정 → 로그는 stdout만, 테스트에서 Redis 접근 없음."""
import asyncio
import json

import httpx
import pytest
import respx
from fastapi.testclient import TestClient

from app.app import app
from app.logger import format_line

OLLAMA = "http://ollama:11434"
GOOD = ('{"intent":"LSM 트리 검색","keywords":["LSM-Tree"],'
        '"expanded":["Log-Structured Merge"],"filters":{"topic":"cs","doc_kind":"summary"}}')
BAD = '{"intent":"x"}'  # keywords 누락


def chat_json(content: str):
    return httpx.Response(200, json={"message": {"content": content}})


def rewrite_body(query: str = "q"):
    return {"request_id": "req-1", "query": query}


@pytest.fixture
def client():
    with TestClient(app) as test_client:   # lifespan 실행
        yield test_client


@respx.mock
def test_rewrite_ok(client):
    respx.post(f"{OLLAMA}/api/chat").mock(return_value=chat_json(GOOD))
    response = client.post("/rewrite", json=rewrite_body("lsm tree가 뭐지"))
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True                                     # 봉투 (#7)
    assert body["data"]["keywords"] == ["LSM-Tree"]
    assert body["data"]["filters"] == {"topic": "cs", "doc_kind": "summary"}   # D2 계약 (L1)


@respx.mock
def test_rewrite_retry_then_ok(client):
    route = respx.post(f"{OLLAMA}/api/chat")
    route.side_effect = [chat_json(BAD), chat_json(GOOD)]
    response = client.post("/rewrite", json=rewrite_body())
    assert response.status_code == 200
    assert route.call_count == 2          # 재시도 정확히 1회


@respx.mock
def test_rewrite_retry_exhausted_422(client):
    route = respx.post(f"{OLLAMA}/api/chat")
    route.side_effect = [chat_json(BAD), chat_json(BAD)]
    response = client.post("/rewrite", json=rewrite_body())
    assert response.status_code == 422
    body = response.json()
    assert body["success"] is False and body["error"]["code"] == "schema_violation"
    assert route.call_count == 2          # 1회 초과 재시도 금지


@respx.mock
def test_rewrite_upstream_down_503(client):
    respx.post(f"{OLLAMA}/api/chat").mock(side_effect=httpx.ConnectError("down"))
    response = client.post("/rewrite", json=rewrite_body())
    assert response.status_code == 503                       # 폴백 트리거 통일 (L9)
    assert response.json()["error"]["code"] == "upstream"


@respx.mock
def test_rewrite_malformed_upstream_json_503(client):
    respx.post(f"{OLLAMA}/api/chat").mock(
        return_value=httpx.Response(200, content=b"<html>proxy error</html>"))
    response = client.post("/rewrite", json=rewrite_body())
    assert response.status_code == 503                       # 500 누출 금지 (L4)
    assert response.json()["error"]["code"] == "upstream"


@respx.mock
def test_rewrite_total_budget_timeout_503(client):
    async def slow_response(request):
        await asyncio.sleep(0.5)
        return chat_json(GOOD)

    respx.post(f"{OLLAMA}/api/chat").mock(side_effect=slow_response)
    original_timeout = client.app.state.rewrite_timeout
    client.app.state.rewrite_timeout = 0.2                   # 총예산 0.2s < 응답 0.5s
    try:
        response = client.post("/rewrite", json=rewrite_body())
        assert response.status_code == 503                   # 요청 단위 총예산 (L2)
        assert response.json()["error"]["code"] == "upstream_timeout"
    finally:
        client.app.state.rewrite_timeout = original_timeout


def test_busy_503_when_saturated(client):
    # 세마포어 2개를 선점해 포화 상태를 만든 뒤, 대기 없이 즉시 503인지 확인
    semaphore = client.app.state.sem
    loop = asyncio.new_event_loop()
    holds = [loop.run_until_complete(semaphore.acquire()) for _ in range(2)]
    try:
        response = client.post("/rewrite", json=rewrite_body())
        assert response.status_code == 503
        assert response.headers["Retry-After"] == "2"
        body = response.json()
        assert body["success"] is False
        assert body["error"] == {"code": "busy", "retry_after": 2}   # 봉투 (#7)
    finally:
        for _ in holds:
            semaphore.release()
        loop.close()


def test_rewrite_input_validation_422(client):
    assert client.post("/rewrite", json=rewrite_body("x" * 301)).status_code == 422  # 길이 상한 (L8)
    assert client.post("/rewrite", json={"query": "q"}).status_code == 422           # request_id 필수 (규약)
    assert client.post(
        "/rewrite", json={"request_id": "req-1", "query": "q", "topics": ["cs"]}
    ).status_code == 422   # 계약 밖 필드 거부 (감사: extra=forbid)


@respx.mock
def test_health_ok(client):
    respx.get(f"{OLLAMA}/api/tags").mock(
        return_value=httpx.Response(200, json={"models": [{"name": "qwen3:8b"}]}))
    response = client.get("/health")
    assert response.status_code == 200 and response.json()["status"] == "ok"


@respx.mock
def test_health_model_missing_503(client):
    respx.get(f"{OLLAMA}/api/tags").mock(
        return_value=httpx.Response(200, json={"models": [{"name": "llama3:8b"}]}))
    response = client.get("/health")
    assert response.status_code == 503                       # healthy 위장 금지 (L3)
    assert response.json()["status"] == "model_missing"


@respx.mock
def test_health_invalid_upstream_503(client):
    respx.get(f"{OLLAMA}/api/tags").mock(
        return_value=httpx.Response(200, content=b"not json"))
    assert client.get("/health").status_code == 503          # 형식 이상도 503 (L4)


def test_validation_error_uses_envelope(client):
    response = client.post("/rewrite", json={"query": "q"})          # request_id 누락
    assert response.status_code == 422
    body = response.json()
    assert body["success"] is False and body["error"]["code"] == "invalid_request"



def test_chat_busy_503_before_stream(client):
    import asyncio as aio
    semaphore = client.app.state.sem
    loop = aio.new_event_loop()
    holds = [loop.run_until_complete(semaphore.acquire()) for _ in range(2)]
    try:
        response = client.post("/chat", json={
            "request_id": "req-1", "messages": [{"role": "user", "content": "hi"}]})
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "busy"
    finally:
        for _ in holds:
            semaphore.release()
        loop.close()


@respx.mock
def test_chat_ndjson_events_when_accepted(client):
    # Accept: application/x-ndjson 이면 이벤트 줄 형식 — 한 줄 = JSON 이벤트 하나
    respx.post(f"{OLLAMA}/api/chat").mock(return_value=httpx.Response(
        200, content=b'{"message":{"content":"hi"}}\n{"done":true}\n'))
    response = client.post("/chat", headers={"Accept": "application/x-ndjson"}, json={
        "request_id": "req-1", "messages": [{"role": "user", "content": "hi"}]})
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/x-ndjson")
    assert response.headers["x-accel-buffering"] == "no"
    lines = [json.loads(line) for line in response.text.splitlines()]
    assert lines == [{"type": "start"}, {"type": "token", "text": "hi"}, {"type": "done"}]


@respx.mock
def test_chat_plain_text_kept_without_accept(client):
    # 구 backend 호환 — Accept 없으면 기존 text 스트림 그대로
    respx.post(f"{OLLAMA}/api/chat").mock(return_value=httpx.Response(
        200, content=b'{"message":{"content":"hi"}}\n{"done":true}\n'))
    response = client.post("/chat", json={
        "request_id": "req-1", "messages": [{"role": "user", "content": "hi"}]})
    assert response.headers["content-type"].startswith("text/plain")
    assert response.text == "hi"


class FakeRequest:
    """is_disconnected()가 정해진 횟수 뒤부터 참 — 연결이 끊긴 상대를 흉내 낸다."""
    def __init__(self, connected_polls: int):
        self.remaining = connected_polls

    async def is_disconnected(self) -> bool:
        self.remaining -= 1
        return self.remaining < 0


def chat_in():
    from app.api import ChatIn
    return ChatIn(request_id="req-1", messages=[{"role": "user", "content": "hi"}])


def make_state(queue):
    from types import SimpleNamespace
    return SimpleNamespace(client=httpx.AsyncClient(base_url=OLLAMA), chat_queue=queue, model="qwen3:8b")


def test_chat_events_stop_and_free_place_when_peer_leaves_while_waiting(monkeypatch):
    """대기 중 상대가 끊으면 다음 이벤트를 기다리지 않고 끝나고, 대기 자리가 빠진다."""
    from app import api
    from app.usecase.chat import ChatQueue
    monkeypatch.setattr(api, "DISCONNECT_POLL_S", 0.01)

    async def scenario():
        queue = ChatQueue(asyncio.Semaphore(1), max_waiting=8, wait_timeout=60.0, heartbeat=10.0)
        await queue.sem.acquire()                              # 포화 — 요청은 줄을 선다
        state = make_state(queue)
        lines = []
        async def consume():
            async for line in api.chat_event_lines(chat_in(), state, FakeRequest(connected_polls=3)):
                lines.append(json.loads(line))
        await asyncio.wait_for(consume(), timeout=2)           # heartbeat(10s)·대기 상한(60s) 전에 끝나야 한다
        waiting_after_leave = queue.waiting_count
        queue.sem.release()
        await asyncio.sleep(0)
        await state.client.aclose()
        return lines, waiting_after_leave, queue

    lines, waiting_after_leave, queue = asyncio.run(scenario())
    assert lines == [{"type": "queued", "position": 1}]
    assert waiting_after_leave == 0
    assert not queue.sem.locked()                              # 떠난 요청이 슬롯을 가져가지 않았다


@respx.mock
def test_chat_events_stop_and_free_slot_when_peer_leaves_while_generating(monkeypatch):
    """생성 중 상대가 끊으면 Ollama 응답을 끝까지 받지 않고 슬롯을 반납한다."""
    from app import api
    from app.usecase.chat import ChatQueue
    monkeypatch.setattr(api, "DISCONNECT_POLL_S", 0.01)

    async def slow_ollama():
        yield b'{"message":{"content":"a"}}\n'
        await asyncio.sleep(5)                                 # 다음 토큰이 한참 안 온다
        yield b'{"message":{"content":"b"}}\n{"done":true}\n'

    respx.post(f"{OLLAMA}/api/chat").mock(return_value=httpx.Response(200, content=slow_ollama()))

    async def scenario():
        queue = ChatQueue(asyncio.Semaphore(1), max_waiting=8, wait_timeout=60.0, heartbeat=10.0)
        state = make_state(queue)
        lines = []
        async def consume():
            async for line in api.chat_event_lines(chat_in(), state, FakeRequest(connected_polls=5)):
                lines.append(json.loads(line))
        await asyncio.wait_for(consume(), timeout=2)
        await state.client.aclose()
        return lines, queue

    lines, queue = asyncio.run(scenario())
    assert lines == [{"type": "start"}, {"type": "token", "text": "a"}]
    assert not queue.sem.locked()


def test_chat_input_validation(client):
    assert client.post("/chat", json={"request_id": "r", "messages": []}).status_code == 422
    assert client.post("/chat", json={"request_id": "r",
        "messages": [{"role": "tool", "content": "x"}]}).status_code == 422   # role 제한


def test_log_format_rule():
    # 규약: requestId:server-name:message (root docs/logging.md)
    assert format_line("req-9", "rewrite ok") == "req-9:llm-wrapper:rewrite ok"


def test_drain_endpoint_marks_queue_and_reports_waiting(client):
    queue = client.app.state.chat_queue
    assert queue.draining is False
    response = client.post("/admin/drain")
    assert response.status_code == 200
    assert response.json() == {"success": True, "data": {"handed_off": 0}}
    assert queue.draining is True


def test_chat_at_saturated_draining_server_gets_retry_event(client):
    state = client.app.state

    async def saturate():
        for _ in range(state.sem._value):
            await state.sem.acquire()
    asyncio.run(saturate())
    state.chat_queue.drain()
    response = client.post("/chat", headers={"Accept": "application/x-ndjson"},
                           json={"request_id": "req-1", "messages": [{"role": "user", "content": "hi"}]})
    assert response.status_code == 200
    assert [json.loads(line) for line in response.text.splitlines() if line] == [{"type": "retry"}]
