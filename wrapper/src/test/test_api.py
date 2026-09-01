"""거절·검증·재시도·업스트림 이상 경로 우선 (spec ⑤). ollama는 respx로 mock.
LOG_REDIS_URL 미설정 → 로그는 stdout만, 테스트에서 Redis 접근 없음."""
import asyncio

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


def test_chat_input_validation(client):
    assert client.post("/chat", json={"request_id": "r", "messages": []}).status_code == 422
    assert client.post("/chat", json={"request_id": "r",
        "messages": [{"role": "tool", "content": "x"}]}).status_code == 422   # role 제한


def test_log_format_rule():
    # 규약: requestId:server-name:message (root docs/logging.md)
    assert format_line("req-9", "rewrite ok") == "req-9:llm-wrapper:rewrite ok"
