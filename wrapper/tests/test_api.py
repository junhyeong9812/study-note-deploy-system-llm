"""거절·검증·재시도 경로 우선 (spec ⑤). ollama는 respx로 mock."""
import httpx
import pytest
import respx
from fastapi.testclient import TestClient

from app import app

OLLAMA = "http://ollama:11434"


def chat_json(content: str):
    return httpx.Response(200, json={"message": {"content": content}})


GOOD = '{"intent":"LSM 트리 검색","keywords":["LSM-Tree"],"expanded":["Log-Structured Merge"],"topics":["cs"]}'
BAD = '{"intent":"x"}'  # keywords 누락


@pytest.fixture
def client():
    with TestClient(app) as c:   # lifespan 실행
        yield c


@respx.mock
def test_rewrite_ok(client):
    respx.post(f"{OLLAMA}/api/chat").mock(return_value=chat_json(GOOD))
    r = client.post("/rewrite", json={"query": "lsm tree가 뭐지"})
    assert r.status_code == 200
    assert r.json()["keywords"] == ["LSM-Tree"]


@respx.mock
def test_rewrite_retry_then_ok(client):
    route = respx.post(f"{OLLAMA}/api/chat")
    route.side_effect = [chat_json(BAD), chat_json(GOOD)]
    r = client.post("/rewrite", json={"query": "q"})
    assert r.status_code == 200
    assert route.call_count == 2          # 재시도 정확히 1회


@respx.mock
def test_rewrite_retry_exhausted_422(client):
    route = respx.post(f"{OLLAMA}/api/chat")
    route.side_effect = [chat_json(BAD), chat_json(BAD)]
    r = client.post("/rewrite", json={"query": "q"})
    assert r.status_code == 422
    assert r.json()["error"] == "schema_violation"
    assert route.call_count == 2          # 1회 초과 재시도 금지


@respx.mock
def test_rewrite_upstream_down_502(client):
    respx.post(f"{OLLAMA}/api/chat").mock(side_effect=httpx.ConnectError("down"))
    r = client.post("/rewrite", json={"query": "q"})
    assert r.status_code == 502


def test_busy_503_when_saturated(client):
    # 세마포어 2개를 선점해 포화 상태를 만든 뒤, 대기 없이 즉시 503인지 확인
    import asyncio
    sem = client.app.state.sem
    loop = asyncio.new_event_loop()
    holds = [loop.run_until_complete(sem.acquire()) for _ in range(2)]
    try:
        r = client.post("/rewrite", json={"query": "q"})
        assert r.status_code == 503
        assert r.headers["Retry-After"] == "2"
    finally:
        for _ in holds:
            sem.release()
        loop.close()


@respx.mock
def test_health_ok(client):
    respx.get(f"{OLLAMA}/api/tags").mock(
        return_value=httpx.Response(200, json={"models": [{"name": "qwen3:8b"}]}))
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["status"] == "ok"


@respx.mock
def test_health_model_missing(client):
    respx.get(f"{OLLAMA}/api/tags").mock(
        return_value=httpx.Response(200, json={"models": [{"name": "llama3:8b"}]}))
    assert client.get("/health").json()["status"] == "model_missing"


def test_digest_501(client):
    assert client.post("/digest").status_code == 501
