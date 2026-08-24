"""app/usecase/rewrite.py 미러 — HTTP 계층 없이 usecase 계약만 검증."""
import asyncio

import httpx
import pytest
import respx

from app.usecase.rewrite import Busy, Timeout, Upstream, run

OLLAMA = "http://ollama:11434"
GOOD = ('{"intent":"i","keywords":["k"],"expanded":[],'
        '"filters":{"topic":null,"doc_kind":null}}')


def make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url=OLLAMA)


def run_usecase(client, semaphore, timeout=5.0):
    return asyncio.run(run(
        "req-1", "query", client=client, sem=semaphore, model="qwen3:8b", timeout=timeout,
    ))


@respx.mock
def test_busy_raised_without_waiting():
    async def saturate_and_call():
        client = make_client()
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()             # 포화
        try:
            await run("req-1", "q", client=client, sem=semaphore,
                      model="qwen3:8b", timeout=5.0)
        finally:
            semaphore.release()
            await client.aclose()

    with pytest.raises(Busy):
        asyncio.run(saturate_and_call())


@respx.mock
def test_upstream_envelope_error_normalized():
    respx.post(f"{OLLAMA}/api/chat").mock(
        return_value=httpx.Response(200, json={"unexpected": "shape"}))
    with pytest.raises(Upstream):
        run_usecase(make_client(), asyncio.Semaphore(1))


@respx.mock
def test_total_budget_enforced_as_timeout():
    async def slow_response(request):
        await asyncio.sleep(0.5)
        return httpx.Response(200, json={"message": {"content": GOOD}})

    respx.post(f"{OLLAMA}/api/chat").mock(side_effect=slow_response)
    with pytest.raises(Timeout):
        run_usecase(make_client(), asyncio.Semaphore(1), timeout=0.2)


@respx.mock
def test_semaphore_released_after_success():
    respx.post(f"{OLLAMA}/api/chat").mock(
        return_value=httpx.Response(200, json={"message": {"content": GOOD}}))
    semaphore = asyncio.Semaphore(1)
    result = run_usecase(make_client(), semaphore)
    assert result.keywords == ["k"]
    assert not semaphore.locked()             # 정상 경로 반환 후 슬롯 복구
