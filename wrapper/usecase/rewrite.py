"""유즈케이스 — 세마포어 획득 → ollama 호출 → 검증·재시도 → 결과. (design D3·D4)"""
import asyncio

import httpx

from domain.prompt import DEFAULT_TOPICS, RewriteResult, rewrite_messages
from validate import parse_with_retry


class Busy(Exception):
    """세마포어 획득 실패 — api 계층에서 즉시 503."""


class Upstream(Exception):
    """Ollama 도달 불가/오류 — api 계층에서 502."""


async def _chat(client: httpx.AsyncClient, model: str, messages: list[dict],
                schema: dict, timeout: float) -> str:
    try:
        response = await client.post(
            "/api/chat",
            json={
                "model": model,
                "messages": messages,
                "stream": False,
                "format": schema,          # 1단: Ollama 구조화 출력 강제
                "options": {"temperature": 0},
            },
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()["message"]["content"]
    except (httpx.HTTPError, KeyError) as e:
        raise Upstream(str(e)) from e


async def run(query: str, topics: list[str] | None, *,
              client: httpx.AsyncClient, sem: asyncio.Semaphore,
              model: str, timeout: float) -> RewriteResult:
    if sem.locked():                      # 대기 없이 즉시 거절 (D3)
        raise Busy()
    async with sem:
        messages = rewrite_messages(query, topics or DEFAULT_TOPICS)
        schema = RewriteResult.model_json_schema()
        raw_output = await _chat(client, model, messages, schema, timeout)

        async def retry(feedback: str) -> str:
            return await _chat(
                client, model,
                messages + [{"role": "assistant", "content": raw_output},
                            {"role": "user", "content": feedback}],
                schema, timeout,
            )

        return await parse_with_retry(RewriteResult, raw_output, retry)  # 2·3단
