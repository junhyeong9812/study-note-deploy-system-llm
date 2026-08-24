"""유즈케이스 — 세마포어 획득 → 총예산 안에서 ollama 호출 → 검증·재시도 → 결과.

(design D3·D4) 타임아웃은 요청 단위 총예산이다 — 재시도가 예산을 늘리지 않는다.
"""
import asyncio

import httpx

from domain.prompt import RewriteResult, rewrite_messages
from logger import log
from validate import parse_with_retry


class Busy(Exception):
    """세마포어 획득 실패 — api 계층에서 즉시 503."""


class Upstream(Exception):
    """Ollama 도달 불가·비정상 응답 — api 계층에서 503."""


class Timeout(Exception):
    """요청 단위 총예산 소진 — api 계층에서 503."""


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
        content = response.json()["message"]["content"]
        if not isinstance(content, str):
            raise Upstream(f"unexpected content type: {type(content).__name__}")
        return content
    except Upstream:
        raise
    except (httpx.HTTPError, ValueError, KeyError, TypeError) as error:
        # ValueError = JSON 디코드 실패 포함 — upstream 이상은 전부 계약된 오류로 정규화 (L4)
        raise Upstream(str(error)) from error


async def run(request_id: str, query: str, *,
              client: httpx.AsyncClient, sem: asyncio.Semaphore,
              model: str, timeout: float) -> RewriteResult:
    if sem.locked():                      # 대기 없이 즉시 거절 (D3)
        raise Busy()
    async with sem:
        try:
            async with asyncio.timeout(timeout):   # 요청 단위 총예산 (L2)
                messages = rewrite_messages(query)
                schema = RewriteResult.model_json_schema()
                raw_output = await _chat(client, model, messages, schema, timeout)

                async def retry(feedback: str) -> str:
                    await log(request_id, "rewrite retry: schema violation feedback", "warning")
                    return await _chat(
                        client, model,
                        messages + [{"role": "assistant", "content": raw_output},
                                    {"role": "user", "content": feedback}],
                        schema, timeout,
                    )

                return await parse_with_retry(RewriteResult, raw_output, retry)  # 2·3단
        except TimeoutError as error:
            raise Timeout() from error
