"""유즈케이스 — 채팅 스트리밍: 세마포어 → Ollama stream 청크 중계. (chat-design)

스트림 시작 전 오류만 봉투로 — 시작 후엔 텍스트 청크가 곧 계약.
"""
import json
from typing import AsyncIterator

import httpx

from app.usecase.rewrite import Busy  # 동일 거절 계약 재사용

MAX_PREDICT = 1024      # 폭주 가드레일 (rewrite 실측 교훈 — 서버측 상한이 유일한 방어선)


async def stream(request_id: str, messages: list[dict], *,
                 client: httpx.AsyncClient, sem, model: str) -> AsyncIterator[str]:
    if sem.locked():
        raise Busy()
    async with sem:
        async with client.stream(
            "POST", "/api/chat",
            json={
                "model": model,
                "messages": messages,
                "stream": True,
                "think": False,
                "options": {"temperature": 0.3, "num_predict": MAX_PREDICT},
            },
            timeout=httpx.Timeout(connect=5, read=120, write=10, pool=5),
        ) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if not line:
                    continue
                data = json.loads(line)
                token = data.get("message", {}).get("content", "")
                if token:
                    yield token
                if data.get("done"):
                    break
