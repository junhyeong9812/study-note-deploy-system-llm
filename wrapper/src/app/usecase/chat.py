"""유즈케이스 — 채팅 스트리밍: 세마포어 → Ollama stream 청크 중계. (chat-design)

두 계약이 공존한다:
- stream(): 텍스트 청크. 포화면 줄 세우지 않고 즉시 Busy (구 backend 호환).
- events(): 이벤트(queued·start·token·error·done). 포화면 ChatQueue에 줄을 세운다.
"""
import asyncio
import json
from contextlib import aclosing
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


class ChatQueue:
    """채팅 대기열 — 세마포어 앞에 선 줄의 상한·순번을 관리한다.

    줄을 서는 건 채팅뿐이다. rewrite는 같은 세마포어를 locked()로만 확인하고 즉시 거절하므로
    (asyncio.Semaphore는 대기자가 있으면 locked()가 참) 채팅 대기자를 새치기하지 않는다.
    """

    def __init__(self, sem: asyncio.Semaphore, *, max_waiting: int,
                 wait_timeout: float, heartbeat: float):
        self.sem = sem
        self.max_waiting = max_waiting
        self.wait_timeout = wait_timeout
        self.heartbeat = heartbeat          # 순번이 그대로여도 이 간격으로 queued 재전송 (프록시 무응답 타임아웃 방지)
        self._tickets: list[asyncio.Event] = []   # 도착 순서 = 세마포어 깨움 순서(FIFO)

    @property
    def waiting_count(self) -> int:
        return len(self._tickets)

    def _enter(self) -> asyncio.Event:
        ticket = asyncio.Event()
        self._tickets.append(ticket)
        return ticket

    def _position(self, ticket: asyncio.Event) -> int:
        return self._tickets.index(ticket) + 1

    def _leave(self, ticket: asyncio.Event) -> None:
        self._tickets.remove(ticket)
        for other in self._tickets:         # 뒤 사람들의 순번이 당겨졌음을 알림
            other.set()


async def _wait_turn(queue: ChatQueue) -> AsyncIterator[dict]:
    """자리가 날 때까지 queued 이벤트를 흘린다. 정상 종료 = 세마포어 획득 완료.
    줄이 꽉 찼거나 시간이 다 되면 error 이벤트를 내고 끝난다(세마포어 미획득)."""
    if queue.waiting_count >= queue.max_waiting:
        yield {"type": "error", "code": "queue_full"}
        return
    ticket = queue._enter()
    acquire_task = asyncio.ensure_future(queue.sem.acquire())
    deadline = asyncio.get_running_loop().time() + queue.wait_timeout
    acquired = False
    try:
        while True:
            yield {"type": "queued", "position": queue._position(ticket)}
            ticket.clear()
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                yield {"type": "error", "code": "queue_timeout"}
                return
            position_changed = asyncio.ensure_future(ticket.wait())
            try:
                await asyncio.wait({acquire_task, position_changed},
                                   timeout=min(queue.heartbeat, remaining),
                                   return_when=asyncio.FIRST_COMPLETED)
            finally:
                position_changed.cancel()
            if acquire_task.done():
                acquired = True
                return
    finally:
        queue._leave(ticket)
        if not acquired:
            # 떠나는 순간 이미 자리를 받았으면 반납, 아직이면 대기 취소 — 유령 대기자·누수 방지
            if acquire_task.done() and not acquire_task.cancelled():
                queue.sem.release()
            else:
                acquire_task.cancel()


async def events(request_id: str, messages: list[dict], *,
                 client: httpx.AsyncClient, queue: ChatQueue, model: str) -> AsyncIterator[dict]:
    """이벤트 스트림 — queued* → start → token* → done, 또는 어느 단계에서든 error 하나로 끝."""
    sem = queue.sem
    if sem.locked():
        # aclosing — 연결이 끊겨 이 제너레이터가 닫히면 대기 쪽 정리(자리 반납)도 즉시 실행된다
        async with aclosing(_wait_turn(queue)) as turn:
            async for event in turn:
                yield event
                if event["type"] == "error":
                    return
    else:
        await sem.acquire()                 # 빈 자리 — 기다림 없이 즉시 획득
    try:
        yield {"type": "start"}
        try:
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
                        yield {"type": "token", "text": token}
                    if data.get("done"):
                        break
        except (httpx.HTTPError, ValueError, AttributeError):
            # Ollama 도달 불가·비정상 응답 — 열린 스트림에서는 이벤트로 알린다
            yield {"type": "error", "code": "upstream"}
            return
        yield {"type": "done"}
    finally:
        sem.release()
