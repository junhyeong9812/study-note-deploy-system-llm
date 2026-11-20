"""app/usecase/chat.py 미러 — 대기열 계약(순번·상한·시간 초과·이탈·반납)을 HTTP 계층 없이 검증."""
import asyncio

import httpx
import respx

from app.usecase import rewrite
from app.usecase.chat import ChatQueue, events

OLLAMA = "http://ollama:11434"
STREAM_BODY = (b'{"message":{"content":"a"}}\n'
               b'{"message":{"content":"b"}}\n'
               b'{"message":{"content":""},"done":true}\n')
MESSAGES = [{"role": "user", "content": "hi"}]


def make_queue(slots=1, max_waiting=8, wait_timeout=60.0, heartbeat=10.0) -> ChatQueue:
    return ChatQueue(asyncio.Semaphore(slots), max_waiting=max_waiting,
                     wait_timeout=wait_timeout, heartbeat=heartbeat)


def open_events(client, queue):
    return events("req-1", MESSAGES, client=client, queue=queue, model="qwen3:8b")


async def collect(generator) -> list[dict]:
    return [event async for event in generator]


@respx.mock
def test_free_slot_streams_without_queueing():
    respx.post(f"{OLLAMA}/api/chat").mock(return_value=httpx.Response(200, content=STREAM_BODY))

    async def scenario():
        queue = make_queue()
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            received = await collect(open_events(client, queue))
        return received, queue

    received, queue = asyncio.run(scenario())
    assert received == [{"type": "start"}, {"type": "token", "text": "a"},
                        {"type": "token", "text": "b"}, {"type": "done"}]
    assert not queue.sem.locked() and queue.waiting_count == 0     # 반납 확인


@respx.mock
def test_saturated_queues_then_starts_in_arrival_order():
    respx.post(f"{OLLAMA}/api/chat").mock(return_value=httpx.Response(200, content=STREAM_BODY))

    async def scenario():
        queue = make_queue()
        await queue.sem.acquire()                              # 포화
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            first, second = open_events(client, queue), open_events(client, queue)
            first_event = await first.__anext__()
            second_event = await second.__anext__()
            queue.sem.release()                                # 앞 건 종료
            first_rest = await collect(first)                  # 먼저 온 쪽이 먼저 시작·종료
            second_rest = await collect(second)
        return first_event, second_event, first_rest, second_rest, queue

    first_event, second_event, first_rest, second_rest, queue = asyncio.run(scenario())
    assert first_event == {"type": "queued", "position": 1}
    assert second_event == {"type": "queued", "position": 2}
    assert first_rest[0] == {"type": "start"} and first_rest[-1] == {"type": "done"}
    assert {"type": "start"} in second_rest and second_rest[-1] == {"type": "done"}
    assert not queue.sem.locked() and queue.waiting_count == 0


def test_position_update_pushed_when_someone_ahead_leaves():
    async def scenario():
        queue = make_queue()
        await queue.sem.acquire()
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            first, second = open_events(client, queue), open_events(client, queue)
            await first.__anext__()
            await second.__anext__()                           # position 2
            next_for_second = asyncio.ensure_future(second.__anext__())
            await asyncio.sleep(0)                             # second가 대기에 들어가게
            await first.aclose()                               # 앞사람 이탈
            pushed = await asyncio.wait_for(next_for_second, timeout=1)   # heartbeat(10s) 전에 도착
            await second.aclose()
        return pushed

    assert asyncio.run(scenario()) == {"type": "queued", "position": 1}


def test_queue_full_rejected_immediately():
    async def scenario():
        queue = make_queue(max_waiting=1)
        await queue.sem.acquire()
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            waiter = open_events(client, queue)
            await waiter.__anext__()                           # 대기 1명 = 상한
            rejected = await collect(open_events(client, queue))
            waiting_after_reject = queue.waiting_count
            await waiter.aclose()
        return rejected, waiting_after_reject

    rejected, waiting_after_reject = asyncio.run(scenario())
    assert rejected == [{"type": "error", "code": "queue_full"}]
    assert waiting_after_reject == 1                           # 거절이 줄을 건드리지 않음


def test_wait_timeout_ends_with_error_and_frees_place():
    async def scenario():
        queue = make_queue(wait_timeout=0.05, heartbeat=0.02)
        await queue.sem.acquire()
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            received = await collect(open_events(client, queue))
        queue.sem.release()
        return received, queue

    received, queue = asyncio.run(scenario())
    assert received[0] == {"type": "queued", "position": 1}
    assert received[-1] == {"type": "error", "code": "queue_timeout"}
    assert all(event["type"] == "queued" for event in received[:-1])
    assert queue.waiting_count == 0 and not queue.sem.locked()  # 자리·세마포어 누수 없음


def test_heartbeat_repeats_queued_while_position_unchanged():
    async def scenario():
        queue = make_queue(wait_timeout=0.2, heartbeat=0.02)
        await queue.sem.acquire()
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            return await collect(open_events(client, queue))

    received = asyncio.run(scenario())
    assert received.count({"type": "queued", "position": 1}) >= 3   # 무응답 구간 없음


def test_leaving_while_waiting_frees_place_and_slot():
    async def scenario():
        queue = make_queue()
        await queue.sem.acquire()
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            waiter = open_events(client, queue)
            await waiter.__anext__()
            await waiter.aclose()                              # 대기 중 연결 끊김
        waiting_after_leave = queue.waiting_count
        queue.sem.release()
        await asyncio.sleep(0)                                 # 취소된 대기자에게 넘어가지 않는지
        return waiting_after_leave, queue

    waiting_after_leave, queue = asyncio.run(scenario())
    assert waiting_after_leave == 0
    assert not queue.sem.locked()                              # 유령 대기자가 자리를 먹지 않음


@respx.mock
def test_upstream_failure_reported_as_event_and_slot_returned():
    respx.post(f"{OLLAMA}/api/chat").mock(side_effect=httpx.ConnectError("down"))

    async def scenario():
        queue = make_queue()
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            received = await collect(open_events(client, queue))
        return received, queue

    received, queue = asyncio.run(scenario())
    assert received == [{"type": "start"}, {"type": "error", "code": "upstream"}]
    assert not queue.sem.locked()


@respx.mock
def test_leaving_mid_stream_returns_slot():
    respx.post(f"{OLLAMA}/api/chat").mock(return_value=httpx.Response(200, content=STREAM_BODY))

    async def scenario():
        queue = make_queue()
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            generator = open_events(client, queue)
            await generator.__anext__()                        # start
            await generator.__anext__()                        # 첫 토큰
            await generator.aclose()                           # 생성 중 연결 끊김
        return queue

    queue = asyncio.run(scenario())
    assert not queue.sem.locked()


def test_rewrite_still_rejected_immediately_while_chat_waits():
    """검색 재작성은 줄을 서지 않는다 — 채팅 대기자가 있어도 즉시 Busy(계약 무변경)."""
    async def scenario():
        queue = make_queue()
        await queue.sem.acquire()
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            waiter = open_events(client, queue)
            await waiter.__anext__()
            try:
                await rewrite.run("req-2", "q", client=client, sem=queue.sem,
                                  model="qwen3:8b", timeout=5.0)
                return "ran"
            except rewrite.Busy:
                return "busy"
            finally:
                await waiter.aclose()

    assert asyncio.run(scenario()) == "busy"


@respx.mock
def test_stream_cut_without_done_is_error_not_done():
    """Ollama가 완료 표시 없이 끊기면 done이 아니라 오류 — 잘린 답이 정상 완료로 저장되지 않게."""
    respx.post(f"{OLLAMA}/api/chat").mock(return_value=httpx.Response(
        200, content=b'{"message":{"content":"a"}}\n'))

    async def scenario():
        queue = make_queue()
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            received = await collect(open_events(client, queue))
        return received, queue

    received, queue = asyncio.run(scenario())
    assert received == [{"type": "start"}, {"type": "token", "text": "a"},
                        {"type": "error", "code": "upstream"}]
    assert not queue.sem.locked()


# ---- 정리(drain): 새 버전으로 교체 중 — 생성 중인 건은 끝까지, 대기자는 순번대로 내보낸다 ----

def make_draining_queue(slots=1, drain_spacing=0.01) -> ChatQueue:
    return ChatQueue(asyncio.Semaphore(slots), max_waiting=8, wait_timeout=60.0,
                     heartbeat=10.0, drain_spacing=drain_spacing)


def test_drain_hands_off_waiters_in_order_and_running_one_finishes():
    async def scenario():
        queue = make_draining_queue()
        await queue.sem.acquire()                              # 생성 중인 한 건이 슬롯을 쥐고 있다
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            first, second = open_events(client, queue), open_events(client, queue)
            assert await first.__anext__() == {"type": "queued", "position": 1}
            assert await second.__anext__() == {"type": "queued", "position": 2}
            handed_off = queue.drain()
            order = []

            async def finish(name, generator):
                received = await collect(generator)
                order.append(name)
                return received

            second_rest, first_rest = await asyncio.gather(finish("second", second), finish("first", first))
        return handed_off, order, first_rest, second_rest, queue

    handed_off, order, first_rest, second_rest, queue = asyncio.run(scenario())
    assert handed_off == 2
    assert order == ["first", "second"]                        # 순번대로 내보낸다
    assert first_rest == [{"type": "retry"}]
    assert second_rest[-1] == {"type": "retry"}                # 앞사람이 나가 순번이 당겨진 뒤 내보낸다
    assert all(event["type"] in ("queued", "retry") for event in second_rest)
    assert queue.waiting_count == 0
    assert queue.sem.locked()                                  # 생성 중인 건의 슬롯은 그대로(대기자가 가져가지 않았다)


def test_new_arrival_at_saturated_draining_queue_is_sent_away_at_once():
    async def scenario():
        queue = make_draining_queue()
        await queue.sem.acquire()
        queue.drain()
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            return await collect(open_events(client, queue)), queue

    received, queue = asyncio.run(scenario())
    assert received == [{"type": "retry"}]
    assert queue.waiting_count == 0


@respx.mock
def test_draining_queue_still_serves_when_a_slot_is_free():
    respx.post(f"{OLLAMA}/api/chat").mock(return_value=httpx.Response(200, content=STREAM_BODY))

    async def scenario():
        queue = make_draining_queue()
        queue.drain()                                          # 대기자 없음 — 정리 중이어도 빈 슬롯이면 처리한다
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            return await collect(open_events(client, queue)), queue

    received, queue = asyncio.run(scenario())
    assert received[0] == {"type": "start"} and received[-1] == {"type": "done"}
    assert not queue.sem.locked()


@respx.mock
def test_waiter_that_already_got_the_slot_generates_instead_of_being_sent_away():
    respx.post(f"{OLLAMA}/api/chat").mock(return_value=httpx.Response(200, content=STREAM_BODY))

    async def scenario():
        queue = make_draining_queue(drain_spacing=0.05)
        await queue.sem.acquire()
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            waiter = open_events(client, queue)
            assert await waiter.__anext__() == {"type": "queued", "position": 1}
            queue.drain()
            queue.sem.release()                                # 내보내기 직전에 슬롯이 났다
            return await collect(waiter), queue

    received, queue = asyncio.run(scenario())
    assert {"type": "retry"} not in received
    assert received[0] == {"type": "start"} and received[-1] == {"type": "done"}
    assert not queue.sem.locked() and queue.waiting_count == 0
