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
# heartbeat 를 길게(60초) 두고 시나리오마다 시간 상한을 건다: drain 의 깨움이 아니라 heartbeat 덕에
# 우연히 통과하는 일이 없게.

HANDOFF_DEADLINE = 3.0


def make_draining_queue(slots=1, drain_spacing=0.01) -> ChatQueue:
    return ChatQueue(asyncio.Semaphore(slots), max_waiting=8, wait_timeout=120.0,
                     heartbeat=60.0, drain_spacing=drain_spacing)


async def waiting_inside_queue(generator) -> asyncio.Future:
    """첫 queued 를 받고, 다음 이벤트를 기다리는 상태(대기 지점 안)까지 들여보낸다."""
    assert (await generator.__anext__())["type"] == "queued"
    pending = asyncio.ensure_future(generator.__anext__())
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    return pending


async def rest_of(pending: asyncio.Future, generator) -> list[dict]:
    received = []
    try:
        received.append(await pending)
        async for event in generator:
            received.append(event)
    except StopAsyncIteration:
        pass
    return received


def test_drain_hands_off_waiters_in_order_while_they_wait():
    async def scenario():
        queue = make_draining_queue()
        await queue.sem.acquire()                              # 생성 중인 한 건이 슬롯을 쥐고 있다
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            first, second = open_events(client, queue), open_events(client, queue)
            first_pending = await waiting_inside_queue(first)
            second_pending = await waiting_inside_queue(second)
            waiting = queue.drain()
            order = []

            async def finish(name, pending, generator):
                received = await rest_of(pending, generator)
                order.append(name)
                return received

            second_rest, first_rest = await asyncio.wait_for(asyncio.gather(
                finish("second", second_pending, second), finish("first", first_pending, first)), HANDOFF_DEADLINE)
        return waiting, order, first_rest, second_rest, queue

    waiting, order, first_rest, second_rest, queue = asyncio.run(scenario())
    assert waiting == 2
    assert order == ["first", "second"]                        # 순번대로 내보낸다
    assert first_rest == [{"type": "retry"}]
    assert second_rest[-1] == {"type": "retry"}
    assert all(event["type"] in ("queued", "retry") for event in second_rest)
    assert queue.waiting_count == 0
    assert queue.sem.locked()                                  # 생성 중인 건의 슬롯은 그대로


def test_drain_is_noticed_even_when_waiter_was_between_events():
    """대기자가 queued 를 막 내보내고(아직 기다리러 가기 전) 멈춰 있을 때 정리가 시작돼도 곧바로 나간다."""
    async def scenario():
        queue = make_draining_queue()
        await queue.sem.acquire()
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            waiter = open_events(client, queue)
            assert await waiter.__anext__() == {"type": "queued", "position": 1}   # yield 에 멈춘 상태
            queue.drain()
            return await asyncio.wait_for(collect(waiter), HANDOFF_DEADLINE), queue

    received, queue = asyncio.run(scenario())
    assert received == [{"type": "retry"}]
    assert queue.waiting_count == 0


def test_new_arrival_at_draining_queue_is_sent_away_at_once():
    async def scenario():
        queue = make_draining_queue()
        await queue.sem.acquire()
        queue.drain()
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            return await asyncio.wait_for(collect(open_events(client, queue)), HANDOFF_DEADLINE), queue

    received, queue = asyncio.run(scenario())
    assert received == [{"type": "retry"}]
    assert queue.waiting_count == 0


def test_draining_queue_does_not_start_new_generation_even_with_a_free_slot():
    async def scenario():
        queue = make_draining_queue()
        queue.drain()                                          # 빈 슬롯이 있어도 교체 중인 서버는 새 생성을 시작하지 않는다
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            return await collect(open_events(client, queue)), queue

    received, queue = asyncio.run(scenario())
    assert received == [{"type": "retry"}]
    assert not queue.sem.locked() and queue.waiting_count == 0


def test_waiter_that_gets_the_slot_during_spacing_is_sent_away_and_returns_it():
    """맨 앞 대기자가 내보내기 간격을 기다리는 사이 슬롯이 났다 — 그래도 여기서 시작하지 않고, 받은 자리는 반납한다."""
    async def scenario():
        queue = make_draining_queue(drain_spacing=0.3)
        await queue.sem.acquire()
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            waiter = open_events(client, queue)
            pending = await waiting_inside_queue(waiter)
            queue.drain()
            await asyncio.sleep(0.05)                          # 대기자가 간격 대기(0.3초) 안에 들어갔다
            queue.sem.release()                                # 그사이 생성이 끝나 슬롯이 났다
            return await asyncio.wait_for(rest_of(pending, waiter), HANDOFF_DEADLINE), queue

    received, queue = asyncio.run(scenario())
    assert received == [{"type": "retry"}]
    assert not queue.sem.locked() and queue.waiting_count == 0


@respx.mock
def test_generation_already_running_finishes_after_drain():
    respx.post(f"{OLLAMA}/api/chat").mock(return_value=httpx.Response(200, content=STREAM_BODY))

    async def scenario():
        queue = make_draining_queue()
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            running = open_events(client, queue)
            assert await running.__anext__() == {"type": "start"}     # 생성이 시작됐다
            queue.drain()
            return await collect(running), queue

    received, queue = asyncio.run(scenario())
    assert received == [{"type": "token", "text": "a"}, {"type": "token", "text": "b"}, {"type": "done"}]
    assert not queue.sem.locked()


def test_drain_releases_waiters_with_spacing():
    async def scenario():
        queue = make_draining_queue(drain_spacing=0.1)
        await queue.sem.acquire()
        async with httpx.AsyncClient(base_url=OLLAMA) as client:
            generators = [open_events(client, queue) for _ in range(3)]
            pendings = [await waiting_inside_queue(generator) for generator in generators]
            started = asyncio.get_running_loop().time()
            queue.drain()
            times = []

            async def finish(pending, generator):
                await rest_of(pending, generator)
                times.append(asyncio.get_running_loop().time() - started)

            await asyncio.wait_for(asyncio.gather(
                *(finish(pending, generator) for pending, generator in zip(pendings, generators))), HANDOFF_DEADLINE)
        return times

    times = asyncio.run(scenario())
    gaps = [later - earlier for earlier, later in zip(times, times[1:])]
    assert all(gap >= 0.08 for gap in gaps), f"대기자를 간격 없이 내보냈다: {times}"
    assert times[-1] < 1.0, f"간격(0.1초) 세 번이면 끝나야 한다: {times}"
