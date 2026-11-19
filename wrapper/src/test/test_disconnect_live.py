"""실제 uvicorn 서버에 소켓으로 붙어, 상대가 연결을 끊었을 때 대기 자리·생성 슬롯이 돌아오는지 확인한다.
(제너레이터를 직접 소비하는 테스트는 서버가 끊김을 어떻게 전달하는지 검증하지 못한다.)"""
import asyncio
import json
import socket
import threading
import time

import httpx
import pytest
import respx
import uvicorn

from app.app import app

OLLAMA = "http://ollama:11434"


async def never_ending_ollama():
    yield b'{"message":{"content":"a"}}\n'
    await asyncio.sleep(60)                     # 생성이 한참 이어지는 중
    yield b'{"done":true}\n'


@pytest.fixture
def live_server():
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    listener.close()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    with respx.mock(assert_all_called=False) as ollama:
        ollama.post(f"{OLLAMA}/api/chat").mock(
            side_effect=lambda request: httpx.Response(200, content=never_ending_ollama()))
        thread.start()
        deadline = time.time() + 5
        while not server.started and time.time() < deadline:
            time.sleep(0.02)
        assert server.started
        yield port
        server.should_exit = True
        thread.join(5)


def open_chat(port: int, request_id: str) -> socket.socket:
    payload = json.dumps({"request_id": request_id,
                          "messages": [{"role": "user", "content": "hi"}]}).encode()
    connection = socket.create_connection(("127.0.0.1", port), timeout=5)
    connection.sendall(b"POST /chat HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\n"
                       b"Accept: application/x-ndjson\r\nContent-Length: %d\r\n\r\n" % len(payload) + payload)
    return connection


def read_until(connection: socket.socket, marker: bytes) -> bytes:
    received = b""
    while marker not in received:
        chunk = connection.recv(4096)
        assert chunk, f"연결이 먼저 닫혔다: {received!r}"
        received += chunk
    return received


def wait_for(condition, seconds: float = 3.0) -> bool:
    deadline = time.time() + seconds
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return False


def test_socket_close_frees_queue_place_and_generation_slot(live_server):
    port = live_server
    queue = app.state.chat_queue
    first, second = open_chat(port, "hold-1"), open_chat(port, "hold-2")
    read_until(first, b'"token"'); read_until(second, b'"token"')        # 동시 2건 생성 중 = 포화
    waiter = open_chat(port, "waiter")
    read_until(waiter, b'"queued"')
    assert queue.waiting_count == 1

    waiter.close()                                                       # 대기 중 이탈
    assert wait_for(lambda: queue.waiting_count == 0), "떠난 대기자가 자리를 붙잡고 있다"

    first.close()                                                        # 생성 중 이탈
    assert wait_for(lambda: not queue.sem.locked()), "떠난 요청이 생성 슬롯을 붙잡고 있다"

    newcomer = open_chat(port, "newcomer")                               # 돌아온 슬롯으로 바로 시작
    assert b'"queued"' not in read_until(newcomer, b'"start"')
    newcomer.close(); second.close()
    assert wait_for(lambda: queue.sem._value == 2), "슬롯이 전부 돌아오지 않았다"
