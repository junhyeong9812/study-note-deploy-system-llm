"""로그 규약 — `requestId:server-name:message`. (root docs/logging.md 정본)

서버 로그(stdout)에 항상 남기고, 같은 라인을 Redis Stream(XADD)으로도 전송해
중앙 수집이 가능하게 한다. **전송 실패는 요청 처리를 절대 방해하지 않는다**
(짧은 타임아웃 + 30초 백오프 + 예외 전량 흡수). requestId는 backend가 발행한다.
"""
import logging
import os
import time

SERVER_NAME = os.environ.get("SERVER_NAME", "llm-wrapper")
LOG_REDIS_URL = os.environ.get("LOG_REDIS_URL", "")   # 비우면 서버 로그만 (배포 .env에서 지정)
LOG_STREAM = os.environ.get("LOG_STREAM", "logs")
_REDIS_BACKOFF_SECONDS = 30.0
_STREAM_MAXLEN = 10_000

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
_server_logger = logging.getLogger("wrapper")


def format_line(request_id: str, message: str) -> str:
    return f"{request_id}:{SERVER_NAME}:{message}"


class _RedisSink:
    def __init__(self) -> None:
        self._client = None
        self._disabled_until = 0.0

    async def send(self, level: str, line: str) -> None:
        if not LOG_REDIS_URL or time.monotonic() < self._disabled_until:
            return
        try:
            if self._client is None:
                import redis.asyncio as redis_async
                self._client = redis_async.from_url(
                    LOG_REDIS_URL, socket_connect_timeout=0.3, socket_timeout=0.3
                )
            await self._client.xadd(
                LOG_STREAM,
                {"level": level, "line": line},
                maxlen=_STREAM_MAXLEN,
                approximate=True,
            )
        except Exception:  # noqa: BLE001 — 로그 전송은 어떤 예외도 밖으로 내보내지 않는다
            self._disabled_until = time.monotonic() + _REDIS_BACKOFF_SECONDS


_sink = _RedisSink()


async def log(request_id: str, message: str, level: str = "info") -> None:
    """성공·실패 모두 기록한다 — 성공 경로도 반드시 호출할 것 (규약)."""
    line = format_line(request_id, message)
    log_method = getattr(_server_logger, level if level in ("info", "warning", "error") else "info")
    log_method(line)
    await _sink.send(level, line)
