"""app/logger.py 미러 — 포맷 규약과 전송 실패 무해성."""
import asyncio

from app.logger import format_line, log


def test_format_line_rule():
    # 규약: requestId:server-name:message (root docs/logging.md)
    assert format_line("req-9", "rewrite ok") == "req-9:llm-wrapper:rewrite ok"


def test_log_without_redis_url_never_raises():
    # LOG_REDIS_URL 미설정 — stdout만 기록하고 예외 없이 끝나야 한다 (fire-and-forget)
    asyncio.run(log("req-1", "success path", "info"))
    asyncio.run(log("req-1", "error path", "error"))
    asyncio.run(log("req-1", "unknown level falls back to info", "debug"))
