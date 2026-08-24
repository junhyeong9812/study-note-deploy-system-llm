"""app/validate.py 미러 — 재시도 정책 단위 검증 (HTTP 무관)."""
import asyncio

import pytest
from pydantic import BaseModel

from app.validate import SchemaViolation, parse_with_retry


class Sample(BaseModel):
    value: int


def test_first_parse_ok_no_retry():
    calls = []

    async def retry(feedback: str) -> str:
        calls.append(feedback)
        return '{"value": 2}'

    result = asyncio.run(parse_with_retry(Sample, '{"value": 1}', retry))
    assert result.value == 1
    assert calls == []                       # 정상 파싱 시 재시도 없음


def test_retry_once_then_ok_with_feedback():
    calls = []

    async def retry(feedback: str) -> str:
        calls.append(feedback)
        return '{"value": 2}'

    result = asyncio.run(parse_with_retry(Sample, "broken", retry))
    assert result.value == 2
    assert len(calls) == 1
    assert "스키마" in calls[0]              # 오류 피드백 포함 재요청


def test_retry_exhausted_raises_schema_violation():
    async def retry(feedback: str) -> str:
        return "still broken"

    with pytest.raises(SchemaViolation):
        asyncio.run(parse_with_retry(Sample, "broken", retry))
