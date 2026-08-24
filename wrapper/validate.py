"""검증 정책 — pydantic 파싱과 재시도 1회. 터진 JSON을 밖으로 내보내지 않는다. (design D4)"""
from typing import Awaitable, Callable, TypeVar

from pydantic import BaseModel, ValidationError

T = TypeVar("T", bound=BaseModel)


class SchemaViolation(Exception):
    """재시도까지 실패 — api 계층에서 422로 확정한다."""

    def __init__(self, last_error: str):
        self.last_error = last_error
        super().__init__(last_error)


async def parse_with_retry(
    schema: type[T],
    raw: str,
    retry: Callable[[str], Awaitable[str]],
) -> T:
    """1차 파싱 실패 시 오류 내용을 피드백해 한 번만 재요청한다."""
    try:
        return schema.model_validate_json(raw)
    except ValidationError as first:
        raw2 = await retry(
            f"이전 출력이 스키마를 위반했다: {first.errors()[:3]}\n"
            f"스키마에 맞는 JSON만 다시 출력하라."
        )
        try:
            return schema.model_validate_json(raw2)
        except ValidationError as second:
            raise SchemaViolation(str(second.errors()[:3])) from second
