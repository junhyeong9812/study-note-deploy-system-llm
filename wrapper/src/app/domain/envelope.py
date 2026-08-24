"""응답 정규화 — 모든 업무 응답은 success 플래그가 있는 봉투(envelope)에 담는다. (design D2)

backend는 상태코드나 본문 모양을 각각 알 필요 없이 `success` 하나로 분기한다.
/health는 docker healthcheck(인프라 계약)라 봉투를 씌우지 않는다.
"""
from typing import Any

from pydantic import BaseModel


class ErrorBody(BaseModel):
    code: str                      # busy | upstream | upstream_timeout
    detail: str | None = None      # | schema_violation | invalid_request
    retry_after: int | None = None


class SuccessEnvelope(BaseModel):
    success: bool = True
    data: Any


class ErrorEnvelope(BaseModel):
    success: bool = False
    error: ErrorBody


def ok(data: Any) -> dict:
    return SuccessEnvelope(data=data).model_dump()


def fail(code: str, *, detail: str | None = None, retry_after: int | None = None) -> dict:
    return ErrorEnvelope(
        error=ErrorBody(code=code, detail=detail, retry_after=retry_after)
    ).model_dump(exclude_none=True)
