"""app/domain/prompt.py 미러 — 프롬프트 조립과 계약 스키마."""
import pytest
from pydantic import ValidationError

from app.domain.prompt import (
    KNOWN_DOC_KINDS, KNOWN_TOPICS, FilterResult, RewriteResult, rewrite_messages,
)


def test_rewrite_messages_embeds_candidates_and_query():
    messages = rewrite_messages("lsm tree")
    system, user = messages
    assert system["role"] == "system" and user["role"] == "user"
    assert user["content"] == "lsm tree"
    for topic in KNOWN_TOPICS:
        assert topic in system["content"]
    for doc_kind in KNOWN_DOC_KINDS:
        assert doc_kind in system["content"]
    assert "/no_think" in system["content"]   # thinking 비활성 (design D2)


def test_rewrite_result_requires_keywords():
    with pytest.raises(ValidationError):
        RewriteResult(intent="x", keywords=[])   # min_length=1


def test_filters_default_to_null():
    result = RewriteResult(intent="x", keywords=["k"])
    assert result.filters == FilterResult(topic=None, doc_kind=None)
