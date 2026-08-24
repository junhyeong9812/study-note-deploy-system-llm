"""도메인 — 역할별 프롬프트와 출력 스키마. 아무것도 import하지 않는 최심층. (design D8)"""
from pydantic import BaseModel, Field


class RewriteResult(BaseModel):
    """검색어 구조화 결과 — backend가 ES 질의를 조립할 재료."""

    intent: str = Field(description="질문 의도 한 줄 (한국어)")
    keywords: list[str] = Field(min_length=1, description="BM25용 핵심 키워드")
    expanded: list[str] = Field(default_factory=list, description="동의어·영↔한 확장어")
    topics: list[str] = Field(default_factory=list, description="추정 주제 폴더명 (모르면 빈 배열)")


REWRITE_SYSTEM = (
    "너는 개발 공부 노트 검색 시스템의 질의 분석기다. /no_think\n"
    "사용자 검색어를 분석해 JSON으로만 답한다. 설명 금지.\n"
    "- intent: 무엇을 찾으려는지 한 줄\n"
    "- keywords: 검색 핵심 키워드 (원문 언어 유지)\n"
    "- expanded: 동의어와 영어↔한국어 대응어\n"
    "- topics: 관련 주제 폴더명. 후보: {topics}. 확신 없으면 빈 배열"
)


def rewrite_messages(query: str, topics: list[str]) -> list[dict]:
    return [
        {"role": "system", "content": REWRITE_SYSTEM.format(topics=", ".join(topics))},
        {"role": "user", "content": query},
    ]


# study-note 주제 폴더 — backend가 요청에 실어 보내면 그걸 쓰고, 없으면 이 기본값
DEFAULT_TOPICS = [
    "algorithm", "api-design", "cs", "data-structure", "db-engine-lab",
    "domain-modeling-advanced", "domain-modeling-basic", "ops-patterns",
    "programmers", "reference",
]
