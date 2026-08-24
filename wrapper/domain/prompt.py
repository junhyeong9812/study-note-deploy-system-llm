"""도메인 — 역할별 프롬프트와 입출력 스키마. 내부 계층을 import하지 않는 최심층. (design D8)"""
from pydantic import BaseModel, ConfigDict, Field


class FilterResult(BaseModel):
    """ES 질의 필터 — 확신 없으면 null."""

    topic: str | None = Field(default=None, description="추정 주제 폴더명 (확신 없으면 null)")
    doc_kind: str | None = Field(
        default=None,
        description="문서 유형: question|summary|answer|index|readme|post (확신 없으면 null)",
    )


class RewriteResult(BaseModel):
    """검색어 구조화 결과 — backend가 ES 질의를 조립할 재료. (design D2)"""

    intent: str = Field(description="질문 의도 한 줄 (한국어)")
    keywords: list[str] = Field(min_length=1, description="BM25용 핵심 키워드")
    expanded: list[str] = Field(default_factory=list, description="동의어·영↔한 확장어")
    filters: FilterResult = Field(default_factory=FilterResult)


class DigestChunk(BaseModel):
    path: str
    heading: str
    content: str


class DigestIn(BaseModel):
    """/digest 입력 계약 — 2차 구현 예약. (design D2·D5)"""

    model_config = ConfigDict(extra="forbid")   # 계약 밖 필드 거부

    query: str = Field(min_length=1, max_length=300)
    chunks: list[DigestChunk] = Field(max_length=20)


class DigestResult(BaseModel):
    """/digest 출력 계약 — 2차 구현 예약."""

    summary: str
    source_paths: list[str]


KNOWN_TOPICS = [
    "algorithm", "api-design", "cs", "data-structure", "db-engine-lab",
    "domain-modeling-advanced", "domain-modeling-basic", "ops-patterns",
    "programmers", "reference",
]

KNOWN_DOC_KINDS = ["question", "summary", "answer", "index", "readme", "post"]

REWRITE_SYSTEM = (
    "너는 개발 공부 노트 검색 시스템의 질의 분석기다. /no_think\n"
    "사용자 검색어를 분석해 JSON으로만 답한다. 설명 금지.\n"
    "- intent: 무엇을 찾으려는지 한 줄\n"
    "- keywords: 검색 핵심 키워드 (원문 언어 유지)\n"
    "- expanded: 동의어와 영어↔한국어 대응어\n"
    "- filters.topic: 관련 주제 폴더명. 후보: {topics}. 확신 없으면 null\n"
    "- filters.doc_kind: 문서 유형. 후보: {doc_kinds}. 확신 없으면 null"
)


def rewrite_messages(query: str) -> list[dict]:
    return [
        {
            "role": "system",
            "content": REWRITE_SYSTEM.format(
                topics=", ".join(KNOWN_TOPICS), doc_kinds=", ".join(KNOWN_DOC_KINDS)
            ),
        },
        {"role": "user", "content": query},
    ]
