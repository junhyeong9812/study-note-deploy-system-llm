# study-note-deploy-system-llm

study-note 검색 시스템의 **로컬 LLM 계층**. GPU 서버에서 Ollama(qwen3:8b)를 돌리고,
그 앞에 얇은 FastAPI 래퍼를 세워 **프롬프트·모델·동시성 제어의 단일 출처**가 된다.
backend는 이 서버의 HTTP API만 알면 되고, 모델 교체·프롬프트 튜닝은 이 리포 배포만으로 끝난다.

## 구조

```
backend ──HTTP──▶ wrapper :8000   (LAN에 노출되는 유일한 포트)
                    │  compose 내부 네트워크 (호스트 비노출)
                    ▼
                  ollama :11434   (GPU · OLLAMA_NUM_PARALLEL=1 로 한 번에 1건 추론)
```

- Ollama 포트를 호스트에 열지 않는 이유: backend가 프롬프트를 우회해 직접 치는 경로를 막아
  "프롬프트는 이 리포가 소유한다"를 구조로 강제.
- 큐를 직접 만들지 않는다 — 직렬화는 Ollama 내부 큐가, 래퍼는 **넘치면 즉시 거절**(세마포어)만.

## API

모든 업무 응답은 봉투로 정규화된다 — `success` 플래그 하나로 분기하면 된다.

| 메서드·경로 | 역할 | 성공 | 실패(코드) |
|---|---|---|---|
| `POST /rewrite` | 사용자의 검색어를 **검색엔진이 잘 찾는 형태로 변환** — 핵심 키워드, 영↔한 대응어, 어느 주제/문서종류를 뒤질지. 답을 만들지 않는다 | 200 | 503 busy·upstream·upstream_timeout / 422 schema_violation·invalid_request |
| `GET /health` | Ollama 도달 + 모델 존재 확인. 봉투 없음(docker healthcheck 계약) | 200 | 503 |

```jsonc
// POST /rewrite  요청 (request_id는 backend가 발행 — docs 로그 규약)
{ "request_id": "req-123", "query": "낙관적 락이랑 비관적 락 차이" }
// 응답
{ "success": true, "data": {
    "intent": "낙관적 락과 비관적 락의 차이 비교",
    "keywords": ["낙관적 락", "비관적 락"],
    "expanded": ["optimistic locking", "pessimistic locking"],
    "filters": { "topic": "db-engine-lab", "doc_kind": "question" } } }
// 거절 예
{ "success": false, "error": { "code": "busy", "retry_after": 2 } }
```

## 실행

```bash
docker compose up -d --build
docker exec llm-ollama ollama pull qwen3:8b   # 최초 1회 (5.2GB)
curl -s localhost:8000/health                  # {"status":"ok",...} 이면 준비 완료
```

| env | 기본 | 의미 |
|---|---|---|
| `MODEL` | qwen3:8b | 서빙 모델 (요청이 모델을 지정할 수 없다) |
| `MAX_INFLIGHT` | 2 | 동시 처리 상한 — 초과는 대기 없이 503 |
| `REWRITE_TIMEOUT_S` | 5 | 요청 단위 총예산(재시도 포함) |
| `LOG_REDIS_URL` | (비움) | 로그 중앙 큐. 비우면 stdout만 |

## 테스트 / 문서

- 테스트: `cd wrapper && pip install -r requirements-dev.txt && pytest` — `src/app` ↔ `src/test` 미러, 28건
- 설계 결정: `docs/design/wrapper-api.md` (D1~D9) · 검증 이연 항목: `docs/design/implementation-verification.md`
- 트러블슈팅: `docs/troubleshooting/qwen3-thinking-runaway.md` — thinking 폭주로 전 요청 타임아웃 났던 건
