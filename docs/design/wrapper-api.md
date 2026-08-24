# 의사결정 — Ollama 래핑 서버(FastAPI) 계약

> 역할: qwen3:8b(Ollama)를 **역할 엔드포인트**로 감싸 프롬프트·스키마의 단일 출처가 된다.
> 큐를 직접 만들지 않는다 — 직렬화는 Ollama(`OLLAMA_NUM_PARALLEL=1`, 내부 큐), 래핑은 **수문(backpressure)+계약 검증**만.

## D1. 토폴로지

```
backend ──HTTP──▶ wrapper :8000 (LAN 노출 유일)
                     │ compose 내부 네트워크 (호스트 비노출)
                     ▼
                  ollama :11434  (GPU, OLLAMA_NUM_PARALLEL=1)
```
- Ollama 포트는 호스트에 바인딩하지 않는다 — backend가 프롬프트를 우회해 직접 치는 경로 차단.
- 모델은 env `MODEL`(기본 `qwen3:8b`)로 고정 — 요청이 모델을 지정할 수 없다.

## D2. 엔드포인트

| 경로 | 역할 | 타임아웃 | 출력 스키마 |
|---|---|---|---|
| `POST /rewrite` | 검색어 → 구조화 질의. thinking 비활성(`/no_think`) | 5s | `{intent, keywords[], expanded[], filters{topic?, doc_kind?}}` |
| `POST /digest` | 검색 청크들 → 한국어 다이제스트 (2차 범위 — 계약만 예약) | 30s | `{summary, source_paths[]}` |
| `GET /health` | ollama `/api/tags` 도달 + 모델 존재 확인 | 2s | `{status, model}` |

- 입력: `/rewrite {query}` · `/digest {query, chunks:[{path, heading, content}]}` — 프롬프트 조립은 전부 wrapper(`prompts.py`).

## D3. 동시성 — 세마포어 + 즉시 거절 (대기 큐 없음)

- `asyncio.Semaphore(2)` — 획득 실패(논블로킹) 시 **즉시 `503 {"retry_after": n}`**.
- 이유: 검색은 LLM 없이 성립한다(원문 그대로 BM25+kNN). backend는 503/타임아웃 수신 시 **rewrite 생략 폴백** — 사용자 검색이 LLM에 인질로 잡히지 않는 게 최우선 불변식.
- 세마포어 2 > NUM_PARALLEL 1 인 이유: 1건 추론 중 1건이 Ollama 내부 큐에 걸치는 것까지만 허용(파이프라이닝), 그 이상은 거절.

## D4. JSON 검증 — 3단 방어

1. Ollama `format`에 JSON 스키마 전달(구조화 출력 강제).
2. pydantic 파싱·검증. 실패 시 **오류 내용을 포함해 1회 재시도**.
3. 재시도도 실패 → `422 {"error": "schema_violation"}` — backend는 폴백. (터진 JSON을 절대 그대로 전달하지 않는다.)

## D5. 비범위(1차)

- 인증 없음(LAN 전용 — 외부 비노출이 전제. 엣지에 붙이는 순간 재설계), 스트리밍 없음, 대화 상태 없음, `/digest` 구현 보류(계약만).
- `[구현 검증]` 세마포어 값 2·타임아웃 5s/30s·재시도 1회는 실측 후 조정 → `implementation-verification.md`.

## D6. 리포 구조

```
docker-compose.yml   # ollama(GPU, expose only) + wrapper(:8000) + 양쪽 healthcheck·restart: unless-stopped
ollama/              # env (OLLAMA_NUM_PARALLEL=1, KEEP_ALIVE 등), 모델 볼륨
wrapper/
  main.py            # 엔드포인트·세마포어·타임아웃
  prompts.py         # 역할별 프롬프트+스키마 (이 리포의 존재 이유)
  Dockerfile
```
