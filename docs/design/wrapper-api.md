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
| `POST /rewrite` | 검색어 → 구조화 질의. thinking 비활성(`/no_think`) | **총예산** 5s (재시도 포함) | `{intent, keywords[], expanded[], filters{topic?, doc_kind?}}` |
| `POST /digest` | 검색 청크들 → 한국어 다이제스트 (2차 범위 — 계약만 예약) | 30s | `{summary, source_paths[]}` |
| `GET /health` | ollama `/api/tags` 도달 + 모델 존재 확인 | 2s | `{status, model}` |

- 입력: `/rewrite {query}`(≤300자, 계약 밖 필드 거부) · `/digest {query, chunks:[{path, heading, content}]}` — 프롬프트 조립은 전부 wrapper(`domain/prompt.py`).
- **오류 계약(`/rewrite` 폴백 트리거 통일)**: `503`(busy | upstream | upstream_timeout — 본문 `error` 필드로 구분) · `422`(schema_violation·입력 검증) — `/rewrite`는 이 두 status 외 오류 상태코드를 내지 않는다. `/digest`는 구현 전까지 `501`(예약 상태 — 구현 시 이 오류 계약으로 편입). `/health`는 정상만 200, 모델 부재·업스트림 이상은 503(healthy 위장 금지).

## D3. 동시성 — 세마포어 + 즉시 거절 (대기 큐 없음)

- `asyncio.Semaphore(2)` — 획득 실패(논블로킹) 시 **즉시 `503 {"retry_after": n}`**.
- 이유: 검색은 LLM 없이 성립한다(원문 그대로 BM25+kNN). backend는 503/422 수신 시 **rewrite 생략 폴백** — 사용자 검색이 LLM에 인질로 잡히지 않는 게 최우선 불변식. 타임아웃은 **요청 단위 총예산**(재시도가 예산을 늘리지 않는다).
- 세마포어 2·타임아웃 5s는 계약 기본값 — env(`MAX_INFLIGHT`·`REWRITE_TIMEOUT_S`)는 `[구현 검증]` 실측 조정용 노브다(임의 해제 목적 아님).
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

## D7. 런타임 — FastAPI (WebFlux 대비 결정)

동시성이 설계상 2로 고정된 워크로드라 리액티브의 이점이 발동하지 않고, 래핑의 핵심 임무(JSON 스키마 검증)는 pydantic이 1급 — 모델 정의가 Ollama `format` 스키마로 그대로 재사용된다. 실패 모드 단순성(코루틴+표준 세마포어 vs Mono/Flux 에러 삼킴 함정)·자원(수십 MB vs JVM 수백 MB) 모두 FastAPI 우위. 스택 통일 필요성은 낮음 — 래핑은 격리 부품(Python=ML 인접 계층 경계와 일치).

## D8. 코드 구조 (심플 레이어드)

```
wrapper/
  pyproject.toml        # pytest 설정 (pythonpath=src, testpaths=src/test)
  Dockerfile
  src/
    app/                # 코드 (패키지 — 절대 임포트 `from app...`)
      app.py            # 프레임워크 구동 설정 — FastAPI 인스턴스·lifespan(세마포어·ollama http client)
      api.py            # 라우터 — /rewrite /digest /health (HTTP 관심사만)
      domain/prompt.py  # 도메인 — 역할별 프롬프트 + 입출력 pydantic 스키마 (이 리포의 존재 이유)
      usecase/rewrite.py# 유즈케이스 — 세마포어 획득→총예산→ollama 호출→검증·재시도
      validate.py       # 검증 정책 — pydantic 파싱, 오류 피드백 재시도 1회, 422 확정
      logger.py         # 로그 규약 구현 (D9)
    test/               # app/ 미러 — 모듈별 단위 + test_api.py 통합
      test_api.py · test_validate.py · test_logger.py
      domain/test_prompt.py · usecase/test_rewrite.py
```
- 의존 방향: api → usecase → (domain, validate). domain은 **내부 계층을 import하지 않는다**(표준 라이브러리·pydantic은 허용).

## D9. 로깅 (정본 = 프로젝트 루트 docs/logging.md)

- 포맷 `requestId:server-name:message` — requestId는 **backend가 발행**, wrapper는 필수 입력으로 받아 기록만 한다(`/rewrite`·`/digest` 입력 계약에 `request_id` 포함).
- 이중 기록: stdout(항상) + Redis Stream `XADD`(env `LOG_REDIS_URL` 설정 시 — 실패 무해: 0.3s 타임아웃·30s 백오프·예외 전량 흡수).
- 성공도 기록한다 — `[구현 검증]` 이연 항목(세마포어·타임아웃·재시도율)의 실측 데이터가 이 로그다.
