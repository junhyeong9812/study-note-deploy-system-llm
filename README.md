# study-note-deploy-system-llm

Ollama(qwen3) 래핑 API — 검색어 구조화(`/rewrite`)·다이제스트(`/digest`, 예약). GPU 서버에 배포.

- 설계: `docs/design/wrapper-api.md` (D1~D8)
- 구동: `docker compose up -d --build` → 최초 1회 `docker exec llm-ollama ollama pull qwen3:8b`
- 테스트: `cd wrapper && pip install -r requirements-dev.txt && pytest`
