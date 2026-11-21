# x402 v4 service (FastAPI + uvicorn) on Fly.io
# Python 3.12 matches the local verification venv (3.12.3).
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY server-v4.py ./
COPY tx_store.json ./
# Public catalog page (LinkedIn shareable): served free at /catalog by
# server-v4.py. Must be copied explicitly — the Dockerfile lists files,
# it does not copy the whole repo.
COPY catalog.html ./
# Ship the discovery file that names the correct payee. The agent
# catalog is deliberately NOT copied: it names a wrong wallet and
# endpoints that do not exist in v3 (see REPAIR-NOTES.md).
COPY .well-known/x402.json ./.well-known/x402.json
# Agent catalog (rewritten Nov 20, 2026 for v4): main wallet as payee,
# all 100 endpoints (99 paid + free /health), stale earnings-report excluded.
COPY .well-known/agent-catalog.json ./.well-known/agent-catalog.json
# AiPayGen site-verification file (token from the gateway route creation,
# Nov 20, 2026) — proves control of the host so the gateway routes can be
# listed on the AiPayGen marketplace.
COPY .well-known/aipaygen-verify.txt ./.well-known/aipaygen-verify.txt
# Extensionless copy of x402.json (identical content): Agent402 probes
# /.well-known/x402 (no extension) and backs off on a 404.
COPY .well-known/x402 ./.well-known/x402

EXPOSE 8080

# Fly sets $PORT (8080 by default); server-v4.py reads it, default 4020.
CMD ["sh", "-c", "uvicorn server-v4:app --host 0.0.0.0 --port ${PORT:-8080}"]
