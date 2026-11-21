# x402 v3 service (FastAPI + uvicorn) on Fly.io
# Python 3.12 matches the local verification venv (3.12.3).
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY server-v3.py ./
COPY tx_store.json ./
# Ship the discovery file that names the correct payee. The agent
# catalog is deliberately NOT copied: it names a wrong wallet and
# endpoints that do not exist in v3 (see REPAIR-NOTES.md).
COPY .well-known/x402.json ./.well-known/x402.json
# Agent catalog (rewritten Nov 19, 2026, Alex-approved): main wallet as
# payee, only the six live v3 endpoints, stale earnings-report excluded.
COPY .well-known/agent-catalog.json ./.well-known/agent-catalog.json

EXPOSE 8080

# Fly sets $PORT (8080 by default); server-v3.py reads it, default 4020.
CMD ["sh", "-c", "uvicorn server-v3:app --host 0.0.0.0 --port ${PORT:-8080}"]
