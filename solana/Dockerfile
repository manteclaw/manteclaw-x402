# x402 Solana-settlement variant (FastAPI + uvicorn) on Fly.io
# Python 3.12 matches the local verification venv (3.12.3).
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY server-solana.py ./
COPY tx_store.json ./
# Public catalog page: served free at /catalog by server-solana.py.
# Must be copied explicitly — the Dockerfile lists files, it does not
# copy the whole repo.
COPY catalog.html ./
# Discovery documents naming the Solana payee, network and USDC mint.
# (The Base service's AiPayGen / 402index domain-verification files are
# NOT shipped: those verifications belong to the Base service's host.)
COPY .well-known/x402.json ./.well-known/x402.json
COPY .well-known/agent-catalog.json ./.well-known/agent-catalog.json
# Extensionless copy of x402.json (identical content): Agent402 probes
# /.well-known/x402 (no extension) and backs off on a 404.
COPY .well-known/x402 ./.well-known/x402

EXPOSE 8080

# Fly sets $PORT (8080 by default); server-solana.py reads it, default 4020.
CMD ["sh", "-c", "uvicorn server-solana:app --host 0.0.0.0 --port ${PORT:-8080}"]
