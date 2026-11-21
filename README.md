# Manteclaw x402 API Service (v4)

Pay-per-call API service for AI agents and apps, on Base via the x402 protocol. No API keys, no subscriptions — every call is priced in USDC and paid per request.

- **Live service:** https://manteclaw-x402.fly.dev
- **Public catalog (pay from the page):** https://manteclaw-x402.fly.dev/catalog
- **Machine-readable catalog:** https://manteclaw-x402.fly.dev/.well-known/agent-catalog.json
- **x402 discovery file:** https://manteclaw-x402.fly.dev/.well-known/x402.json

## What it is

100 endpoints (99 paid + free `/health`): token prices and security checks, wallet analytics, DEX pairs and Uniswap V3 pools, DeFi yields and TVL, blocks and transactions, NFTs, ENS, and on-chain utilities. All data comes from live sources (Base RPC, Dexscreener, DeFiLlama, CoinGecko, ENS) — no stubs, no fake data.

Prices: $0.001–$0.02 USDC per call. Payments go to `0xea0aad6dfa33d6ea4aec842c24d2015e3a4b3175` on Base (chain 8453, USDC `0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913`).

## How to call an endpoint

Unpaid calls return HTTP 402 with the payment terms in the `payment-required` header (x402 v2, `exact` scheme). Sign an EIP-3009 `transferWithAuthorization` with your wallet (off-chain, no gas) and retry with the signature in the `X-PAYMENT` header. Payment is valid for 300 seconds.

Cheapest test call: `GET /mcp/status` ($0.001). Full how-to with TypeScript and Python examples: see `HOW-TO-CALL.md` in this repo.

Free endpoints (no payment): `GET /` (landing page), `GET /health`, `GET /catalog`, `GET /.well-known/*`.

## Running it yourself

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python server-v4.py
```

Health check (no payment needed):

```bash
curl http://localhost:4020/health
```

## Deploy

Deploys go through GitHub Actions (`.github/workflows/deploy.yml`) to Fly.io (`flyctl deploy --remote-only`). The Dockerfile lists every file the image ships explicitly — add a `COPY` line for any new file, or it will not be in the container.
