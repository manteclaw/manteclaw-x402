# x402 Agent Services (v3)

Monetized API services for AI agents via the x402 protocol on Base.
Other agents pay USDC per call; payments go to the wallet in `.env`
(confirmed payee: Alex's main wallet).

## Services

| Endpoint | Price | Description |
|----------|-------|-------------|
| `GET /health` | free | Liveness probe — no payment, no on-chain call |
| `GET /defi/gas-tracker` | 0.005 USDC | Current Base gas prices (slow/average/fast) |
| `GET /defi/token-price/{address}` | 0.01 USDC | Token metadata via on-chain calls |
| `GET /defi/wallet-analyzer/{address}` | 0.01 USDC | Wallet balance and activity |
| `GET /defi/token-security/{address}` | 0.02 USDC | Basic token security check |
| `GET /agent/earnings-report` | 0.05 USDC | Earnings summary (figures are stale placeholders — see REPAIR-NOTES.md) |
| `GET /mcp/status` | 0.001 USDC | Service status |
| `GET /mcp/tools` | 0.001 USDC | Tool list and pricing |

Unpaid calls to a priced endpoint return HTTP 402 with payment details.

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env   # edit if needed; WALLET_ADDRESS is pre-filled
./run-v3.sh
```

Health check (no payment needed):

```bash
curl http://localhost:4020/health
```

## Deploying as a service

See `x402-v3.service` (systemd unit). Adjust the paths in it to the
deploy host, copy it into place, then start it. The unit restarts only
on failure, with a burst limit — a service that crashes on startup will
not loop forever.

## Links

- **GitHub:** https://github.com/manteclaw/x402-agent-services
- **Status:** Repaired Nov 2026, deploy-ready, not yet launched — see REPAIR-NOTES.md

## Earning Potential

Unverified projections from the original README — do not treat as a forecast:

- 1,000 calls/day at 0.02 USDC = $20/day
- 30,000 calls/month = $600/month
- Scale with agent marketplace discovery
