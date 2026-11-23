# Manteclaw MCP Lite

A free, thin MCP (Model Context Protocol) server. It is a discovery funnel
for the paid **Manteclaw x402 API** — 199 pay-per-call endpoints on Base,
paid in USDC via the x402 protocol ($0.01–0.02/call, no account, no API
key). Full catalog: https://manteclaw-x402.fly.dev/catalog

The lite server gives away a small, genuinely useful sample for free.
Every data tool's result ends with one line pointing at the paid catalog.

## Tools

| Tool | What it does | Data source |
|---|---|---|
| `base_gas` | Base base-fee snapshot (slow/average/fast, gwei) | Base public RPC |
| `base_block` | Latest Base block summary | Base public RPC |
| `token_price` | Spot USD price + 24h change by symbol or Base contract | DexScreener |
| `token_info` | ERC20 metadata (on-chain) + market snapshot for a Base contract | Base RPC + DexScreener |
| `ens_resolve` | ENS name → address, or address → ENS name | ENSIdeas / ensdata.net |
| `wallet_snapshot` | ETH + USDC balance of an address on Base | Base public RPC |
| `dex_pair` | Top Base DEX pair for a token (price, liquidity, volume) | DexScreener |
| `manteclaw_catalog` | Menu of the paid service + catalog URL | static |

All upstreams are free public APIs — the same sources the paid service
uses. No API keys, no secrets, and the lite server never calls the paid
endpoints. Upstream failures return a clean error result; the server
never crashes on a bad upstream.

## Run locally

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# stdio transport (for local MCP clients):
.venv/bin/python server.py

# streamable HTTP transport:
PORT=8080 .venv/bin/python server.py --http
# MCP endpoint: http://localhost:8080/mcp
# Health:       http://localhost:8080/health
```

## Connect a client

Streamable HTTP (after deploy, app `manteclaw-mcp-lite`, region iad):

```json
{
  "mcpServers": {
    "manteclaw-lite": {
      "url": "https://manteclaw-mcp-lite.fly.dev/mcp"
    }
  }
}
```

Stdio:

```json
{
  "mcpServers": {
    "manteclaw-lite": {
      "command": "python",
      "args": ["/path/to/mcp-lite/server.py"]
    }
  }
}
```

## Test

```bash
PORT=18080 .venv/bin/python server.py --http &   # in another shell
.venv/bin/python test_client.py                  # HTTP round-trips
.venv/bin/python test_client.py --stdio          # stdio smoke test
```

See `test-output.log` for a captured passing run.

## Deploy (Fly.io)

```bash
fly launch --no-deploy   # first time only, config already in fly.toml
fly deploy
```

The server binds `0.0.0.0:$PORT` (default 8080) and serves `/health`
for Fly's checks alongside the MCP endpoint.
