# Manteclaw MCP Lite

A free, thin MCP (Model Context Protocol) server — **100 tools**, all
previews: bands, buckets, rounded figures and short top-N lists. It is a
discovery funnel for the paid **Manteclaw x402 API** — 199 pay-per-call
endpoints on Base, paid in USDC via the x402 protocol ($0.01–0.02/call,
no account, no API key). Full catalog:
https://manteclaw-x402.fly.dev/catalog

**No-overlap rule (v1.1.1):** no lite tool returns the same answer as a
paid endpoint. Where the paid service sells an exact value, the lite
tool returns a coarsened preview (2 significant figures, whole-percent
changes, balance bands, masked addresses, top-3 instead of top-10) or a
derived/educational tool with no paid counterpart at all (ETH/BTC
ratio, market mood, market breadth, median Base APY, selector
reference, network reference cards). Every data tool's result ends with
one line pointing at the paid catalog.

All upstreams are free public APIs — the same sources the paid service
uses. No API keys, no secrets, and the lite server never calls the paid
endpoints. Hot/heavy endpoints are cached in-process for 30–60s
(CoinGecko's free tier rate-limits aggressively). Upstream failures
return a clean error result; the server never crashes on a bad
upstream.

## Tools (100)

### Original sample (8)

| Tool | What it does | Data source |
|---|---|---|
| `base_gas` | Base fee climate: LOW/TYPICAL/HIGH band + trend vs last 20 blocks | Base public RPC |
| `base_block` | Latest Base block preview: number, age, activity level | Base public RPC |
| `token_price` | Coarse spot price preview (2 sig figs) + 24h direction + liquidity band | DexScreener |
| `token_info` | ERC20 metadata preview: name, symbol, decimals, rounded total supply | Base RPC |
| `ens_resolve` | ENS resolve check with masked address/name preview | ENSIdeas / ensdata.net |
| `wallet_snapshot` | ETH + USDC balance bands (ranges, not exact figures) | Base public RPC |
| `dex_pair` | Top Base DEX pair preview: coarse price + liquidity band | DexScreener |
| `manteclaw_catalog` | Menu of the paid service + catalog URL | static |

### Chain & blocks (10)

| Tool | What it does | Data source |
|---|---|---|
| `chain_id` | Base chain ID, decimal + hex | Base public RPC |
| `base_block_by_number` | Block preview: timestamp + activity level | Base public RPC |
| `base_recent_blocks` | Last up-to-3 blocks with activity levels | Base public RPC |
| `base_tx` | Transaction preview: from/to, type, value band, block | Base public RPC |
| `base_tx_receipt` | Receipt preview: status, block, confirmations band | Base public RPC |
| `base_nonce` | Sending-activity band from an address's tx count | Base public RPC |
| `base_pending_txs` | Mempool climate: quiet / normal / busy | Base public RPC |
| `base_client_version` | RPC node client family (version string is paid) | Base public RPC |
| `base_sync_status` | Qualitative node health from chain-head freshness | Base public RPC |
| `base_fee_history` | Fee trend: rounded average + range over ≤20 blocks | Base public RPC |

### Gas & fees (5)

| Tool | What it does | Data source |
|---|---|---|
| `base_gas_price` | eth_gasPrice oracle value, rounded to 2 sig figs | Base public RPC |
| `base_priority_fee` | Suggested priority fee (tip), rounded | Base public RPC |
| `base_transfer_cost` | Rounded cost of a plain ETH transfer, ETH + USD | Base RPC + DexScreener |
| `gas_cost_estimate` | Approximate cost at a given gwei × gas amount | DexScreener (ETH price) |
| `base_tx_costs` | Rounded typical costs: transfer / ERC20 / approve / swap | Base RPC + DexScreener |

### Tokens & prices (14)

| Tool | What it does | Data source |
|---|---|---|
| `token_pairs` | Top 5 Base pairs for a token by liquidity (rounded figures) | DexScreener |
| `token_liquidity` | Liquidity band + pair-count bucket + deepest-pair DEX | DexScreener |
| `token_volume` | Rounded 24h volume of the best Base pair | DexScreener |
| `token_price_change` | 24h change, whole percent, best Base pair | DexScreener |
| `token_txns` | 24h flow tilt (buy/sell-heavy) + bucketed trade count | DexScreener |
| `token_fdv` | Rounded FDV + market cap from the best Base pair | DexScreener |
| `token_balance` | Rounded ERC20 balance preview for any wallet | Base public RPC |
| `token_supply` | Rounded total supply preview of a Base ERC20 | Base public RPC |
| `token_links` | Official websites/socials for a token (lite-only extra) | DexScreener |
| `token_allowance` | Allowance risk category: none / limited / UNLIMITED | Base public RPC |
| `function_selector` | EVM 4-byte selector reference table (educational) | static |
| `token_search` | Search tokens, top Base pairs (rounded figures) | DexScreener |
| `eth_price` | Coarse ETH price preview + 24h direction | DexScreener |
| `token_price_eth` | Rounded token price in quote-asset (ETH) terms | DexScreener |

### DEX & pools (7)

| Tool | What it does | Data source |
|---|---|---|
| `dex_search` | Search pairs across all chains, top 10, rounded prices | DexScreener |
| `dex_pair_lookup` | Pair preview by address: coarse price + liquidity band | DexScreener |
| `dex_trending` | Top boosted tokens right now (DexScreener trending board) | DexScreener |
| `dex_latest_boosts` | Most recently boosted tokens | DexScreener |
| `dex_new_profiles` | Newest token profiles with short descriptions | DexScreener |
| `dex_pairs_count` | Pair-count bucket + distinct-DEX count for a token | DexScreener |
| `uniswap_v3_pool` | UniV3 pool preview: tokens, fee tier, rounded price/liquidity | Base public RPC |

### DeFi: TVL, volumes, fees, yields (14)

| Tool | What it does | Data source |
|---|---|---|
| `base_tvl` | Rounded total value locked on Base | DeFiLlama |
| `chain_tvl` | TVL rank + rounded TVL for any chain | DeFiLlama |
| `top_chains_tvl` | Top 15 chains by TVL (rounded figures) | DeFiLlama |
| `protocol_tvl` | Rounded TVL for one protocol by slug | DeFiLlama |
| `protocol_info` | Protocol summary: category, rounded TVL, whole-% changes, chain count | DeFiLlama |
| `top_protocols` | Top 15 protocols by TVL, all chains (rounded) | DeFiLlama |
| `top_base_protocols` | Top 15 protocols on Base by TVL (rounded) | DeFiLlama |
| `dex_volume_global` | Global DEX volume (rounded) + top 3 DEX names | DeFiLlama |
| `dex_volume_base` | Base DEX volume (rounded) + top 3 Base DEX names | DeFiLlama |
| `dex_fees_top` | Top 3 protocols by 24h fees (rounded) + total | DeFiLlama |
| `yields_top` | Top 3 yield pools by APY, whole-% APYs | DeFiLlama Yields |
| `yields_base` | Top 3 Base yield pools by APY, whole-% APYs | DeFiLlama Yields |
| `yields_stablecoins` | Top 3 stablecoin yield pools by APY, whole-% APYs | DeFiLlama Yields |
| `yields_token` | Top 3 yield pools for a token symbol, whole-% APYs | DeFiLlama Yields |

### Stablecoins (6)

| Tool | What it does | Data source |
|---|---|---|
| `top_stablecoins` | Top 3 stablecoins by circulating supply (rounded) | DeFiLlama Stablecoins |
| `stablecoin_info` | One stablecoin: rounded supply, peg type, top 3 chains | DeFiLlama Stablecoins |
| `stablecoin_total` | Rounded total supply of all tracked stablecoins + count | DeFiLlama Stablecoins |
| `stablecoin_chains` | Top 10 chains by stablecoin supply (rounded) | DeFiLlama Stablecoins |
| `stablecoins_base` | Top 10 stablecoins by supply on Base (rounded) | DeFiLlama Stablecoins |
| `stablecoin_pegs` | Top 5 deviations from the $1 peg (rounded) | DeFiLlama Stablecoins |

### Markets & global (13)

| Tool | What it does | Data source |
|---|---|---|
| `fear_greed` | Current Fear & Greed classification (value is paid) | Alternative.me |
| `fear_greed_week` | 7-day summary: rounded average + trend + latest class | Alternative.me |
| `market_global` | Rounded total mcap/volume, whole-% change + dominance | CoinGecko |
| `top_coins` | Top 3 coins by market cap (rounded figures) | CoinGecko |
| `trending_coins` | Top 3 trending coins on CoinGecko search | CoinGecko |
| `coin_price` | Rounded USD price preview by CoinGecko id | CoinGecko |
| `eth_btc_ratio` | ETH/BTC ratio — derived gauge, no paid counterpart | CoinGecko |
| `coin_info` | Coin summary: rank + rounded market cap | CoinGecko |
| `market_mood` | Risk-on / Cautious / Risk-off gauge (derived, lite-only) | Alternative.me + CoinGecko |
| `coin_change` | 7d change, whole percent, by CoinGecko id | CoinGecko |
| `defi_global` | Global DeFi market cap, DeFi/ETH ratio, dominance | CoinGecko |
| `btc_dominance` | BTC + ETH dominance, whole percents | CoinGecko |
| `exchange_rates` | Rates per 1 BTC: major fiats + ETH | CoinGecko |

### ENS (4 new, plus `ens_resolve` above)

| Tool | What it does | Data source |
|---|---|---|
| `ens_avatar` | Avatar-set yes/no (URL itself is paid) | ENSIdeas |
| `ens_profile` | Profile field-presence checklist (values are paid) | ensdata.net |
| `ens_records` | Text-record key names + count (values are paid) | ensdata.net |
| `ens_content_hash` | Content-hash set yes/no + scheme (hash is paid) | ensdata.net |

### Wallet, network & derived (8 new, plus `wallet_snapshot` above)

| Tool | What it does | Data source |
|---|---|---|
| `wallet_eth_value` | Rounded USD value of an address's Base ETH | Base RPC + DexScreener |
| `base_tps` | Base throughput (tx/sec over last 5 blocks) — derived | Base public RPC |
| `stablecoin_dominance` | Stablecoins' share of total market cap — derived | DeFiLlama + CoinGecko |
| `yields_median_base` | Median APY across Base yield pools — derived | DeFiLlama Yields |
| `base_network_info` | Base mainnet reference card (educational) | static |
| `top_gainer_today` | Biggest 24h gainer in the top 100 — derived | CoinGecko |
| `market_breadth` | Share of top-100 coins up in 24h — derived | CoinGecko |
| `wallet_known_holdings` | Which well-known Base tokens an address holds (names only) | Base public RPC |

### Oracles — Chainlink on Base (4)

| Tool | What it does | Data source |
|---|---|---|
| `chainlink_eth` | Chainlink ETH/USD, rounded price + update-age bucket | Base RPC (Chainlink) |
| `chainlink_btc` | Chainlink BTC/USD, rounded price + update-age bucket | Base RPC (Chainlink) |
| `chainlink_staleness` | Update ages for all three feeds, no prices | Base RPC (Chainlink) |
| `chainlink_feeds` | All three feeds in one view, rounded prices + age buckets | Base RPC (Chainlink) |

### Utilities & reference (7)

| Tool | What it does | Data source |
|---|---|---|
| `convert_eth_usd` | ETH amount → USD, approximate at a rounded price | DexScreener |
| `convert_usd_eth` | USD amount → ETH, approximate at a rounded price | DexScreener |
| `eth_units` | Wei/gwei/ether denomination reference (educational) | static |
| `contract_storage` | Read one storage slot of any Base contract (dev utility) | Base public RPC |
| `base_sepolia_info` | Base Sepolia testnet reference card (educational) | static |
| `block_eta` | Estimated arrival time of a future Base block | Base public RPC |
| `base_block_time` | Measured average block time (last 100 blocks) + blocks/day | Base public RPC |

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
.venv/bin/python test_100.py     # 100-tool suite: HTTP + stdio, live calls
                                 # across every data source, bogus-input checks
.venv/bin/python test_audit.py   # overlap-fix suite (v1.1.1): every changed /
                                 # replaced tool + unchanged samples, HTTP + stdio
.venv/bin/python test_client.py  # original smoke test (HTTP)
.venv/bin/python test_client.py --stdio
```

See `test-100-output.log` for a captured passing run of the full suite
and `test-output.log` for the original smoke test.

## Deploy (Fly.io)

```bash
fly launch --no-deploy   # first time only, config already in fly.toml
fly deploy
```

The server binds `0.0.0.0:$PORT` (default 8080) and serves `/health`
for Fly's checks alongside the MCP endpoint.
