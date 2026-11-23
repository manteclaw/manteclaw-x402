# Manteclaw MCP Lite

A free, thin MCP (Model Context Protocol) server — **100 tools**, all
spot values, current figures and top-N summaries. It is a discovery
funnel for the paid **Manteclaw x402 API** — 199 pay-per-call endpoints
on Base, paid in USDC via the x402 protocol ($0.01–0.02/call, no
account, no API key). Full catalog: https://manteclaw-x402.fly.dev/catalog

The lite server gives away a genuinely useful sample for free; the paid
service keeps the depth (full histories, holder breakdowns, bulk
exports). Every data tool's result ends with one line pointing at the
paid catalog.

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
| `base_gas` | Base base-fee snapshot (slow/average/fast, gwei) | Base public RPC |
| `base_block` | Latest Base block summary | Base public RPC |
| `token_price` | Spot USD price + 24h change by symbol or Base contract | DexScreener |
| `token_info` | ERC20 metadata (on-chain) + market snapshot for a Base contract | Base RPC + DexScreener |
| `ens_resolve` | ENS name → address, or address → ENS name | ENSIdeas / ensdata.net |
| `wallet_snapshot` | ETH + USDC balance of an address on Base | Base public RPC |
| `dex_pair` | Top Base DEX pair for a token (price, liquidity, volume) | DexScreener |
| `manteclaw_catalog` | Menu of the paid service + catalog URL | static |

### Chain & blocks (10)

| Tool | What it does | Data source |
|---|---|---|
| `chain_id` | Base chain ID, decimal + hex | Base public RPC |
| `base_block_by_number` | One block by number: hash, time, tx count, gas, base fee | Base public RPC |
| `base_recent_blocks` | Last N blocks (1–10) with tx counts, gas, base fees | Base public RPC |
| `base_tx` | One transaction: from/to, value, gas, nonce, block | Base public RPC |
| `base_tx_receipt` | Receipt: success/failure, gas used, fee paid, log count | Base public RPC |
| `base_nonce` | Address transaction count, confirmed + pending | Base public RPC |
| `base_pending_txs` | Transaction count in the pending block | Base public RPC |
| `base_client_version` | RPC node client version string | Base public RPC |
| `base_sync_status` | Sync status + latest-block age (health signal) | Base public RPC |
| `base_fee_history` | Per-block base fees, gas-used ratios, median priority rewards | Base public RPC |

### Gas & fees (5)

| Tool | What it does | Data source |
|---|---|---|
| `base_gas_price` | eth_gasPrice oracle value in gwei/wei | Base public RPC |
| `base_priority_fee` | Suggested priority fee (tip) right now | Base public RPC |
| `base_transfer_cost` | Cost of a plain ETH transfer now, ETH + USD | Base RPC + DexScreener |
| `gas_cost_estimate` | Cost at a given gwei × gas amount, ETH + USD | DexScreener (ETH price) |
| `base_tx_costs` | Typical costs now: transfer / ERC20 transfer / approve / swap | Base RPC + DexScreener |

### Tokens & prices (14)

| Tool | What it does | Data source |
|---|---|---|
| `token_pairs` | Top 5 Base pairs for a token by liquidity | DexScreener |
| `token_liquidity` | Total liquidity across a token's Base pairs + deepest pair | DexScreener |
| `token_volume` | Volume 24h/6h/1h/5m on the best Base pair | DexScreener |
| `token_price_change` | Price change 24h/6h/1h/5m on the best Base pair | DexScreener |
| `token_txns` | Buy/sell counts 24h/6h/1h/5m on the best Base pair | DexScreener |
| `token_fdv` | FDV + market cap from the best Base pair | DexScreener |
| `token_balance` | Any ERC20 balance for any wallet, on-chain | Base public RPC |
| `token_supply` | Total supply of a Base ERC20, on-chain | Base public RPC |
| `token_decimals` | Decimals of a Base ERC20, on-chain | Base public RPC |
| `token_allowance` | Owner → spender allowance, flags unlimited approvals | Base public RPC |
| `token_name_symbol` | Name + symbol of a Base ERC20, on-chain | Base public RPC |
| `token_search` | Search tokens, top Base pairs with prices | DexScreener |
| `eth_price` | Live ETH USD price + 24h change (WETH pair) | DexScreener |
| `token_price_eth` | Token price in quote-asset (ETH) terms + USD | DexScreener |

### DEX & pools (7)

| Tool | What it does | Data source |
|---|---|---|
| `dex_search` | Search pairs across all chains, top 10 by liquidity | DexScreener |
| `dex_pair_lookup` | One Base pair by pair address: price, liquidity, volume, mcap | DexScreener |
| `dex_trending` | Top boosted tokens right now (DexScreener trending board) | DexScreener |
| `dex_latest_boosts` | Most recently boosted tokens | DexScreener |
| `dex_new_profiles` | Newest token profiles with short descriptions | DexScreener |
| `dex_pairs_count` | Pair count + DEX list for a Base token | DexScreener |
| `uniswap_v3_pool` | UniV3 pool state on-chain: tokens, fee, tick, liquidity, implied price | Base public RPC |

### DeFi: TVL, volumes, fees, yields (14)

| Tool | What it does | Data source |
|---|---|---|
| `base_tvl` | Total value locked on Base | DeFiLlama |
| `chain_tvl` | TVL for any chain by name | DeFiLlama |
| `top_chains_tvl` | Top 15 chains by TVL | DeFiLlama |
| `protocol_tvl` | TVL for one protocol by slug | DeFiLlama |
| `protocol_info` | Protocol summary: category, TVL, 1d/7d change, chains, about | DeFiLlama |
| `top_protocols` | Top 15 protocols by TVL, all chains | DeFiLlama |
| `top_base_protocols` | Top 15 protocols on Base by TVL | DeFiLlama |
| `dex_volume_global` | Global DEX volume 24h/7d + top 5 DEXes | DeFiLlama |
| `dex_volume_base` | Base DEX volume 24h/7d + top 5 Base DEXes | DeFiLlama |
| `dex_fees_top` | Top 10 protocols by 24h fees + overall total | DeFiLlama |
| `yields_top` | Top 10 yield pools by APY, all chains | DeFiLlama Yields |
| `yields_base` | Top 10 Base yield pools by APY | DeFiLlama Yields |
| `yields_stablecoins` | Top 10 stablecoin yield pools by APY | DeFiLlama Yields |
| `yields_token` | Top 10 yield pools for a token symbol, by APY | DeFiLlama Yields |

### Stablecoins (6)

| Tool | What it does | Data source |
|---|---|---|
| `top_stablecoins` | Top 10 stablecoins by circulating supply | DeFiLlama Stablecoins |
| `stablecoin_info` | One stablecoin: supply, peg type/mechanism, price, top chains | DeFiLlama Stablecoins |
| `stablecoin_total` | Total supply of all tracked stablecoins + count | DeFiLlama Stablecoins |
| `stablecoin_chains` | Top 10 chains by stablecoin supply | DeFiLlama Stablecoins |
| `stablecoins_base` | Top 10 stablecoins by supply on Base | DeFiLlama Stablecoins |
| `stablecoin_pegs` | Largest current deviations from the $1 peg | DeFiLlama Stablecoins |

### Markets & global (13)

| Tool | What it does | Data source |
|---|---|---|
| `fear_greed` | Current Fear & Greed value + classification | Alternative.me |
| `fear_greed_week` | Fear & Greed, last 7 days | Alternative.me |
| `market_global` | Total mcap, 24h volume, BTC/ETH dominance, active coins | CoinGecko |
| `top_coins` | Top 10 coins by market cap with price + 24h change | CoinGecko |
| `trending_coins` | Trending coins on CoinGecko search, top 10 | CoinGecko |
| `coin_price` | One coin's USD price + 24h change by CoinGecko id | CoinGecko |
| `coin_prices` | Prices/mcaps/24h changes for up to 10 ids at once | CoinGecko |
| `coin_info` | Coin summary: price, mcap + rank, volume, changes, supplies | CoinGecko |
| `coin_ath` | All-time high price/date + distance below ATH | CoinGecko |
| `coin_change` | Price changes over 24h / 7d / 30d for one coin | CoinGecko |
| `defi_global` | Global DeFi market cap, DeFi/ETH ratio, dominance | CoinGecko |
| `btc_dominance` | Market-cap dominance table (BTC, ETH, next assets) | CoinGecko |
| `exchange_rates` | Rates per 1 BTC: major fiats + ETH | CoinGecko |

### ENS (4 new, plus `ens_resolve` above)

| Tool | What it does | Data source |
|---|---|---|
| `ens_avatar` | Avatar URL + resolved address for an ENS name | ENSIdeas |
| `ens_profile` | Profile summary: description, URL, socials | ensdata.net |
| `ens_records` | Text records: description, URL, socials, content hash | ensdata.net |
| `ens_content_hash` | Content hash (website pointer) + avatar | ensdata.net |

### Wallet & address (8 new, plus `wallet_snapshot` above)

| Tool | What it does | Data source |
|---|---|---|
| `wallet_eth_value` | USD value of an address's Base ETH balance | Base RPC + DexScreener |
| `wallet_usdc_balance` | USDC balance on Base | Base public RPC |
| `wallet_weth_balance` | WETH balance on Base | Base public RPC |
| `wallet_dai_balance` | DAI balance on Base | Base public RPC |
| `wallet_aero_balance` | AERO balance on Base | Base public RPC |
| `wallet_cbeth_balance` | cbETH balance on Base | Base public RPC |
| `wallet_is_contract` | Contract or EOA, plus bytecode size | Base public RPC |
| `wallet_known_holdings` | Which well-known Base tokens an address holds, with amounts | Base public RPC |

### Oracles — Chainlink on Base (4)

| Tool | What it does | Data source |
|---|---|---|
| `chainlink_eth` | Chainlink ETH/USD on-chain, with last-update time | Base RPC (Chainlink) |
| `chainlink_btc` | Chainlink BTC/USD on-chain, with last-update time | Base RPC (Chainlink) |
| `chainlink_usdc` | Chainlink USDC/USD on-chain, with last-update time | Base RPC (Chainlink) |
| `chainlink_feeds` | All three feeds in one view, with update ages | Base RPC (Chainlink) |

### Utilities & converters (7)

| Tool | What it does | Data source |
|---|---|---|
| `convert_eth_usd` | ETH amount → USD at the live price | DexScreener |
| `convert_usd_eth` | USD amount → ETH at the live price | DexScreener |
| `convert_wei` | Wei → ETH and gwei | computation (mirrors paid API) |
| `convert_gwei` | Gwei → wei/ETH + USD value at the live price | DexScreener |
| `address_info` | Address facts: valid format, contract/EOA, nonce, ETH balance | Base public RPC |
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
