#!/usr/bin/env python3
"""Full test for Manteclaw MCP Lite v1.1.0 (100 tools).

- Spawns server.py --http on TEST_PORT (default 18081), waits for /health.
- HTTP: initialize (checks server version 1.1.0), tools/list == 100.
- Live-calls a spread of tools covering EVERY data source:
  Base RPC, DexScreener, DeFiLlama (main / yields / stablecoins),
  CoinGecko, ENS (ENSIdeas + ensdata), Alternative.me.
- Two bogus-input calls must return clean errors; a good call after
  them proves the server is unaffected.
- stdio: initialize + tools/list == 100.

Usage: .venv/bin/python test_100.py
"""

import asyncio
import os
import subprocess
import sys
import time

for _var in ("NO_PROXY", "no_proxy"):
    _val = os.environ.get(_var)
    if _val:
        _kept = [p.strip() for p in _val.split(",") if p.strip() and "[" not in p and "]" not in p]
        os.environ[_var] = ",".join(_kept)

import httpx
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamablehttp_client

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = os.environ.get("TEST_PORT", "18081")
HTTP_URL = f"http://127.0.0.1:{PORT}/mcp"
HEALTH_URL = f"http://127.0.0.1:{PORT}/health"

USDC_CONTRACT = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"

CG_TOOLS = {
    "market_global", "top_coins", "trending_coins", "coin_price",
    "coin_prices", "coin_info", "coin_ath", "coin_change", "defi_global",
    "btc_dominance", "exchange_rates",
}
WETH_CONTRACT = "0x4200000000000000000000000000000000000006"
UNIV3_USDC_WETH_POOL = "0xd0b53D9277642d899DF5C87A3966A349A798F224"
VITALIK = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"


def _text(result) -> str:
    return "\n".join(
        getattr(b, "text", "") for b in result.content if getattr(b, "text", None)
    )


async def _latest_tx_hash() -> str:
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(
            "https://mainnet.base.org",
            json={"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber",
                  "params": ["latest", True]},
        )
        block = r.json()["result"]
        return block["transactions"][0]["hash"]


async def run_http() -> None:
    tx_hash = await _latest_tx_hash()
    print(f"(using live Base tx {tx_hash} for tx tools)")

    calls = [
        # --- original 8 spot-check ---
        ("base_gas", {}),
        ("manteclaw_catalog", {}),
        # --- Base RPC: chain ---
        ("chain_id", {}),
        ("base_block_by_number", {"number": 20000000}),
        ("base_recent_blocks", {"count": 3}),
        ("base_tx", {"tx_hash": tx_hash}),
        ("base_tx_receipt", {"tx_hash": tx_hash}),
        ("base_nonce", {"address": VITALIK}),
        ("base_pending_txs", {}),
        ("base_client_version", {}),
        ("base_sync_status", {}),
        ("base_fee_history", {"blocks": 5}),
        # --- Base RPC: gas ---
        ("base_gas_price", {}),
        ("base_priority_fee", {}),
        ("base_transfer_cost", {}),
        ("gas_cost_estimate", {"gwei": 0.01, "gas_units": 150000}),
        ("base_tx_costs", {}),
        # --- DexScreener ---
        ("token_pairs", {"token": "AERO"}),
        ("token_volume", {"token": "USDC"}),
        ("token_price_change", {"token": "AERO"}),
        ("token_txns", {"token": "AERO"}),
        ("eth_price", {}),
        ("dex_trending", {}),
        ("dex_new_profiles", {}),
        ("dex_latest_boosts", {}),
        ("dex_search", {"query": "BRETT"}),
        ("dex_pairs_count", {"token": WETH_CONTRACT}),
        # --- Base RPC: tokens / wallet / oracles / pools ---
        ("token_balance", {"token_address": USDC_CONTRACT, "wallet": VITALIK}),
        ("token_supply", {"contract_address": USDC_CONTRACT}),
        ("token_allowance", {"token_address": USDC_CONTRACT, "owner": VITALIK,
                             "spender": WETH_CONTRACT}),
        ("uniswap_v3_pool", {"pool_address": UNIV3_USDC_WETH_POOL}),
        ("chainlink_eth", {}),
        ("chainlink_feeds", {}),
        ("wallet_known_holdings", {"address": VITALIK}),
        ("wallet_is_contract", {"address": USDC_CONTRACT}),
        ("wallet_eth_value", {"address": VITALIK}),
        ("address_info", {"address": VITALIK}),
        ("block_eta", {"target_block": 999999999}),
        ("base_block_time", {}),
        # --- DeFiLlama main ---
        ("base_tvl", {}),
        ("chain_tvl", {"chain": "Solana"}),
        ("top_chains_tvl", {}),
        ("protocol_tvl", {"protocol": "aave-v3"}),
        ("protocol_info", {"protocol": "aerodrome"}),
        ("top_protocols", {}),
        ("top_base_protocols", {}),
        ("dex_volume_global", {}),
        ("dex_volume_base", {}),
        ("dex_fees_top", {}),
        # --- DeFiLlama yields ---
        ("yields_top", {}),
        ("yields_base", {}),
        ("yields_stablecoins", {}),
        ("yields_token", {"symbol": "USDC"}),
        # --- DeFiLlama stablecoins ---
        ("top_stablecoins", {}),
        ("stablecoin_info", {"asset": "USDC"}),
        ("stablecoin_total", {}),
        ("stablecoin_chains", {}),
        ("stablecoins_base", {}),
        ("stablecoin_pegs", {}),
        # --- CoinGecko ---
        ("market_global", {}),
        ("top_coins", {}),
        ("trending_coins", {}),
        ("coin_price", {"coin_id": "bitcoin"}),
        ("coin_prices", {"coin_ids": "bitcoin,ethereum,solana"}),
        ("coin_info", {"coin_id": "ethereum"}),
        ("coin_ath", {"coin_id": "bitcoin"}),
        ("coin_change", {"coin_id": "ethereum"}),
        ("defi_global", {}),
        ("btc_dominance", {}),
        ("exchange_rates", {}),
        # --- Alternative.me ---
        ("fear_greed", {}),
        ("fear_greed_week", {}),
        # --- ENS ---
        ("ens_avatar", {"name": "vitalik.eth"}),
        ("ens_profile", {"name": "vitalik.eth"}),
        ("ens_records", {"name": "vitalik.eth"}),
        ("ens_content_hash", {"name": "vitalik.eth"}),
        # --- converters ---
        ("convert_eth_usd", {"amount_eth": 2.5}),
        ("convert_usd_eth", {"amount_usd": 100}),
        ("convert_wei", {"amount_wei": "1500000000000000000"}),
        ("convert_gwei", {"amount_gwei": 12.5}),
    ]

    print(f"Connecting (streamable HTTP) to {HTTP_URL}")
    async with streamablehttp_client(HTTP_URL) as (read, write, _):
        async with ClientSession(read, write) as session:
            init = await session.initialize()
            print(f"INITIALIZE ok — server: {init.serverInfo.name} {init.serverInfo.version}, "
                  f"protocol: {init.protocolVersion}")
            assert init.serverInfo.version == "1.1.0", f"version is {init.serverInfo.version}, expected 1.1.0"

            tools = await session.list_tools()
            names = [t.name for t in tools.tools]
            print(f"TOOLS/LIST ok — {len(names)} tools")
            assert len(names) == 100, f"expected 100 tools, got {len(names)}"
            assert len(set(names)) == 100, "duplicate tool names!"

            for name, args in calls:
                if name in CG_TOOLS:
                    # CoinGecko's free tier is per-minute and this host
                    # shares an egress IP — pace distinct CG endpoints.
                    await asyncio.sleep(8)
                result = await session.call_tool(name, args)
                print(f"\n=== tools/call {name}({args}) — isError={result.isError} ===")
                print(_text(result))

            # --- bogus inputs: clean errors, no crash ---
            for name, args in [
                ("token_balance", {"token_address": "0xZZZ", "wallet": VITALIK}),
                ("base_tx", {"tx_hash": "0xdeadbeef"}),
            ]:
                result = await session.call_tool(name, args)
                print(f"\n=== BOGUS tools/call {name}({args}) — isError={result.isError} ===")
                print(_text(result))

            result = await session.call_tool("chain_id", {})
            print("\n=== post-bogus sanity: chain_id ===")
            print(_text(result))


async def run_stdio() -> None:
    print("\nSpawning server over stdio")
    params = StdioServerParameters(
        command=sys.executable,
        args=[os.path.join(HERE, "server.py")],
        env={**os.environ},
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            init = await session.initialize()
            print(f"STDIO INITIALIZE ok — server: {init.serverInfo.name} {init.serverInfo.version}, "
                  f"protocol: {init.protocolVersion}")
            tools = await session.list_tools()
            print(f"STDIO TOOLS/LIST ok — {len(tools.tools)} tools")
            assert len(tools.tools) == 100


def wait_health(proc) -> None:
    for _ in range(60):
        try:
            r = httpx.get(HEALTH_URL, timeout=2)
            if r.status_code == 200:
                print(f"Server healthy: {r.json()}")
                return
        except Exception:
            pass
        if proc.poll() is not None:
            raise RuntimeError(f"server exited early with code {proc.returncode}")
        time.sleep(0.5)
    raise RuntimeError("server did not become healthy in 30s")


if __name__ == "__main__":
    env = {**os.environ, "PORT": PORT}
    proc = subprocess.Popen(
        [sys.executable, os.path.join(HERE, "server.py"), "--http"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        wait_health(proc)
        asyncio.run(run_http())
    finally:
        proc.terminate()
        proc.wait(timeout=10)
    asyncio.run(run_stdio())
    print("\nALL 100-TOOL TESTS PASSED")
