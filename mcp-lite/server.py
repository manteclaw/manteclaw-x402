#!/usr/bin/env python3
"""Manteclaw MCP Lite — a free, thin MCP server.

A discovery funnel for the paid Manteclaw x402 API service
(https://manteclaw-x402.fly.dev/catalog — 199 endpoints, USDC on Base,
$0.01-0.02 per call via x402). This lite server exposes a small set of
free tools backed by the same free public data sources the paid service
uses. No API keys, no secrets, no calls to the paid endpoints.

Data sources (all free, no keys — same as the paid service's server-v5):
  - Base public RPC (https://mainnet.base.org): blocks, gas, balances,
    ERC20 metadata via eth_call.
  - DexScreener public API: token prices, pairs, liquidity, volume.
  - ENSIdeas / ensdata.net public APIs: ENS resolve / reverse.
  (Blockscout is deliberately not used — its API is Cloudflare-challenged
  from this hosting environment, same finding as the paid service.)

Transports:
  - stdio (default):            python server.py
  - streamable HTTP:            python server.py --http
    Binds 0.0.0.0:$PORT (default 8080), MCP endpoint at /mcp,
    plus a plain GET /health route.
"""

import os

# --- proxy env sanitization (mirrors the paid service) -------------------
# httpx builds a URL pattern out of every NO_PROXY entry; bracketed IPv6
# entries such as "[::1]" make that raise httpx.InvalidURL. Drop bracketed
# entries; the bare addresses stay, so loopback still bypasses the proxy.
for _var in ("NO_PROXY", "no_proxy"):
    _val = os.environ.get(_var)
    if _val:
        _kept = [p.strip() for p in _val.split(",") if p.strip() and "[" not in p and "]" not in p]
        os.environ[_var] = ",".join(_kept)

import sys
import time

import httpx
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import JSONResponse

# ===== config =====

PORT = int(os.environ.get("PORT", "8080"))
BASE_RPC = "https://mainnet.base.org"
DEXSCREENER = "https://api.dexscreener.com"
ENSIDEAS = "https://api.ensideas.com"
ENSDATA = "https://api.ensdata.net"
HTTP_TIMEOUT = 10.0  # seconds, per upstream call

CATALOG_URL = "https://manteclaw-x402.fly.dev/catalog"

# Well-known Base token contracts (public constants, same as paid service).
USDC = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
WETH = "0x4200000000000000000000000000000000000006"

# Symbol -> Base contract, for the well-known tokens (the paid service's
# curated list). Needed because e.g. USDC appears as the *quote* token in
# most pairs, so symbol search alone cannot price it.
SYMBOL_ADDRESSES = {
    "ETH": WETH,
    "WETH": WETH,
    "USDC": USDC,
    "AERO": "0x3f5bF3EdAc59d650D8fDf9657a71F2f904A3a4Ee",
    "CBETH": "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22",
    "DAI": "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb",
    "USDBC": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
    "HIGHER": "0x0578d8A44db98B23BF096A382e016e29a5Ce0FFE",
}

# ERC20 function selectors.
SEL_NAME = "06fdde03"
SEL_SYMBOL = "95d89b41"
SEL_DECIMALS = "313ce567"
SEL_TOTAL_SUPPLY = "18160ddd"
SEL_BALANCE_OF = "70a08231"

FUNNEL = (
    "More: full holder breakdowns, history and 199 endpoints at "
    f"{CATALOG_URL} ($0.01/call, USDC on Base via x402)."
)

_client = httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=True)

# ===== helpers =====


def _is_address(value: str) -> bool:
    v = value.strip()
    return v.startswith("0x") and len(v) == 42 and all(
        c in "0123456789abcdefABCDEF" for c in v[2:]
    )


async def _rpc(method: str, params: list):
    """One JSON-RPC call to the Base public RPC. Raises on failure."""
    r = await _client.post(
        BASE_RPC,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
    )
    r.raise_for_status()
    body = r.json()
    if "error" in body:
        raise RuntimeError(f"Base RPC error: {body['error'].get('message', body['error'])}")
    return body["result"]


async def _get_json(url: str):
    """GET a public JSON API. Raises on failure."""
    r = await _client.get(url)
    r.raise_for_status()
    return r.json()


async def _eth_call(to: str, data_hex: str) -> bytes:
    raw = await _rpc("eth_call", [{"to": to, "data": data_hex}, "latest"])
    return bytes.fromhex(raw[2:] if raw.startswith("0x") else raw)


def _decode_string(raw: bytes) -> str:
    """Decode an ABI string return, with the bytes32 fallback (mirrors v5)."""
    if len(raw) >= 64:
        try:
            offset = int.from_bytes(raw[0:32], "big")
            length = int.from_bytes(raw[offset : offset + 32], "big")
            return raw[offset + 32 : offset + 32 + length].decode("utf-8", "replace")
        except Exception:
            pass
    return raw[:32].split(b"\x00")[0].decode("utf-8", "replace")


def _decode_uint(raw: bytes) -> int:
    return int.from_bytes(raw, "big") if raw else 0


async def _erc20_str(token: str, selector: str) -> str:
    return _decode_string(await _eth_call(token, "0x" + selector))


async def _erc20_uint(token: str, selector: str, args: bytes = b"") -> int:
    return _decode_uint(await _eth_call(token, "0x" + selector + args.hex()))


def _addr32(addr: str) -> bytes:
    return b"\x00" * 12 + bytes.fromhex(addr[2:])


def _best_pair(pairs):
    """DexScreener pair with the most liquidity (mirrors v5)."""
    if not pairs:
        return None

    def liq(p):
        try:
            return float((p.get("liquidity") or {}).get("usd") or 0)
        except Exception:
            return 0.0

    return max(pairs, key=liq)


async def _base_pairs_for_token(address: str):
    """DexScreener pairs where the token is the BASE token, on Base.

    The /tokens endpoint also returns pairs where the token is merely the
    quote currency (e.g. WETH/USDC for a USDC query); those pairs price the
    other asset, so they must be excluded or prices come out wrong.
    """
    data = await _get_json(f"{DEXSCREENER}/latest/dex/tokens/{address}")
    pairs = data.get("pairs") if isinstance(data, dict) else None
    if not pairs:
        return []
    return [
        p
        for p in pairs
        if p.get("chainId") == "base"
        and (p.get("baseToken") or {}).get("address", "").lower() == address.lower()
    ]


async def _pairs_for_query(query: str):
    """Resolve a symbol or contract address to Base DexScreener pairs."""
    q = query.strip()
    if _is_address(q):
        return await _base_pairs_for_token(q)
    known = SYMBOL_ADDRESSES.get(q.upper())
    if known:
        return await _base_pairs_for_token(known)
    data = await _get_json(f"{DEXSCREENER}/latest/dex/search?q={q}")
    pairs = data.get("pairs") if isinstance(data, dict) else None
    if not pairs:
        return []
    return [
        p
        for p in pairs
        if p.get("chainId") == "base"
        and (p.get("baseToken") or {}).get("symbol", "").upper() == q.upper()
    ]


def _fmt_usd(value) -> str:
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return "n/a"


def _err(tool: str, exc: Exception) -> str:
    msg = str(exc).splitlines()[0][:200] if str(exc) else exc.__class__.__name__
    return f"Error: {tool} failed — {msg}. Try again shortly."


# ===== MCP server =====

mcp = FastMCP(
    "manteclaw-lite",
    instructions=(
        "Free lite tools from Manteclaw: live Base chain gas and blocks, "
        "token prices and DEX pairs, ENS resolution and Base wallet "
        "balances. For the full 199-endpoint paid API (holder breakdowns, "
        f"history, security scans), see {CATALOG_URL}."
    ),
    host="0.0.0.0",
    port=PORT,
    stateless_http=True,
    json_response=True,
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=False
    ),
)


@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok", "service": "manteclaw-mcp-lite"})


@mcp.tool()
async def base_gas() -> str:
    """Current Base gas price / fee snapshot: base fee (slow/average/fast in gwei), latest block."""
    try:
        block = await _rpc("eth_getBlockByNumber", ["latest", False])
        base_fee = int(block["baseFeePerGas"], 16)
        gwei = base_fee / 1e9
        lines = [
            "Base gas snapshot",
            f"Base fee: {gwei:.4f} gwei",
            f"Slow (1.0x): {gwei * 1.0:.4f} gwei",
            f"Average (1.2x): {gwei * 1.2:.4f} gwei",
            f"Fast (1.5x): {gwei * 1.5:.4f} gwei",
            f"Block: {int(block['number'], 16)}",
            f"Block time (unix): {int(block['timestamp'], 16)}",
            FUNNEL,
        ]
        return "\n".join(lines)
    except Exception as e:
        return _err("base_gas", e)


@mcp.tool()
async def base_block() -> str:
    """Latest Base block summary: number, hash, timestamp, transaction count, gas used/limit, base fee."""
    try:
        block = await _rpc("eth_getBlockByNumber", ["latest", False])
        base_fee = block.get("baseFeePerGas")
        lines = [
            "Latest Base block",
            f"Number: {int(block['number'], 16)}",
            f"Hash: {block['hash']}",
            f"Parent hash: {block['parentHash']}",
            f"Timestamp (unix): {int(block['timestamp'], 16)}",
            f"Transactions: {len(block.get('transactions', []))}",
            f"Gas used: {int(block['gasUsed'], 16):,}",
            f"Gas limit: {int(block['gasLimit'], 16):,}",
            f"Base fee: {(int(base_fee, 16) / 1e9):.4f} gwei" if base_fee else "Base fee: n/a",
            f"Miner/sequencer: {block.get('miner')}",
            FUNNEL,
        ]
        return "\n".join(lines)
    except Exception as e:
        return _err("base_block", e)


@mcp.tool()
async def token_price(token: str) -> str:
    """Spot USD price and 24h change for a token, by symbol (e.g. "AERO") or Base contract address. Source: DexScreener best-liquidity Base pair."""
    try:
        pairs = await _pairs_for_query(token)
        best = _best_pair(pairs)
        if not best:
            return f"Error: no Base DEX pair found for '{token}'."
        change = (best.get("priceChange") or {}).get("h24")
        lines = [
            f"{(best.get('baseToken') or {}).get('name')} ({(best.get('baseToken') or {}).get('symbol')}) on Base",
            f"Price: {_fmt_usd(best.get('priceUsd'))}",
            f"24h change: {change}%" if change is not None else "24h change: n/a",
            f"Pair: {best.get('baseToken', {}).get('symbol')}/{(best.get('quoteToken') or {}).get('symbol')} on {best.get('dexId')}",
            f"Liquidity: {_fmt_usd((best.get('liquidity') or {}).get('usd'))}",
            f"24h volume: {_fmt_usd((best.get('volume') or {}).get('h24'))}",
            FUNNEL,
        ]
        return "\n".join(lines)
    except Exception as e:
        return _err("token_price", e)


@mcp.tool()
async def token_info(contract_address: str) -> str:
    """Basic metadata (name, symbol, decimals, total supply, read on-chain) plus a market snapshot for a Base ERC20 contract address."""
    try:
        if not _is_address(contract_address):
            return "Error: token_info needs a Base contract address (0x, 42 chars)."
        token = contract_address.strip()
        code = await _rpc("eth_getCode", [token, "latest"])
        if not code or code == "0x":
            return f"Error: no contract code at {token} on Base."
        name = symbol = None
        decimals = None
        supply = None
        try:
            name = await _erc20_str(token, SEL_NAME)
        except Exception:
            pass
        try:
            symbol = await _erc20_str(token, SEL_SYMBOL)
        except Exception:
            pass
        try:
            decimals = _decode_uint(await _eth_call(token, "0x" + SEL_DECIMALS))
            if decimals > 36:
                decimals = None
        except Exception:
            pass
        try:
            supply = await _erc20_uint(token, SEL_TOTAL_SUPPLY)
        except Exception:
            pass
        lines = [
            f"Token {token} (Base)",
            f"Name: {name}",
            f"Symbol: {symbol}",
            f"Decimals: {decimals}",
        ]
        if supply is not None and decimals is not None:
            lines.append(f"Total supply: {supply / (10 ** decimals):,.4f} {symbol or ''}".rstrip())
        elif supply is not None:
            lines.append(f"Total supply (raw): {supply}")
        try:
            best = _best_pair(await _base_pairs_for_token(token))
            if best:
                lines += [
                    f"Price: {_fmt_usd(best.get('priceUsd'))}",
                    f"Market cap: {_fmt_usd(best.get('marketCap'))}",
                    f"FDV: {_fmt_usd(best.get('fdv'))}",
                    f"Liquidity: {_fmt_usd((best.get('liquidity') or {}).get('usd'))}",
                    f"24h volume: {_fmt_usd((best.get('volume') or {}).get('h24'))}",
                ]
        except Exception:
            pass  # market snapshot is best-effort; on-chain metadata already answered
        lines.append(FUNNEL)
        return "\n".join(lines)
    except Exception as e:
        return _err("token_info", e)


@mcp.tool()
async def ens_resolve(name_or_address: str) -> str:
    """Resolve an ENS name to its Ethereum address, or reverse-resolve an address to its ENS name."""
    try:
        value = name_or_address.strip()
        if _is_address(value):
            try:
                data = await _get_json(f"{ENSDATA}/{value}")
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    # No ENS name for this address: a valid answer (mirrors v5).
                    return "\n".join([f"Address: {value}", "ENS name: none found", FUNNEL])
                raise
            if isinstance(data, dict):
                name = data.get("name") or data.get("ens") or data.get("displayName")
                if name:
                    return "\n".join(
                        [
                            f"Address: {value}",
                            f"ENS name: {name}",
                            f"Avatar: {data.get('avatar')}",
                            FUNNEL,
                        ]
                    )
            return "\n".join([f"Address: {value}", "ENS name: none found", FUNNEL])
        data = await _get_json(f"{ENSIDEAS}/ens/resolve/{value}")
        if not isinstance(data, dict) or not data.get("address"):
            return f"Error: ENS name '{value}' not found."
        return "\n".join(
            [
                f"Name: {value}",
                f"Address: {data.get('address')}",
                f"Display name: {data.get('displayName')}",
                f"Avatar: {data.get('avatar')}",
                FUNNEL,
            ]
        )
    except Exception as e:
        return _err("ens_resolve", e)


@mcp.tool()
async def wallet_snapshot(address: str) -> str:
    """ETH and USDC balances for an address on Base."""
    try:
        if not _is_address(address):
            return "Error: wallet_snapshot needs an address (0x, 42 chars)."
        addr = address.strip()
        wei = int(await _rpc("eth_getBalance", [addr, "latest"]), 16)
        usdc_raw = await _erc20_uint(USDC, SEL_BALANCE_OF, _addr32(addr))
        return "\n".join(
            [
                f"Wallet {addr} (Base)",
                f"ETH: {wei / 1e18:.6f}",
                f"USDC: {usdc_raw / 1e6:,.2f}",
                FUNNEL,
            ]
        )
    except Exception as e:
        return _err("wallet_snapshot", e)


@mcp.tool()
async def dex_pair(token: str) -> str:
    """Top DEX pair (by liquidity) on Base for a token, by symbol or contract address: pair, price, liquidity, 24h volume. Source: DexScreener."""
    try:
        pairs = await _pairs_for_query(token)
        best = _best_pair(pairs)
        if not best:
            return f"Error: no Base DEX pair found for '{token}'."
        return "\n".join(
            [
                f"Top Base pair for {(best.get('baseToken') or {}).get('symbol')}",
                f"Pair: {(best.get('baseToken') or {}).get('symbol')}/{(best.get('quoteToken') or {}).get('symbol')} on {best.get('dexId')}",
                f"Pair address: {best.get('pairAddress')}",
                f"Price: {_fmt_usd(best.get('priceUsd'))}",
                f"Liquidity: {_fmt_usd((best.get('liquidity') or {}).get('usd'))}",
                f"24h volume: {_fmt_usd((best.get('volume') or {}).get('h24'))}",
                f"Market cap: {_fmt_usd(best.get('marketCap'))}",
                f"URL: {best.get('url')}",
                FUNNEL,
            ]
        )
    except Exception as e:
        return _err("dex_pair", e)


@mcp.tool()
async def manteclaw_catalog() -> str:
    """What the full paid Manteclaw x402 API offers, and where to get it."""
    return "\n".join(
        [
            "Manteclaw x402 API — the full paid service behind these lite tools.",
            "199 pay-per-call endpoints on Base, paid in USDC via the x402 protocol ($0.01-$0.02 per call, no account, no API key):",
            "- Tokens: live prices, market cap/FDV, liquidity, holder breakdowns, transfer history, security checks",
            "- Wallets: balances, portfolios, token holdings, activity and PnL-style breakdowns",
            "- DEX/DeFi: pair data, pool info, protocol volumes and fees, lending and yield data",
            "- Chain: blocks, transactions, gas history, contract reads",
            "- ENS: resolve, reverse, avatars, text records and profiles",
            "- Plus NFTs, governance, stablecoins, prediction markets and market sentiment",
            f"Catalog and how to call: {CATALOG_URL}",
            "The free lite tools here (base_gas, base_block, token_price, token_info, ens_resolve, wallet_snapshot, dex_pair) are a small sample.",
        ]
    )


def main():
    if "--http" in sys.argv:
        mcp.run(transport="streamable-http")
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
