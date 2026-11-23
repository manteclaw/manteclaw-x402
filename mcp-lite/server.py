#!/usr/bin/env python3
"""Manteclaw MCP Lite — a free, thin MCP server (100 tools).

A discovery funnel for the paid Manteclaw x402 API service
(https://manteclaw-x402.fly.dev/catalog — 199 endpoints, USDC on Base,
$0.01-0.02 per call via x402). This lite server exposes 100 free tools
backed by the same free public data sources the paid service uses.
Every tool is a summary / spot-value version: current values, top-N
lists, headline figures. The paid service keeps the depth (full
histories, holder breakdowns, bulk exports). No API keys, no secrets,
no calls to the paid endpoints.

Data sources (all free, no keys — same as the paid service's server-v5):
  - Base public RPC (https://mainnet.base.org): blocks, transactions,
    gas/fees, balances, ERC20 reads, Chainlink feeds, Uniswap V3 pools.
  - DexScreener public API: token prices, pairs, liquidity, volume,
    boosted/trending tokens, new token profiles.
  - DeFiLlama (api.llama.fi): chain/protocol TVL, DEX volumes, fees.
  - DeFiLlama Yields (yields.llama.fi): yield pool APYs.
  - DeFiLlama Stablecoins (stablecoins.llama.fi): stablecoin supply
    and peg prices.
  - CoinGecko free API: global market stats, top/trending coins, coin
    prices and details. (Rate-limits aggressively — cached 60s.)
  - Alternative.me: Crypto Fear & Greed Index.
  - ENSIdeas / ensdata.net public APIs: ENS resolve / profiles.
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

import asyncio
import sys
import time
from datetime import datetime, timezone

import httpx
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import JSONResponse

# ===== config =====

VERSION = "1.1.0"

PORT = int(os.environ.get("PORT", "8080"))
BASE_RPC = "https://mainnet.base.org"
DEXSCREENER = "https://api.dexscreener.com"
ENSIDEAS = "https://api.ensideas.com"
ENSDATA = "https://api.ensdata.net"
LLAMA = "https://api.llama.fi"
YIELDS = "https://yields.llama.fi"
STABLECOINS_API = "https://stablecoins.llama.fi"
COINGECKO = "https://api.coingecko.com/api/v3"
FNG = "https://api.alternative.me"
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
SEL_ALLOWANCE = "dd62ed3e"

# Chainlink aggregator selectors + Base feed addresses (same as paid service).
SEL_DESCRIPTION = "7284e416"
SEL_LATEST_ROUND = "feaf968c"
CHAINLINK_FEEDS = {
    "eth": {"pair": "ETH / USD", "address": "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70"},
    "btc": {"pair": "BTC / USD", "address": "0x64c911996D3c6aC71f9b455B1E8E7266BcbD848F"},
    "usdc": {"pair": "USDC / USD", "address": "0x7e860098F58bBFC8648a4311b374B1D669a2bc6B"},
}

# Uniswap V3 pool selectors.
SEL_SLOT0 = "3850c7bd"
SEL_TOKEN0 = "0dfe1681"
SEL_TOKEN1 = "d21220a7"
SEL_FEE = "ddca3f43"
SEL_LIQUIDITY = "1a686502"

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


def _is_txhash(value: str) -> bool:
    v = value.strip()
    return v.startswith("0x") and len(v) == 66 and all(
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


async def _get_json(url: str, params: dict | None = None):
    """GET a public JSON API. Raises on failure."""
    r = await _client.get(url, params=params)
    r.raise_for_status()
    return r.json()


# Light in-process cache for hot / heavy GET endpoints (CoinGecko rate
# limits, DeFiLlama's multi-MB protocol and yield-pool lists). Keyed by
# URL + params; entries live `ttl` seconds.
_cache: dict[str, tuple[float, object]] = {}


async def _get_cached(url: str, ttl: float = 60.0, params: dict | None = None):
    key = url
    if params:
        key += "?" + "&".join(f"{k}={params[k]}" for k in sorted(params))
    now = time.monotonic()
    hit = _cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    try:
        data = await _get_json(url, params=params)
    except httpx.HTTPStatusError as e:
        if e.response.status_code != 429:
            raise
        # One retry after a short backoff — CoinGecko's free tier
        # rate-limits in bursts when several distinct endpoints are
        # called in quick succession.
        await asyncio.sleep(3.0)
        data = await _get_json(url, params=params)
    _cache[key] = (now, data)
    return data


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


def _word(raw: bytes, i: int) -> int:
    """The i-th 32-byte ABI word as an unsigned int."""
    return int.from_bytes(raw[i * 32 : (i + 1) * 32], "big")


def _word_signed(raw: bytes, i: int) -> int:
    """The i-th 32-byte ABI word as a signed int (values are sign-extended)."""
    return int.from_bytes(raw[i * 32 : (i + 1) * 32], "big", signed=True)


async def _erc20_str(token: str, selector: str) -> str:
    return _decode_string(await _eth_call(token, "0x" + selector))


async def _erc20_uint(token: str, selector: str, args: bytes = b"") -> int:
    return _decode_uint(await _eth_call(token, "0x" + selector + args.hex()))


async def _erc20_decimals(token: str) -> int:
    d = _decode_uint(await _eth_call(token, "0x" + SEL_DECIMALS))
    return d if 0 <= d <= 36 else 18


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


async def _eth_price_usd():
    """Live ETH price in USD from the best WETH DexScreener pair (mirrors v5)."""
    best = _best_pair(await _base_pairs_for_token(WETH))
    if not best:
        return None
    try:
        return float(best.get("priceUsd"))
    except Exception:
        return None


def _fmt_usd(value) -> str:
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return "n/a"


def _fmt_big(value) -> str:
    """Compact USD for TVL / market-cap scale figures: $1.23B, $456.7M."""
    try:
        v = float(value)
    except Exception:
        return "n/a"
    for suffix, scale in (("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if abs(v) >= scale:
            return f"${v / scale:,.2f}{suffix}"
    return f"${v:,.2f}"


def _fmt_pct(value, digits: int = 2) -> str:
    try:
        return f"{float(value):+.{digits}f}%"
    except Exception:
        return "n/a"


def _fmt_ts(ts) -> str:
    """Unix timestamp -> 'YYYY-MM-DD HH:MM UTC'."""
    try:
        return datetime.fromtimestamp(int(ts), timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return "n/a"


def _err(tool: str, exc: Exception) -> str:
    msg = str(exc).splitlines()[0][:200] if str(exc) else exc.__class__.__name__
    return f"Error: {tool} failed — {msg}. Try again shortly."


def _join(lines: list) -> str:
    lines.append(FUNNEL)
    return "\n".join(lines)


# ===== MCP server =====

mcp = FastMCP(
    "manteclaw-lite",
    instructions=(
        "Free lite tools from Manteclaw: live Base chain gas, blocks and "
        "transactions, token prices and DEX pairs, DeFi TVL and yields, "
        "stablecoins, global market stats, ENS, wallets, Chainlink "
        "oracles and converters — 100 tools, all spot values and top-N "
        "summaries. For the full 199-endpoint paid API (holder "
        f"breakdowns, history, security scans), see {CATALOG_URL}."
    ),
    host="0.0.0.0",
    port=PORT,
    stateless_http=True,
    json_response=True,
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=False
    ),
)
# The SDK reports its own package version when the server version is
# unset; pin ours so clients see the lite server's release.
mcp._mcp_server.version = VERSION


@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok", "service": "manteclaw-mcp-lite"})


# Glama listing ownership verification (public by design — this token is
# meant to be served openly so Glama can confirm we run this server).
_GLAMA_CLAIM = {
    "$schema": "https://glama.ai/mcp/schemas/connector.json",
    "claim": "glama_claim_uL6_nSyDswxysn9fwue16BGvgxFfF17s",
}


@mcp.custom_route("/.well-known/glama.json", methods=["GET"])
async def glama_claim_root(request: Request) -> JSONResponse:
    return JSONResponse(_GLAMA_CLAIM)


@mcp.custom_route("/mcp/.well-known/glama.json", methods=["GET"])
async def glama_claim_endpoint(request: Request) -> JSONResponse:
    return JSONResponse(_GLAMA_CLAIM)



# ===== original tools (unchanged) =====


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
            "The free lite tools here (100 of them — chain, gas, tokens, DEX, DeFi, stablecoins, markets, ENS, wallets, oracles, converters) are a small sample.",
        ]
    )



# ===== chain & blocks (Base RPC) =====


@mcp.tool()
async def chain_id() -> str:
    """Base chain ID (decimal and hex) from the public RPC."""
    try:
        cid = int(await _rpc("eth_chainId", []), 16)
        return _join(["Base network", f"Chain ID: {cid}", f"Hex: {hex(cid)}"])
    except Exception as e:
        return _err("chain_id", e)


@mcp.tool()
async def base_block_by_number(number: int) -> str:
    """Summary of one Base block by number: hash, timestamp, tx count, gas used/limit, base fee."""
    try:
        if number < 0:
            return "Error: block number must be >= 0."
        block = await _rpc("eth_getBlockByNumber", [hex(number), False])
        if not block:
            return f"Error: block {number} not found on Base."
        base_fee = block.get("baseFeePerGas")
        return _join(
            [
                f"Base block {int(block['number'], 16)}",
                f"Hash: {block['hash']}",
                f"Timestamp: {int(block['timestamp'], 16)} ({_fmt_ts(int(block['timestamp'], 16))})",
                f"Transactions: {len(block.get('transactions', []))}",
                f"Gas used: {int(block['gasUsed'], 16):,} / {int(block['gasLimit'], 16):,}",
                f"Base fee: {(int(base_fee, 16) / 1e9):.4f} gwei" if base_fee else "Base fee: n/a",
            ]
        )
    except Exception as e:
        return _err("base_block_by_number", e)


@mcp.tool()
async def base_recent_blocks(count: int = 5) -> str:
    """The most recent Base blocks (1-10): number, tx count, gas used, base fee for each."""
    try:
        n = max(1, min(int(count), 10))
        latest = await _rpc("eth_getBlockByNumber", ["latest", False])
        latest_num = int(latest["number"], 16)
        lines = [f"Last {n} Base blocks (latest: {latest_num})"]
        for num in range(latest_num, latest_num - n, -1):
            b = await _rpc("eth_getBlockByNumber", [hex(num), False])
            bf = b.get("baseFeePerGas")
            lines.append(
                f"#{num}: {len(b.get('transactions', []))} txs, gas {int(b['gasUsed'], 16):,}, "
                f"base fee {(int(bf, 16) / 1e9):.4f} gwei" if bf else f"#{num}: {len(b.get('transactions', []))} txs"
            )
        return _join(lines)
    except Exception as e:
        return _err("base_recent_blocks", e)


@mcp.tool()
async def base_tx(tx_hash: str) -> str:
    """Summary of one Base transaction by hash: from, to, value, gas, nonce, block."""
    try:
        if not _is_txhash(tx_hash):
            return "Error: base_tx needs a transaction hash (0x, 66 chars)."
        tx = await _rpc("eth_getTransactionByHash", [tx_hash.strip()])
        if not tx:
            return f"Error: transaction {tx_hash} not found on Base."
        gas_price = tx.get("gasPrice")
        return _join(
            [
                f"Transaction {tx['hash']}",
                f"From: {tx.get('from')}",
                f"To: {tx.get('to')}",
                f"Value: {int(tx.get('value', '0x0'), 16) / 1e18:.6f} ETH",
                f"Gas limit: {int(tx.get('gas', '0x0'), 16):,}",
                f"Gas price: {int(gas_price, 16) / 1e9:.4f} gwei" if gas_price else "Gas price: n/a",
                f"Nonce: {int(tx.get('nonce', '0x0'), 16)}",
                f"Block: {int(tx['blockNumber'], 16) if tx.get('blockNumber') else 'pending'}",
                f"Input size: {(len(tx.get('input', '0x')) - 2) // 2} bytes",
            ]
        )
    except Exception as e:
        return _err("base_tx", e)


@mcp.tool()
async def base_tx_receipt(tx_hash: str) -> str:
    """Receipt for one Base transaction: success/failure, gas used, effective price, fee paid, log count."""
    try:
        if not _is_txhash(tx_hash):
            return "Error: base_tx_receipt needs a transaction hash (0x, 66 chars)."
        r = await _rpc("eth_getTransactionReceipt", [tx_hash.strip()])
        if not r:
            return f"Error: no receipt for {tx_hash} (unknown or still pending)."
        gas_used = int(r.get("gasUsed", "0x0"), 16)
        eff = r.get("effectiveGasPrice")
        lines = [
            f"Receipt {r.get('transactionHash')}",
            f"Status: {'success' if int(r.get('status', '0x0'), 16) == 1 else 'failed'}",
            f"Block: {int(r.get('blockNumber', '0x0'), 16)}",
            f"Gas used: {gas_used:,}",
            f"Logs: {len(r.get('logs', []))}",
        ]
        if eff:
            eff_i = int(eff, 16)
            lines.append(f"Effective gas price: {eff_i / 1e9:.4f} gwei")
            lines.append(f"Fee paid: {gas_used * eff_i / 1e18:.6f} ETH")
        if r.get("contractAddress"):
            lines.append(f"Contract created: {r['contractAddress']}")
        return _join(lines)
    except Exception as e:
        return _err("base_tx_receipt", e)


@mcp.tool()
async def base_nonce(address: str) -> str:
    """Transaction count (nonce) of an address on Base — confirmed and pending."""
    try:
        if not _is_address(address):
            return "Error: base_nonce needs an address (0x, 42 chars)."
        addr = address.strip()
        confirmed = int(await _rpc("eth_getTransactionCount", [addr, "latest"]), 16)
        pending = int(await _rpc("eth_getTransactionCount", [addr, "pending"]), 16)
        return _join(
            [
                f"Address {addr} (Base)",
                f"Nonce (confirmed): {confirmed}",
                f"Nonce (pending): {pending}",
            ]
        )
    except Exception as e:
        return _err("base_nonce", e)


@mcp.tool()
async def base_pending_txs() -> str:
    """Transaction count in the pending Base block, as reported by the public RPC."""
    try:
        block = await _rpc("eth_getBlockByNumber", ["pending", False])
        if not block:
            return "Error: the Base public RPC did not return a pending block."
        return _join(
            [
                "Base pending block",
                f"Transactions pending: {len(block.get('transactions', []))}",
                f"Pending block number: {int(block['number'], 16) if block.get('number') else 'n/a'}",
            ]
        )
    except Exception as e:
        return _err("base_pending_txs", e)


@mcp.tool()
async def base_client_version() -> str:
    """Client version string of the Base public RPC node."""
    try:
        v = await _rpc("web3_clientVersion", [])
        return _join(["Base RPC client", f"Version: {v}"])
    except Exception as e:
        return _err("base_client_version", e)


@mcp.tool()
async def base_sync_status() -> str:
    """Base node sync status plus latest-block age (the practical health signal — the public RPC filters eth_syncing, same note as the paid service)."""
    try:
        try:
            syncing = await _rpc("eth_syncing", [])
        except Exception:
            # The public RPC rejects/filters eth_syncing (HTTP 401);
            # block recency is the health signal, as in the paid service.
            syncing = "unavailable (filtered by the public RPC)"
        block = await _rpc("eth_getBlockByNumber", ["latest", False])
        ts = int(block["timestamp"], 16)
        age = max(0, int(time.time()) - ts)
        return _join(
            [
                "Base sync status",
                f"eth_syncing: {syncing}",
                f"Latest block: {int(block['number'], 16)}",
                f"Block age: {age}s (Base targets ~2s blocks; a small age means the chain head is fresh)",
            ]
        )
    except Exception as e:
        return _err("base_sync_status", e)


@mcp.tool()
async def base_fee_history(blocks: int = 10) -> str:
    """Recent Base fee history (eth_feeHistory): per-block base fee in gwei, gas-used ratio, median priority reward."""
    try:
        n = max(1, min(int(blocks), 25))
        data = await _rpc("eth_feeHistory", [hex(n), "latest", [25, 50, 75]])
        oldest = int(data["oldestBlock"], 16)
        fees = data.get("baseFeePerGas") or []
        ratios = data.get("gasUsedRatio") or []
        rewards = data.get("reward") or []
        lines = [f"Base fee history — {n} blocks from #{oldest}"]
        for i in range(min(n, len(ratios))):
            fee_gwei = int(fees[i], 16) / 1e9 if i < len(fees) else 0.0
            med = int(rewards[i][1], 16) / 1e9 if i < len(rewards) and len(rewards[i]) > 1 else 0.0
            lines.append(
                f"#{oldest + i}: base fee {fee_gwei:.4f} gwei, gas used {ratios[i] * 100:.1f}%, median priority {med:.4f} gwei"
            )
        if fees:
            lines.append(f"Next block base fee: {int(fees[-1], 16) / 1e9:.4f} gwei")
        return _join(lines)
    except Exception as e:
        return _err("base_fee_history", e)


# ===== gas & fees =====


@mcp.tool()
async def base_gas_price() -> str:
    """Current Base gas price from the RPC gas-price oracle (eth_gasPrice), in gwei and wei."""
    try:
        wei = int(await _rpc("eth_gasPrice", []), 16)
        return _join(
            [
                "Base gas price (eth_gasPrice)",
                f"{wei / 1e9:.4f} gwei",
                f"{wei} wei",
            ]
        )
    except Exception as e:
        return _err("base_gas_price", e)


@mcp.tool()
async def base_priority_fee() -> str:
    """Suggested priority fee (tip) for Base right now (eth_maxPriorityFeePerGas), in gwei."""
    try:
        wei = int(await _rpc("eth_maxPriorityFeePerGas", []), 16)
        return _join(
            [
                "Base priority fee suggestion",
                f"{wei / 1e9:.4f} gwei",
                f"{wei} wei",
            ]
        )
    except Exception as e:
        return _err("base_priority_fee", e)


@mcp.tool()
async def base_transfer_cost() -> str:
    """Estimated cost of a plain ETH transfer on Base right now: 21,000 gas at the current gas price, in ETH and USD."""
    try:
        wei = int(await _rpc("eth_gasPrice", []), 16)
        cost_eth = 21000 * wei / 1e18
        lines = [
            "Cost of a plain ETH transfer on Base (21,000 gas)",
            f"Gas price: {wei / 1e9:.4f} gwei",
            f"Cost: {cost_eth:.6f} ETH",
        ]
        price = await _eth_price_usd()
        if price:
            lines.append(f"Cost in USD: ${cost_eth * price:.4f} (ETH at {_fmt_usd(price)})")
        return _join(lines)
    except Exception as e:
        return _err("base_transfer_cost", e)


@mcp.tool()
async def gas_cost_estimate(gwei: float, gas_units: int) -> str:
    """Cost of a transaction at a given gas price and gas amount: in ETH and USD at the live ETH price."""
    try:
        g = float(gwei)
        units = int(gas_units)
        if g < 0 or units <= 0:
            return "Error: gwei must be >= 0 and gas_units > 0."
        cost_eth = g * units / 1e9
        lines = [
            f"Gas cost at {g} gwei for {units:,} gas",
            f"Cost: {cost_eth:.6f} ETH",
        ]
        price = await _eth_price_usd()
        if price:
            lines.append(f"Cost in USD: ${cost_eth * price:.2f} (ETH at {_fmt_usd(price)})")
        return _join(lines)
    except Exception as e:
        return _err("gas_cost_estimate", e)


@mcp.tool()
async def base_tx_costs() -> str:
    """What common Base operations cost right now at the current base fee: ETH transfer, ERC20 transfer, approve, typical swap — in ETH and USD (typical gas amounts, labelled estimates)."""
    try:
        block = await _rpc("eth_getBlockByNumber", ["latest", False])
        base_fee_gwei = int(block["baseFeePerGas"], 16) / 1e9
        price = await _eth_price_usd()
        ops = [
            ("ETH transfer", 21000),
            ("ERC20 transfer", 65000),
            ("ERC20 approve", 46000),
            ("DEX swap (typical)", 150000),
        ]
        lines = [f"Typical Base transaction costs at base fee {base_fee_gwei:.4f} gwei"]
        for name, gas in ops:
            cost_eth = base_fee_gwei * gas / 1e9
            usd = f", ${cost_eth * price:.4f}" if price else ""
            lines.append(f"{name} (~{gas:,} gas): {cost_eth:.6f} ETH{usd}")
        lines.append("Gas amounts are typical estimates; actual usage varies by contract.")
        return _join(lines)
    except Exception as e:
        return _err("base_tx_costs", e)



# ===== tokens & prices (DexScreener + Base RPC) =====


def _pair_line(p) -> str:
    return (
        f"{(p.get('baseToken') or {}).get('symbol')}/{(p.get('quoteToken') or {}).get('symbol')} "
        f"on {p.get('dexId')}: {_fmt_usd(p.get('priceUsd'))}, "
        f"liquidity {_fmt_big((p.get('liquidity') or {}).get('usd'))}, "
        f"24h vol {_fmt_big((p.get('volume') or {}).get('h24'))}"
    )


@mcp.tool()
async def token_pairs(token: str) -> str:
    """Top 5 Base DEX pairs for a token (symbol or contract address), by liquidity. Source: DexScreener."""
    try:
        pairs = await _pairs_for_query(token)
        if not pairs:
            return f"Error: no Base DEX pairs found for '{token}'."
        pairs = sorted(
            pairs,
            key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0),
            reverse=True,
        )[:5]
        lines = [f"Top Base pairs for {token} ({len(pairs)} shown)"]
        lines += [f"{i + 1}. {_pair_line(p)}" for i, p in enumerate(pairs)]
        return _join(lines)
    except Exception as e:
        return _err("token_pairs", e)


@mcp.tool()
async def token_liquidity(token: str) -> str:
    """Total liquidity across a token's Base DEX pairs, pair count, and the deepest pair. Source: DexScreener."""
    try:
        pairs = await _pairs_for_query(token)
        if not pairs:
            return f"Error: no Base DEX pairs found for '{token}'."
        total = sum(float((p.get("liquidity") or {}).get("usd") or 0) for p in pairs)
        best = _best_pair(pairs)
        return _join(
            [
                f"Liquidity for {token} on Base",
                f"Total across {len(pairs)} pairs: {_fmt_usd(total)}",
                f"Deepest pair: {_pair_line(best)}",
            ]
        )
    except Exception as e:
        return _err("token_liquidity", e)


@mcp.tool()
async def token_volume(token: str) -> str:
    """Trading volume of a token's best Base pair over 24h / 6h / 1h / 5m. Source: DexScreener."""
    try:
        best = _best_pair(await _pairs_for_query(token))
        if not best:
            return f"Error: no Base DEX pair found for '{token}'."
        vol = best.get("volume") or {}
        sym = (best.get("baseToken") or {}).get("symbol")
        return _join(
            [
                f"Volume for {sym} (best Base pair on {best.get('dexId')})",
                f"24h: {_fmt_usd(vol.get('h24'))}",
                f"6h: {_fmt_usd(vol.get('h6'))}",
                f"1h: {_fmt_usd(vol.get('h1'))}",
                f"5m: {_fmt_usd(vol.get('m5'))}",
            ]
        )
    except Exception as e:
        return _err("token_volume", e)


@mcp.tool()
async def token_price_change(token: str) -> str:
    """Price changes of a token's best Base pair over 24h / 6h / 1h / 5m. Source: DexScreener."""
    try:
        best = _best_pair(await _pairs_for_query(token))
        if not best:
            return f"Error: no Base DEX pair found for '{token}'."
        ch = best.get("priceChange") or {}
        sym = (best.get("baseToken") or {}).get("symbol")

        def f(v):
            return f"{v}%" if v is not None else "n/a"

        return _join(
            [
                f"Price change for {sym} (best Base pair, now {_fmt_usd(best.get('priceUsd'))})",
                f"24h: {f(ch.get('h24'))}",
                f"6h: {f(ch.get('h6'))}",
                f"1h: {f(ch.get('h1'))}",
                f"5m: {f(ch.get('m5'))}",
            ]
        )
    except Exception as e:
        return _err("token_price_change", e)


@mcp.tool()
async def token_txns(token: str) -> str:
    """Buy/sell transaction counts for a token's best Base pair over 24h / 6h / 1h / 5m. Source: DexScreener."""
    try:
        best = _best_pair(await _pairs_for_query(token))
        if not best:
            return f"Error: no Base DEX pair found for '{token}'."
        txns = best.get("txns") or {}
        sym = (best.get("baseToken") or {}).get("symbol")
        lines = [f"Transactions for {sym} (best Base pair on {best.get('dexId')})"]
        for window in ("h24", "h6", "h1", "m5"):
            w = txns.get(window) or {}
            lines.append(f"{window}: {w.get('buys', 0)} buys / {w.get('sells', 0)} sells")
        return _join(lines)
    except Exception as e:
        return _err("token_txns", e)


@mcp.tool()
async def token_fdv(token: str) -> str:
    """Fully diluted valuation and market cap of a token from its best Base pair. Source: DexScreener."""
    try:
        best = _best_pair(await _pairs_for_query(token))
        if not best:
            return f"Error: no Base DEX pair found for '{token}'."
        sym = (best.get("baseToken") or {}).get("symbol")
        return _join(
            [
                f"Valuation for {sym} on Base",
                f"Price: {_fmt_usd(best.get('priceUsd'))}",
                f"Market cap: {_fmt_usd(best.get('marketCap'))}",
                f"FDV: {_fmt_usd(best.get('fdv'))}",
            ]
        )
    except Exception as e:
        return _err("token_fdv", e)


@mcp.tool()
async def token_balance(token_address: str, wallet: str) -> str:
    """ERC20 balance of any Base token for any wallet, read on-chain (balanceOf + decimals)."""
    try:
        if not _is_address(token_address) or not _is_address(wallet):
            return "Error: token_balance needs a token contract address and a wallet address (0x, 42 chars each)."
        token, addr = token_address.strip(), wallet.strip()
        raw = await _erc20_uint(token, SEL_BALANCE_OF, _addr32(addr))
        decimals = await _erc20_decimals(token)
        symbol = None
        try:
            symbol = await _erc20_str(token, SEL_SYMBOL)
        except Exception:
            pass
        return _join(
            [
                f"Balance of {token} for {addr} (Base)",
                f"{raw / (10 ** decimals):,.6f} {symbol or ''}".rstrip(),
                f"Raw: {raw}",
            ]
        )
    except Exception as e:
        return _err("token_balance", e)


@mcp.tool()
async def token_supply(contract_address: str) -> str:
    """Total supply of a Base ERC20, read on-chain and formatted with its decimals."""
    try:
        if not _is_address(contract_address):
            return "Error: token_supply needs a contract address (0x, 42 chars)."
        token = contract_address.strip()
        supply = await _erc20_uint(token, SEL_TOTAL_SUPPLY)
        decimals = await _erc20_decimals(token)
        symbol = None
        try:
            symbol = await _erc20_str(token, SEL_SYMBOL)
        except Exception:
            pass
        return _join(
            [
                f"Total supply of {token} (Base)",
                f"{supply / (10 ** decimals):,.4f} {symbol or ''}".rstrip(),
                f"Raw: {supply} (decimals: {decimals})",
            ]
        )
    except Exception as e:
        return _err("token_supply", e)


@mcp.tool()
async def token_decimals(contract_address: str) -> str:
    """Decimals of a Base ERC20 contract, read on-chain."""
    try:
        if not _is_address(contract_address):
            return "Error: token_decimals needs a contract address (0x, 42 chars)."
        token = contract_address.strip()
        decimals = await _erc20_decimals(token)
        return _join([f"Decimals of {token} (Base): {decimals}"])
    except Exception as e:
        return _err("token_decimals", e)


@mcp.tool()
async def token_allowance(token_address: str, owner: str, spender: str) -> str:
    """ERC20 allowance an owner granted a spender on Base, read on-chain. Flags unlimited (max) approvals."""
    try:
        if not all(_is_address(a) for a in (token_address, owner, spender)):
            return "Error: token_allowance needs token, owner and spender addresses (0x, 42 chars each)."
        token = token_address.strip()
        raw = await _erc20_uint(token, SEL_ALLOWANCE, _addr32(owner.strip()) + _addr32(spender.strip()))
        decimals = await _erc20_decimals(token)
        lines = [
            f"Allowance on Base: {owner} -> {spender}",
            f"Token: {token}",
            f"Allowance: {raw / (10 ** decimals):,.6f}",
            f"Raw: {raw}",
        ]
        if raw >= 2**200:
            lines.append("This is effectively an unlimited (max) approval.")
        return _join(lines)
    except Exception as e:
        return _err("token_allowance", e)


@mcp.tool()
async def token_name_symbol(contract_address: str) -> str:
    """Name and symbol of a Base ERC20 contract, read on-chain."""
    try:
        if not _is_address(contract_address):
            return "Error: token_name_symbol needs a contract address (0x, 42 chars)."
        token = contract_address.strip()
        name = await _erc20_str(token, SEL_NAME)
        symbol = await _erc20_str(token, SEL_SYMBOL)
        return _join([f"Token {token} (Base)", f"Name: {name}", f"Symbol: {symbol}"])
    except Exception as e:
        return _err("token_name_symbol", e)


@mcp.tool()
async def token_search(query: str) -> str:
    """Search DexScreener for a token by name/symbol and show its top Base pairs with prices."""
    try:
        data = await _get_json(f"{DEXSCREENER}/latest/dex/search", params={"q": query})
        pairs = data.get("pairs") if isinstance(data, dict) else None
        base_pairs = [p for p in (pairs or []) if p.get("chainId") == "base"]
        if not base_pairs:
            return f"Error: no Base pairs found matching '{query}'."
        base_pairs = sorted(
            base_pairs,
            key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0),
            reverse=True,
        )[:8]
        lines = [f"Base search results for '{query}' ({len(base_pairs)} shown)"]
        lines += [f"{i + 1}. {_pair_line(p)}" for i, p in enumerate(base_pairs)]
        return _join(lines)
    except Exception as e:
        return _err("token_search", e)


@mcp.tool()
async def eth_price() -> str:
    """Live ETH price in USD (plus 24h change) from the best WETH pair on Base. Source: DexScreener."""
    try:
        best = _best_pair(await _base_pairs_for_token(WETH))
        if not best:
            return "Error: no WETH pair found on Base."
        change = (best.get("priceChange") or {}).get("h24")
        lines = [
            "Ether (ETH)",
            f"Price: {_fmt_usd(best.get('priceUsd'))}",
            f"24h change: {change}%" if change is not None else "24h change: n/a",
            f"Reference pair: WETH/{(best.get('quoteToken') or {}).get('symbol')} on {best.get('dexId')}",
        ]
        return _join(lines)
    except Exception as e:
        return _err("eth_price", e)


@mcp.tool()
async def token_price_eth(token: str) -> str:
    """Price of a token denominated in its pair's quote asset (usually ETH) on its best Base pair, plus the USD price. Source: DexScreener."""
    try:
        best = _best_pair(await _pairs_for_query(token))
        if not best:
            return f"Error: no Base DEX pair found for '{token}'."
        sym = (best.get("baseToken") or {}).get("symbol")
        quote = (best.get("quoteToken") or {}).get("symbol")
        return _join(
            [
                f"{sym} price in {quote} terms (best Base pair on {best.get('dexId')})",
                f"Price: {best.get('priceNative')} {quote}",
                f"USD price: {_fmt_usd(best.get('priceUsd'))}",
            ]
        )
    except Exception as e:
        return _err("token_price_eth", e)


# ===== DEX (DexScreener + on-chain pools) =====


@mcp.tool()
async def dex_search(query: str) -> str:
    """Search DEX pairs across all chains by token name, symbol or address — top 10 by liquidity. Source: DexScreener."""
    try:
        data = await _get_json(f"{DEXSCREENER}/latest/dex/search", params={"q": query})
        pairs = data.get("pairs") if isinstance(data, dict) else None
        if not pairs:
            return f"Error: no DEX pairs found matching '{query}'."
        pairs = sorted(
            pairs,
            key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0),
            reverse=True,
        )[:10]
        lines = [f"DEX search for '{query}' ({len(pairs)} shown)"]
        for i, p in enumerate(pairs):
            lines.append(
                f"{i + 1}. [{p.get('chainId')}] {(p.get('baseToken') or {}).get('symbol')}/"
                f"{(p.get('quoteToken') or {}).get('symbol')} on {p.get('dexId')}: "
                f"{_fmt_usd(p.get('priceUsd'))}, liquidity {_fmt_big((p.get('liquidity') or {}).get('usd'))}"
            )
        return _join(lines)
    except Exception as e:
        return _err("dex_search", e)


@mcp.tool()
async def dex_pair_lookup(pair_address: str) -> str:
    """Full summary of one Base DEX pair by its pair contract address: tokens, price, liquidity, volume, market cap. Source: DexScreener."""
    try:
        if not _is_address(pair_address):
            return "Error: dex_pair_lookup needs a pair address (0x, 42 chars)."
        data = await _get_json(f"{DEXSCREENER}/latest/dex/pairs/base/{pair_address.strip()}")
        pairs = data.get("pairs") if isinstance(data, dict) else None
        if not pairs:
            return f"Error: pair {pair_address} not found on Base."
        p = pairs[0]
        return _join(
            [
                f"Pair {(p.get('baseToken') or {}).get('symbol')}/{(p.get('quoteToken') or {}).get('symbol')} on {p.get('dexId')} (Base)",
                f"Pair address: {p.get('pairAddress')}",
                f"Base token: {(p.get('baseToken') or {}).get('address')}",
                f"Quote token: {(p.get('quoteToken') or {}).get('address')}",
                f"Price: {_fmt_usd(p.get('priceUsd'))}",
                f"Liquidity: {_fmt_usd((p.get('liquidity') or {}).get('usd'))}",
                f"24h volume: {_fmt_usd((p.get('volume') or {}).get('h24'))}",
                f"Market cap: {_fmt_usd(p.get('marketCap'))}",
                f"URL: {p.get('url')}",
            ]
        )
    except Exception as e:
        return _err("dex_pair_lookup", e)


def _boost_lines(items, title):
    lines = [title]
    for i, it in enumerate(items):
        lines.append(
            f"{i + 1}. [{it.get('chainId')}] {it.get('tokenAddress')} "
            f"(boost total: {it.get('totalAmount')}) — {it.get('url')}"
        )
    return lines


@mcp.tool()
async def dex_trending() -> str:
    """Top boosted tokens on DexScreener right now (their trending board), top 10 across chains."""
    try:
        data = await _get_json(f"{DEXSCREENER}/token-boosts/top/v1")
        items = data if isinstance(data, list) else []
        if not items:
            return "Error: no boosted tokens returned by DexScreener."
        return _join(_boost_lines(items[:10], "Top boosted tokens on DexScreener"))
    except Exception as e:
        return _err("dex_trending", e)


@mcp.tool()
async def dex_latest_boosts() -> str:
    """Most recently boosted tokens on DexScreener, latest 10 across chains."""
    try:
        data = await _get_json(f"{DEXSCREENER}/token-boosts/latest/v1")
        items = data if isinstance(data, list) else []
        if not items:
            return "Error: no boosted tokens returned by DexScreener."
        return _join(_boost_lines(items[:10], "Latest boosted tokens on DexScreener"))
    except Exception as e:
        return _err("dex_latest_boosts", e)


@mcp.tool()
async def dex_new_profiles() -> str:
    """Newest token profiles created on DexScreener, latest 10 across chains, with short descriptions."""
    try:
        data = await _get_json(f"{DEXSCREENER}/token-profiles/latest/v1")
        items = data if isinstance(data, list) else []
        if not items:
            return "Error: no token profiles returned by DexScreener."
        lines = ["Newest token profiles on DexScreener"]
        for i, it in enumerate(items[:10]):
            desc = (it.get("description") or "").strip().replace("\n", " ")
            if len(desc) > 100:
                desc = desc[:97] + "..."
            lines.append(f"{i + 1}. [{it.get('chainId')}] {it.get('tokenAddress')}" + (f" — {desc}" if desc else ""))
        return _join(lines)
    except Exception as e:
        return _err("dex_new_profiles", e)


@mcp.tool()
async def dex_pairs_count(token: str) -> str:
    """How many Base DEX pairs a token (contract address) has, and on which DEXes. Source: DexScreener."""
    try:
        if not _is_address(token):
            return "Error: dex_pairs_count needs a token contract address (0x, 42 chars)."
        pairs = await _base_pairs_for_token(token.strip())
        if not pairs:
            return f"Error: no Base pairs found for {token}."
        dexes = sorted({p.get("dexId") for p in pairs if p.get("dexId")})
        return _join(
            [
                f"Base pairs for {token}",
                f"Pair count (token as base): {len(pairs)}",
                f"DEXes: {', '.join(dexes)}",
            ]
        )
    except Exception as e:
        return _err("dex_pairs_count", e)


@mcp.tool()
async def uniswap_v3_pool(pool_address: str) -> str:
    """Live state of a Uniswap V3 pool on Base, read on-chain: both tokens, fee tier, current tick, in-pool liquidity, and the implied price both ways."""
    try:
        if not _is_address(pool_address):
            return "Error: uniswap_v3_pool needs a pool address (0x, 42 chars)."
        pool = pool_address.strip()
        code = await _rpc("eth_getCode", [pool, "latest"])
        if not code or code == "0x":
            return f"Error: no contract code at {pool} on Base."
        t0 = "0x" + (await _eth_call(pool, "0x" + SEL_TOKEN0))[-20:].hex()
        t1 = "0x" + (await _eth_call(pool, "0x" + SEL_TOKEN1))[-20:].hex()
        fee = _decode_uint(await _eth_call(pool, "0x" + SEL_FEE))
        slot0 = await _eth_call(pool, "0x" + SEL_SLOT0)
        sqrt_price = _word(slot0, 0)
        tick = _word_signed(slot0, 1)
        liquidity = _decode_uint(await _eth_call(pool, "0x" + SEL_LIQUIDITY))
        d0 = await _erc20_decimals(t0)
        d1 = await _erc20_decimals(t1)
        s0 = s1 = "?"
        try:
            s0 = await _erc20_str(t0, SEL_SYMBOL)
        except Exception:
            pass
        try:
            s1 = await _erc20_str(t1, SEL_SYMBOL)
        except Exception:
            pass
        ratio = (sqrt_price / 2**96) ** 2
        price0_in_1 = ratio * 10 ** (d0 - d1)
        price1_in_0 = (1.0 / price0_in_1) if price0_in_1 else 0.0
        return _join(
            [
                f"Uniswap V3 pool {pool} (Base)",
                f"Token0: {s0} ({t0})",
                f"Token1: {s1} ({t1})",
                f"Fee tier: {fee / 10000}%",
                f"Current tick: {tick}",
                f"In-pool liquidity: {liquidity:,}",
                f"Price: 1 {s0} = {price0_in_1:,.6f} {s1}",
                f"Price: 1 {s1} = {price1_in_0:,.6f} {s0}",
            ]
        )
    except Exception as e:
        return _err("uniswap_v3_pool", e)



# ===== DeFi: TVL, volumes, fees, yields (DeFiLlama) =====


async def _llama_chains():
    return await _get_cached(f"{LLAMA}/v2/chains", ttl=60.0)


async def _llama_protocols():
    return await _get_cached(f"{LLAMA}/protocols", ttl=60.0)


async def _yields_pools():
    data = await _get_cached(f"{YIELDS}/pools", ttl=60.0)
    if isinstance(data, dict):
        return data.get("data") or []
    return []


def _pool_line(p) -> str:
    apy = p.get("apy")
    apy_s = f"{float(apy):.2f}%" if isinstance(apy, (int, float)) else "n/a"
    return (
        f"{p.get('project')} {p.get('symbol')} ({p.get('chain')}): "
        f"APY {apy_s}, TVL {_fmt_big(p.get('tvlUsd'))}"
    )


@mcp.tool()
async def base_tvl() -> str:
    """Total value locked (TVL) on Base right now. Source: DeFiLlama."""
    try:
        chains = await _llama_chains()
        base = next((c for c in chains if str(c.get("name", "")).lower() == "base"), None)
        if not base:
            return "Error: Base not found in DeFiLlama chain list."
        return _join(["Base TVL (DeFiLlama)", f"TVL: {_fmt_big(base.get('tvl'))}"])
    except Exception as e:
        return _err("base_tvl", e)


@mcp.tool()
async def chain_tvl(chain: str) -> str:
    """Total value locked (TVL) for any chain by name (e.g. "Ethereum", "Solana", "Arbitrum"). Source: DeFiLlama."""
    try:
        chains = await _llama_chains()
        match = next((c for c in chains if str(c.get("name", "")).lower() == chain.strip().lower()), None)
        if not match:
            return f"Error: chain '{chain}' not found on DeFiLlama."
        return _join(
            [
                f"{match.get('name')} TVL (DeFiLlama)",
                f"TVL: {_fmt_big(match.get('tvl'))}",
                f"Native token: {match.get('tokenSymbol')}",
            ]
        )
    except Exception as e:
        return _err("chain_tvl", e)


@mcp.tool()
async def top_chains_tvl() -> str:
    """Top 15 chains by total value locked. Source: DeFiLlama."""
    try:
        chains = await _llama_chains()
        top = sorted(chains, key=lambda c: float(c.get("tvl") or 0), reverse=True)[:15]
        lines = ["Top chains by TVL (DeFiLlama)"]
        lines += [f"{i + 1}. {c.get('name')}: {_fmt_big(c.get('tvl'))}" for i, c in enumerate(top)]
        return _join(lines)
    except Exception as e:
        return _err("top_chains_tvl", e)


@mcp.tool()
async def protocol_tvl(protocol: str) -> str:
    """Total value locked for one DeFi protocol by its DeFiLlama slug (e.g. "aave-v3", "uniswap-v3", "aerodrome")."""
    try:
        slug = protocol.strip().lower().replace(" ", "-")
        data = await _get_json(f"{LLAMA}/tvl/{slug}")
        if not isinstance(data, (int, float)):
            return f"Error: protocol '{protocol}' not found on DeFiLlama."
        return _join([f"{protocol} TVL (DeFiLlama)", f"TVL: {_fmt_big(data)}"])
    except Exception as e:
        return _err("protocol_tvl", e)


@mcp.tool()
async def protocol_info(protocol: str) -> str:
    """Summary of one DeFi protocol (by slug or name): category, current TVL, 1d/7d change, chains, description. Source: DeFiLlama."""
    try:
        protos = await _llama_protocols()
        key = protocol.strip().lower()
        match = next(
            (
                p
                for p in protos
                if str(p.get("slug", "")).lower() == key
                or str(p.get("name", "")).lower() == key
                or str(p.get("name", "")).lower().replace(" ", "-") == key
            ),
            None,
        )
        if not match:
            # Fall back to a slug/name prefix match (e.g. "aerodrome"
            # -> "aerodrome-slipstream"), most TVL first.
            candidates = [
                p
                for p in protos
                if str(p.get("slug", "")).lower().startswith(key)
                or str(p.get("name", "")).lower().startswith(key)
            ]
            if candidates:
                match = max(candidates, key=lambda p: float(p.get("tvl") or 0))
        if not match:
            return f"Error: protocol '{protocol}' not found on DeFiLlama."
        desc = (match.get("description") or "").strip()
        if len(desc) > 240:
            desc = desc[:237] + "..."
        lines = [
            f"{match.get('name')} ({match.get('symbol')}) — DeFiLlama summary",
            f"Category: {match.get('category')}",
            f"TVL: {_fmt_big(match.get('tvl'))}",
            f"Change 1d: {_fmt_pct(match.get('change_1d'))} | 7d: {_fmt_pct(match.get('change_7d'))}",
            f"Chains: {', '.join(match.get('chains') or [])}",
        ]
        if desc:
            lines.append(f"About: {desc}")
        if match.get("url"):
            lines.append(f"URL: {match.get('url')}")
        return _join(lines)
    except Exception as e:
        return _err("protocol_info", e)


@mcp.tool()
async def top_protocols() -> str:
    """Top 15 DeFi protocols by TVL across all chains. Source: DeFiLlama."""
    try:
        protos = await _llama_protocols()
        top = sorted(protos, key=lambda p: float(p.get("tvl") or 0), reverse=True)[:15]
        lines = ["Top protocols by TVL (DeFiLlama)"]
        lines += [
            f"{i + 1}. {p.get('name')} ({p.get('symbol')}): {_fmt_big(p.get('tvl'))}"
            for i, p in enumerate(top)
        ]
        return _join(lines)
    except Exception as e:
        return _err("top_protocols", e)


@mcp.tool()
async def top_base_protocols() -> str:
    """Top 15 DeFi protocols deployed on Base, by TVL. Source: DeFiLlama."""
    try:
        protos = await _llama_protocols()
        on_base = [p for p in protos if "Base" in (p.get("chains") or [])]
        top = sorted(on_base, key=lambda p: float(p.get("tvl") or 0), reverse=True)[:15]
        lines = ["Top protocols on Base by TVL (DeFiLlama)"]
        for i, p in enumerate(top):
            base_part = (p.get("chainTvls") or {}).get("Base")
            extra = f" (Base: {_fmt_big(base_part)})" if base_part else ""
            lines.append(f"{i + 1}. {p.get('name')} ({p.get('symbol')}): {_fmt_big(p.get('tvl'))}{extra}")
        return _join(lines)
    except Exception as e:
        return _err("top_base_protocols", e)


@mcp.tool()
async def dex_volume_global() -> str:
    """Global DEX trading volume (24h and 7d totals) plus the top 5 DEXes by 24h volume. Source: DeFiLlama."""
    try:
        data = await _get_cached(f"{LLAMA}/overview/dexs", ttl=60.0)
        protos = (data or {}).get("protocols") or []
        top = sorted(protos, key=lambda x: float(x.get("total24h") or 0), reverse=True)[:5]
        lines = [
            "Global DEX volume (DeFiLlama)",
            f"24h: {_fmt_big(data.get('total24h'))}",
            f"7d: {_fmt_big(data.get('total7d'))}",
            "Top DEXes by 24h volume:",
        ]
        lines += [f"{i + 1}. {p.get('name')}: {_fmt_big(p.get('total24h'))}" for i, p in enumerate(top)]
        return _join(lines)
    except Exception as e:
        return _err("dex_volume_global", e)


@mcp.tool()
async def dex_volume_base() -> str:
    """DEX trading volume on Base (24h and 7d totals) plus the top 5 Base DEXes by 24h volume. Source: DeFiLlama."""
    try:
        data = await _get_cached(f"{LLAMA}/overview/dexs/Base", ttl=60.0)
        protos = (data or {}).get("protocols") or []
        top = sorted(protos, key=lambda x: float(x.get("total24h") or 0), reverse=True)[:5]
        lines = [
            "Base DEX volume (DeFiLlama)",
            f"24h: {_fmt_big(data.get('total24h'))}",
            f"7d: {_fmt_big(data.get('total7d'))}",
            "Top Base DEXes by 24h volume:",
        ]
        lines += [f"{i + 1}. {p.get('name')}: {_fmt_big(p.get('total24h'))}" for i, p in enumerate(top)]
        return _join(lines)
    except Exception as e:
        return _err("dex_volume_base", e)


@mcp.tool()
async def dex_fees_top() -> str:
    """Top 10 protocols by fees generated in the last 24h, with the all-protocol 24h total. Source: DeFiLlama."""
    try:
        data = await _get_cached(f"{LLAMA}/overview/fees", ttl=60.0)
        protos = (data or {}).get("protocols") or []
        top = sorted(protos, key=lambda x: float(x.get("total24h") or 0), reverse=True)[:10]
        lines = [
            "Top protocols by 24h fees (DeFiLlama)",
            f"All protocols, 24h fees: {_fmt_big(data.get('total24h'))}",
        ]
        lines += [
            f"{i + 1}. {p.get('name')}: {_fmt_big(p.get('total24h'))} (7d: {_fmt_big(p.get('total7d'))})"
            for i, p in enumerate(top)
        ]
        return _join(lines)
    except Exception as e:
        return _err("dex_fees_top", e)


@mcp.tool()
async def yields_top() -> str:
    """Top 10 yield pools by APY across all chains. Source: DeFiLlama Yields."""
    try:
        pools = await _yields_pools()
        if not pools:
            return "Error: no yield pools returned by DeFiLlama."
        top = sorted(pools, key=lambda p: float(p.get("apy") or 0), reverse=True)[:10]
        lines = ["Top yield pools by APY (DeFiLlama Yields)"]
        lines += [f"{i + 1}. {_pool_line(p)}" for i, p in enumerate(top)]
        return _join(lines)
    except Exception as e:
        return _err("yields_top", e)


@mcp.tool()
async def yields_base() -> str:
    """Top 10 yield pools on Base by APY. Source: DeFiLlama Yields."""
    try:
        pools = await _yields_pools()
        on_base = [p for p in pools if p.get("chain") == "Base"]
        if not on_base:
            return "Error: no Base yield pools returned by DeFiLlama."
        top = sorted(on_base, key=lambda p: float(p.get("apy") or 0), reverse=True)[:10]
        lines = ["Top Base yield pools by APY (DeFiLlama Yields)"]
        lines += [f"{i + 1}. {_pool_line(p)}" for i, p in enumerate(top)]
        return _join(lines)
    except Exception as e:
        return _err("yields_base", e)


@mcp.tool()
async def yields_stablecoins() -> str:
    """Top 10 stablecoin yield pools by APY across chains. Source: DeFiLlama Yields."""
    try:
        pools = await _yields_pools()
        stables = [p for p in pools if p.get("stablecoin") is True]
        if not stables:
            return "Error: no stablecoin yield pools returned by DeFiLlama."
        top = sorted(stables, key=lambda p: float(p.get("apy") or 0), reverse=True)[:10]
        lines = ["Top stablecoin yield pools by APY (DeFiLlama Yields)"]
        lines += [f"{i + 1}. {_pool_line(p)}" for i, p in enumerate(top)]
        return _join(lines)
    except Exception as e:
        return _err("yields_stablecoins", e)


@mcp.tool()
async def yields_token(symbol: str) -> str:
    """Top 10 yield pools involving a token symbol (e.g. "USDC", "ETH"), by APY. Source: DeFiLlama Yields."""
    try:
        pools = await _yields_pools()
        key = symbol.strip().upper()
        mine = [p for p in pools if key in str(p.get("symbol", "")).upper()]
        if not mine:
            return f"Error: no yield pools found for symbol '{symbol}'."
        top = sorted(mine, key=lambda p: float(p.get("apy") or 0), reverse=True)[:10]
        lines = [f"Top yield pools for {key} by APY (DeFiLlama Yields)"]
        lines += [f"{i + 1}. {_pool_line(p)}" for i, p in enumerate(top)]
        return _join(lines)
    except Exception as e:
        return _err("yields_token", e)


# ===== stablecoins (DeFiLlama Stablecoins) =====


async def _stablecoin_assets():
    data = await _get_cached(f"{STABLECOINS_API}/stablecoins", ttl=60.0)
    return (data or {}).get("peggedAssets") or []


def _circ(asset) -> float:
    try:
        return float((asset.get("circulating") or {}).get("peggedUSD") or 0)
    except Exception:
        return 0.0


@mcp.tool()
async def top_stablecoins() -> str:
    """Top 10 stablecoins by circulating supply, with peg type and current price. Source: DeFiLlama Stablecoins."""
    try:
        assets = await _stablecoin_assets()
        top = sorted(assets, key=_circ, reverse=True)[:10]
        lines = ["Top stablecoins by circulating supply (DeFiLlama)"]
        for i, a in enumerate(top):
            price = a.get("price")
            price_s = f", price ${float(price):.4f}" if isinstance(price, (int, float)) else ""
            lines.append(
                f"{i + 1}. {a.get('name')} ({a.get('symbol')}): {_fmt_big(_circ(a))} "
                f"[{a.get('pegType')}/{a.get('pegMechanism')}]{price_s}"
            )
        return _join(lines)
    except Exception as e:
        return _err("top_stablecoins", e)


@mcp.tool()
async def stablecoin_info(asset: str) -> str:
    """One stablecoin by symbol, name or CoinGecko id (e.g. "USDC"): total circulating supply, peg type/mechanism, price, and its top 5 chains. Source: DeFiLlama Stablecoins."""
    try:
        assets = await _stablecoin_assets()
        key = asset.strip().lower()
        match = next(
            (
                a
                for a in assets
                if key
                in (
                    str(a.get("symbol", "")).lower(),
                    str(a.get("name", "")).lower(),
                    str(a.get("gecko_id", "")).lower(),
                    str(a.get("id", "")).lower(),
                )
            ),
            None,
        )
        if not match:
            return f"Error: stablecoin '{asset}' not found on DeFiLlama."
        lines = [
            f"{match.get('name')} ({match.get('symbol')}) — stablecoin summary",
            f"Circulating: {_fmt_big(_circ(match))}",
            f"Peg: {match.get('pegType')} / {match.get('pegMechanism')}",
        ]
        if isinstance(match.get("price"), (int, float)):
            lines.append(f"Price: ${float(match['price']):.4f}")
        cc = match.get("chainCirculating") or {}
        per_chain = []
        for ch, v in cc.items():
            amt = ((v or {}).get("current") or {}).get("peggedUSD")
            if isinstance(amt, (int, float)):
                per_chain.append((ch, amt))
        per_chain.sort(key=lambda x: x[1], reverse=True)
        if per_chain:
            lines.append("Top chains: " + ", ".join(f"{ch} {_fmt_big(amt)}" for ch, amt in per_chain[:5]))
        return _join(lines)
    except Exception as e:
        return _err("stablecoin_info", e)


@mcp.tool()
async def stablecoin_total() -> str:
    """Total circulating supply of all tracked stablecoins, and how many there are. Source: DeFiLlama Stablecoins."""
    try:
        assets = await _stablecoin_assets()
        total = sum(_circ(a) for a in assets)
        return _join(
            [
                "All stablecoins (DeFiLlama)",
                f"Total circulating: {_fmt_big(total)}",
                f"Tracked stablecoins: {len(assets)}",
            ]
        )
    except Exception as e:
        return _err("stablecoin_total", e)


@mcp.tool()
async def stablecoin_chains() -> str:
    """Top 10 chains by stablecoin supply hosted on them. Source: DeFiLlama Stablecoins."""
    try:
        rows = await _get_cached(f"{STABLECOINS_API}/stablecoinchains", ttl=60.0)

        def amt(r):
            for k in ("totalCirculatingUSD", "totalCirculating", "circulating"):
                v = r.get(k)
                if isinstance(v, (int, float)):
                    return float(v)
                if isinstance(v, dict) and isinstance(v.get("peggedUSD"), (int, float)):
                    return float(v["peggedUSD"])
            return 0.0

        top = sorted(rows if isinstance(rows, list) else [], key=amt, reverse=True)[:10]
        lines = ["Top chains by stablecoin supply (DeFiLlama)"]
        lines += [f"{i + 1}. {r.get('name')}: {_fmt_big(amt(r))}" for i, r in enumerate(top)]
        return _join(lines)
    except Exception as e:
        return _err("stablecoin_chains", e)


@mcp.tool()
async def stablecoins_base() -> str:
    """Top 10 stablecoins by circulating supply on Base. Source: DeFiLlama Stablecoins."""
    try:
        assets = await _stablecoin_assets()
        out = []
        for a in assets:
            cur = ((a.get("chainCirculating") or {}).get("Base") or {}).get("current") or {}
            amt = cur.get("peggedUSD")
            if isinstance(amt, (int, float)) and amt > 0:
                out.append((a, amt))
        out.sort(key=lambda x: x[1], reverse=True)
        lines = ["Top stablecoins on Base by supply (DeFiLlama)"]
        lines += [
            f"{i + 1}. {a.get('name')} ({a.get('symbol')}): {_fmt_big(amt)}"
            for i, (a, amt) in enumerate(out[:10])
        ]
        return _join(lines)
    except Exception as e:
        return _err("stablecoins_base", e)


@mcp.tool()
async def stablecoin_pegs() -> str:
    """Stablecoins trading furthest from their peg right now (largest price deviations; the tail is mostly illiquid or dead coins). Source: DeFiLlama Stablecoins."""
    try:
        data = await _get_cached(f"{STABLECOINS_API}/stablecoinprices", ttl=60.0)
        prices = {}
        for row in data if isinstance(data, list) else []:
            if isinstance(row, dict) and isinstance(row.get("prices"), dict):
                prices = row["prices"]
                break
        out = []
        for k, v in prices.items():
            if isinstance(v, (int, float)) and v > 0:
                out.append((k, v, (v - 1) * 100))
        out.sort(key=lambda x: abs(x[2]), reverse=True)
        lines = ["Largest stablecoin peg deviations (DeFiLlama)"]
        lines += [
            f"{i + 1}. {k}: ${v:.4f} ({dev:+.2f}% from $1.00)"
            for i, (k, v, dev) in enumerate(out[:10])
        ]
        return _join(lines)
    except Exception as e:
        return _err("stablecoin_pegs", e)



# ===== markets & global (CoinGecko + Alternative.me) =====


@mcp.tool()
async def fear_greed() -> str:
    """Current Crypto Fear & Greed Index value and classification. Source: Alternative.me."""
    try:
        data = await _get_cached(f"{FNG}/fng/", ttl=60.0, params={"limit": 1})
        items = (data or {}).get("data") or []
        if not items:
            return "Error: no Fear & Greed data returned."
        it = items[0]
        return _join(
            [
                "Crypto Fear & Greed Index",
                f"Value: {it.get('value')} — {it.get('value_classification')}",
                f"Updated: {_fmt_ts(it.get('timestamp'))}",
            ]
        )
    except Exception as e:
        return _err("fear_greed", e)


@mcp.tool()
async def fear_greed_week() -> str:
    """The last 7 days of the Crypto Fear & Greed Index, day by day. Source: Alternative.me."""
    try:
        data = await _get_cached(f"{FNG}/fng/", ttl=60.0, params={"limit": 7})
        items = (data or {}).get("data") or []
        if not items:
            return "Error: no Fear & Greed data returned."
        lines = ["Fear & Greed Index — last 7 days"]
        for it in items:
            day = datetime.fromtimestamp(int(it.get("timestamp", 0)), timezone.utc).strftime("%Y-%m-%d")
            lines.append(f"{day}: {it.get('value')} ({it.get('value_classification')})")
        return _join(lines)
    except Exception as e:
        return _err("fear_greed_week", e)


@mcp.tool()
async def market_global() -> str:
    """Global crypto market stats: total market cap, 24h volume, BTC/ETH dominance, active coins. Source: CoinGecko."""
    try:
        data = await _get_cached(f"{COINGECKO}/global", ttl=60.0)
        d = (data or {}).get("data") or {}
        pct = d.get("market_cap_percentage") or {}
        return _join(
            [
                "Global crypto market (CoinGecko)",
                f"Total market cap: {_fmt_big((d.get('total_market_cap') or {}).get('usd'))}",
                f"24h volume: {_fmt_big((d.get('total_volume') or {}).get('usd'))}",
                f"Market cap change 24h: {_fmt_pct(d.get('market_cap_change_percentage_24h_usd'))}",
                f"BTC dominance: {float(pct.get('btc')):.1f}%" if pct.get("btc") is not None else "BTC dominance: n/a",
                f"ETH dominance: {float(pct.get('eth')):.1f}%" if pct.get("eth") is not None else "ETH dominance: n/a",
                f"Active cryptocurrencies: {d.get('active_cryptocurrencies'):,}" if d.get("active_cryptocurrencies") else "Active cryptocurrencies: n/a",
                f"Markets/exchanges tracked: {d.get('markets')}",
            ]
        )
    except Exception as e:
        return _err("market_global", e)


@mcp.tool()
async def top_coins() -> str:
    """Top 10 cryptocurrencies by market cap, with price and 24h change. Source: CoinGecko."""
    try:
        data = await _get_cached(
            f"{COINGECKO}/coins/markets",
            ttl=60.0,
            params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": 10, "page": 1},
        )
        coins = data if isinstance(data, list) else []
        lines = ["Top 10 coins by market cap (CoinGecko)"]
        for c in coins:
            lines.append(
                f"{c.get('market_cap_rank')}. {c.get('name')} ({str(c.get('symbol')).upper()}): "
                f"{_fmt_usd(c.get('current_price'))} ({_fmt_pct(c.get('price_change_percentage_24h'))} 24h), "
                f"mcap {_fmt_big(c.get('market_cap'))}"
            )
        return _join(lines)
    except Exception as e:
        return _err("top_coins", e)


@mcp.tool()
async def trending_coins() -> str:
    """Coins trending on CoinGecko search right now, top 10. Source: CoinGecko."""
    try:
        data = await _get_cached(f"{COINGECKO}/search/trending", ttl=60.0)
        coins = (data or {}).get("coins") or []
        lines = ["Trending on CoinGecko"]
        for i, c in enumerate(coins[:10]):
            item = c.get("item") or {}
            price = (item.get("data") or {}).get("price")
            price_s = f": {_fmt_usd(price)}" if isinstance(price, (int, float)) else ""
            rank = item.get("market_cap_rank")
            rank_s = f" (rank #{rank})" if rank else ""
            lines.append(f"{i + 1}. {item.get('name')} ({item.get('symbol')}){rank_s}{price_s}")
        return _join(lines)
    except Exception as e:
        return _err("trending_coins", e)


@mcp.tool()
async def coin_price(coin_id: str) -> str:
    """Live USD price and 24h change for one coin by CoinGecko id (e.g. "bitcoin", "ethereum", "aerodrome-finance")."""
    try:
        cid = coin_id.strip().lower()
        data = await _get_cached(
            f"{COINGECKO}/simple/price",
            ttl=60.0,
            params={"ids": cid, "vs_currencies": "usd", "include_24hr_change": "true"},
        )
        entry = (data or {}).get(cid)
        if not entry:
            return f"Error: coin '{coin_id}' not found on CoinGecko."
        change = entry.get("usd_24h_change")
        lines = [
            f"{cid} (CoinGecko)",
            f"Price: {_fmt_usd(entry.get('usd'))}",
            f"24h change: {_fmt_pct(change)}" if change is not None else "24h change: n/a",
        ]
        return _join(lines)
    except Exception as e:
        return _err("coin_price", e)


@mcp.tool()
async def coin_prices(coin_ids: str) -> str:
    """USD prices, market caps and 24h changes for several coins at once — comma-separated CoinGecko ids (e.g. "bitcoin,ethereum,solana"), up to 10."""
    try:
        ids = [s.strip().lower() for s in coin_ids.split(",") if s.strip()][:10]
        if not ids:
            return "Error: coin_prices needs at least one CoinGecko id."
        data = await _get_cached(
            f"{COINGECKO}/simple/price",
            ttl=60.0,
            params={
                "ids": ",".join(ids),
                "vs_currencies": "usd",
                "include_24hr_change": "true",
                "include_market_cap": "true",
            },
        )
        lines = ["Coin prices (CoinGecko)"]
        for cid in ids:
            entry = (data or {}).get(cid)
            if not entry:
                lines.append(f"{cid}: not found")
                continue
            lines.append(
                f"{cid}: {_fmt_usd(entry.get('usd'))} "
                f"({_fmt_pct(entry.get('usd_24h_change'))} 24h), mcap {_fmt_big(entry.get('usd_market_cap'))}"
            )
        return _join(lines)
    except Exception as e:
        return _err("coin_prices", e)


async def _cg_coin(coin_id: str):
    return await _get_cached(
        f"{COINGECKO}/coins/{coin_id}",
        ttl=60.0,
        params={
            "localization": "false",
            "tickers": "false",
            "community_data": "false",
            "developer_data": "false",
        },
    )


@mcp.tool()
async def coin_info(coin_id: str) -> str:
    """Summary for one coin by CoinGecko id: price, market cap + rank, 24h volume, 24h/7d change, supplies, homepage."""
    try:
        cid = coin_id.strip().lower()
        data = await _cg_coin(cid)
        if not isinstance(data, dict) or not data.get("id"):
            return f"Error: coin '{coin_id}' not found on CoinGecko."
        md = data.get("market_data") or {}
        lines = [
            f"{data.get('name')} ({str(data.get('symbol')).upper()}) — CoinGecko summary",
            f"Price: {_fmt_usd((md.get('current_price') or {}).get('usd'))}",
            f"Market cap: {_fmt_big((md.get('market_cap') or {}).get('usd'))} (rank #{data.get('market_cap_rank')})",
            f"24h volume: {_fmt_big((md.get('total_volume') or {}).get('usd'))}",
            f"Change 24h: {_fmt_pct(md.get('price_change_percentage_24h'))} | 7d: {_fmt_pct(md.get('price_change_percentage_7d'))}",
            f"Circulating supply: {float(md['circulating_supply']):,.0f}" if md.get("circulating_supply") else "Circulating supply: n/a",
            f"Total supply: {float(md['total_supply']):,.0f}" if md.get("total_supply") else "Total supply: n/a",
            f"Max supply: {float(md['max_supply']):,.0f}" if md.get("max_supply") else "Max supply: none/infinite",
        ]
        homepage = ((data.get("links") or {}).get("homepage") or [None])[0]
        if homepage:
            lines.append(f"Homepage: {homepage}")
        return _join(lines)
    except Exception as e:
        return _err("coin_info", e)


@mcp.tool()
async def coin_ath(coin_id: str) -> str:
    """All-time high for one coin by CoinGecko id: ATH price, date, and how far the current price is below it."""
    try:
        cid = coin_id.strip().lower()
        data = await _cg_coin(cid)
        if not isinstance(data, dict) or not data.get("id"):
            return f"Error: coin '{coin_id}' not found on CoinGecko."
        md = data.get("market_data") or {}
        ath = (md.get("ath") or {}).get("usd")
        ath_date = (md.get("ath_date") or {}).get("usd")
        ath_change = (md.get("ath_change_percentage") or {}).get("usd")
        return _join(
            [
                f"{data.get('name')} ({str(data.get('symbol')).upper()}) all-time high (CoinGecko)",
                f"ATH: {_fmt_usd(ath)}",
                f"ATH date: {str(ath_date)[:10] if ath_date else 'n/a'}",
                f"Current price: {_fmt_usd((md.get('current_price') or {}).get('usd'))}",
                f"From ATH: {_fmt_pct(ath_change)}" if ath_change is not None else "From ATH: n/a",
            ]
        )
    except Exception as e:
        return _err("coin_ath", e)


@mcp.tool()
async def coin_change(coin_id: str) -> str:
    """Price changes for one coin by CoinGecko id over 24h / 7d / 30d, with current price and rank."""
    try:
        cid = coin_id.strip().lower()
        data = await _get_cached(
            f"{COINGECKO}/coins/markets",
            ttl=60.0,
            params={"vs_currency": "usd", "ids": cid, "price_change_percentage": "24h,7d,30d"},
        )
        rows = data if isinstance(data, list) else []
        if not rows:
            return f"Error: coin '{coin_id}' not found on CoinGecko."
        c = rows[0]
        return _join(
            [
                f"{c.get('name')} ({str(c.get('symbol')).upper()}) price changes (CoinGecko)",
                f"Price: {_fmt_usd(c.get('current_price'))} (rank #{c.get('market_cap_rank')})",
                f"24h: {_fmt_pct(c.get('price_change_percentage_24h_in_currency'))}",
                f"7d: {_fmt_pct(c.get('price_change_percentage_7d_in_currency'))}",
                f"30d: {_fmt_pct(c.get('price_change_percentage_30d_in_currency'))}",
            ]
        )
    except Exception as e:
        return _err("coin_change", e)


@mcp.tool()
async def defi_global() -> str:
    """Global DeFi market stats: DeFi market cap, DeFi/ETH ratio, 24h DeFi trading volume, DeFi dominance, top DeFi coin. Source: CoinGecko."""
    try:
        data = await _get_cached(f"{COINGECKO}/global/decentralized_finance_defi", ttl=60.0)
        d = (data or {}).get("data") or {}
        return _join(
            [
                "Global DeFi market (CoinGecko)",
                f"DeFi market cap: {_fmt_big(d.get('defi_market_cap'))}",
                f"ETH market cap: {_fmt_big(d.get('eth_market_cap'))}",
                f"DeFi / ETH ratio: {float(d['defi_to_eth_ratio']):.1f}%" if d.get("defi_to_eth_ratio") else "DeFi / ETH ratio: n/a",
                f"DeFi trading volume 24h: {_fmt_big(d.get('trading_volume_24h'))}",
                f"DeFi dominance: {float(d['defi_dominance']):.2f}%" if d.get("defi_dominance") else "DeFi dominance: n/a",
                f"Top DeFi coin: {d.get('top_coin_name')} ({float(d['top_coin_defi_dominance']):.1f}% of DeFi)" if d.get("top_coin_name") else "Top DeFi coin: n/a",
            ]
        )
    except Exception as e:
        return _err("defi_global", e)


@mcp.tool()
async def btc_dominance() -> str:
    """Market-cap dominance table: BTC, ETH and the next-largest assets' share of total crypto market cap. Source: CoinGecko."""
    try:
        data = await _get_cached(f"{COINGECKO}/global", ttl=60.0)
        pct = ((data or {}).get("data") or {}).get("market_cap_percentage") or {}
        if not pct:
            return "Error: no dominance data returned by CoinGecko."
        top = sorted(pct.items(), key=lambda kv: float(kv[1] or 0), reverse=True)[:8]
        lines = ["Market cap dominance (CoinGecko)"]
        lines += [f"{k.upper()}: {float(v):.2f}%" for k, v in top]
        return _join(lines)
    except Exception as e:
        return _err("btc_dominance", e)


@mcp.tool()
async def exchange_rates() -> str:
    """Exchange rates denominated in BTC (1 BTC = how much of each): major fiats and ETH. Source: CoinGecko."""
    try:
        data = await _get_cached(f"{COINGECKO}/exchange_rates", ttl=60.0)
        rates = (data or {}).get("rates") or {}
        wanted = ["usd", "eur", "gbp", "jpy", "aud", "cad", "chf", "cny", "eth"]
        lines = ["Exchange rates per 1 BTC (CoinGecko)"]
        for code in wanted:
            r = rates.get(code)
            if r:
                lines.append(f"{r.get('name')} ({r.get('unit')}): {float(r.get('value')):,.4f}")
        return _join(lines)
    except Exception as e:
        return _err("exchange_rates", e)


# ===== ENS (ENSIdeas / ensdata.net) =====


@mcp.tool()
async def ens_avatar(name: str) -> str:
    """Avatar URL set on an ENS name, plus its resolved address. Source: ENSIdeas."""
    try:
        data = await _get_json(f"{ENSIDEAS}/ens/resolve/{name.strip()}")
        if not isinstance(data, dict) or not data.get("address"):
            return f"Error: ENS name '{name}' not found."
        return _join(
            [
                f"ENS avatar for {name}",
                f"Address: {data.get('address')}",
                f"Avatar: {data.get('avatar') or 'none set'}",
            ]
        )
    except Exception as e:
        return _err("ens_avatar", e)


@mcp.tool()
async def ens_profile(name: str) -> str:
    """Public profile summary for an ENS name: address, avatar, description, website and socials. Source: ensdata.net."""
    try:
        data = await _get_json(f"{ENSDATA}/{name.strip()}")
        if not isinstance(data, dict) or not data.get("address"):
            return f"Error: ENS name '{name}' not found."
        lines = [
            f"ENS profile: {name}",
            f"Address: {data.get('address')}",
            f"Avatar: {data.get('avatar')}",
            f"Description: {data.get('description')}",
            f"URL: {data.get('url')}",
        ]
        for key in ("twitter", "github", "discord", "telegram"):
            if data.get(key):
                lines.append(f"{key.capitalize()}: {data[key]}")
        return _join(lines)
    except Exception as e:
        return _err("ens_profile", e)


@mcp.tool()
async def ens_records(name: str) -> str:
    """Text records set on an ENS name: description, URL, socials, content hash. Source: ensdata.net."""
    try:
        data = await _get_json(f"{ENSDATA}/{name.strip()}")
        if not isinstance(data, dict) or not data.get("address"):
            return f"Error: ENS name '{name}' not found."
        lines = [f"ENS text records: {name}", f"Address: {data.get('address')}"]
        found = False
        for key, label in (
            ("description", "Description"),
            ("url", "URL"),
            ("twitter", "Twitter"),
            ("github", "GitHub"),
            ("discord", "Discord"),
            ("telegram", "Telegram"),
            ("contentHash", "Content hash"),
            ("header", "Header"),
            ("pgp", "PGP"),
        ):
            if data.get(key):
                lines.append(f"{label}: {data[key]}")
                found = True
        if not found:
            lines.append("No public text records set.")
        return _join(lines)
    except Exception as e:
        return _err("ens_records", e)


@mcp.tool()
async def ens_content_hash(name: str) -> str:
    """Content hash (decentralized website pointer) and avatar set on an ENS name. Source: ensdata.net."""
    try:
        data = await _get_json(f"{ENSDATA}/{name.strip()}")
        if not isinstance(data, dict) or not data.get("address"):
            return f"Error: ENS name '{name}' not found."
        return _join(
            [
                f"ENS content hash: {name}",
                f"Address: {data.get('address')}",
                f"Content hash: {data.get('contentHash') or 'none set'}",
                f"Avatar: {data.get('avatar')}",
            ]
        )
    except Exception as e:
        return _err("ens_content_hash", e)


# ===== wallet & address (Base RPC) =====


async def _wallet_token_line(address: str, token: str, label: str) -> str:
    raw = await _erc20_uint(token, SEL_BALANCE_OF, _addr32(address))
    decimals = await _erc20_decimals(token)
    return f"{label}: {raw / (10 ** decimals):,.6f}"


@mcp.tool()
async def wallet_eth_value(address: str) -> str:
    """USD value of an address's ETH balance on Base, at the live ETH price."""
    try:
        if not _is_address(address):
            return "Error: wallet_eth_value needs an address (0x, 42 chars)."
        addr = address.strip()
        wei = int(await _rpc("eth_getBalance", [addr, "latest"]), 16)
        eth = wei / 1e18
        lines = [f"Wallet {addr} (Base)", f"ETH: {eth:.6f}"]
        price = await _eth_price_usd()
        if price:
            lines.append(f"Value: ${eth * price:,.2f} (ETH at {_fmt_usd(price)})")
        return _join(lines)
    except Exception as e:
        return _err("wallet_eth_value", e)


@mcp.tool()
async def wallet_usdc_balance(address: str) -> str:
    """USDC balance of an address on Base, read on-chain."""
    try:
        if not _is_address(address):
            return "Error: wallet_usdc_balance needs an address (0x, 42 chars)."
        addr = address.strip()
        return _join([f"Wallet {addr} (Base)", await _wallet_token_line(addr, USDC, "USDC")])
    except Exception as e:
        return _err("wallet_usdc_balance", e)


@mcp.tool()
async def wallet_weth_balance(address: str) -> str:
    """WETH (wrapped ETH) balance of an address on Base, read on-chain."""
    try:
        if not _is_address(address):
            return "Error: wallet_weth_balance needs an address (0x, 42 chars)."
        addr = address.strip()
        return _join([f"Wallet {addr} (Base)", await _wallet_token_line(addr, WETH, "WETH")])
    except Exception as e:
        return _err("wallet_weth_balance", e)


@mcp.tool()
async def wallet_dai_balance(address: str) -> str:
    """DAI balance of an address on Base, read on-chain."""
    try:
        if not _is_address(address):
            return "Error: wallet_dai_balance needs an address (0x, 42 chars)."
        addr = address.strip()
        return _join([f"Wallet {addr} (Base)", await _wallet_token_line(addr, SYMBOL_ADDRESSES["DAI"], "DAI")])
    except Exception as e:
        return _err("wallet_dai_balance", e)


@mcp.tool()
async def wallet_aero_balance(address: str) -> str:
    """AERO (Aerodrome) balance of an address on Base, read on-chain."""
    try:
        if not _is_address(address):
            return "Error: wallet_aero_balance needs an address (0x, 42 chars)."
        addr = address.strip()
        return _join([f"Wallet {addr} (Base)", await _wallet_token_line(addr, SYMBOL_ADDRESSES["AERO"], "AERO")])
    except Exception as e:
        return _err("wallet_aero_balance", e)


@mcp.tool()
async def wallet_cbeth_balance(address: str) -> str:
    """cbETH (Coinbase Wrapped Staked ETH) balance of an address on Base, read on-chain."""
    try:
        if not _is_address(address):
            return "Error: wallet_cbeth_balance needs an address (0x, 42 chars)."
        addr = address.strip()
        return _join([f"Wallet {addr} (Base)", await _wallet_token_line(addr, SYMBOL_ADDRESSES["CBETH"], "cbETH")])
    except Exception as e:
        return _err("wallet_cbeth_balance", e)


@mcp.tool()
async def wallet_is_contract(address: str) -> str:
    """Whether an address on Base is a smart contract (has deployed bytecode), and its code size."""
    try:
        if not _is_address(address):
            return "Error: wallet_is_contract needs an address (0x, 42 chars)."
        addr = address.strip()
        code = await _rpc("eth_getCode", [addr, "latest"])
        size = (len(code) - 2) // 2 if code and code != "0x" else 0
        return _join(
            [
                f"Address {addr} (Base)",
                f"Is contract: {'yes' if size > 0 else 'no (externally owned account)'}",
                f"Code size: {size:,} bytes",
            ]
        )
    except Exception as e:
        return _err("wallet_is_contract", e)


@mcp.tool()
async def wallet_known_holdings(address: str) -> str:
    """Which of the well-known Base tokens (WETH, USDC, AERO, cbETH, DAI, USDbC, HIGHER) an address holds, with amounts. Read on-chain."""
    try:
        if not _is_address(address):
            return "Error: wallet_known_holdings needs an address (0x, 42 chars)."
        addr = address.strip()
        seen = {}
        for symbol, token in SYMBOL_ADDRESSES.items():
            # ETH and WETH share one contract; label it WETH (the ERC20).
            if token not in seen or symbol == "WETH":
                seen[token] = "WETH" if token == WETH else symbol
        lines = [f"Known-token holdings of {addr} (Base)"]
        held = 0
        for token, symbol in seen.items():
            raw = await _erc20_uint(token, SEL_BALANCE_OF, _addr32(addr))
            if raw > 0:
                decimals = await _erc20_decimals(token)
                lines.append(f"{symbol}: {raw / (10 ** decimals):,.6f}")
                held += 1
        lines.append(f"Tokens held: {held} of {len(seen)} checked")
        if held == 0:
            lines.append("No balance in any of the checked tokens.")
        return _join(lines)
    except Exception as e:
        return _err("wallet_known_holdings", e)


# ===== oracles (Chainlink on Base, via RPC) =====


async def _chainlink_read(key: str) -> list:
    feed = CHAINLINK_FEEDS[key]
    addr = feed["address"]
    decimals = _decode_uint(await _eth_call(addr, "0x" + SEL_DECIMALS))
    desc = None
    try:
        desc = await _erc20_str(addr, SEL_DESCRIPTION)
    except Exception:
        pass
    raw = await _eth_call(addr, "0x" + SEL_LATEST_ROUND)
    answer = _word_signed(raw, 1)
    updated = _word(raw, 3)
    price = answer / (10 ** decimals)
    age = max(0, int(time.time()) - updated) if updated else None
    lines = [
        f"Chainlink {desc or feed['pair']} on Base",
        f"Price: ${price:,.4f}" if price < 100 else f"Price: {_fmt_usd(price)}",
        f"Feed: {addr}",
        f"Last update: {_fmt_ts(updated)}" + (f" ({age}s ago)" if age is not None else ""),
    ]
    return lines


@mcp.tool()
async def chainlink_eth() -> str:
    """Chainlink ETH/USD price on Base, read on-chain from the aggregator, with last-update time."""
    try:
        return _join(await _chainlink_read("eth"))
    except Exception as e:
        return _err("chainlink_eth", e)


@mcp.tool()
async def chainlink_btc() -> str:
    """Chainlink BTC/USD price on Base, read on-chain from the aggregator, with last-update time."""
    try:
        return _join(await _chainlink_read("btc"))
    except Exception as e:
        return _err("chainlink_btc", e)


@mcp.tool()
async def chainlink_usdc() -> str:
    """Chainlink USDC/USD price on Base, read on-chain from the aggregator, with last-update time."""
    try:
        return _join(await _chainlink_read("usdc"))
    except Exception as e:
        return _err("chainlink_usdc", e)


@mcp.tool()
async def chainlink_feeds() -> str:
    """All three Chainlink price feeds the service tracks on Base (ETH/USD, BTC/USD, USDC/USD) in one view, with update ages."""
    try:
        lines = ["Chainlink feeds on Base"]
        for key in ("eth", "btc", "usdc"):
            sub = await _chainlink_read(key)
            lines.append(f"{sub[0]}: {sub[1]} — {sub[3]}")
        return _join(lines)
    except Exception as e:
        return _err("chainlink_feeds", e)


# ===== utilities & converters =====


@mcp.tool()
async def convert_eth_usd(amount_eth: float) -> str:
    """Convert an ETH amount to USD at the live ETH price (DexScreener WETH pair)."""
    try:
        amt = float(amount_eth)
        price = await _eth_price_usd()
        if not price:
            return "Error: could not fetch the live ETH price."
        return _join(
            [
                f"{amt} ETH = ${amt * price:,.2f}",
                f"ETH price: {_fmt_usd(price)}",
            ]
        )
    except Exception as e:
        return _err("convert_eth_usd", e)


@mcp.tool()
async def convert_usd_eth(amount_usd: float) -> str:
    """Convert a USD amount to ETH at the live ETH price (DexScreener WETH pair)."""
    try:
        amt = float(amount_usd)
        price = await _eth_price_usd()
        if not price:
            return "Error: could not fetch the live ETH price."
        return _join(
            [
                f"${amt:,.2f} = {amt / price:.6f} ETH",
                f"ETH price: {_fmt_usd(price)}",
            ]
        )
    except Exception as e:
        return _err("convert_usd_eth", e)


@mcp.tool()
async def convert_wei(amount_wei: str) -> str:
    """Convert a wei amount to ether and gwei (1 ETH = 10^18 wei = 10^9 gwei)."""
    try:
        wei = int(str(amount_wei).strip())
        if wei < 0:
            return "Error: amount must be >= 0."
        return _join(
            [
                f"{wei} wei",
                f"= {wei / 1e18:.6f} ETH",
                f"= {wei / 1e9:.4f} gwei",
            ]
        )
    except Exception as e:
        return _err("convert_wei", e)


@mcp.tool()
async def convert_gwei(amount_gwei: float) -> str:
    """Convert a gwei amount to wei and ETH, plus its USD value at the live ETH price."""
    try:
        g = float(amount_gwei)
        if g < 0:
            return "Error: amount must be >= 0."
        eth = g / 1e9
        lines = [
            f"{g} gwei",
            f"= {int(g * 1e9)} wei",
            f"= {eth:.9f} ETH",
        ]
        price = await _eth_price_usd()
        if price:
            lines.append(f"Value: ${eth * price:.6f} (ETH at {_fmt_usd(price)})")
        return _join(lines)
    except Exception as e:
        return _err("convert_gwei", e)


@mcp.tool()
async def address_info(address: str) -> str:
    """Quick facts about any Base address: format validity, contract or account, nonce, ETH balance."""
    try:
        if not _is_address(address):
            return f"Error: '{address}' is not a valid address format (0x + 40 hex chars)."
        addr = address.strip()
        code = await _rpc("eth_getCode", [addr, "latest"])
        is_contract = bool(code and code != "0x")
        nonce = int(await _rpc("eth_getTransactionCount", [addr, "latest"]), 16)
        wei = int(await _rpc("eth_getBalance", [addr, "latest"]), 16)
        return _join(
            [
                f"Address {addr} (Base)",
                "Format: valid",
                f"Type: {'smart contract' if is_contract else 'externally owned account'}",
                f"Nonce (txs sent): {nonce}",
                f"ETH balance: {wei / 1e18:.6f}",
            ]
        )
    except Exception as e:
        return _err("address_info", e)


@mcp.tool()
async def block_eta(target_block: int) -> str:
    """Estimate when a future Base block will arrive, from the measured average block time; also reports how far away it is."""
    try:
        target = int(target_block)
        latest = await _rpc("eth_getBlockByNumber", ["latest", False])
        latest_num = int(latest["number"], 16)
        if target <= latest_num:
            return _join(
                [
                    f"Block {target} is not in the future — the chain is already at {latest_num}.",
                ]
            )
        older = await _rpc("eth_getBlockByNumber", [hex(latest_num - 100), False])
        avg = (int(latest["timestamp"], 16) - int(older["timestamp"], 16)) / 100.0
        remaining = target - latest_num
        eta_secs = remaining * avg
        eta_ts = int(time.time() + eta_secs)
        return _join(
            [
                f"ETA for Base block {target}",
                f"Current block: {latest_num} ({remaining:,} to go)",
                f"Average block time (last 100): {avg:.2f}s",
                f"Estimated wait: {eta_secs / 60:.1f} minutes",
                f"Estimated arrival: {_fmt_ts(eta_ts)}",
            ]
        )
    except Exception as e:
        return _err("block_eta", e)


@mcp.tool()
async def base_block_time() -> str:
    """Measured average Base block time over the last 100 blocks, and the implied blocks per day."""
    try:
        latest = await _rpc("eth_getBlockByNumber", ["latest", False])
        latest_num = int(latest["number"], 16)
        older = await _rpc("eth_getBlockByNumber", [hex(latest_num - 100), False])
        avg = (int(latest["timestamp"], 16) - int(older["timestamp"], 16)) / 100.0
        return _join(
            [
                "Base block time",
                f"Average over last 100 blocks: {avg:.2f}s",
                f"Implied blocks per day: {86400 / avg:,.0f}" if avg > 0 else "Implied blocks per day: n/a",
                f"Latest block: {latest_num}",
            ]
        )
    except Exception as e:
        return _err("base_block_time", e)


def main():
    if "--http" in sys.argv:
        mcp.run(transport="streamable-http")
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()

