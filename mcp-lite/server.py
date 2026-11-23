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
import statistics
import sys
import time
from datetime import datetime, timezone

import httpx
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse

# ===== config =====

VERSION = "1.1.2"

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
SOLANA_CATALOG_URL = "https://manteclaw-x402-solana.fly.dev/catalog"

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
    f"{CATALOG_URL} ($0.01/call, USDC on Base via x402); Solana-native "
    f"paid endpoints at {SOLANA_CATALOG_URL} ($0.01/call, USDC on Solana via x402)."
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


def _coarse(value, sig: int = 2) -> str:
    """A number rounded to `sig` significant figures, plainly formatted.

    The lite tier coarsens every figure a paid endpoint sells exactly:
    enough to be a useful preview, never a substitute for the paid value.
    """
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if v == 0:
        return "0"
    r = float(f"{v:.{sig}g}")
    if abs(r) >= 100:
        return f"{r:,.0f}"
    if abs(r) >= 1:
        return f"{r:g}"
    return f"{r:.12f}".rstrip("0").rstrip(".")


def _coarse_usd(value, sig: int = 2) -> str:
    """Coarse USD: $5.1B / $970M / $3,400 / $0.42 (2 significant figures)."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if abs(v) >= 1e6:
        for suffix, scale in (("T", 1e12), ("B", 1e9), ("M", 1e6)):
            if abs(v) >= scale:
                return f"${_coarse(v / scale, sig)}{suffix}"
    return f"${_coarse(v, sig)}"


def _whole_pct(value) -> str:
    """A percentage rounded to the nearest whole point, signed."""
    try:
        return f"{round(float(value)):+d}%"
    except (TypeError, ValueError):
        return "n/a"


def _mask_addr(addr) -> str:
    a = str(addr)
    return f"{a[:6]}…{a[-4:]}" if len(a) > 12 else a


def _band(value, cuts, labels) -> str:
    """Bucket a number: labels[i] applies below cuts[i]; last label above all."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return labels[-1]
    for cut, label in zip(cuts, labels):
        if v < cut:
            return label
    return labels[-1]


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


@mcp.custom_route("/", methods=["GET"])
async def landing(request: Request) -> HTMLResponse:
    return HTMLResponse(
        """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>Manteclaw MCP Lite</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>body{background:#0f0f1a;color:#e8e8f0;font-family:system-ui,sans-serif;margin:0;padding:2rem 1rem}main{max-width:640px;margin:0 auto}h1{color:#00d4ff}a{color:#00d4ff}code{background:#1a1a2e;padding:.15rem .4rem;border-radius:4px}li{margin:.35rem 0}</style>
</head><body><main>
<h1>Manteclaw MCP Lite</h1>
<p>A free MCP server with <strong>100 tools</strong> for Base L2 and crypto market data:
gas, blocks, token prices, DEX pairs, DeFi TVL &amp; yields, stablecoins, ENS,
wallet snapshots, Chainlink feeds and more.</p>
<p><strong>MCP endpoint:</strong> <code>https://manteclaw-mcp-lite.fly.dev/mcp</code> (streamable HTTP)</p>
<p>Add it to any MCP client with that URL — no API key, no payment.</p>
<ul>
<li><a href="https://manteclaw-x402.fly.dev/catalog">Full paid API catalog</a> — 200 pay-per-call endpoints (the deep data this lite edition samples)</li>
<li><a href="https://glama.ai/mcp/connectors/dev.fly.manteclaw-mcp-lite/manteclaw-mcp-lite">Glama listing</a></li>
<li><a href="https://github.com/manteclaw/manteclaw-mcp-lite">GitHub repo</a></li>
<li><a href="https://payhip.com/Manteclaw">Manteclaw store</a></li>
</ul>
<p>Health check: <a href="/health">/health</a></p>
</main></body></html>"""
    )


@mcp.custom_route("/.well-known/glama.json", methods=["GET"])
async def glama_claim_root(request: Request) -> JSONResponse:
    return JSONResponse(_GLAMA_CLAIM)


@mcp.custom_route("/mcp/.well-known/glama.json", methods=["GET"])
async def glama_claim_endpoint(request: Request) -> JSONResponse:
    return JSONResponse(_GLAMA_CLAIM)



# ===== original tools (unchanged) =====


@mcp.tool()
async def base_gas() -> str:
    """Base fee climate right now: whether the current base fee is LOW, TYPICAL or HIGH versus the last 20 blocks, and whether fees are rising, falling or flat. Exact fee tiers are a paid lookup; this is the lite preview."""
    try:
        data = await _rpc("eth_feeHistory", [hex(20), "latest", [50]])
        fees = [int(x, 16) / 1e9 for x in (data.get("baseFeePerGas") or [])[:-1]]
        if not fees:
            return "Error: no fee history returned by the Base RPC."
        current = fees[-1]
        ordered = sorted(fees)
        median = ordered[len(ordered) // 2]
        if current < 0.7 * median:
            band = "LOW"
        elif current > 1.5 * median:
            band = "HIGH"
        else:
            band = "TYPICAL"
        half = len(fees) // 2
        early = sum(fees[:half]) / half
        late = sum(fees[half:]) / (len(fees) - half)
        if late > early * 1.1:
            trend = "rising"
        elif late < early * 0.9:
            trend = "falling"
        else:
            trend = "flat"
        block = await _rpc("eth_blockNumber", [])
        return _join(
            [
                "Base fee climate (last 20 blocks)",
                f"Current base fee: {band} for Base right now",
                f"Trend: {trend}",
                f"Latest block: {int(block, 16)}",
            ]
        )
    except Exception as e:
        return _err("base_gas", e)


@mcp.tool()
async def base_block() -> str:
    """Latest Base block preview: block number, how long ago it landed, and a coarse activity level. Full block detail is a paid lookup."""
    try:
        block = await _rpc("eth_getBlockByNumber", ["latest", False])
        ts = int(block["timestamp"], 16)
        age = max(0, int(time.time()) - ts)
        txs = len(block.get("transactions", []))
        activity = _band(txs, [100, 250], ["quiet", "typical", "busy"])
        lines = [
            "Latest Base block (preview)",
            f"Number: {int(block['number'], 16)}",
            f"Landed: {age}s ago",
            f"Activity: {activity}",
            FUNNEL,
        ]
        return "\n".join(lines)
    except Exception as e:
        return _err("base_block", e)


@mcp.tool()
async def token_price(token: str) -> str:
    """Coarse spot price preview for a token (symbol like "AERO" or Base contract address): price rounded to 2 significant figures, 24h direction, and a liquidity band. Exact prices are a paid lookup. Source: DexScreener best Base pair."""
    try:
        pairs = await _pairs_for_query(token)
        best = _best_pair(pairs)
        if not best:
            return f"Error: no Base DEX pair found for '{token}'."
        change = (best.get("priceChange") or {}).get("h24")
        if change is None:
            direction = "24h direction: n/a"
        elif float(change) > 0.5:
            direction = "24h direction: up"
        elif float(change) < -0.5:
            direction = "24h direction: down"
        else:
            direction = "24h direction: roughly flat"
        liq = float((best.get("liquidity") or {}).get("usd") or 0)
        depth = _band(liq, [10_000, 250_000], ["thin", "moderate", "deep"])
        lines = [
            f"{(best.get('baseToken') or {}).get('name')} ({(best.get('baseToken') or {}).get('symbol')}) on Base — lite preview",
            f"Price: ≈{_coarse_usd(best.get('priceUsd'))}",
            direction,
            f"Pair: {best.get('baseToken', {}).get('symbol')}/{(best.get('quoteToken') or {}).get('symbol')} on {best.get('dexId')}",
            f"Liquidity: {depth}",
            FUNNEL,
        ]
        return "\n".join(lines)
    except Exception as e:
        return _err("token_price", e)


@mcp.tool()
async def token_info(contract_address: str) -> str:
    """Lite metadata preview for a Base ERC20: name, symbol, decimals, and total supply rounded to 2 significant figures. Exact supply and market data are paid lookups."""
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
            f"Token {token} (Base) — lite preview",
            f"Name: {name}",
            f"Symbol: {symbol}",
            f"Decimals: {decimals}",
        ]
        if supply is not None and decimals is not None:
            lines.append(f"Total supply: ≈{_coarse(supply / (10 ** decimals))} {symbol or ''}".rstrip())
        lines.append(FUNNEL)
        return "\n".join(lines)
    except Exception as e:
        return _err("token_info", e)


@mcp.tool()
async def ens_resolve(name_or_address: str) -> str:
    """Check whether an ENS name resolves (or an address has a primary ENS name). Lite shows a masked preview only — full addresses and names are paid lookups."""
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
                    masked = (str(name)[:3] + "…") if len(str(name)) > 3 else str(name)
                    return "\n".join(
                        [
                            f"Address: {value}",
                            f"Has a primary ENS name: yes ({masked})",
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
                f"Resolves: yes → {_mask_addr(data.get('address'))} (masked preview)",
                FUNNEL,
            ]
        )
    except Exception as e:
        return _err("ens_resolve", e)


@mcp.tool()
async def wallet_snapshot(address: str) -> str:
    """Coarse balance bands for an address on Base (ETH and USDC ranges, not exact figures — exact balances are paid lookups)."""
    try:
        if not _is_address(address):
            return "Error: wallet_snapshot needs an address (0x, 42 chars)."
        addr = address.strip()
        wei = int(await _rpc("eth_getBalance", [addr, "latest"]), 16)
        eth = wei / 1e18
        usdc = await _erc20_uint(USDC, SEL_BALANCE_OF, _addr32(addr)) / 1e6
        eth_band = _band(eth, [0.000001, 0.01, 0.1, 1, 10], ["0", "under 0.01", "0.01–0.1", "0.1–1", "1–10", "over 10"])
        usdc_band = _band(usdc, [0.01, 10, 100, 1000, 10000], ["0", "under 10", "10–100", "100–1,000", "1,000–10,000", "over 10,000"])
        return "\n".join(
            [
                f"Wallet {addr} (Base) — lite bands",
                f"ETH: {eth_band}",
                f"USDC: {usdc_band}",
                FUNNEL,
            ]
        )
    except Exception as e:
        return _err("wallet_snapshot", e)


@mcp.tool()
async def dex_pair(token: str) -> str:
    """Top Base DEX pair for a token (symbol or contract address) — lite preview: pair, coarse price, liquidity band. Exact pair data is a paid lookup. Source: DexScreener."""
    try:
        pairs = await _pairs_for_query(token)
        best = _best_pair(pairs)
        if not best:
            return f"Error: no Base DEX pair found for '{token}'."
        liq = float((best.get("liquidity") or {}).get("usd") or 0)
        depth = _band(liq, [10_000, 250_000], ["thin (under $10K)", "moderate ($10K–$250K)", "deep (over $250K)"])
        return "\n".join(
            [
                f"Top Base pair for {(best.get('baseToken') or {}).get('symbol')} — lite preview",
                f"Pair: {(best.get('baseToken') or {}).get('symbol')}/{(best.get('quoteToken') or {}).get('symbol')} on {best.get('dexId')}",
                f"Pair address: {best.get('pairAddress')}",
                f"Price: ≈{_coarse_usd(best.get('priceUsd'))}",
                f"Liquidity: {depth}",
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
            "A second paid service covers Solana natively (199 endpoints: Solana chain, SPL tokens, wallets, DEX, DeFi, Pyth oracles, memecoins, NFTs), paid in USDC on Solana:",
            f"Solana catalog: {SOLANA_CATALOG_URL}",
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
    """Lite preview of one Base block: when it landed and a coarse transaction-activity level. Full block detail is a paid lookup."""
    try:
        if number < 0:
            return "Error: block number must be >= 0."
        block = await _rpc("eth_getBlockByNumber", [hex(number), False])
        if not block:
            return f"Error: block {number} not found on Base."
        txs = len(block.get("transactions", []))
        activity = _band(txs, [100, 250], ["quiet", "typical", "busy"])
        return _join(
            [
                f"Base block {int(block['number'], 16)} (preview)",
                f"Timestamp: {_fmt_ts(int(block['timestamp'], 16))}",
                f"Activity: {activity}",
            ]
        )
    except Exception as e:
        return _err("base_block_by_number", e)


@mcp.tool()
async def base_recent_blocks(count: int = 5) -> str:
    """The most recent Base blocks (up to 3 in lite): number and a coarse activity level for each. Full per-block detail is a paid lookup."""
    try:
        n = max(1, min(int(count), 3))
        latest = await _rpc("eth_getBlockByNumber", ["latest", False])
        latest_num = int(latest["number"], 16)
        lines = [f"Last {n} Base blocks (latest: {latest_num}) — lite preview"]
        for num in range(latest_num, latest_num - n, -1):
            b = await _rpc("eth_getBlockByNumber", [hex(num), False])
            activity = _band(len(b.get("transactions", [])), [100, 250], ["quiet", "typical", "busy"])
            lines.append(f"#{num}: {activity}")
        return _join(lines)
    except Exception as e:
        return _err("base_recent_blocks", e)


@mcp.tool()
async def base_tx(tx_hash: str) -> str:
    """Lite preview of one Base transaction: from, to, plain transfer vs contract call, a value band, and its block. Full transaction detail is a paid lookup."""
    try:
        if not _is_txhash(tx_hash):
            return "Error: base_tx needs a transaction hash (0x, 66 chars)."
        tx = await _rpc("eth_getTransactionByHash", [tx_hash.strip()])
        if not tx:
            return f"Error: transaction {tx_hash} not found on Base."
        kind = "contract interaction" if len(tx.get("input", "0x")) > 2 else "plain ETH transfer"
        value_eth = int(tx.get("value", "0x0"), 16) / 1e18
        vband = _band(value_eth, [0.000001, 0.01, 1], ["0 ETH", "under 0.01 ETH", "0.01–1 ETH", "over 1 ETH"])
        return _join(
            [
                f"Transaction {tx['hash']} (preview)",
                f"From: {tx.get('from')}",
                f"To: {tx.get('to') if tx.get('to') else 'contract creation'}",
                f"Type: {kind}",
                f"Value: {vband}",
                f"Block: {int(tx['blockNumber'], 16) if tx.get('blockNumber') else 'pending'}",
            ]
        )
    except Exception as e:
        return _err("base_tx", e)


@mcp.tool()
async def base_tx_receipt(tx_hash: str) -> str:
    """Lite receipt preview for one Base transaction: success/failure, its block, and a confirmations band. Full receipt detail (gas, fee, logs) is a paid lookup."""
    try:
        if not _is_txhash(tx_hash):
            return "Error: base_tx_receipt needs a transaction hash (0x, 66 chars)."
        r = await _rpc("eth_getTransactionReceipt", [tx_hash.strip()])
        if not r:
            return f"Error: no receipt for {tx_hash} (unknown or still pending)."
        block_num = int(r.get("blockNumber", "0x0"), 16)
        latest = int(await _rpc("eth_blockNumber", []), 16)
        conf = max(1, latest - block_num + 1)
        conf_band = _band(conf, [6, 26, 101], ["1–5 confirmations", "6–25 confirmations", "26–100 confirmations", "100+ confirmations"])
        lines = [
            f"Receipt {r.get('transactionHash')} (preview)",
            f"Status: {'success' if int(r.get('status', '0x0'), 16) == 1 else 'failed'}",
            f"Block: {block_num}",
            f"Confirmations: {conf_band}",
        ]
        if r.get("contractAddress"):
            lines.append(f"Contract created: {r['contractAddress']}")
        return _join(lines)
    except Exception as e:
        return _err("base_tx_receipt", e)


@mcp.tool()
async def base_nonce(address: str) -> str:
    """Coarse activity band for an address on Base, from how many transactions it has sent. The exact nonce is a paid lookup."""
    try:
        if not _is_address(address):
            return "Error: base_nonce needs an address (0x, 42 chars)."
        addr = address.strip()
        confirmed = int(await _rpc("eth_getTransactionCount", [addr, "latest"]), 16)
        band = _band(
            confirmed,
            [1, 11, 101, 1001],
            ["no transactions sent yet", "light (1–10 sent)", "moderate (11–100 sent)", "active (101–1,000 sent)", "very active (1,000+ sent)"],
        )
        return _join(
            [
                f"Address {addr} (Base) — lite preview",
                f"Sending activity: {band}",
            ]
        )
    except Exception as e:
        return _err("base_nonce", e)


@mcp.tool()
async def base_pending_txs() -> str:
    """Base mempool climate: whether the pending block is quiet, normal or busy right now. The exact pending count is a paid lookup."""
    try:
        block = await _rpc("eth_getBlockByNumber", ["pending", False])
        if not block:
            return "Error: the Base public RPC did not return a pending block."
        load = _band(len(block.get("transactions", [])), [50, 200], ["quiet", "normal", "busy"])
        return _join(
            [
                "Base mempool climate (preview)",
                f"Pending load: {load}",
                f"Pending block number: {int(block['number'], 16) if block.get('number') else 'n/a'}",
            ]
        )
    except Exception as e:
        return _err("base_pending_txs", e)


@mcp.tool()
async def base_client_version() -> str:
    """Client family of the Base public RPC node (e.g. Reth, Geth). The full version string is a paid lookup."""
    try:
        v = str(await _rpc("web3_clientVersion", []))
        family = v.split("/")[0].strip() if "/" in v else v.split("-")[0].strip()
        return _join(["Base RPC client (preview)", f"Client family: {family or 'unknown'}"])
    except Exception as e:
        return _err("base_client_version", e)


@mcp.tool()
async def base_sync_status() -> str:
    """Qualitative Base node health: whether the chain head is fresh, slightly behind, or lagging, from the latest block's age."""
    try:
        block = await _rpc("eth_getBlockByNumber", ["latest", False])
        ts = int(block["timestamp"], 16)
        age = max(0, int(time.time()) - ts)
        if age <= 10:
            status = "UP — chain head is fresh"
        elif age <= 60:
            status = "slightly behind"
        else:
            status = "lagging"
        return _join(
            [
                "Base node health (preview)",
                f"Status: {status}",
                f"Latest block: {int(block['number'], 16)}",
            ]
        )
    except Exception as e:
        return _err("base_sync_status", e)


@mcp.tool()
async def base_fee_history(blocks: int = 10) -> str:
    """Base fee trend over a recent window (up to 20 blocks in lite): a rounded average base fee, a rounded range, and the direction. The per-block fee series is a paid lookup."""
    try:
        n = max(5, min(int(blocks), 20))
        data = await _rpc("eth_feeHistory", [hex(n), "latest", [50]])
        fees = [int(x, 16) / 1e9 for x in (data.get("baseFeePerGas") or [])[:-1]]
        if not fees:
            return "Error: no fee history returned by the Base RPC."
        avg = sum(fees) / len(fees)
        half = len(fees) // 2
        early = sum(fees[:half]) / half
        late = sum(fees[half:]) / (len(fees) - half)
        if late > early * 1.1:
            trend = "rising"
        elif late < early * 0.9:
            trend = "falling"
        else:
            trend = "flat"
        return _join(
            [
                f"Base fee trend — last {len(fees)} blocks (preview)",
                f"Average base fee: ≈{_coarse(avg)} gwei",
                f"Range: ≈{_coarse(min(fees))}–{_coarse(max(fees))} gwei",
                f"Trend: {trend}",
            ]
        )
    except Exception as e:
        return _err("base_fee_history", e)


# ===== gas & fees =====


@mcp.tool()
async def base_gas_price() -> str:
    """Current Base gas price (eth_gasPrice), rounded to 2 significant figures. The exact oracle value is a paid lookup."""
    try:
        wei = int(await _rpc("eth_gasPrice", []), 16)
        return _join(
            [
                "Base gas price (eth_gasPrice) — lite preview",
                f"≈{_coarse(wei / 1e9)} gwei",
            ]
        )
    except Exception as e:
        return _err("base_gas_price", e)


@mcp.tool()
async def base_priority_fee() -> str:
    """Suggested priority fee (tip) for Base right now (eth_maxPriorityFeePerGas), rounded to 2 significant figures. The exact value is a paid lookup."""
    try:
        wei = int(await _rpc("eth_maxPriorityFeePerGas", []), 16)
        return _join(
            [
                "Base priority fee suggestion — lite preview",
                f"≈{_coarse(wei / 1e9)} gwei",
            ]
        )
    except Exception as e:
        return _err("base_priority_fee", e)


@mcp.tool()
async def base_transfer_cost() -> str:
    """Rounded cost of a plain ETH transfer on Base right now (21,000 gas at the current gas price), in ETH and USD. Exact figures are a paid lookup."""
    try:
        wei = int(await _rpc("eth_gasPrice", []), 16)
        cost_eth = 21000 * wei / 1e18
        lines = [
            "Cost of a plain ETH transfer on Base (21,000 gas) — lite preview",
            f"Cost: ≈{_coarse(cost_eth, 3)} ETH",
        ]
        price = await _eth_price_usd()
        if price:
            usd = cost_eth * price
            lines.append(f"Cost in USD: {'under $0.01' if usd < 0.01 else '≈' + _coarse_usd(usd)}")
        return _join(lines)
    except Exception as e:
        return _err("base_transfer_cost", e)


@mcp.tool()
async def gas_cost_estimate(gwei: float, gas_units: int) -> str:
    """Approximate cost of a transaction at a given gas price and gas amount, in ETH and USD at the live ETH price. Lite figures are rounded — exact conversions are a paid lookup."""
    try:
        g = float(gwei)
        units = int(gas_units)
        if g < 0 or units <= 0:
            return "Error: gwei must be >= 0 and gas_units > 0."
        cost_eth = g * units / 1e9
        lines = [
            f"Gas cost at {g} gwei for {units:,} gas (approximate)",
            f"Cost: ≈{_coarse(cost_eth, 3)} ETH",
        ]
        price = await _eth_price_usd()
        if price:
            lines.append(f"Cost in USD: ≈{_coarse_usd(cost_eth * price)}")
        return _join(lines)
    except Exception as e:
        return _err("gas_cost_estimate", e)


@mcp.tool()
async def base_tx_costs() -> str:
    """Rounded costs of common Base operations right now (ETH transfer, ERC20 transfer, approve, typical swap) at the current base fee, in ETH and USD. Exact figures are a paid lookup; gas amounts are typical estimates."""
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
        lines = [f"Typical Base transaction costs at base fee ≈{_coarse(base_fee_gwei)} gwei (lite preview)"]
        for name, gas in ops:
            cost_eth = base_fee_gwei * gas / 1e9
            usd = f", ≈{_coarse_usd(cost_eth * price)}" if price else ""
            lines.append(f"{name} (~{gas:,} gas): ≈{_coarse(cost_eth, 3)} ETH{usd}")
        lines.append("Gas amounts are typical estimates; actual usage varies by contract.")
        return _join(lines)
    except Exception as e:
        return _err("base_tx_costs", e)



# ===== tokens & prices (DexScreener + Base RPC) =====


def _pair_line(p) -> str:
    return (
        f"{(p.get('baseToken') or {}).get('symbol')}/{(p.get('quoteToken') or {}).get('symbol')} "
        f"on {p.get('dexId')}: ≈{_coarse_usd(p.get('priceUsd'))}, "
        f"liquidity ≈{_coarse_usd((p.get('liquidity') or {}).get('usd'))}, "
        f"24h vol ≈{_coarse_usd((p.get('volume') or {}).get('h24'))}"
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
    """Liquidity preview for a token on Base: a total-liquidity band, a pair-count bucket, and which DEX holds the deepest pair. Exact totals are a paid lookup. Source: DexScreener."""
    try:
        pairs = await _pairs_for_query(token)
        if not pairs:
            return f"Error: no Base DEX pairs found for '{token}'."
        total = sum(float((p.get("liquidity") or {}).get("usd") or 0) for p in pairs)
        best = _best_pair(pairs)
        total_band = _band(total, [10_000, 100_000, 1_000_000, 10_000_000], ["under $10K", "$10K–$100K", "$100K–$1M", "$1M–$10M", "over $10M"])
        count_bucket = _band(len(pairs), [2, 6, 21], ["1 pair", "2–5 pairs", "6–20 pairs", "over 20 pairs"])
        return _join(
            [
                f"Liquidity for {token} on Base — lite preview",
                f"Total liquidity: {total_band}",
                f"Pairs: {count_bucket}",
                f"Deepest pair is on: {best.get('dexId')}",
            ]
        )
    except Exception as e:
        return _err("token_liquidity", e)


@mcp.tool()
async def token_volume(token: str) -> str:
    """Rounded 24h trading volume of a token's best Base pair. The multi-window volume breakdown (6h/1h/5m) is a paid lookup. Source: DexScreener."""
    try:
        best = _best_pair(await _pairs_for_query(token))
        if not best:
            return f"Error: no Base DEX pair found for '{token}'."
        vol = best.get("volume") or {}
        sym = (best.get("baseToken") or {}).get("symbol")
        return _join(
            [
                f"Volume for {sym} (best Base pair on {best.get('dexId')}) — lite preview",
                f"24h: ≈{_coarse_usd(vol.get('h24'))}",
            ]
        )
    except Exception as e:
        return _err("token_volume", e)


@mcp.tool()
async def token_price_change(token: str) -> str:
    """24h price change of a token's best Base pair, rounded to the nearest whole percent. The multi-window changes (6h/1h/5m) are a paid lookup. Source: DexScreener."""
    try:
        best = _best_pair(await _pairs_for_query(token))
        if not best:
            return f"Error: no Base DEX pair found for '{token}'."
        ch = best.get("priceChange") or {}
        sym = (best.get("baseToken") or {}).get("symbol")
        return _join(
            [
                f"Price change for {sym} (best Base pair) — lite preview",
                f"24h: {_whole_pct(ch.get('h24'))}",
            ]
        )
    except Exception as e:
        return _err("token_price_change", e)


@mcp.tool()
async def token_txns(token: str) -> str:
    """24h trading tilt for a token's best Base pair: whether flow is buy-heavy, sell-heavy or balanced, plus a bucketed trade count. Exact buy/sell counts per window are a paid lookup. Source: DexScreener."""
    try:
        best = _best_pair(await _pairs_for_query(token))
        if not best:
            return f"Error: no Base DEX pair found for '{token}'."
        txns = best.get("txns") or {}
        sym = (best.get("baseToken") or {}).get("symbol")
        w = txns.get("h24") or {}
        buys = int(w.get("buys", 0) or 0)
        sells = int(w.get("sells", 0) or 0)
        total = buys + sells
        if total == 0:
            tilt = "no trades in the last 24h"
        else:
            ratio = buys / total
            tilt = "buy-heavy" if ratio > 0.6 else "sell-heavy" if ratio < 0.4 else "balanced"
        bucket = _band(total, [1, 51, 501, 5001], ["no trades in 24h", "1–50 trades", "51–500 trades", "501–5,000 trades", "over 5,000 trades"])
        return _join(
            [
                f"Transactions for {sym} (best Base pair on {best.get('dexId')}) — lite preview",
                f"24h flow: {tilt}",
                f"24h trades: {bucket}",
            ]
        )
    except Exception as e:
        return _err("token_txns", e)


@mcp.tool()
async def token_fdv(token: str) -> str:
    """Rounded market cap and FDV of a token from its best Base pair (2 significant figures). Exact valuations are a paid lookup. Source: DexScreener."""
    try:
        best = _best_pair(await _pairs_for_query(token))
        if not best:
            return f"Error: no Base DEX pair found for '{token}'."
        sym = (best.get("baseToken") or {}).get("symbol")
        return _join(
            [
                f"Valuation for {sym} on Base — lite preview",
                f"Market cap: ≈{_coarse_usd(best.get('marketCap'))}",
                f"FDV: ≈{_coarse_usd(best.get('fdv'))}",
            ]
        )
    except Exception as e:
        return _err("token_fdv", e)


@mcp.tool()
async def token_balance(token_address: str, wallet: str) -> str:
    """Rounded ERC20 balance preview of any Base token for any wallet (2 significant figures, read on-chain). Exact balances are a paid lookup."""
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
                f"Balance of {token} for {addr} (Base) — lite preview",
                f"≈{_coarse(raw / (10 ** decimals))} {symbol or ''}".rstrip(),
            ]
        )
    except Exception as e:
        return _err("token_balance", e)


@mcp.tool()
async def token_supply(contract_address: str) -> str:
    """Rounded total supply preview of a Base ERC20 (2 significant figures, read on-chain). The exact supply is a paid lookup."""
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
                f"Total supply of {token} (Base) — lite preview",
                f"≈{_coarse(supply / (10 ** decimals))} {symbol or ''}".rstrip(),
            ]
        )
    except Exception as e:
        return _err("token_supply", e)


@mcp.tool()
async def token_links(token: str) -> str:
    """Official links for a token (symbol or Base contract address): websites and socials as listed on DexScreener. A lite-only extra — not part of the paid catalog."""
    try:
        pairs = await _pairs_for_query(token)
        if not pairs:
            return f"Error: no Base DEX pair found for '{token}'."
        by_liq = sorted(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0), reverse=True)
        best = next(
            (p for p in by_liq if (p.get("info") or {}).get("websites") or (p.get("info") or {}).get("socials")),
            by_liq[0],
        )
        info = best.get("info") or {}
        sym = (best.get("baseToken") or {}).get("symbol")
        lines = [f"Official links for {sym} (via DexScreener)"]
        for w in info.get("websites") or []:
            url = w.get("url") if isinstance(w, dict) else str(w)
            if url:
                lines.append(f"Website: {url}")
        for s in info.get("socials") or []:
            if isinstance(s, dict):
                handle = s.get("handle") or s.get("url") or ""
                lines.append(f"{str(s.get('type', 'social')).capitalize()}: {handle}")
        if len(lines) == 1:
            lines.append("No official website or socials listed.")
        return _join(lines)
    except Exception as e:
        return _err("token_links", e)


@mcp.tool()
async def token_allowance(token_address: str, owner: str, spender: str) -> str:
    """Allowance risk category for an owner → spender pair on Base, read on-chain: none, limited, or UNLIMITED. The exact allowance amount is a paid lookup."""
    try:
        if not all(_is_address(a) for a in (token_address, owner, spender)):
            return "Error: token_allowance needs token, owner and spender addresses (0x, 42 chars each)."
        token = token_address.strip()
        raw = await _erc20_uint(token, SEL_ALLOWANCE, _addr32(owner.strip()) + _addr32(spender.strip()))
        if raw == 0:
            verdict = "No allowance — the spender cannot move this token."
        elif raw >= 2**200:
            verdict = "UNLIMITED allowance — the spender can move the owner's full balance of this token."
        else:
            verdict = "A limited allowance is set (exact amount is a paid lookup)."
        return _join(
            [
                f"Allowance on Base: {owner} -> {spender}",
                f"Token: {token}",
                verdict,
            ]
        )
    except Exception as e:
        return _err("token_allowance", e)


@mcp.tool()
async def function_selector(query: str) -> str:
    """Explain a common EVM 4-byte function selector (e.g. "0x70a08231") or look one up by function name (e.g. "balanceOf"). A small built-in reference table — educational, not part of the paid catalog."""
    table = {
        "70a08231": ("balanceOf(address)", "Read an account's balance of a token."),
        "a9059cbb": ("transfer(address,uint256)", "Send tokens to an address."),
        "095ea7b3": ("approve(address,uint256)", "Grant an allowance to a spender."),
        "23b872dd": ("transferFrom(address,address,uint256)", "Move tokens using an allowance."),
        "dd62ed3e": ("allowance(address,address)", "Read the allowance an owner granted a spender."),
        "18160ddd": ("totalSupply()", "Read a token's total supply."),
        "06fdde03": ("name()", "Read a token contract's name."),
        "95d89b41": ("symbol()", "Read a token contract's symbol."),
        "313ce567": ("decimals()", "Read a token contract's decimals."),
        "6352211e": ("ownerOf(uint256)", "Read the owner of an NFT token id."),
        "0dfe1681": ("token0()", "First token of a V3 pool or pair."),
        "d21220a7": ("token1()", "Second token of a V3 pool or pair."),
        "3850c7bd": ("slot0()", "Current price state of a Uniswap V3 pool."),
        "1a686502": ("liquidity()", "In-range liquidity of a Uniswap V3 pool."),
        "ddca3f43": ("fee()", "Fee tier of a Uniswap V3 pool."),
        "feaf968c": ("latestRoundData()", "Latest answer from a Chainlink aggregator."),
    }
    try:
        q = query.strip().lower()
        if q.startswith("0x"):
            key = q[2:10]
            hit = table.get(key)
            if not hit:
                return f"Error: selector 0x{key} is not in the lite reference table (it covers the most common ERC20 / NFT / DEX / oracle functions)."
            sig, desc = hit
            return _join([f"Selector 0x{key}", f"Function: {sig}", f"What it does: {desc}"])
        for key, (sig, desc) in table.items():
            if sig.lower().startswith(q):
                return _join([f"Function: {sig}", f"Selector: 0x{key}", f"What it does: {desc}"])
        return f"Error: no function matching '{query}' in the lite reference table."
    except Exception as e:
        return _err("function_selector", e)


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
    """Coarse ETH price preview (2 significant figures) plus 24h direction, from the best WETH pair on Base. The exact price is a paid lookup. Source: DexScreener."""
    try:
        best = _best_pair(await _base_pairs_for_token(WETH))
        if not best:
            return "Error: no WETH pair found on Base."
        change = (best.get("priceChange") or {}).get("h24")
        if change is None:
            direction = "24h direction: n/a"
        elif float(change) > 0.5:
            direction = "24h direction: up"
        elif float(change) < -0.5:
            direction = "24h direction: down"
        else:
            direction = "24h direction: roughly flat"
        lines = [
            "Ether (ETH) — lite preview",
            f"Price: ≈{_coarse_usd(best.get('priceUsd'))}",
            direction,
            f"Reference pair: WETH/{(best.get('quoteToken') or {}).get('symbol')} on {best.get('dexId')}",
        ]
        return _join(lines)
    except Exception as e:
        return _err("eth_price", e)


@mcp.tool()
async def token_price_eth(token: str) -> str:
    """Rounded price of a token denominated in its pair's quote asset (usually ETH) on its best Base pair. The exact native price is a paid lookup. Source: DexScreener."""
    try:
        best = _best_pair(await _pairs_for_query(token))
        if not best:
            return f"Error: no Base DEX pair found for '{token}'."
        sym = (best.get("baseToken") or {}).get("symbol")
        quote = (best.get("quoteToken") or {}).get("symbol")
        try:
            native = _coarse(float(best.get("priceNative")), 3)
        except (TypeError, ValueError):
            native = "n/a"
        return _join(
            [
                f"{sym} price in {quote} terms (best Base pair on {best.get('dexId')}) — lite preview",
                f"Price: ≈{native} {quote}",
            ]
        )
    except Exception as e:
        return _err("token_price_eth", e)


# ===== DEX (DexScreener + on-chain pools) =====


@mcp.tool()
async def dex_search(query: str) -> str:
    """Search DEX pairs by token name, symbol or address — top 10 by liquidity, with rounded prices. Solana pairs are excluded: Solana DEX data is served by the paid Solana API. Source: DexScreener."""
    try:
        data = await _get_json(f"{DEXSCREENER}/latest/dex/search", params={"q": query})
        pairs = data.get("pairs") if isinstance(data, dict) else None
        pairs = [p for p in (pairs or []) if p.get("chainId") != "solana"]
        if not pairs:
            return f"Error: no DEX pairs found matching '{query}' (Solana pairs are served by the paid Solana API: {SOLANA_CATALOG_URL})."
        pairs = sorted(
            pairs,
            key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0),
            reverse=True,
        )[:10]
        lines = [f"DEX search for '{query}' ({len(pairs)} shown, Solana excluded) — lite preview"]
        for i, p in enumerate(pairs):
            lines.append(
                f"{i + 1}. [{p.get('chainId')}] {(p.get('baseToken') or {}).get('symbol')}/"
                f"{(p.get('quoteToken') or {}).get('symbol')} on {p.get('dexId')}: "
                f"≈{_coarse_usd(p.get('priceUsd'))}, liquidity ≈{_coarse_usd((p.get('liquidity') or {}).get('usd'))}"
            )
        return _join(lines)
    except Exception as e:
        return _err("dex_search", e)


@mcp.tool()
async def dex_pair_lookup(pair_address: str) -> str:
    """Lite preview of one Base DEX pair by its pair contract address: tokens, rounded price, liquidity band. Exact pair data is a paid lookup. Source: DexScreener."""
    try:
        if not _is_address(pair_address):
            return "Error: dex_pair_lookup needs a pair address (0x, 42 chars)."
        data = await _get_json(f"{DEXSCREENER}/latest/dex/pairs/base/{pair_address.strip()}")
        pairs = data.get("pairs") if isinstance(data, dict) else None
        if not pairs:
            return f"Error: pair {pair_address} not found on Base."
        p = pairs[0]
        liq = float((p.get("liquidity") or {}).get("usd") or 0)
        depth = _band(liq, [10_000, 250_000], ["thin (under $10K)", "moderate ($10K–$250K)", "deep (over $250K)"])
        return _join(
            [
                f"Pair {(p.get('baseToken') or {}).get('symbol')}/{(p.get('quoteToken') or {}).get('symbol')} on {p.get('dexId')} (Base) — lite preview",
                f"Pair address: {p.get('pairAddress')}",
                f"Base token: {(p.get('baseToken') or {}).get('address')}",
                f"Quote token: {(p.get('quoteToken') or {}).get('address')}",
                f"Price: ≈{_coarse_usd(p.get('priceUsd'))}",
                f"Liquidity: {depth}",
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
    """Top boosted tokens on DexScreener right now (their trending board), top 10, Solana excluded (Solana boosts are a paid Solana-API lookup)."""
    try:
        data = await _get_json(f"{DEXSCREENER}/token-boosts/top/v1")
        items = [it for it in (data if isinstance(data, list) else []) if it.get("chainId") != "solana"]
        if not items:
            return "Error: no boosted tokens returned by DexScreener."
        return _join(_boost_lines(items[:10], "Top boosted tokens on DexScreener (Solana excluded — paid Solana API)"))
    except Exception as e:
        return _err("dex_trending", e)


@mcp.tool()
async def dex_latest_boosts() -> str:
    """Most recently boosted tokens on DexScreener, latest 10, Solana excluded (Solana boosts are a paid Solana-API lookup)."""
    try:
        data = await _get_json(f"{DEXSCREENER}/token-boosts/latest/v1")
        items = [it for it in (data if isinstance(data, list) else []) if it.get("chainId") != "solana"]
        if not items:
            return "Error: no boosted tokens returned by DexScreener."
        return _join(_boost_lines(items[:10], "Latest boosted tokens on DexScreener (Solana excluded — paid Solana API)"))
    except Exception as e:
        return _err("dex_latest_boosts", e)


@mcp.tool()
async def dex_new_profiles() -> str:
    """Newest token profiles created on DexScreener, latest 10, Solana excluded (Solana profiles are a paid Solana-API lookup), with short descriptions."""
    try:
        data = await _get_json(f"{DEXSCREENER}/token-profiles/latest/v1")
        items = [it for it in (data if isinstance(data, list) else []) if it.get("chainId") != "solana"]
        if not items:
            return "Error: no token profiles returned by DexScreener."
        lines = ["Newest token profiles on DexScreener (Solana excluded — paid Solana API)"]
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
    """Pair-count bucket for a Base token (contract address), plus how many distinct DEXes host its pairs. The exact pair list is a paid lookup. Source: DexScreener."""
    try:
        if not _is_address(token):
            return "Error: dex_pairs_count needs a token contract address (0x, 42 chars)."
        pairs = await _base_pairs_for_token(token.strip())
        if not pairs:
            return f"Error: no Base pairs found for {token}."
        dexes = {p.get("dexId") for p in pairs if p.get("dexId")}
        bucket = _band(len(pairs), [2, 6, 21], ["1 pair", "2–5 pairs", "6–20 pairs", "over 20 pairs"])
        return _join(
            [
                f"Base pairs for {token} — lite preview",
                f"Pair count (token as base): {bucket}",
                f"Distinct DEXes: {len(dexes)}",
            ]
        )
    except Exception as e:
        return _err("dex_pairs_count", e)


@mcp.tool()
async def uniswap_v3_pool(pool_address: str) -> str:
    """Lite preview of a Uniswap V3 pool on Base, read on-chain: both tokens, fee tier, a rounded implied price (one direction), and a rounded in-pool liquidity figure. Exact pool state (tick, full precision) is a paid lookup."""
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
        return _join(
            [
                f"Uniswap V3 pool {pool} (Base) — lite preview",
                f"Token0: {s0} ({t0})",
                f"Token1: {s1} ({t1})",
                f"Fee tier: {fee / 10000}%",
                f"Price: 1 {s0} ≈ {_coarse(price0_in_1, 3)} {s1}",
                f"In-pool liquidity (raw units): ≈{_coarse(liquidity)}",
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
    apy_s = f"≈{round(float(apy))}%" if isinstance(apy, (int, float)) else "n/a"
    return (
        f"{p.get('project')} {p.get('symbol')} ({p.get('chain')}): "
        f"APY {apy_s}, TVL ≈{_coarse_usd(p.get('tvlUsd'))}"
    )


@mcp.tool()
async def base_tvl() -> str:
    """Rounded total value locked (TVL) on Base right now (2 significant figures). The exact figure is a paid lookup. Source: DeFiLlama."""
    try:
        chains = await _llama_chains()
        base = next((c for c in chains if str(c.get("name", "")).lower() == "base"), None)
        if not base:
            return "Error: Base not found in DeFiLlama chain list."
        return _join(["Base TVL (DeFiLlama) — lite preview", f"TVL: ≈{_coarse_usd(base.get('tvl'))}"])
    except Exception as e:
        return _err("base_tvl", e)


@mcp.tool()
async def chain_tvl(chain: str) -> str:
    """TVL preview for a chain by name (e.g. "Ethereum", "Arbitrum"): its TVL rank and a rounded TVL. Solana is excluded — Solana TVL is served by the paid Solana API. Exact figures are a paid lookup. Source: DeFiLlama."""
    try:
        if chain.strip().lower() == "solana":
            return _join(
                [
                    "Solana TVL is served by the paid Solana x402 API (Solana DeFi TVL, history, dominance and protocol breakdowns).",
                    f"Solana catalog: {SOLANA_CATALOG_URL}",
                ]
            )
        chains = await _llama_chains()
        match = next((c for c in chains if str(c.get("name", "")).lower() == chain.strip().lower()), None)
        if not match:
            return f"Error: chain '{chain}' not found on DeFiLlama."
        ordered = sorted(chains, key=lambda c: float(c.get("tvl") or 0), reverse=True)
        rank = ordered.index(match) + 1
        return _join(
            [
                f"{match.get('name')} TVL (DeFiLlama) — lite preview",
                f"TVL rank: #{rank} of {len(chains)}",
                f"TVL: ≈{_coarse_usd(match.get('tvl'))}",
                f"Native token: {match.get('tokenSymbol')}",
            ]
        )
    except Exception as e:
        return _err("chain_tvl", e)


@mcp.tool()
async def top_chains_tvl() -> str:
    """Top 15 chains by total value locked, with rounded TVL figures. Source: DeFiLlama."""
    try:
        chains = await _llama_chains()
        top = sorted(chains, key=lambda c: float(c.get("tvl") or 0), reverse=True)[:15]
        lines = ["Top chains by TVL (DeFiLlama) — lite preview"]
        lines += [f"{i + 1}. {c.get('name')}: ≈{_coarse_usd(c.get('tvl'))}" for i, c in enumerate(top)]
        return _join(lines)
    except Exception as e:
        return _err("top_chains_tvl", e)


@mcp.tool()
async def protocol_tvl(protocol: str) -> str:
    """Rounded TVL for one DeFi protocol by its DeFiLlama slug (e.g. "aave-v3", "uniswap-v3", "aerodrome"). The exact figure is a paid lookup."""
    try:
        slug = protocol.strip().lower().replace(" ", "-")
        data = await _get_json(f"{LLAMA}/tvl/{slug}")
        if not isinstance(data, (int, float)):
            return f"Error: protocol '{protocol}' not found on DeFiLlama."
        return _join([f"{protocol} TVL (DeFiLlama) — lite preview", f"TVL: ≈{_coarse_usd(data)}"])
    except Exception as e:
        return _err("protocol_tvl", e)


@mcp.tool()
async def protocol_info(protocol: str) -> str:
    """Lite summary of one DeFi protocol (by slug or name): category, rounded TVL, whole-percent 1d/7d changes, how many chains it runs on, and a short description. Source: DeFiLlama."""
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
        chains = match.get("chains") or []
        lines = [
            f"{match.get('name')} ({match.get('symbol')}) — DeFiLlama summary (lite)",
            f"Category: {match.get('category')}",
            f"TVL: ≈{_coarse_usd(match.get('tvl'))}",
            f"Change 1d: {_whole_pct(match.get('change_1d'))} | 7d: {_whole_pct(match.get('change_7d'))}",
            f"Chains: {len(chains)}" + (f" (incl. {', '.join(chains[:5])})" if chains else ""),
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
    """Top 15 DeFi protocols by TVL across all chains, with rounded TVL figures. Source: DeFiLlama."""
    try:
        protos = await _llama_protocols()
        top = sorted(protos, key=lambda p: float(p.get("tvl") or 0), reverse=True)[:15]
        lines = ["Top protocols by TVL (DeFiLlama) — lite preview"]
        lines += [
            f"{i + 1}. {p.get('name')} ({p.get('symbol')}): ≈{_coarse_usd(p.get('tvl'))}"
            for i, p in enumerate(top)
        ]
        return _join(lines)
    except Exception as e:
        return _err("top_protocols", e)


@mcp.tool()
async def top_base_protocols() -> str:
    """Top 15 DeFi protocols deployed on Base, by TVL, with rounded figures. Source: DeFiLlama."""
    try:
        protos = await _llama_protocols()
        on_base = [p for p in protos if "Base" in (p.get("chains") or [])]
        top = sorted(on_base, key=lambda p: float(p.get("tvl") or 0), reverse=True)[:15]
        lines = ["Top protocols on Base by TVL (DeFiLlama) — lite preview"]
        for i, p in enumerate(top):
            base_part = (p.get("chainTvls") or {}).get("Base")
            extra = f" (Base: ≈{_coarse_usd(base_part)})" if base_part else ""
            lines.append(f"{i + 1}. {p.get('name')} ({p.get('symbol')}): ≈{_coarse_usd(p.get('tvl'))}{extra}")
        return _join(lines)
    except Exception as e:
        return _err("top_base_protocols", e)


@mcp.tool()
async def dex_volume_global() -> str:
    """Global DEX trading volume preview: rounded 24h/7d totals and the top 3 DEXes by name. Exact volumes are a paid lookup. Source: DeFiLlama."""
    try:
        data = await _get_cached(f"{LLAMA}/overview/dexs", ttl=60.0)
        protos = (data or {}).get("protocols") or []
        top = sorted(protos, key=lambda x: float(x.get("total24h") or 0), reverse=True)[:3]
        lines = [
            "Global DEX volume (DeFiLlama) — lite preview",
            f"24h: ≈{_coarse_usd(data.get('total24h'))}",
            f"7d: ≈{_coarse_usd(data.get('total7d'))}",
            "Top DEXes by 24h volume:",
        ]
        lines += [f"{i + 1}. {p.get('name')}" for i, p in enumerate(top)]
        return _join(lines)
    except Exception as e:
        return _err("dex_volume_global", e)


@mcp.tool()
async def dex_volume_base() -> str:
    """Base DEX trading volume preview: rounded 24h/7d totals and the top 3 Base DEXes by name. Exact volumes are a paid lookup. Source: DeFiLlama."""
    try:
        data = await _get_cached(f"{LLAMA}/overview/dexs/Base", ttl=60.0)
        protos = (data or {}).get("protocols") or []
        top = sorted(protos, key=lambda x: float(x.get("total24h") or 0), reverse=True)[:3]
        lines = [
            "Base DEX volume (DeFiLlama) — lite preview",
            f"24h: ≈{_coarse_usd(data.get('total24h'))}",
            f"7d: ≈{_coarse_usd(data.get('total7d'))}",
            "Top Base DEXes by 24h volume:",
        ]
        lines += [f"{i + 1}. {p.get('name')}" for i, p in enumerate(top)]
        return _join(lines)
    except Exception as e:
        return _err("dex_volume_base", e)


@mcp.tool()
async def dex_fees_top() -> str:
    """Top 3 protocols by fees generated in the last 24h (rounded), with the rounded all-protocol 24h total. The full fee table is a paid lookup. Source: DeFiLlama."""
    try:
        data = await _get_cached(f"{LLAMA}/overview/fees", ttl=60.0)
        protos = (data or {}).get("protocols") or []
        top = sorted(protos, key=lambda x: float(x.get("total24h") or 0), reverse=True)[:3]
        lines = [
            "Top protocols by 24h fees (DeFiLlama) — lite preview",
            f"All protocols, 24h fees: ≈{_coarse_usd(data.get('total24h'))}",
        ]
        lines += [
            f"{i + 1}. {p.get('name')}: ≈{_coarse_usd(p.get('total24h'))}"
            for i, p in enumerate(top)
        ]
        return _join(lines)
    except Exception as e:
        return _err("dex_fees_top", e)


@mcp.tool()
async def yields_top() -> str:
    """Top 3 yield pools by APY across all chains (whole-percent APYs, rounded TVLs). The full top-10 table with exact APYs is a paid lookup. Source: DeFiLlama Yields."""
    try:
        pools = [p for p in await _yields_pools() if p.get("chain") != "Solana"]
        if not pools:
            return "Error: no yield pools returned by DeFiLlama."
        top = sorted(pools, key=lambda p: float(p.get("apy") or 0), reverse=True)[:3]
        lines = ["Top yield pools by APY (DeFiLlama Yields; Solana pools excluded — paid Solana API) — lite preview"]
        lines += [f"{i + 1}. {_pool_line(p)}" for i, p in enumerate(top)]
        return _join(lines)
    except Exception as e:
        return _err("yields_top", e)


@mcp.tool()
async def yields_base() -> str:
    """Top 3 yield pools on Base by APY (whole-percent APYs, rounded TVLs). The full top-10 table with exact APYs is a paid lookup. Source: DeFiLlama Yields."""
    try:
        pools = await _yields_pools()
        on_base = [p for p in pools if p.get("chain") == "Base"]
        if not on_base:
            return "Error: no Base yield pools returned by DeFiLlama."
        top = sorted(on_base, key=lambda p: float(p.get("apy") or 0), reverse=True)[:3]
        lines = ["Top Base yield pools by APY (DeFiLlama Yields) — lite preview"]
        lines += [f"{i + 1}. {_pool_line(p)}" for i, p in enumerate(top)]
        return _join(lines)
    except Exception as e:
        return _err("yields_base", e)


@mcp.tool()
async def yields_stablecoins() -> str:
    """Top 3 stablecoin yield pools by APY across chains (whole-percent APYs, rounded TVLs). The full top-10 table with exact APYs is a paid lookup. Source: DeFiLlama Yields."""
    try:
        pools = await _yields_pools()
        stables = [p for p in pools if p.get("stablecoin") is True and p.get("chain") != "Solana"]
        if not stables:
            return "Error: no stablecoin yield pools returned by DeFiLlama."
        top = sorted(stables, key=lambda p: float(p.get("apy") or 0), reverse=True)[:3]
        lines = ["Top stablecoin yield pools by APY (DeFiLlama Yields; Solana pools excluded — paid Solana API) — lite preview"]
        lines += [f"{i + 1}. {_pool_line(p)}" for i, p in enumerate(top)]
        return _join(lines)
    except Exception as e:
        return _err("yields_stablecoins", e)


@mcp.tool()
async def yields_token(symbol: str) -> str:
    """Top 3 yield pools involving a token symbol (e.g. "USDC", "ETH"), by APY (whole-percent APYs, rounded TVLs). The full top-10 table with exact APYs is a paid lookup. Source: DeFiLlama Yields."""
    try:
        pools = await _yields_pools()
        key = symbol.strip().upper()
        mine = [p for p in pools if key in str(p.get("symbol", "")).upper() and p.get("chain") != "Solana"]
        if not mine:
            return f"Error: no yield pools found for symbol '{symbol}' (Solana pools are served by the paid Solana API)."
        top = sorted(mine, key=lambda p: float(p.get("apy") or 0), reverse=True)[:3]
        lines = [f"Top yield pools for {key} by APY (DeFiLlama Yields; Solana pools excluded — paid Solana API) — lite preview"]
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
    """Top 3 stablecoins by circulating supply (rounded), with peg type. The full top-10 table with prices is a paid lookup. Source: DeFiLlama Stablecoins."""
    try:
        assets = await _stablecoin_assets()
        top = sorted(assets, key=_circ, reverse=True)[:3]
        lines = ["Top stablecoins by circulating supply (DeFiLlama) — lite preview"]
        for i, a in enumerate(top):
            lines.append(
                f"{i + 1}. {a.get('name')} ({a.get('symbol')}): ≈{_coarse_usd(_circ(a))} "
                f"[{a.get('pegType')}/{a.get('pegMechanism')}]"
            )
        return _join(lines)
    except Exception as e:
        return _err("top_stablecoins", e)


@mcp.tool()
async def stablecoin_info(asset: str) -> str:
    """Lite summary of one stablecoin by symbol, name or CoinGecko id (e.g. "USDC"): rounded circulating supply, peg type/mechanism, and its top 3 chains (rounded). Exact supply and price are paid lookups. Source: DeFiLlama Stablecoins."""
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
            f"{match.get('name')} ({match.get('symbol')}) — stablecoin summary (lite)",
            f"Circulating: ≈{_coarse_usd(_circ(match))}",
            f"Peg: {match.get('pegType')} / {match.get('pegMechanism')}",
        ]
        cc = match.get("chainCirculating") or {}
        per_chain = []
        for ch, v in cc.items():
            amt = ((v or {}).get("current") or {}).get("peggedUSD")
            if isinstance(amt, (int, float)):
                per_chain.append((ch, amt))
        per_chain = [(ch, amt) for ch, amt in per_chain if ch.lower() != "solana"]
        per_chain.sort(key=lambda x: x[1], reverse=True)
        if per_chain:
            lines.append("Top chains (excluding Solana — its per-chain amount is a paid Solana-API lookup): " + ", ".join(f"{ch} ≈{_coarse_usd(amt)}" for ch, amt in per_chain[:3]))
        return _join(lines)
    except Exception as e:
        return _err("stablecoin_info", e)


@mcp.tool()
async def stablecoin_total() -> str:
    """Rounded total circulating supply of all tracked stablecoins, and how many there are. The exact total is a paid lookup. Source: DeFiLlama Stablecoins."""
    try:
        assets = await _stablecoin_assets()
        total = sum(_circ(a) for a in assets)
        return _join(
            [
                "All stablecoins (DeFiLlama) — lite preview",
                f"Total circulating: ≈{_coarse_usd(total)}",
                f"Tracked stablecoins: {len(assets)}",
            ]
        )
    except Exception as e:
        return _err("stablecoin_total", e)


@mcp.tool()
async def stablecoin_chains() -> str:
    """Top 10 chains by stablecoin supply hosted on them, with rounded figures. Source: DeFiLlama Stablecoins."""
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
        lines = ["Top chains by stablecoin supply (DeFiLlama) — lite preview"]
        lines += [f"{i + 1}. {r.get('name')}: ≈{_coarse_usd(amt(r))}" for i, r in enumerate(top)]
        return _join(lines)
    except Exception as e:
        return _err("stablecoin_chains", e)


@mcp.tool()
async def stablecoins_base() -> str:
    """Top 10 stablecoins by circulating supply on Base, with rounded figures. Source: DeFiLlama Stablecoins."""
    try:
        assets = await _stablecoin_assets()
        out = []
        for a in assets:
            cur = ((a.get("chainCirculating") or {}).get("Base") or {}).get("current") or {}
            amt = cur.get("peggedUSD")
            if isinstance(amt, (int, float)) and amt > 0:
                out.append((a, amt))
        out.sort(key=lambda x: x[1], reverse=True)
        lines = ["Top stablecoins on Base by supply (DeFiLlama) — lite preview"]
        lines += [
            f"{i + 1}. {a.get('name')} ({a.get('symbol')}): ≈{_coarse_usd(amt)}"
            for i, (a, amt) in enumerate(out[:10])
        ]
        return _join(lines)
    except Exception as e:
        return _err("stablecoins_base", e)


@mcp.tool()
async def stablecoin_pegs() -> str:
    """The 3 stablecoins trading furthest from their peg right now (coarse prices, whole-percent deviations; the tail is mostly illiquid or dead coins). The full peg table is a paid lookup. Source: DeFiLlama Stablecoins."""
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
        lines = ["Largest stablecoin peg deviations (DeFiLlama) — lite preview"]
        lines += [
            f"{i + 1}. {k}: ≈{_coarse_usd(v)} ({_whole_pct(dev)} from $1.00)"
            for i, (k, v, dev) in enumerate(out[:3])
        ]
        return _join(lines)
    except Exception as e:
        return _err("stablecoin_pegs", e)



# ===== markets & global (CoinGecko + Alternative.me) =====


@mcp.tool()
async def fear_greed() -> str:
    """Current Crypto Fear & Greed classification only (e.g. "Greed") — the numeric index value is a paid lookup. Source: Alternative.me."""
    try:
        data = await _get_cached(f"{FNG}/fng/", ttl=60.0, params={"limit": 1})
        items = (data or {}).get("data") or []
        if not items:
            return "Error: no Fear & Greed data returned."
        it = items[0]
        day = datetime.fromtimestamp(int(it.get("timestamp", 0)), timezone.utc).strftime("%Y-%m-%d")
        return _join(
            [
                "Crypto Fear & Greed Index — lite preview",
                f"Sentiment: {it.get('value_classification')}",
                f"Date: {day}",
            ]
        )
    except Exception as e:
        return _err("fear_greed", e)


@mcp.tool()
async def fear_greed_week() -> str:
    """Fear & Greed over the last 7 days as a summary: the average (rounded to the nearest 5), the week's trend, and the latest classification. The day-by-day values are a paid lookup. Source: Alternative.me."""
    try:
        data = await _get_cached(f"{FNG}/fng/", ttl=60.0, params={"limit": 7})
        items = (data or {}).get("data") or []
        if not items:
            return "Error: no Fear & Greed data returned."
        values = []
        for it in items:
            try:
                values.append(int(it.get("value")))
            except (TypeError, ValueError):
                pass
        if not values:
            return "Error: no Fear & Greed values returned."
        avg = sum(values) / len(values)
        avg5 = round(avg / 5) * 5
        if avg5 <= 24:
            band = "Extreme Fear"
        elif avg5 <= 44:
            band = "Fear"
        elif avg5 <= 55:
            band = "Neutral"
        elif avg5 <= 74:
            band = "Greed"
        else:
            band = "Extreme Greed"
        diff = values[0] - values[-1]
        trend = "sentiment improving" if diff > 3 else "sentiment worsening" if diff < -3 else "sentiment flat"
        return _join(
            [
                "Fear & Greed Index — last 7 days (lite summary)",
                f"Average: ≈{avg5} ({band})",
                f"Trend over the week: {trend}",
                f"Latest: {items[0].get('value_classification')}",
            ]
        )
    except Exception as e:
        return _err("fear_greed_week", e)


@mcp.tool()
async def market_global() -> str:
    """Global crypto market preview: rounded total market cap and 24h volume, whole-percent 24h change and BTC/ETH dominance. Exact figures are a paid lookup. Source: CoinGecko."""
    try:
        data = await _get_cached(f"{COINGECKO}/global", ttl=60.0)
        d = (data or {}).get("data") or {}
        pct = d.get("market_cap_percentage") or {}
        return _join(
            [
                "Global crypto market (CoinGecko) — lite preview",
                f"Total market cap: ≈{_coarse_usd((d.get('total_market_cap') or {}).get('usd'))}",
                f"24h volume: ≈{_coarse_usd((d.get('total_volume') or {}).get('usd'))}",
                f"Market cap change 24h: {_whole_pct(d.get('market_cap_change_percentage_24h_usd'))}",
                f"BTC dominance: {round(float(pct['btc']))}%" if pct.get("btc") is not None else "BTC dominance: n/a",
                f"ETH dominance: {round(float(pct['eth']))}%" if pct.get("eth") is not None else "ETH dominance: n/a",
            ]
        )
    except Exception as e:
        return _err("market_global", e)


@mcp.tool()
async def top_coins() -> str:
    """Top 3 cryptocurrencies by market cap, with rounded prices, whole-percent 24h changes and rounded market caps. The full top-10 table with exact figures is a paid lookup. Source: CoinGecko."""
    try:
        data = await _get_cached(
            f"{COINGECKO}/coins/markets",
            ttl=60.0,
            params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": 3, "page": 1},
        )
        coins = data if isinstance(data, list) else []
        lines = ["Top 3 coins by market cap (CoinGecko) — lite preview"]
        for c in coins:
            lines.append(
                f"{c.get('market_cap_rank')}. {c.get('name')} ({str(c.get('symbol')).upper()}): "
                f"≈{_coarse_usd(c.get('current_price'))} ({_whole_pct(c.get('price_change_percentage_24h'))} 24h), "
                f"mcap ≈{_coarse_usd(c.get('market_cap'))}"
            )
        return _join(lines)
    except Exception as e:
        return _err("top_coins", e)


@mcp.tool()
async def trending_coins() -> str:
    """The top 3 coins trending on CoinGecko search right now (names and market-cap ranks). The full trending list with prices is a paid lookup. Source: CoinGecko."""
    try:
        data = await _get_cached(f"{COINGECKO}/search/trending", ttl=60.0)
        coins = (data or {}).get("coins") or []
        lines = ["Trending on CoinGecko — lite preview"]
        for i, c in enumerate(coins[:3]):
            item = c.get("item") or {}
            rank = item.get("market_cap_rank")
            rank_s = f" (rank #{rank})" if rank else ""
            lines.append(f"{i + 1}. {item.get('name')} ({item.get('symbol')}){rank_s}")
        return _join(lines)
    except Exception as e:
        return _err("trending_coins", e)


@mcp.tool()
async def coin_price(coin_id: str) -> str:
    """Rounded USD price preview for one coin by CoinGecko id (e.g. "bitcoin", "ethereum", "aerodrome-finance"). The exact price is a paid lookup."""
    try:
        cid = coin_id.strip().lower()
        data = await _get_cached(
            f"{COINGECKO}/simple/price",
            ttl=60.0,
            params={"ids": cid, "vs_currencies": "usd"},
        )
        entry = (data or {}).get(cid)
        if not entry:
            return f"Error: coin '{coin_id}' not found on CoinGecko."
        return _join(
            [
                f"{cid} (CoinGecko) — lite preview",
                f"Price: ≈{_coarse_usd(entry.get('usd'))}",
            ]
        )
    except Exception as e:
        return _err("coin_price", e)


@mcp.tool()
async def eth_btc_ratio() -> str:
    """The ETH/BTC ratio — how much BTC one ETH is worth — a derived market gauge that is not part of the paid catalog. Source: CoinGecko."""
    try:
        data = await _get_cached(
            f"{COINGECKO}/simple/price",
            ttl=60.0,
            params={"ids": "bitcoin,ethereum", "vs_currencies": "usd"},
        )
        btc = ((data or {}).get("bitcoin") or {}).get("usd")
        eth = ((data or {}).get("ethereum") or {}).get("usd")
        if not btc or not eth:
            return "Error: could not fetch BTC and ETH prices from CoinGecko."
        ratio = float(eth) / float(btc)
        return _join(
            [
                "ETH / BTC ratio (CoinGecko)",
                f"1 ETH ≈ {_coarse(ratio, 3)} BTC",
                f"(from ETH ≈{_coarse_usd(eth)} and BTC ≈{_coarse_usd(btc)})",
            ]
        )
    except Exception as e:
        return _err("eth_btc_ratio", e)


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
    """Lite summary for one coin by CoinGecko id: name, market-cap rank, and a rounded market cap. Prices, changes, supplies and links are paid lookups."""
    try:
        cid = coin_id.strip().lower()
        data = await _cg_coin(cid)
        if not isinstance(data, dict) or not data.get("id"):
            return f"Error: coin '{coin_id}' not found on CoinGecko."
        md = data.get("market_data") or {}
        return _join(
            [
                f"{data.get('name')} ({str(data.get('symbol')).upper()}) — CoinGecko summary (lite)",
                f"Market cap rank: #{data.get('market_cap_rank')}",
                f"Market cap: ≈{_coarse_usd((md.get('market_cap') or {}).get('usd'))}",
            ]
        )
    except Exception as e:
        return _err("coin_info", e)


@mcp.tool()
async def market_mood() -> str:
    """A one-word read on overall crypto market mood (Risk-on / Cautious / Risk-off), synthesized from the Fear & Greed classification and the global market's 24h direction. A lite-only derived gauge — not part of the paid catalog."""
    try:
        score = 0
        notes = []
        try:
            fng = await _get_cached(f"{FNG}/fng/", ttl=60.0, params={"limit": 1})
            items = (fng or {}).get("data") or []
            if items:
                val = int(items[0].get("value"))
                cls = items[0].get("value_classification")
                if val >= 55:
                    score += 1
                elif val <= 45:
                    score -= 1
                notes.append(f"sentiment is {cls}")
        except Exception:
            pass
        try:
            g = await _get_cached(f"{COINGECKO}/global", ttl=60.0)
            chg = ((g or {}).get("data") or {}).get("market_cap_change_percentage_24h_usd")
            if chg is not None:
                if float(chg) > 1:
                    score += 1
                    notes.append("the total market rose over the last day")
                elif float(chg) < -1:
                    score -= 1
                    notes.append("the total market fell over the last day")
                else:
                    notes.append("the total market was roughly flat over the last day")
        except Exception:
            pass
        mood = "Risk-on" if score >= 2 else "Risk-off" if score <= -2 else "Cautious"
        lines = ["Crypto market mood (lite gauge)", f"Mood: {mood}"]
        if notes:
            lines.append("Why: " + "; ".join(notes) + ".")
        return _join(lines)
    except Exception as e:
        return _err("market_mood", e)


@mcp.tool()
async def coin_change(coin_id: str) -> str:
    """7d price change for one coin by CoinGecko id, rounded to the nearest whole percent, with a rounded current price and its rank. The full 24h/7d/30d change table is a paid lookup."""
    try:
        cid = coin_id.strip().lower()
        data = await _get_cached(
            f"{COINGECKO}/coins/markets",
            ttl=60.0,
            params={"vs_currency": "usd", "ids": cid, "price_change_percentage": "7d"},
        )
        rows = data if isinstance(data, list) else []
        if not rows:
            return f"Error: coin '{coin_id}' not found on CoinGecko."
        c = rows[0]
        return _join(
            [
                f"{c.get('name')} ({str(c.get('symbol')).upper()}) — lite preview (CoinGecko)",
                f"Price: ≈{_coarse_usd(c.get('current_price'))} (rank #{c.get('market_cap_rank')})",
                f"7d: {_whole_pct(c.get('price_change_percentage_7d_in_currency'))}",
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
    """BTC and ETH market-cap dominance, rounded to whole percents. The full dominance table is a paid lookup. Source: CoinGecko."""
    try:
        data = await _get_cached(f"{COINGECKO}/global", ttl=60.0)
        pct = ((data or {}).get("data") or {}).get("market_cap_percentage") or {}
        if not pct:
            return "Error: no dominance data returned by CoinGecko."
        lines = ["Market cap dominance (CoinGecko) — lite preview"]
        for key in ("btc", "eth"):
            if pct.get(key) is not None:
                lines.append(f"{key.upper()}: {round(float(pct[key]))}%")
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
    """Whether an ENS name has an avatar set (yes/no) — the avatar URL itself is a paid lookup. Source: ENSIdeas."""
    try:
        data = await _get_json(f"{ENSIDEAS}/ens/resolve/{name.strip()}")
        if not isinstance(data, dict) or not data.get("address"):
            return f"Error: ENS name '{name}' not found."
        return _join(
            [
                f"ENS avatar for {name} — lite preview",
                f"Avatar set: {'yes' if data.get('avatar') else 'no'}",
                f"Address: {_mask_addr(data.get('address'))} (masked preview)",
            ]
        )
    except Exception as e:
        return _err("ens_avatar", e)


@mcp.tool()
async def ens_profile(name: str) -> str:
    """Which public profile fields an ENS name has set (a presence checklist — the field values are a paid lookup). Source: ensdata.net."""
    try:
        data = await _get_json(f"{ENSDATA}/{name.strip()}")
        if not isinstance(data, dict) or not data.get("address"):
            return f"Error: ENS name '{name}' not found."
        fields = [
            ("description", "Description"),
            ("url", "Website"),
            ("avatar", "Avatar"),
            ("twitter", "Twitter"),
            ("github", "GitHub"),
            ("discord", "Discord"),
            ("telegram", "Telegram"),
        ]
        set_names = [label for key, label in fields if data.get(key)]
        return _join(
            [
                f"ENS profile: {name} — lite preview",
                f"Address: {_mask_addr(data.get('address'))} (masked preview)",
                f"Fields set: {', '.join(set_names) if set_names else 'none of the common public fields'}",
            ]
        )
    except Exception as e:
        return _err("ens_profile", e)


@mcp.tool()
async def ens_records(name: str) -> str:
    """Which text-record keys an ENS name has set (key names and count only — the record values are a paid lookup). Source: ensdata.net."""
    try:
        data = await _get_json(f"{ENSDATA}/{name.strip()}")
        if not isinstance(data, dict) or not data.get("address"):
            return f"Error: ENS name '{name}' not found."
        keys = [
            ("description", "Description"),
            ("url", "URL"),
            ("twitter", "Twitter"),
            ("github", "GitHub"),
            ("discord", "Discord"),
            ("telegram", "Telegram"),
            ("contentHash", "Content hash"),
            ("header", "Header"),
            ("pgp", "PGP"),
        ]
        present = [label for key, label in keys if data.get(key)]
        return _join(
            [
                f"ENS text records: {name} — lite preview",
                f"Address: {_mask_addr(data.get('address'))} (masked preview)",
                f"Records set ({len(present)}): {', '.join(present) if present else 'none'}",
            ]
        )
    except Exception as e:
        return _err("ens_records", e)


@mcp.tool()
async def ens_content_hash(name: str) -> str:
    """Whether an ENS name has a content hash (decentralized website pointer) set, and its scheme (e.g. ipfs). The hash itself is a paid lookup. Source: ensdata.net."""
    try:
        data = await _get_json(f"{ENSDATA}/{name.strip()}")
        if not isinstance(data, dict) or not data.get("address"):
            return f"Error: ENS name '{name}' not found."
        ch = data.get("contentHash")
        lines = [
            f"ENS content hash: {name} — lite preview",
            f"Address: {_mask_addr(data.get('address'))} (masked preview)",
        ]
        if ch:
            scheme = str(ch).split("://")[0] if "://" in str(ch) else "non-standard format"
            lines.append("Content hash set: yes")
            lines.append(f"Type: {scheme}")
        else:
            lines.append("Content hash set: no")
        return _join(lines)
    except Exception as e:
        return _err("ens_content_hash", e)


# ===== wallet & address (Base RPC) =====


async def _wallet_token_line(address: str, token: str, label: str) -> str:
    raw = await _erc20_uint(token, SEL_BALANCE_OF, _addr32(address))
    decimals = await _erc20_decimals(token)
    return f"{label}: {raw / (10 ** decimals):,.6f}"


@mcp.tool()
async def wallet_eth_value(address: str) -> str:
    """Rounded USD value preview of an address's ETH balance on Base, at a rounded live ETH price. Exact balances and values are paid lookups."""
    try:
        if not _is_address(address):
            return "Error: wallet_eth_value needs an address (0x, 42 chars)."
        addr = address.strip()
        wei = int(await _rpc("eth_getBalance", [addr, "latest"]), 16)
        eth = wei / 1e18
        lines = [f"Wallet {addr} (Base) — lite preview", f"ETH: ≈{_coarse(eth, 3)}"]
        price = await _eth_price_usd()
        if price:
            lines.append(f"Value: ≈{_coarse_usd(eth * price)} (ETH at ≈{_coarse_usd(price)})")
        return _join(lines)
    except Exception as e:
        return _err("wallet_eth_value", e)


@mcp.tool()
async def base_tps() -> str:
    """Base network throughput right now: average transactions per second over the last few blocks — a derived lite gauge, not part of the paid catalog. Source: Base public RPC."""
    try:
        latest = await _rpc("eth_getBlockByNumber", ["latest", False])
        latest_num = int(latest["number"], 16)
        counts = [len(latest.get("transactions", []))]
        for num in range(latest_num - 1, latest_num - 5, -1):
            b = await _rpc("eth_getBlockByNumber", [hex(num), False])
            counts.append(len(b.get("transactions", [])))
        # Block timestamps have 1s resolution, so measure the block time
        # over a 20-block window and combine it with the 5-block average
        # tx count — a 4-block span can round to ~1s and inflate the TPS.
        older = await _rpc("eth_getBlockByNumber", [hex(latest_num - 20), False])
        span = int(latest["timestamp"], 16) - int(older["timestamp"], 16)
        if span <= 0:
            return "Error: could not measure block times."
        avg_tx = sum(counts) / len(counts)
        tps = avg_tx / (span / 20.0)
        return _join(
            [
                "Base throughput (last 5 blocks)",
                f"≈{tps:.0f} transactions per second",
                f"Average block: ≈{_coarse(sum(counts) / len(counts), 3)} transactions",
            ]
        )
    except Exception as e:
        return _err("base_tps", e)


@mcp.tool()
async def stablecoin_dominance() -> str:
    """Stablecoins' share of the total crypto market cap — a derived lite gauge, not part of the paid catalog. Sources: DeFiLlama Stablecoins + CoinGecko."""
    try:
        assets = await _stablecoin_assets()
        total_stable = sum(_circ(a) for a in assets)
        g = await _get_cached(f"{COINGECKO}/global", ttl=60.0)
        mcap = ((g or {}).get("data") or {}).get("total_market_cap", {}).get("usd")
        if not mcap:
            return "Error: global market cap unavailable from CoinGecko."
        share = total_stable / float(mcap) * 100
        return _join(
            [
                "Stablecoin dominance (derived)",
                f"Stablecoins are ≈{share:.1f}% of total crypto market cap",
                f"Stablecoin supply: ≈{_coarse_usd(total_stable)} of ≈{_coarse_usd(mcap)} total",
            ]
        )
    except Exception as e:
        return _err("stablecoin_dominance", e)


@mcp.tool()
async def yields_median_base() -> str:
    """The MEDIAN APY across Base yield pools with a positive APY — the typical earning pool, not the top of the table (top-pool lists are paid lookups). A lite-only derived statistic. Source: DeFiLlama Yields."""
    try:
        pools = await _yields_pools()
        apys = sorted(
            float(p.get("apy"))
            for p in pools
            if p.get("chain") == "Base" and isinstance(p.get("apy"), (int, float)) and float(p.get("apy")) > 0
        )
        if not apys:
            return "Error: no Base yield pools returned by DeFiLlama."
        med = statistics.median(apys)
        return _join(
            [
                "Base yield pools — median APY (DeFiLlama Yields)",
                f"Median APY: ≈{med:.1f}%",
                f"Pools counted: {len(apys)}",
            ]
        )
    except Exception as e:
        return _err("yields_median_base", e)


@mcp.tool()
async def base_network_info() -> str:
    """Base mainnet reference card: chain ID, native currency, block target, explorer, bridge and public RPC. Static educational reference — not part of the paid catalog."""
    return _join(
        [
            "Base mainnet — reference card",
            "Chain ID: 8453 (0x2105)",
            "Native currency: ETH (gas is paid in ETH)",
            "Block target: ~2 seconds",
            "Type: Ethereum L2 (OP Stack rollup), sequencer run by Coinbase",
            "Explorer: https://basescan.org",
            "Bridge: https://bridge.base.org",
            "Public RPC: https://mainnet.base.org",
        ]
    )


@mcp.tool()
async def top_gainer_today() -> str:
    """The biggest 24h gainer among the top 100 coins by market cap (whole-percent gain, rounded price) — a derived lite stat. (Solana-ecosystem gainers are a paid Solana-API lookup.) Source: CoinGecko."""
    try:
        data = await _get_cached(
            f"{COINGECKO}/coins/markets",
            ttl=60.0,
            params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": 100, "page": 1},
        )
        coins = [
            c
            for c in (data if isinstance(data, list) else [])
            if isinstance(c.get("price_change_percentage_24h"), (int, float))
        ]
        if not coins:
            return "Error: no market data returned by CoinGecko."
        best = max(coins, key=lambda c: float(c["price_change_percentage_24h"]))
        return _join(
            [
                "Biggest 24h gainer in the top 100 (CoinGecko)",
                f"{best.get('name')} ({str(best.get('symbol')).upper()}): {_whole_pct(best.get('price_change_percentage_24h'))} in 24h",
                f"Price: ≈{_coarse_usd(best.get('current_price'))} (rank #{best.get('market_cap_rank')})",
            ]
        )
    except Exception as e:
        return _err("top_gainer_today", e)


@mcp.tool()
async def market_breadth() -> str:
    """Market breadth: how many of the top 100 coins by market cap are up in the last 24h — a derived lite gauge, not part of the paid catalog. Source: CoinGecko."""
    try:
        data = await _get_cached(
            f"{COINGECKO}/coins/markets",
            ttl=60.0,
            params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": 100, "page": 1},
        )
        coins = [
            c
            for c in (data if isinstance(data, list) else [])
            if isinstance(c.get("price_change_percentage_24h"), (int, float))
        ]
        if not coins:
            return "Error: no market data returned by CoinGecko."
        up = sum(1 for c in coins if float(c["price_change_percentage_24h"]) > 0)
        return _join(
            [
                "Market breadth — top 100 coins (CoinGecko)",
                f"Up in 24h: {up} of {len(coins)} ({round(up / len(coins) * 100)}%)",
                f"Down or flat: {len(coins) - up}",
            ]
        )
    except Exception as e:
        return _err("market_breadth", e)


@mcp.tool()
async def wallet_known_holdings(address: str) -> str:
    """Which of the well-known Base tokens (WETH, USDC, AERO, cbETH, DAI, USDbC, HIGHER) an address holds — names only; the amounts are a paid lookup. Read on-chain."""
    try:
        if not _is_address(address):
            return "Error: wallet_known_holdings needs an address (0x, 42 chars)."
        addr = address.strip()
        seen = {}
        for symbol, token in SYMBOL_ADDRESSES.items():
            # ETH and WETH share one contract; label it WETH (the ERC20).
            if token not in seen or symbol == "WETH":
                seen[token] = "WETH" if token == WETH else symbol
        held_names = []
        for token, symbol in seen.items():
            raw = await _erc20_uint(token, SEL_BALANCE_OF, _addr32(addr))
            if raw > 0:
                held_names.append(symbol)
        lines = [f"Known-token holdings of {addr} (Base) — lite preview"]
        if held_names:
            lines.append("Holds: " + ", ".join(held_names))
        else:
            lines.append("None of the well-known tokens (WETH, USDC, AERO, cbETH, DAI, USDbC, HIGHER) show a balance.")
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
    age_s = (
        "n/a"
        if age is None
        else _band(age, [60, 300, 3600], ["under a minute ago", "a few minutes ago", "within the hour", "over an hour ago"])
    )
    lines = [
        f"Chainlink {desc or feed['pair']} on Base",
        f"Price: ≈{_coarse_usd(price)}",
        f"Feed: {addr}",
        f"Last update: {age_s}",
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
async def chainlink_staleness() -> str:
    """How fresh each tracked Chainlink feed on Base is (ETH/USD, BTC/USD, USDC/USD) — update ages only, no prices (prices are paid lookups). A lite health check. Source: Chainlink via Base RPC."""
    try:
        lines = ["Chainlink feed freshness on Base"]
        for key in ("eth", "btc", "usdc"):
            feed = CHAINLINK_FEEDS[key]
            raw = await _eth_call(feed["address"], "0x" + SEL_LATEST_ROUND)
            updated = _word(raw, 3)
            age = max(0, int(time.time()) - updated) if updated else None
            age_s = (
                "unknown"
                if age is None
                else _band(age, [60, 300, 3600], ["fresh — under a minute old", "a few minutes old", "within the hour", "over an hour old"])
            )
            lines.append(f"{feed['pair']}: {age_s}")
        return _join(lines)
    except Exception as e:
        return _err("chainlink_staleness", e)


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
    """Approximate conversion of an ETH amount to USD at a rounded live ETH price. Lite conversions are approximate — exact conversions are a paid lookup."""
    try:
        amt = float(amount_eth)
        price = await _eth_price_usd()
        if not price:
            return "Error: could not fetch the live ETH price."
        return _join(
            [
                f"{amt} ETH ≈ {_coarse_usd(amt * price)}",
                f"At a rounded ETH price of ≈{_coarse_usd(price)}",
            ]
        )
    except Exception as e:
        return _err("convert_eth_usd", e)


@mcp.tool()
async def convert_usd_eth(amount_usd: float) -> str:
    """Approximate conversion of a USD amount to ETH at a rounded live ETH price. Lite conversions are approximate — exact conversions are a paid lookup."""
    try:
        amt = float(amount_usd)
        price = await _eth_price_usd()
        if not price:
            return "Error: could not fetch the live ETH price."
        return _join(
            [
                f"${amt:,.2f} ≈ {_coarse(amt / price, 4)} ETH",
                f"At a rounded ETH price of ≈{_coarse_usd(price)}",
            ]
        )
    except Exception as e:
        return _err("convert_usd_eth", e)


@mcp.tool()
async def eth_units() -> str:
    """Ethereum denomination reference: wei, gwei, szabo, finney and ether, and what each is typically used for. Static educational reference — not part of the paid catalog."""
    return _join(
        [
            "Ethereum denominations",
            "1 wei = 1 wei (the smallest unit; contract values are stored in wei)",
            "1 gwei = 1,000,000,000 wei (10^9) — gas prices are quoted in gwei",
            "1 szabo (microether) = 10^12 wei",
            "1 finney (milliether) = 10^15 wei",
            "1 ether (ETH) = 10^18 wei = 10^9 gwei — account balances are shown in ether",
            "Rule of thumb: cost in ETH = gas used x gas price in gwei / 10^9.",
        ]
    )


@mcp.tool()
async def contract_storage(contract_address: str, slot: str = "0") -> str:
    """Read one storage slot of any Base contract (eth_getStorageAt) — a raw dev utility that is not part of the paid catalog. Slot accepts a decimal or 0x-hex index."""
    try:
        if not _is_address(contract_address):
            return "Error: contract_storage needs a contract address (0x, 42 chars)."
        try:
            slot_int = int(str(slot).strip(), 0)
        except Exception:
            return "Error: slot must be an integer (decimal or 0x-hex)."
        if slot_int < 0:
            return "Error: slot must be >= 0."
        addr = contract_address.strip()
        raw = await _rpc("eth_getStorageAt", [addr, hex(slot_int), "latest"])
        lines = [f"Storage of {addr} (Base), slot {slot_int}", f"Raw: {raw}"]
        try:
            value = int(raw, 16)
            lines.append(f"As integer: {value}")
            body = bytes.fromhex(raw[2:])
            if len(body) == 32 and body[:12] == b"\x00" * 12 and any(body[12:]):
                lines.append(f"As address: 0x{body[12:].hex()}")
        except Exception:
            pass
        return _join(lines)
    except Exception as e:
        return _err("contract_storage", e)


@mcp.tool()
async def base_sepolia_info() -> str:
    """Base Sepolia TESTNET reference card: chain ID, RPC, explorer and where to get test ETH. Static educational reference — not part of the paid catalog."""
    return _join(
        [
            "Base Sepolia (testnet) — reference card",
            "Chain ID: 84532 (0x14a34)",
            "Native currency: test ETH (no real value)",
            "Public RPC: https://sepolia.base.org",
            "Explorer: https://sepolia.basescan.org",
            "Test ETH: bridge Sepolia ETH or use a faucet — the Coinbase Developer Platform faucet and the list at docs.base.org are the usual starting points",
            "Use testnet for contract deploys and agent dry-runs before touching mainnet.",
        ]
    )


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

