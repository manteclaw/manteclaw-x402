# Manteclaw x402 service — SOLANA-NATIVE EDITION (FastAPI + x402 2.9.0)
#
# 200 endpoints total: 199 paid + free /health. Every endpoint serves
# SOLANA data, and payments settle in USDC on Solana mainnet via the
# x402 "exact" SVM scheme through PayAI's facilitator.
#
# (This service previously cloned the Base service's endpoint set and
# served Base/EVM data with Solana settlement. Alex directed on
# 2026-11-23 that the Solana APIs be Solana-focused, so the endpoint
# set was rebuilt Solana-native: the payment wiring is unchanged, the
# data is all Solana now.)
#
# Prices mirror the Base service's shape: 193 endpoints at $0.01 and 6
# premium endpoints at $0.02 (the price map lives in the REGISTRY
# below).
#
# Data sources (all free, no API keys):
#   - Solana public RPC (api.mainnet-beta.solana.com): slots, blocks,
#     epochs, supply, validators, SPL token mints/accounts, wallet
#     balances and signatures, transactions, stake accounts.
#   - Jupiter lite API (lite-api.jup.ag): token prices (price v3) and
#     token metadata search.
#   - Dexscreener public API: Solana DEX pairs, prices, liquidity,
#     volume, boosts and token profiles (chainId=solana; pair lists are
#     filtered to pairs where the token is the BASE asset).
#   - GeckoTerminal public API: Solana pools, trending/new pools,
#     pool trades and OHLCV.
#   - DeFiLlama public APIs: Solana TVL, protocols, DEX volumes, fees,
#     yields and stablecoins.
#   - Pyth Hermes public API: price-feed directory/metadata. (Pyth's
#     price-update endpoints now require an API key, so live prices
#     here come from Jupiter / Dexscreener / CoinGecko cross-checks;
#     the Pyth endpoints serve the real feed directory data.)
#   - pump.fun public API (frontend-api-v3): new / trending /
#     graduating memecoins.
#   - CoinGecko public API (no key): SOL and Solana-ecosystem market
#     data, global stats, trending.
#   - Magic Eden public API: NFT collection list and stats (small
#     family — it is the only free NFT source that answers reliably).
#   - Alternative.me public API: Fear & Greed index.
#
# No secrets, keys, or private keys anywhere in this file. The only
# wallet address is the payee (Alex's Solana payout wallet,
# receive-only), overridable via SOLANA_WALLET_ADDRESS env.

import os

# --- proxy env sanitization -------------------------------------------
# httpx (used by the x402 facilitator client) builds a URL pattern out of
# every NO_PROXY entry when it constructs a client. Bracketed IPv6
# entries such as "[::1]" make that construction raise
# `httpx.InvalidURL: Invalid port: ':1]'`, which turned every paid
# endpoint into a 500. Drop bracketed entries; the bare addresses stay,
# so localhost/loopback traffic still bypasses the proxy.
for _var in ("NO_PROXY", "no_proxy"):
    _val = os.environ.get(_var)
    if _val:
        _kept = [p.strip() for p in _val.split(",") if p.strip() and "[" not in p and "]" not in p]
        os.environ[_var] = ",".join(_kept)

import asyncio
import hashlib
import math
import sys
import time
import base64
import inspect
import json

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
import httpx

# x402 v2.9.0 imports
from x402 import x402ResourceServer
from x402.http import HTTPFacilitatorClient, FacilitatorConfig
from x402.schemas import SupportedResponse
from x402.http.middleware.fastapi import payment_middleware
from x402.mechanisms.svm.exact import ExactSvmServerScheme
from x402.extensions.bazaar import OutputConfig, declare_discovery_extension

# Load environment variables from .env
load_dotenv()

# Wallet for receiving payments — Alex's Solana payout wallet
# (receive-only; no key material for it exists anywhere in this
# service). Overridable via SOLANA_WALLET_ADDRESS in the environment;
# never put a private key or seed phrase here or in .env.
WALLET_ADDRESS = os.environ.get(
    "SOLANA_WALLET_ADDRESS", "Bkd6W9THp8qKgCYcamEkYx4Rk6TTHkUnRsegMaDZXwz2"
)

# x402 v2 network id (CAIP-2) for Solana mainnet, and the Solana USDC
# mint (6 decimals — same prices as the Base service).
SOLANA_NETWORK = "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp"
SOLANA_USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

# Port: env-overridable, default 4020 (the port v3 has always used).
PORT = int(os.environ.get("PORT", "4020"))

# ===== x402 v2.9.0 SETUP =====

# Facilitator: PayAI — the same facilitator the Base service uses.
# Verified 2026-11-22 against https://facilitator.payai.network/supported:
# it lists x402Version 2, scheme "exact", network
# "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp" (Solana mainnet), and its
# Solana batch-settlement entry names the Solana USDC mint
# EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v as the asset.
# FACILITATOR_URL is env-configurable so a different Solana-capable
# facilitator can be swapped in without a code change.
FACILITATOR_URL = os.environ.get(
    "FACILITATOR_URL", "https://facilitator.payai.network"
)
facilitator = HTTPFacilitatorClient(FacilitatorConfig(url=FACILITATOR_URL))

server = x402ResourceServer(facilitator)
server.register(network=SOLANA_NETWORK, server=ExactSvmServerScheme())

# ===== shared HTTP client (free public APIs, no keys) =====

HTTP_TIMEOUT = 20.0
http_client = httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=True)

SOLANA_RPC = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
JUPITER = "https://lite-api.jup.ag"
DEXSCREENER = "https://api.dexscreener.com"
GECKOTERMINAL = "https://api.geckoterminal.com/api/v2"
LLAMA = "https://api.llama.fi"
YIELDS = "https://yields.llama.fi"
STABLECOINS_API = "https://stablecoins.llama.fi"
COINGECKO = "https://api.coingecko.com/api/v3"
PYTH_HERMES = "https://hermes.pyth.network"
PUMPFUN = "https://frontend-api-v3.pump.fun"
MAGICEDEN = "https://api-mainnet.magiceden.dev/v2"
FNG = "https://api.alternative.me"

LAMPORTS_PER_SOL = 1_000_000_000
U64_MAX = 18446744073709551615

# Well-known Solana programs and mints.
SYSTEM_PROGRAM = "11111111111111111111111111111111"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
STAKE_PROGRAM = "Stake11111111111111111111111111111111111111"
VOTE_PROGRAM = "Vote111111111111111111111111111111111111111"
ATA_PROGRAM = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
WSOL = "So11111111111111111111111111111111111111112"
USDT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"

# Curated Solana majors for batch-price / cross-source endpoints.
MAJOR_MINTS = {
    "SOL": WSOL,
    "USDC": SOLANA_USDC,
    "USDT": USDT,
    "JUP": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
    "JTO": "jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL",
    "PYTH": "HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3",
    "WIF": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",
    "BONK": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
    "RAY": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6P",
}

# ===== tiny TTL cache (public RPC / free APIs are rate-limited) =====

_CACHE = {}


def _cache_get(key):
    ent = _CACHE.get(key)
    if ent and ent[0] > time.time():
        return ent[1]
    return None


def _cache_put(key, value, ttl):
    if ttl and ttl > 0:
        _CACHE[key] = (time.time() + ttl, value)
    return value


# ===== generic JSON GET with cache + one connection retry =====

async def _get_json(url, params=None, ttl=0):
    """GET a public JSON API. Returns the parsed body, or an error dict."""
    key = None
    if ttl:
        key = "GET:" + url + ":" + json.dumps(params or {}, sort_keys=True, default=str)
        hit = _cache_get(key)
        if hit is not None:
            return hit
    last_err = None
    for attempt in range(2):
        try:
            r = await http_client.get(url, params=params)
            if r.status_code != 200:
                return {"error": f"upstream HTTP {r.status_code}", "url": url}
            try:
                data = r.json()
            except Exception:
                return {"error": "upstream returned non-JSON", "url": url}
            if key:
                _cache_put(key, data, ttl)
            return data
        except Exception as e:
            last_err = e
            if attempt == 0:
                await asyncio.sleep(0.6)
    return {"error": str(last_err)[:200], "url": url}


def _is_error(data):
    return isinstance(data, dict) and "error" in data


# ===== Solana JSON-RPC =====

# Per-method cache TTLs (seconds). Freshness-critical reads get tiny
# TTLs; slow-moving cluster data gets longer ones. The public RPC
# rate-limits aggressively, so even 2-5s of caching matters.
_RPC_TTL = {
    "getSlot": 2, "getBlockHeight": 2, "getEpochInfo": 5,
    "getRecentPerformanceSamples": 10, "getRecentPrioritizationFees": 10,
    "getBlockProduction": 30, "getSupply": 30, "getTransactionCount": 5,
    "getVoteAccounts": 60, "getLeaderSchedule": 120, "getClusterNodes": 120,
    "getVersion": 3600, "getGenesisHash": 3600, "getEpochSchedule": 3600,
    "getInflationRate": 300, "getInflationGovernor": 300,
    "getHealth": 20, "minimumLedgerSlot": 10, "getHighestSnapshotSlot": 10,
    "getMaxRetransmitSlot": 5, "getMaxShredInsertSlot": 5,
}

# Methods the public RPC throttles hard: retry with backoff on 429.
_RPC_HEAVY = {
    "getTokenLargestAccounts", "getProgramAccounts", "getBlock",
    "getVoteAccounts", "getClusterNodes", "getLeaderSchedule", "getSupply",
}

_rpc_id = [0]


async def _rpc(method, params=None, ttl=None, retries=None):
    """One Solana JSON-RPC call. Returns the `result` value, or an
    error dict {"error": ...} (the service-wide error convention)."""
    params = params or []
    if ttl is None:
        ttl = _RPC_TTL.get(method, 0)
    key = None
    if ttl:
        key = "RPC:" + method + ":" + json.dumps(params, sort_keys=True, default=str)
        hit = _cache_get(key)
        if hit is not None:
            return hit
    if retries is None:
        retries = 3 if method in _RPC_HEAVY else 1
    last_err = None
    for attempt in range(retries + 1):
        _rpc_id[0] += 1
        try:
            r = await http_client.post(SOLANA_RPC, json={
                "jsonrpc": "2.0", "id": _rpc_id[0], "method": method, "params": params,
            })
            if r.status_code == 429:
                last_err = "upstream HTTP 429"
                if attempt < retries:
                    await asyncio.sleep(0.8 * (attempt + 1))
                    continue
                return {"error": "upstream HTTP 429", "url": SOLANA_RPC}
            if r.status_code != 200:
                return {"error": f"upstream HTTP {r.status_code}", "url": SOLANA_RPC}
            data = r.json()
        except Exception as e:
            last_err = str(e)[:200]
            if attempt < retries:
                await asyncio.sleep(0.6 * (attempt + 1))
                continue
            return {"error": last_err, "url": SOLANA_RPC}
        if isinstance(data, dict) and data.get("error"):
            err = data["error"]
            msg = err.get("message", str(err)) if isinstance(err, dict) else str(err)
            code = err.get("code") if isinstance(err, dict) else None
            if code == -32429 or "429" in str(msg) or "rate" in str(msg).lower():
                if attempt < retries:
                    await asyncio.sleep(0.8 * (attempt + 1))
                    continue
            return {"error": f"RPC error: {msg}", "url": SOLANA_RPC}
        result = data.get("result") if isinstance(data, dict) else None
        if key:
            _cache_put(key, result, ttl)
        return result
    return {"error": str(last_err)[:200], "url": SOLANA_RPC}


# ===== small helpers =====

def _sol(lamports):
    """Lamports -> SOL as a float (None-safe)."""
    if lamports is None:
        return None
    return lamports / LAMPORTS_PER_SOL


def _round(x, nd=6):
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        return round(float(x), nd)
    return x


def _fnum(x):
    """Best-effort float conversion (APIs mix strings and numbers)."""
    try:
        if x is None:
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _is_sol_address(s):
    if not isinstance(s, str) or not (32 <= len(s) <= 44):
        return False
    try:
        from solders.pubkey import Pubkey
        Pubkey.from_string(s)
        return True
    except Exception:
        return False


def _on_curve(s):
    try:
        from solders.pubkey import Pubkey
        return bool(Pubkey.from_string(s).is_on_curve())
    except Exception:
        return None


def _err_addr(param="address"):
    return {"error": f"invalid Solana {param} (expected a base58 public key)"}


# ===== Jupiter (prices + token metadata) =====

async def _jupiter_prices(mints, ttl=15):
    """Jupiter price v3 for a list of mints -> {mint: price-entry}."""
    if not mints:
        return {}
    data = await _get_json(
        f"{JUPITER}/price/v3", params={"ids": ",".join(mints)}, ttl=ttl
    )
    if _is_error(data) or not isinstance(data, dict):
        return data if _is_error(data) else {}
    return data


async def _sol_price_usd():
    """Live SOL price in USD from Jupiter price v3 (cached 30s)."""
    data = await _jupiter_prices([WSOL], ttl=30)
    entry = data.get(WSOL) if isinstance(data, dict) else None
    if isinstance(entry, dict):
        return _fnum(entry.get("usdPrice"))
    return None


async def _jupiter_search(query, ttl=60):
    """Jupiter token search -> list of token metadata entries."""
    data = await _get_json(f"{JUPITER}/tokens/v2/search", params={"query": query}, ttl=ttl)
    if _is_error(data):
        return data
    return data if isinstance(data, list) else []


def _jupiter_token_trim(t):
    stats = t.get("stats24h") or {}
    return {
        "mint": t.get("id"),
        "name": t.get("name"),
        "symbol": t.get("symbol"),
        "decimals": t.get("decimals"),
        "icon": t.get("icon"),
        "tags": t.get("tags"),
        "buy_volume_24h": _round(_fnum(stats.get("buyVolume")), 2),
        "sell_volume_24h": _round(_fnum(stats.get("sellVolume")), 2),
        "holder_count": t.get("holderCount"),
        "fdv": _round(_fnum(t.get("fdv")), 2),
        "market_cap": _round(_fnum(t.get("mcap")), 2),
        "liquidity": _round(_fnum(t.get("liquidity")), 2),
        "price_usd": _round(_fnum(t.get("usdPrice")), 8),
        "organic_score": t.get("organicScore"),
        "is_verified": "verified" in (t.get("tags") or []),
    }


# ===== Dexscreener (Solana pairs; BASE-asset filtering) =====

async def _ds_token_pairs(mint, ttl=20):
    """All Dexscreener pairs for a token on Solana (either side)."""
    data = await _get_json(f"{DEXSCREENER}/latest/dex/tokens/{mint}", ttl=ttl)
    if _is_error(data):
        return data
    pairs = data.get("pairs") if isinstance(data, dict) else None
    if not pairs:
        return {"error": "no DEX pairs found for token", "token": mint}
    return [p for p in pairs if p.get("chainId") == "solana"]


async def _ds_base_pairs(mint, ttl=20):
    """Solana pairs where the token is the BASE asset.

    Dexscreener's token endpoint also returns pairs where the token is
    only the QUOTE currency; counting those as the token's own pairs
    misstates its price/liquidity (the quote-side bug found on the Base
    service). Always filter to base-asset pairs for token statistics.
    """
    pairs = await _ds_token_pairs(mint, ttl=ttl)
    if _is_error(pairs):
        return pairs
    return [p for p in pairs if (p.get("baseToken") or {}).get("address") == mint]


def _best_pair(pairs):
    """Pick the Dexscreener pair with the most liquidity."""
    if not pairs:
        return None
    def liq(p):
        return _fnum((p.get("liquidity") or {}).get("usd")) or 0.0
    return max(pairs, key=liq)


def _trim_pair(p):
    return {
        "chain": p.get("chainId"),
        "dex": p.get("dexId"),
        "pair": p.get("pairAddress"),
        "url": p.get("url"),
        "base_token": (p.get("baseToken") or {}).get("symbol"),
        "base_address": (p.get("baseToken") or {}).get("address"),
        "quote_token": (p.get("quoteToken") or {}).get("symbol"),
        "quote_address": (p.get("quoteToken") or {}).get("address"),
        "price_usd": p.get("priceUsd"),
        "price_native": p.get("priceNative"),
        "liquidity_usd": (p.get("liquidity") or {}).get("usd"),
        "volume_24h": (p.get("volume") or {}).get("h24"),
        "market_cap": p.get("marketCap"),
        "fdv": p.get("fdv"),
        "pair_created_at": p.get("pairCreatedAt"),
    }


async def _ds_search(query, ttl=20):
    """Dexscreener search, Solana pairs only."""
    data = await _get_json(f"{DEXSCREENER}/latest/dex/search", params={"q": query}, ttl=ttl)
    if _is_error(data):
        return data
    pairs = data.get("pairs") if isinstance(data, dict) else None
    if not pairs:
        return {"error": "no DEX pairs found", "query": query}
    return [p for p in pairs if p.get("chainId") == "solana"]


async def _ds_boosts(kind, ttl=30):
    """Dexscreener token boosts ('top' or 'latest'), Solana only."""
    data = await _get_json(f"{DEXSCREENER}/token-boosts/{kind}/v1", ttl=ttl)
    if _is_error(data):
        return data
    items = data if isinstance(data, list) else []
    return [b for b in items if b.get("chainId") == "solana"]


def _trim_boost(b):
    return {
        "token": b.get("tokenAddress"),
        "chain": b.get("chainId"),
        "url": b.get("url"),
        "description": (b.get("description") or "")[:200] or None,
        "boost_amount": b.get("amount"),
        "boost_total": b.get("totalAmount"),
        "links": b.get("links"),
    }


# ===== GeckoTerminal (Solana pools) =====

async def _gt(path, ttl=30):
    return await _get_json(f"{GECKOTERMINAL}{path}", ttl=ttl)


def _gt_pool_trim(pool):
    a = pool.get("attributes") or {}
    rel = pool.get("relationships") or {}
    return {
        "pool": a.get("address"),
        "name": a.get("name"),
        "dex": ((rel.get("dex") or {}).get("data") or {}).get("id"),
        "base_token": ((rel.get("base_token") or {}).get("data") or {}).get("id"),
        "quote_token": ((rel.get("quote_token") or {}).get("data") or {}).get("id"),
        "base_price_usd": _fnum(a.get("base_token_price_usd")),
        "quote_price_usd": _fnum(a.get("quote_token_price_usd")),
        "reserve_usd": _round(_fnum(a.get("reserve_in_usd")), 2),
        "volume_24h_usd": _round(_fnum((a.get("volume_usd") or {}).get("h24")), 2),
        "volume_1h_usd": _round(_fnum((a.get("volume_usd") or {}).get("h1")), 2),
        "price_change_24h_pct": _fnum((a.get("price_change_percentage") or {}).get("h24")),
        "txns_24h": (a.get("transactions") or {}).get("h24"),
        "created_at": a.get("pool_created_at"),
        "fdv_usd": _round(_fnum(a.get("fdv_usd")), 2),
        "market_cap_usd": _round(_fnum(a.get("market_cap_usd")), 2),
    }


# ===== DeFiLlama (cached heavy payloads) =====

async def _llama_protocols(ttl=300):
    return await _get_json(f"{LLAMA}/protocols", ttl=ttl)


async def _llama_chains(ttl=120):
    return await _get_json(f"{LLAMA}/v2/chains", ttl=ttl)


async def _llama_yield_pools(ttl=300):
    data = await _get_json(f"{YIELDS}/pools", ttl=ttl)
    if _is_error(data):
        return data
    pools = data.get("data") if isinstance(data, dict) else None
    return pools or []


async def _llama_stablecoins(ttl=300):
    data = await _get_json(
        f"{STABLECOINS_API}/stablecoins", params={"includePrices": "true"}, ttl=ttl
    )
    if _is_error(data):
        return data
    return data.get("peggedAssets") if isinstance(data, dict) else None


def _solana_protocols(protocols):
    """Protocols with a Solana TVL entry, richest first."""
    out = []
    for p in protocols or []:
        ct = (p.get("chainTvls") or {}).get("Solana")
        if ct:
            out.append(p)
    out.sort(key=lambda p: -((p.get("chainTvls") or {}).get("Solana") or 0))
    return out


def _trim_protocol(p):
    return {
        "name": p.get("name"),
        "slug": p.get("slug"),
        "symbol": p.get("symbol"),
        "category": p.get("category"),
        "solana_tvl": _round(_fnum((p.get("chainTvls") or {}).get("Solana")), 2),
        "total_tvl": _round(_fnum(p.get("tvl")), 2),
        "change_1d_pct": _round(_fnum(p.get("change_1d")), 3),
        "change_7d_pct": _round(_fnum(p.get("change_7d")), 3),
        "url": p.get("url"),
    }


def _trim_yield_pool(p):
    return {
        "pool": p.get("pool"),
        "project": p.get("project"),
        "symbol": p.get("symbol"),
        "chain": p.get("chain"),
        "tvl_usd": _round(_fnum(p.get("tvlUsd")), 2),
        "apy": _round(_fnum(p.get("apy")), 4),
        "apy_base": _round(_fnum(p.get("apyBase")), 4),
        "apy_reward": _round(_fnum(p.get("apyReward")), 4),
        "stablecoin": p.get("stablecoin"),
        "il_risk": p.get("ilRisk"),
        "exposure": p.get("exposure"),
    }


def _overview_top_protocols(overview, limit=10):
    """Top protocols from a DeFiLlama overview payload (dexs/fees).

    The overview response shape varies: prefer its `protocols` list
    (entries carry total24h), then breakdown24h, then the last day of
    totalDataChartBreakdown. Returns [] if none is usable.
    """
    if not isinstance(overview, dict):
        return []
    protos = overview.get("protocols")
    if isinstance(protos, list) and protos:
        rows = []
        for p in protos:
            if not isinstance(p, dict):
                continue
            total = _fnum(p.get("total24h"))
            if total is None:
                continue
            rows.append({
                "name": p.get("displayName") or p.get("name"),
                "slug": p.get("slug") or p.get("name"),
                "total_24h": _round(total, 2),
                "change_1d_pct": _round(_fnum(p.get("change_1d")), 3),
            })
        rows.sort(key=lambda r: -(r["total_24h"] or 0))
        if rows:
            return rows[:limit]
    breakdown = overview.get("breakdown24h")
    if isinstance(breakdown, dict) and breakdown:
        rows = []
        for chain_map in breakdown.values():
            if isinstance(chain_map, dict):
                for name, val in chain_map.items():
                    v = _fnum(val[0] if isinstance(val, list) and val else val)
                    if v is not None:
                        rows.append({"name": name, "slug": name, "total_24h": _round(v, 2),
                                     "change_1d_pct": None})
        rows.sort(key=lambda r: -(r["total_24h"] or 0))
        if rows:
            return rows[:limit]
    chart_bd = overview.get("totalDataChartBreakdown")
    if isinstance(chart_bd, list) and chart_bd:
        last = chart_bd[-1]
        if isinstance(last, list) and len(last) == 2 and isinstance(last[1], dict):
            rows = [{"name": k, "slug": k, "total_24h": _round(_fnum(v), 2),
                     "change_1d_pct": None}
                    for k, v in last[1].items() if _fnum(v) is not None]
            rows.sort(key=lambda r: -(r["total_24h"] or 0))
            return rows[:limit]
    return []


# ===== Pyth Hermes (feed directory — price updates are key-gated) =====

async def _pyth_feeds(query=None, ttl=600):
    params = {"asset_type": "crypto"}
    if query:
        params["query"] = query
    return await _get_json(f"{PYTH_HERMES}/v2/price_feeds", params=params, ttl=ttl)


def _trim_pyth_feed(f):
    a = f.get("attributes") or {}
    return {
        "id": f.get("id"),
        "symbol": a.get("symbol"),
        "base": a.get("base"),
        "quote_currency": a.get("quote_currency"),
        "display_symbol": a.get("display_symbol"),
        "asset_type": a.get("asset_type"),
        "market_open": (f.get("market_hours") or {}).get("is_open"),
    }


# ===== SPL token helpers (RPC) =====

async def _mint_info(mint):
    """Parsed mint account info via getAccountInfo (jsonParsed)."""
    res = await _rpc("getAccountInfo", [mint, {"encoding": "jsonParsed"}])
    if _is_error(res):
        return res
    value = (res or {}).get("value")
    if not value:
        return {"error": "account not found", "mint": mint}
    parsed = ((value.get("data") or {}).get("parsed") or {})
    if parsed.get("type") != "mint":
        return {"error": "account is not an SPL token mint", "mint": mint,
                "owner_program": value.get("owner")}
    return {"info": parsed.get("info") or {}, "owner_program": value.get("owner"),
            "lamports": value.get("lamports")}


async def _token_accounts_by_owner(owner, mint=None, program=None):
    """getTokenAccountsByOwner (jsonParsed). Returns the value list.

    The RPC filter map must contain a SINGLE key — {mint} XOR
    {programId}; passing both is rejected ("expected map with a single
    key"). When both are requested, query by mint (the canonical form;
    the node resolves the mint's token program) and post-filter the
    returned accounts by their owner program.
    """
    filt = {}
    if mint:
        filt["mint"] = mint
    elif program:
        filt["programId"] = program
    res = await _rpc("getTokenAccountsByOwner", [owner, filt, {"encoding": "jsonParsed"}])
    if _is_error(res):
        return res
    value = (res or {}).get("value") or []
    if mint and program:
        value = [e for e in value
                 if (e.get("account") or {}).get("owner") == program]
    return value


def _trim_token_account(entry, program=None):
    acct = entry.get("account") or {}
    parsed = ((acct.get("data") or {}).get("parsed") or {})
    info = parsed.get("info") or {}
    amt = info.get("tokenAmount") or {}
    return {
        "account": entry.get("pubkey"),
        "mint": info.get("mint"),
        "owner": info.get("owner"),
        "amount": amt.get("uiAmount"),
        "amount_raw": amt.get("amount"),
        "decimals": amt.get("decimals"),
        "program": program,
        "lamports": acct.get("lamports"),
        "state": info.get("state"),
    }


# ===== endpoint registry =====
# Every paid endpoint registers here. The FastAPI routes and the x402
# payment config are both built from this list at the bottom of the file,
# so the two can never drift apart.

REGISTRY = []


def endpoint(path, price, description, category, tags, eid):
    """Decorator: register a paid endpoint."""
    def deco(fn):
        REGISTRY.append({
            "id": eid,
            "path": path,
            "price": price,
            "description": description,
            "category": category,
            "tags": tags,
            "handler": fn,
        })
        return fn
    return deco


# ===== SERVICE STATUS =====

@endpoint("/mcp/status", "$0.01",
          "MCP server status for the Solana x402 service. $0.01 USDC per call.",
          "status", ["status", "mcp", "solana"], "mcp-status")
async def mcp_status():
    return {"status": "online", "version": "5.0", "endpoints": len(REGISTRY) + 1,
            "network": "solana", "data_chain": "solana", "x402_version": "2.9.0"}


@endpoint("/mcp/tools", "$0.01",
          "MCP tools list for the Solana x402 service. $0.01 USDC per call.",
          "status", ["tools", "mcp", "solana"], "mcp-tools")
async def mcp_tools():
    # Generated from the registry at request time, so it always matches
    # the live route set.
    return {
        "tools": [
            {"name": e["id"], "price": e["price"].lstrip("$"), "endpoint": e["path"]}
            for e in REGISTRY
        ],
        "pricing_model": "per_call_usdc_on_solana",
        "wallet": WALLET_ADDRESS,
        "x402_version": "2.9.0",
    }


# ===== CHAIN (Solana RPC) =====

@endpoint("/chain/slot", "$0.01",
          "Current Solana slot (finalized). $0.01 USDC per call.",
          "chain", ["chain", "slot", "solana"], "chain-slot")
async def chain_slot():
    slot = await _rpc("getSlot", [{"commitment": "finalized"}])
    if _is_error(slot):
        return slot
    height = await _rpc("getBlockHeight", [{"commitment": "finalized"}])
    return {"slot": slot, "block_height": height if not _is_error(height) else None,
            "network": "solana"}


@endpoint("/chain/block/latest", "$0.01",
          "Latest finalized Solana block summary: hash, height, time, transaction count. $0.01 USDC per call.",
          "chain", ["chain", "block", "solana"], "chain-block-latest")
async def chain_block_latest():
    slot = await _rpc("getSlot", [{"commitment": "finalized"}])
    if _is_error(slot):
        return slot
    block = await _rpc("getBlock", [slot, {"encoding": "json",
                                           "transactionDetails": "signatures",
                                           "rewards": False,
                                           "maxSupportedTransactionVersion": 0}])
    if _is_error(block) or not block:
        return block if _is_error(block) else {"error": "block not available", "slot": slot}
    sigs = block.get("signatures") or []
    return {
        "slot": slot,
        "block_height": block.get("blockHeight"),
        "blockhash": block.get("blockhash"),
        "parent_slot": block.get("parentSlot"),
        "previous_blockhash": block.get("previousBlockhash"),
        "block_time": block.get("blockTime"),
        "transaction_count": len(sigs),
        "first_signatures": sigs[:5],
        "network": "solana",
    }


@endpoint("/chain/block/{slot}", "$0.01",
          "One Solana block by slot: hash, height, time, parent, transaction count and sample signatures. $0.01 USDC per call.",
          "chain", ["chain", "block", "solana"], "chain-block")
async def chain_block(slot: int):
    block = await _rpc("getBlock", [slot, {"encoding": "json",
                                           "transactionDetails": "signatures",
                                           "rewards": False,
                                           "maxSupportedTransactionVersion": 0}])
    if _is_error(block):
        return block
    if not block:
        return {"error": "block not available (pruned or skipped slot)", "slot": slot}
    sigs = block.get("signatures") or []
    return {
        "slot": slot,
        "block_height": block.get("blockHeight"),
        "blockhash": block.get("blockhash"),
        "parent_slot": block.get("parentSlot"),
        "previous_blockhash": block.get("previousBlockhash"),
        "block_time": block.get("blockTime"),
        "transaction_count": len(sigs),
        "signatures": sigs[:25],
        "network": "solana",
    }


@endpoint("/chain/block/{slot}/signatures", "$0.01",
          "Transaction signatures included in one Solana block (first 50 plus total count). $0.01 USDC per call.",
          "chain", ["chain", "block", "signatures"], "chain-block-signatures")
async def chain_block_signatures(slot: int):
    block = await _rpc("getBlock", [slot, {"encoding": "json",
                                           "transactionDetails": "signatures",
                                           "rewards": False,
                                           "maxSupportedTransactionVersion": 0}])
    if _is_error(block):
        return block
    if not block:
        return {"error": "block not available (pruned or skipped slot)", "slot": slot}
    sigs = block.get("signatures") or []
    return {"slot": slot, "count": len(sigs), "signatures": sigs[:50], "network": "solana"}


@endpoint("/chain/block-time/{slot}", "$0.01",
          "Estimated Unix timestamp of one Solana slot (getBlockTime). $0.01 USDC per call.",
          "chain", ["chain", "block", "time"], "chain-block-time")
async def chain_block_time(slot: int):
    t = await _rpc("getBlockTime", [slot])
    if _is_error(t):
        return t
    return {"slot": slot, "block_time": t, "network": "solana",
            "note": None if t is not None else "no block was produced at this slot"}


@endpoint("/chain/blocks/recent", "$0.01",
          "The most recent slots that produced blocks on Solana (getBlocksWithLimit). $0.01 USDC per call.",
          "chain", ["chain", "blocks", "recent"], "chain-blocks-recent")
async def chain_blocks_recent():
    slot = await _rpc("getSlot", [{"commitment": "finalized"}])
    if _is_error(slot):
        return slot
    start = max(slot - 1000, 0)
    blocks = await _rpc("getBlocksWithLimit", [start, 25])
    if _is_error(blocks):
        return blocks
    return {"current_slot": slot, "recent_block_slots": blocks,
            "count": len(blocks or []), "network": "solana"}


@endpoint("/chain/block-production", "$0.01",
          "Solana block production for the current epoch: validator count, blocks produced, leader slots, top producers. $0.01 USDC per call.",
          "chain", ["chain", "validators", "blocks"], "chain-block-production")
async def chain_block_production():
    res = await _rpc("getBlockProduction", [])
    if _is_error(res):
        return res
    value = (res or {}).get("value") or {}
    by_id = value.get("byIdentity") or {}
    rows = [{"identity": k, "leader_slots": v[0], "blocks_produced": v[1]}
            for k, v in by_id.items() if isinstance(v, list) and len(v) == 2]
    rows.sort(key=lambda r: -r["blocks_produced"])
    return {
        "range": value.get("range"),
        "validator_count": len(rows),
        "total_blocks_produced": sum(r["blocks_produced"] for r in rows),
        "total_leader_slots": sum(r["leader_slots"] for r in rows),
        "top_producers": rows[:5],
        "network": "solana",
    }


@endpoint("/chain/block-production/{identity}", "$0.01",
          "Block production of one Solana validator identity in the current epoch. $0.01 USDC per call.",
          "chain", ["chain", "validators", "blocks"], "chain-block-production-validator")
async def chain_block_production_validator(identity: str):
    if not _is_sol_address(identity):
        return _err_addr("identity")
    res = await _rpc("getBlockProduction", [{"identity": identity}])
    if _is_error(res):
        return res
    value = (res or {}).get("value") or {}
    entry = (value.get("byIdentity") or {}).get(identity)
    return {
        "identity": identity,
        "range": value.get("range"),
        "leader_slots": entry[0] if entry else 0,
        "blocks_produced": entry[1] if entry else 0,
        "network": "solana",
    }


@endpoint("/chain/epoch/info", "$0.01",
          "Current Solana epoch info: epoch, absolute slot, slot index, slots in epoch, block height, transaction count. $0.01 USDC per call.",
          "chain", ["chain", "epoch", "solana"], "chain-epoch-info")
async def chain_epoch_info():
    info = await _rpc("getEpochInfo", [])
    if _is_error(info):
        return info
    info = dict(info or {})
    info["network"] = "solana"
    return info


@endpoint("/chain/epoch/schedule", "$0.01",
          "Solana epoch schedule: slots per epoch, leader schedule offset, warmup, first normal epoch/slot. $0.01 USDC per call.",
          "chain", ["chain", "epoch", "schedule"], "chain-epoch-schedule")
async def chain_epoch_schedule():
    sched = await _rpc("getEpochSchedule", [])
    if _is_error(sched):
        return sched
    sched = dict(sched or {})
    sched["network"] = "solana"
    return sched


@endpoint("/chain/epoch/progress", "$0.01",
          "Progress through the current Solana epoch: percent complete, slots remaining, estimated time to epoch end. $0.01 USDC per call.",
          "chain", ["chain", "epoch", "progress"], "chain-epoch-progress")
async def chain_epoch_progress():
    info = await _rpc("getEpochInfo", [])
    if _is_error(info):
        return info
    samples = await _rpc("getRecentPerformanceSamples", [12])
    slot_secs = None
    if not _is_error(samples) and samples:
        tot_slots = sum(s.get("numSlots") or 0 for s in samples)
        tot_secs = sum(s.get("samplePeriodSecs") or 0 for s in samples)
        if tot_slots and tot_secs:
            slot_secs = tot_secs / tot_slots
    idx = info.get("slotIndex") or 0
    total = info.get("slotsInEpoch") or 432000
    remaining = total - idx
    return {
        "epoch": info.get("epoch"),
        "slot_index": idx,
        "slots_in_epoch": total,
        "percent_complete": round(100.0 * idx / total, 3) if total else None,
        "slots_remaining": remaining,
        "avg_slot_seconds": _round(slot_secs, 4),
        "estimated_seconds_remaining": int(remaining * slot_secs) if slot_secs else None,
        "estimated_end_unix": int(time.time() + remaining * slot_secs) if slot_secs else None,
        "network": "solana",
    }


@endpoint("/chain/leader-schedule/{epoch}", "$0.01",
          "Leader schedule summary for one Solana epoch: validator count, total leader slots, top leaders. $0.01 USDC per call.",
          "chain", ["chain", "epoch", "leaders"], "chain-leader-schedule")
async def chain_leader_schedule(epoch: int):
    sched = await _rpc("getEpochSchedule", [])
    if _is_error(sched):
        return sched
    slots_per_epoch = sched.get("slotsPerEpoch") or 432000
    first_normal_slot = sched.get("firstNormalSlot") or 0
    first_slot = first_normal_slot + epoch * slots_per_epoch
    res = await _rpc("getLeaderSchedule", [first_slot, {"commitment": "finalized"}])
    if _is_error(res):
        return res
    if not res:
        return {"error": "no leader schedule available for epoch", "epoch": epoch}
    rows = [{"identity": k, "leader_slots": len(v)} for k, v in res.items()]
    rows.sort(key=lambda r: -r["leader_slots"])
    return {
        "epoch": epoch,
        "epoch_first_slot": first_slot,
        "validator_count": len(rows),
        "total_leader_slots": sum(r["leader_slots"] for r in rows),
        "top_leaders": rows[:10],
        "network": "solana",
    }


@endpoint("/chain/slot-leaders/{start}/{limit}", "$0.01",
          "Solana slot leaders starting at a slot (limit capped at 100). $0.01 USDC per call.",
          "chain", ["chain", "leaders", "slots"], "chain-slot-leaders")
async def chain_slot_leaders(start: int, limit: int):
    limit = max(1, min(limit, 100))
    leaders = await _rpc("getSlotLeaders", [start, limit])
    if _is_error(leaders):
        return leaders
    return {"start_slot": start, "count": len(leaders or []),
            "leaders": leaders, "network": "solana"}


@endpoint("/chain/supply", "$0.01",
          "Solana supply: total, circulating and non-circulating SOL (non-circulating account list replaced by its count). $0.01 USDC per call.",
          "chain", ["chain", "supply", "sol"], "chain-supply")
async def chain_supply():
    res = await _rpc("getSupply", [{"excludeNonCirculatingAccountsList": True}])
    if _is_error(res):
        # Some nodes ignore the exclude flag; retry plain and trim locally.
        res = await _rpc("getSupply", [])
        if _is_error(res):
            return res
    value = (res or {}).get("value") or {}
    accounts = value.get("nonCirculatingAccounts") or []
    total = value.get("total")
    circ = value.get("circulating")
    return {
        "total_lamports": total,
        "total_sol": _round(_sol(total), 2),
        "circulating_lamports": circ,
        "circulating_sol": _round(_sol(circ), 2),
        "non_circulating_lamports": value.get("nonCirculating"),
        "non_circulating_sol": _round(_sol(value.get("nonCirculating")), 2),
        "non_circulating_accounts": len(accounts),
        "circulating_percent": round(100.0 * circ / total, 3) if total and circ else None,
        "network": "solana",
    }


@endpoint("/chain/supply/circulating", "$0.01",
          "Circulating SOL supply with its USD value at the live SOL price. $0.01 USDC per call.",
          "chain", ["chain", "supply", "circulating"], "chain-supply-circulating")
async def chain_supply_circulating():
    res = await _rpc("getSupply", [{"excludeNonCirculatingAccountsList": True}])
    if _is_error(res):
        res = await _rpc("getSupply", [])
        if _is_error(res):
            return res
    value = (res or {}).get("value") or {}
    circ_sol = _sol(value.get("circulating"))
    price = await _sol_price_usd()
    return {
        "circulating_sol": _round(circ_sol, 2),
        "sol_price_usd": _round(price, 4),
        "circulating_value_usd": _round(circ_sol * price, 2) if circ_sol and price else None,
        "network": "solana",
    }


@endpoint("/chain/inflation", "$0.01",
          "Current Solana inflation rate: total, validator and foundation percentages for the current epoch. $0.01 USDC per call.",
          "chain", ["chain", "inflation", "sol"], "chain-inflation")
async def chain_inflation():
    rate = await _rpc("getInflationRate", [])
    if _is_error(rate):
        return rate
    rate = dict(rate or {})
    rate["network"] = "solana"
    return rate


@endpoint("/chain/inflation/governor", "$0.01",
          "Solana inflation governor parameters: initial, terminal, taper, foundation percent and term. $0.01 USDC per call.",
          "chain", ["chain", "inflation", "governor"], "chain-inflation-governor")
async def chain_inflation_governor():
    gov = await _rpc("getInflationGovernor", [])
    if _is_error(gov):
        return gov
    gov = dict(gov or {})
    gov["network"] = "solana"
    return gov


@endpoint("/chain/performance", "$0.01",
          "Latest Solana performance sample: transactions per second over the most recent sample window. $0.01 USDC per call.",
          "chain", ["chain", "performance", "tps"], "chain-performance")
async def chain_performance():
    samples = await _rpc("getRecentPerformanceSamples", [1])
    if _is_error(samples):
        return samples
    if not samples:
        return {"error": "no performance samples returned"}
    s = samples[0]
    secs = s.get("samplePeriodSecs") or 0
    return {
        "slot": s.get("slot"),
        "sample_period_secs": secs,
        "num_slots": s.get("numSlots"),
        "num_transactions": s.get("numTransactions"),
        "num_non_vote_transactions": s.get("numNonVoteTransactions"),
        "tps": round(s["numTransactions"] / secs, 2) if secs else None,
        "non_vote_tps": round(s["numNonVoteTransactions"] / secs, 2) if secs else None,
        "network": "solana",
    }


@endpoint("/chain/performance/{limit}", "$0.01",
          "Recent Solana performance samples with average TPS (limit capped at 60). $0.01 USDC per call.",
          "chain", ["chain", "performance", "tps"], "chain-performance-history")
async def chain_performance_history(limit: int):
    limit = max(1, min(limit, 60))
    samples = await _rpc("getRecentPerformanceSamples", [limit])
    if _is_error(samples):
        return samples
    rows = []
    tps_vals = []
    for s in samples or []:
        secs = s.get("samplePeriodSecs") or 0
        tps = round(s["numTransactions"] / secs, 2) if secs and s.get("numTransactions") is not None else None
        if tps is not None:
            tps_vals.append(tps)
        rows.append({"slot": s.get("slot"), "num_slots": s.get("numSlots"),
                     "num_transactions": s.get("numTransactions"),
                     "num_non_vote_transactions": s.get("numNonVoteTransactions"),
                     "sample_period_secs": secs, "tps": tps})
    return {
        "samples": rows,
        "count": len(rows),
        "avg_tps": round(sum(tps_vals) / len(tps_vals), 2) if tps_vals else None,
        "network": "solana",
    }


@endpoint("/chain/version", "$0.01",
          "Solana cluster software version and feature set of the RPC node. $0.01 USDC per call.",
          "chain", ["chain", "version", "node"], "chain-version")
async def chain_version():
    v = await _rpc("getVersion", [])
    if _is_error(v):
        return v
    return {"solana_core": (v or {}).get("solana-core"),
            "feature_set": (v or {}).get("feature-set"), "network": "solana"}


@endpoint("/chain/cluster-nodes", "$0.01",
          "Solana cluster nodes summary: node count, nodes with RPC, version histogram, sample nodes. $0.01 USDC per call.",
          "chain", ["chain", "nodes", "cluster"], "chain-cluster-nodes")
async def chain_cluster_nodes():
    nodes = await _rpc("getClusterNodes", [])
    if _is_error(nodes):
        return nodes
    nodes = nodes or []
    versions = {}
    for n in nodes:
        ver = n.get("version") or "unknown"
        versions[ver] = versions.get(ver, 0) + 1
    top_versions = sorted(versions.items(), key=lambda kv: -kv[1])[:5]
    return {
        "node_count": len(nodes),
        "nodes_with_rpc": sum(1 for n in nodes if n.get("rpc")),
        "versions": [{"version": v, "nodes": c} for v, c in top_versions],
        "sample": [{"pubkey": n.get("pubkey"), "gossip": n.get("gossip"),
                    "version": n.get("version")} for n in nodes[:10]],
        "network": "solana",
    }


@endpoint("/chain/vote-accounts", "$0.01",
          "Solana validator vote accounts summary: current and delinquent counts, total activated stake, largest validator share. $0.01 USDC per call.",
          "chain", ["chain", "validators", "stake"], "chain-vote-accounts")
async def chain_vote_accounts():
    res = await _rpc("getVoteAccounts", [])
    if _is_error(res):
        return res
    current = (res or {}).get("current") or []
    delinquent = (res or {}).get("delinquent") or []
    total_stake = sum(v.get("activatedStake") or 0 for v in current)
    top = max(current, key=lambda v: v.get("activatedStake") or 0) if current else None
    return {
        "current_validators": len(current),
        "delinquent_validators": len(delinquent),
        "total_activated_stake_sol": _round(_sol(total_stake), 2),
        "largest_validator": {
            "vote_pubkey": top.get("votePubkey"),
            "node_pubkey": top.get("nodePubkey"),
            "stake_sol": _round(_sol(top.get("activatedStake")), 2),
            "share_percent": round(100.0 * (top.get("activatedStake") or 0) / total_stake, 3) if total_stake else None,
        } if top else None,
        "network": "solana",
    }


@endpoint("/chain/vote-accounts/current", "$0.01",
          "Top 20 current Solana validators by activated stake: vote account, node identity, stake, commission, last vote. $0.01 USDC per call.",
          "chain", ["chain", "validators", "stake"], "chain-vote-accounts-current")
async def chain_vote_accounts_current():
    res = await _rpc("getVoteAccounts", [])
    if _is_error(res):
        return res
    current = sorted((res or {}).get("current") or [],
                     key=lambda v: -(v.get("activatedStake") or 0))[:20]
    return {
        "validators": [{
            "vote_pubkey": v.get("votePubkey"),
            "node_pubkey": v.get("nodePubkey"),
            "stake_sol": _round(_sol(v.get("activatedStake")), 4),
            "commission": v.get("commission"),
            "last_vote": v.get("lastVote"),
            "root_slot": v.get("rootSlot"),
        } for v in current],
        "count": len(current),
        "network": "solana",
    }


@endpoint("/chain/vote-accounts/delinquent", "$0.01",
          "Delinquent Solana validators: count and the 20 with the most stake at risk. $0.01 USDC per call.",
          "chain", ["chain", "validators", "delinquent"], "chain-vote-accounts-delinquent")
async def chain_vote_accounts_delinquent():
    res = await _rpc("getVoteAccounts", [])
    if _is_error(res):
        return res
    delinquent = sorted((res or {}).get("delinquent") or [],
                        key=lambda v: -(v.get("activatedStake") or 0))
    return {
        "delinquent_count": len(delinquent),
        "validators": [{
            "vote_pubkey": v.get("votePubkey"),
            "node_pubkey": v.get("nodePubkey"),
            "stake_sol": _round(_sol(v.get("activatedStake")), 4),
            "commission": v.get("commission"),
            "last_vote": v.get("lastVote"),
        } for v in delinquent[:20]],
        "network": "solana",
    }


@endpoint("/chain/block-commitment/{slot}", "$0.01",
          "Block commitment for one Solana slot: per-cluster stake commitment levels and total active stake. $0.01 USDC per call.",
          "chain", ["chain", "block", "commitment"], "chain-block-commitment")
async def chain_block_commitment(slot: int):
    res = await _rpc("getBlockCommitment", [slot])
    if _is_error(res):
        return res
    commitment = (res or {}).get("commitment")
    return {
        "slot": slot,
        "total_stake_lamports": (res or {}).get("totalStake"),
        "total_stake_sol": _round(_sol((res or {}).get("totalStake")), 2),
        "commitment_levels": commitment,
        "note": None if commitment else "commitment is only tracked for recent slots",
        "network": "solana",
    }


@endpoint("/chain/prioritization-fees", "$0.01",
          "Recent Solana prioritization fees (micro-lamports per compute unit): min, median, p75, p90, max across recent slots. $0.01 USDC per call.",
          "chain", ["chain", "fees", "priority"], "chain-prioritization-fees")
async def chain_prioritization_fees():
    fees = await _rpc("getRecentPrioritizationFees", [])
    if _is_error(fees):
        return fees
    vals = sorted(f.get("prioritizationFee") or 0 for f in (fees or []))
    if not vals:
        return {"error": "no prioritization fee data returned"}
    def pct(p):
        return vals[min(len(vals) - 1, int(len(vals) * p))]
    nonzero = [v for v in vals if v > 0]
    return {
        "slots_sampled": len(vals),
        "min_micro_lamports": vals[0],
        "median_micro_lamports": vals[len(vals) // 2],
        "p75_micro_lamports": pct(0.75),
        "p90_micro_lamports": pct(0.90),
        "max_micro_lamports": vals[-1],
        "slots_with_fees_percent": round(100.0 * len(nonzero) / len(vals), 1),
        "network": "solana",
    }


@endpoint("/chain/prioritization-fees/{address}", "$0.01",
          "Recent prioritization fees for transactions writing to one Solana account: median, p90 and max. $0.01 USDC per call.",
          "chain", ["chain", "fees", "priority"], "chain-prioritization-fees-account")
async def chain_prioritization_fees_account(address: str):
    if not _is_sol_address(address):
        return _err_addr()
    fees = await _rpc("getRecentPrioritizationFees", [[address]])
    if _is_error(fees):
        return fees
    vals = sorted(f.get("prioritizationFee") or 0 for f in (fees or []))
    if not vals:
        return {"error": "no prioritization fee data for account", "address": address}
    return {
        "address": address,
        "slots_sampled": len(vals),
        "median_micro_lamports": vals[len(vals) // 2],
        "p90_micro_lamports": vals[min(len(vals) - 1, int(len(vals) * 0.9))],
        "max_micro_lamports": vals[-1],
        "network": "solana",
    }


@endpoint("/chain/minimum-ledger-slot", "$0.01",
          "Minimum ledger slot still available on the Solana RPC node. $0.01 USDC per call.",
          "chain", ["chain", "ledger", "slot"], "chain-minimum-ledger-slot")
async def chain_minimum_ledger_slot():
    slot = await _rpc("minimumLedgerSlot", [])
    if _is_error(slot):
        return slot
    return {"minimum_ledger_slot": slot, "network": "solana"}


@endpoint("/chain/genesis-hash", "$0.01",
          "Solana mainnet genesis hash. $0.01 USDC per call.",
          "chain", ["chain", "genesis"], "chain-genesis-hash")
async def chain_genesis_hash():
    h = await _rpc("getGenesisHash", [])
    if _is_error(h):
        return h
    return {"genesis_hash": h, "network": "solana"}


@endpoint("/chain/identity", "$0.01",
          "Identity public key of the Solana RPC node answering. $0.01 USDC per call.",
          "chain", ["chain", "node", "identity"], "chain-identity")
async def chain_identity():
    res = await _rpc("getIdentity", [])
    if _is_error(res):
        return res
    return {"identity": (res or {}).get("identity"), "network": "solana"}


@endpoint("/chain/health", "$0.01",
          "Solana cluster health as reported by the RPC node (getHealth). $0.01 USDC per call.",
          "chain", ["chain", "health", "node"], "chain-health")
async def chain_health():
    h = await _rpc("getHealth", [])
    if _is_error(h):
        return h
    return {"health": h, "network": "solana"}


@endpoint("/chain/snapshot-slot", "$0.01",
          "Highest full and incremental snapshot slots on the Solana RPC node. $0.01 USDC per call.",
          "chain", ["chain", "snapshot", "node"], "chain-snapshot-slot")
async def chain_snapshot_slot():
    res = await _rpc("getHighestSnapshotSlot", [])
    if _is_error(res):
        return res
    return {"full": (res or {}).get("full"),
            "incremental": (res or {}).get("incremental"), "network": "solana"}


@endpoint("/chain/transaction-count", "$0.01",
          "Total transaction count processed by the Solana cluster. $0.01 USDC per call.",
          "chain", ["chain", "transactions", "count"], "chain-transaction-count")
async def chain_transaction_count():
    n = await _rpc("getTransactionCount", [])
    if _is_error(n):
        return n
    return {"transaction_count": n, "network": "solana"}


@endpoint("/chain/stake-activation/{address}", "$0.01",
          "Activation state of one Solana stake account, computed from its on-chain delegation epochs (the legacy getStakeActivation RPC was removed). $0.01 USDC per call.",
          "chain", ["chain", "stake", "activation"], "chain-stake-activation")
async def chain_stake_activation(address: str):
    if not _is_sol_address(address):
        return _err_addr("stake account")
    res = await _rpc("getAccountInfo", [address, {"encoding": "jsonParsed"}])
    if _is_error(res):
        return res
    value = (res or {}).get("value")
    if not value:
        return {"error": "account not found", "address": address}
    if value.get("owner") != STAKE_PROGRAM:
        return {"error": "account is not a stake account", "address": address,
                "owner_program": value.get("owner")}
    parsed = ((value.get("data") or {}).get("parsed") or {})
    info = parsed.get("info") or {}
    stake = info.get("stake")
    epoch_info = await _rpc("getEpochInfo", [])
    epoch = epoch_info.get("epoch") if isinstance(epoch_info, dict) else None
    if not stake:
        return {"address": address, "state": "initialized",
                "note": "stake account exists but has no delegation",
                "lamports": value.get("lamports"), "network": "solana"}
    delegation = stake.get("delegation") or {}
    act_epoch = delegation.get("activationEpoch")
    deact_epoch = delegation.get("deactivationEpoch")
    stake_lamports = delegation.get("stake")
    if deact_epoch is not None and deact_epoch != U64_MAX:
        state = "deactivating" if (epoch is not None and deact_epoch > epoch) else "inactive"
    elif act_epoch is not None and epoch is not None:
        if act_epoch < epoch:
            state = "active"
        elif act_epoch == epoch:
            state = "activating"
        else:
            state = "inactive"
    else:
        state = "unknown"
    return {
        "address": address,
        "state": state,
        "current_epoch": epoch,
        "activation_epoch": act_epoch,
        "deactivation_epoch": None if deact_epoch == U64_MAX else deact_epoch,
        "delegated_stake_lamports": stake_lamports,
        "delegated_stake_sol": _round(_sol(stake_lamports), 6),
        "voter_pubkey": delegation.get("voterPubkey"),
        "account_lamports": value.get("lamports"),
        "network": "solana",
    }


@endpoint("/chain/rent/{size}", "$0.01",
          "Minimum balance for rent exemption for a Solana account of the given data size in bytes. $0.01 USDC per call.",
          "chain", ["chain", "rent", "lamports"], "chain-rent")
async def chain_rent(size: int):
    lamports = await _rpc("getMinimumBalanceForRentExemption", [size])
    if _is_error(lamports):
        return lamports
    return {"data_bytes": size, "lamports": lamports,
            "sol": _round(_sol(lamports), 9), "network": "solana"}


# ===== TOKENS (SPL mints + Jupiter + Dexscreener + GeckoTerminal) =====

@endpoint("/tokens/{mint}/supply", "$0.01",
          "Live SPL token supply for one Solana mint (getTokenSupply): raw amount, decimals, formatted supply. $0.01 USDC per call.",
          "token", ["token", "supply", "spl"], "token-supply")
async def token_supply(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    res = await _rpc("getTokenSupply", [mint])
    if _is_error(res):
        return res
    value = (res or {}).get("value") or {}
    return {
        "mint": mint,
        "amount_raw": value.get("amount"),
        "decimals": value.get("decimals"),
        "supply": value.get("uiAmount"),
        "supply_string": value.get("uiAmountString"),
        "network": "solana",
    }


@endpoint("/tokens/{mint}/info", "$0.01",
          "SPL mint account info: decimals, supply, mint authority, freeze authority, token program. $0.01 USDC per call.",
          "token", ["token", "mint", "spl"], "token-info")
async def token_info(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    res = await _mint_info(mint)
    if _is_error(res):
        return res
    info = res["info"]
    supply_raw = _fnum(info.get("supply"))
    decimals = info.get("decimals")
    supply_ui = supply_raw / (10 ** decimals) if supply_raw is not None and decimals is not None else None
    return {
        "mint": mint,
        "decimals": decimals,
        "supply_raw": info.get("supply"),
        "supply": supply_ui,
        "mint_authority": info.get("mintAuthority"),
        "freeze_authority": info.get("freezeAuthority"),
        "is_initialized": info.get("isInitialized"),
        "token_program": res.get("owner_program"),
        "network": "solana",
    }


@endpoint("/tokens/{mint}/decimals", "$0.01",
          "Decimals and initialization state of one SPL mint. $0.01 USDC per call.",
          "token", ["token", "decimals", "spl"], "token-decimals")
async def token_decimals(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    res = await _mint_info(mint)
    if _is_error(res):
        return res
    return {"mint": mint, "decimals": res["info"].get("decimals"),
            "is_initialized": res["info"].get("isInitialized"), "network": "solana"}


@endpoint("/tokens/{mint}/authorities", "$0.01",
          "Mint and freeze authorities of one SPL token (null means the authority is revoked). $0.01 USDC per call.",
          "token", ["token", "authority", "spl"], "token-authorities")
async def token_authorities(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    res = await _mint_info(mint)
    if _is_error(res):
        return res
    info = res["info"]
    return {
        "mint": mint,
        "mint_authority": info.get("mintAuthority"),
        "mint_authority_revoked": info.get("mintAuthority") is None,
        "freeze_authority": info.get("freezeAuthority"),
        "freeze_authority_revoked": info.get("freezeAuthority") is None,
        "network": "solana",
    }


@endpoint("/tokens/{mint}/largest-accounts", "$0.01",
          "Largest accounts holding one SPL token (getTokenLargestAccounts): top 20 token accounts with balances and share of supply. $0.01 USDC per call.",
          "token", ["token", "holders", "spl"], "token-largest-accounts")
async def token_largest_accounts(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    res = await _rpc("getTokenLargestAccounts", [mint])
    if _is_error(res):
        return res
    value = (res or {}).get("value") or []
    supply_res = await _rpc("getTokenSupply", [mint])
    supply_raw = None
    if not _is_error(supply_res):
        supply_raw = _fnum(((supply_res or {}).get("value") or {}).get("amount"))
    rows = []
    for a in value:
        raw = _fnum(a.get("amount"))
        rows.append({
            "account": a.get("address"),
            "amount": a.get("uiAmount"),
            "amount_raw": a.get("amount"),
            "decimals": a.get("decimals"),
            "share_percent": round(100.0 * raw / supply_raw, 4) if raw is not None and supply_raw else None,
        })
    return {"mint": mint, "accounts": rows, "count": len(rows), "network": "solana"}


@endpoint("/tokens/{mint}/holders/concentration", "$0.02",
          "Holder concentration of one SPL token: share of supply held by the top 1, 5, 10 and 20 accounts. $0.02 USDC per call.",
          "token", ["token", "holders", "concentration"], "token-holder-concentration")
async def token_holder_concentration(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    res = await _rpc("getTokenLargestAccounts", [mint])
    if _is_error(res):
        return res
    value = (res or {}).get("value") or []
    supply_res = await _rpc("getTokenSupply", [mint])
    if _is_error(supply_res):
        return supply_res
    svalue = (supply_res or {}).get("value") or {}
    supply_raw = _fnum(svalue.get("amount"))
    amounts = [_fnum(a.get("amount")) or 0 for a in value]
    def share(n):
        if not supply_raw:
            return None
        return round(100.0 * sum(amounts[:n]) / supply_raw, 4)
    return {
        "mint": mint,
        "supply": svalue.get("uiAmount"),
        "decimals": svalue.get("decimals"),
        "accounts_sampled": len(amounts),
        "top_1_share_percent": share(1),
        "top_5_share_percent": share(5),
        "top_10_share_percent": share(10),
        "top_20_share_percent": share(20),
        "network": "solana",
    }


@endpoint("/tokens/{mint}/price", "$0.01",
          "Live USD price of one Solana token from Jupiter price v3, with 24h change and Jupiter-side liquidity. $0.01 USDC per call.",
          "token", ["token", "price", "jupiter"], "token-price")
async def token_price(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    data = await _jupiter_prices([mint])
    if _is_error(data):
        return data
    entry = data.get(mint)
    if not entry:
        return {"error": "Jupiter has no price for this mint", "mint": mint}
    return {
        "mint": mint,
        "price_usd": _round(_fnum(entry.get("usdPrice")), 10),
        "price_change_24h_percent": _round(_fnum(entry.get("priceChange24h")), 4),
        "liquidity_usd": _round(_fnum(entry.get("liquidity")), 2),
        "decimals": entry.get("decimals"),
        "source": "jupiter",
        "network": "solana",
    }


@endpoint("/tokens/{mint}/price/dexscreener", "$0.01",
          "USD price of one Solana token from its deepest Dexscreener pair (token as base asset). $0.01 USDC per call.",
          "token", ["token", "price", "dexscreener"], "token-price-dexscreener")
async def token_price_dexscreener(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    pairs = await _ds_base_pairs(mint)
    if _is_error(pairs):
        return pairs
    best = _best_pair(pairs)
    if not best:
        return {"error": "token is not the base asset of any Solana pair", "mint": mint}
    return {
        "mint": mint,
        "price_usd": best.get("priceUsd"),
        "price_native": best.get("priceNative"),
        "pair": best.get("pairAddress"),
        "dex": best.get("dexId"),
        "liquidity_usd": (best.get("liquidity") or {}).get("usd"),
        "source": "dexscreener",
        "network": "solana",
    }


@endpoint("/tokens/{mint}/pairs", "$0.01",
          "All Solana DEX pairs where the token is the BASE asset, deepest first (Dexscreener, quote-side pairs excluded). $0.01 USDC per call.",
          "token", ["token", "dex", "pairs"], "token-pairs")
async def token_pairs(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    pairs = await _ds_base_pairs(mint)
    if _is_error(pairs):
        return pairs
    pairs = sorted(pairs, key=lambda p: -(_fnum((p.get("liquidity") or {}).get("usd")) or 0))
    return {"mint": mint, "pairs": [_trim_pair(p) for p in pairs[:20]],
            "count": len(pairs), "network": "solana"}


@endpoint("/tokens/{mint}/best-pair", "$0.01",
          "Deepest Solana DEX pair for one token (as base asset): price, liquidity, volume, market cap. $0.01 USDC per call.",
          "token", ["token", "dex", "pairs"], "token-best-pair")
async def token_best_pair(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    pairs = await _ds_base_pairs(mint)
    if _is_error(pairs):
        return pairs
    best = _best_pair(pairs)
    if not best:
        return {"error": "token is not the base asset of any Solana pair", "mint": mint}
    out = _trim_pair(best)
    out["mint"] = mint
    out["network"] = "solana"
    return out


@endpoint("/tokens/{mint}/liquidity", "$0.01",
          "Total liquidity across a token's Solana pairs (as base asset), pair count, and the deepest pair. $0.01 USDC per call.",
          "token", ["token", "liquidity", "dex"], "token-liquidity")
async def token_liquidity(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    pairs = await _ds_base_pairs(mint)
    if _is_error(pairs):
        return pairs
    total = sum(_fnum((p.get("liquidity") or {}).get("usd")) or 0 for p in pairs)
    best = _best_pair(pairs)
    return {
        "mint": mint,
        "total_liquidity_usd": _round(total, 2),
        "pair_count": len(pairs),
        "deepest_pair": _trim_pair(best) if best else None,
        "network": "solana",
    }


@endpoint("/tokens/{mint}/volume", "$0.01",
          "Trading volume of one Solana token: total 24h volume across its base-asset pairs plus the best pair's volume windows. $0.01 USDC per call.",
          "token", ["token", "volume", "dex"], "token-volume")
async def token_volume(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    pairs = await _ds_base_pairs(mint)
    if _is_error(pairs):
        return pairs
    total24 = sum(_fnum((p.get("volume") or {}).get("h24")) or 0 for p in pairs)
    best = _best_pair(pairs)
    return {
        "mint": mint,
        "total_volume_24h_usd": _round(total24, 2),
        "pair_count": len(pairs),
        "best_pair_volume": (best.get("volume") if best else None),
        "best_pair": best.get("pairAddress") if best else None,
        "network": "solana",
    }


@endpoint("/tokens/{mint}/volume/cross-dex", "$0.01",
          "24h volume of one Solana token broken down by DEX across its base-asset pairs. $0.01 USDC per call.",
          "token", ["token", "volume", "dex"], "token-volume-cross-dex")
async def token_volume_cross_dex(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    pairs = await _ds_base_pairs(mint)
    if _is_error(pairs):
        return pairs
    by_dex = {}
    for p in pairs:
        dex = p.get("dexId") or "unknown"
        by_dex[dex] = by_dex.get(dex, 0.0) + (_fnum((p.get("volume") or {}).get("h24")) or 0)
    rows = [{"dex": k, "volume_24h_usd": _round(v, 2)} for k, v in by_dex.items()]
    rows.sort(key=lambda r: -r["volume_24h_usd"])
    return {"mint": mint, "by_dex": rows,
            "total_volume_24h_usd": _round(sum(by_dex.values()), 2), "network": "solana"}


@endpoint("/tokens/{mint}/price-change", "$0.01",
          "Price changes (5m, 1h, 6h, 24h) of one Solana token on its deepest pair. $0.01 USDC per call.",
          "token", ["token", "price", "change"], "token-price-change")
async def token_price_change(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    pairs = await _ds_base_pairs(mint)
    if _is_error(pairs):
        return pairs
    best = _best_pair(pairs)
    if not best:
        return {"error": "token is not the base asset of any Solana pair", "mint": mint}
    return {
        "mint": mint,
        "price_usd": best.get("priceUsd"),
        "price_change": best.get("priceChange"),
        "pair": best.get("pairAddress"),
        "dex": best.get("dexId"),
        "network": "solana",
    }


@endpoint("/tokens/{mint}/txns", "$0.01",
          "Buy/sell transaction counts (5m, 1h, 6h, 24h) for one Solana token on its deepest pair. $0.01 USDC per call.",
          "token", ["token", "transactions", "dex"], "token-txns")
async def token_txns(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    pairs = await _ds_base_pairs(mint)
    if _is_error(pairs):
        return pairs
    best = _best_pair(pairs)
    if not best:
        return {"error": "token is not the base asset of any Solana pair", "mint": mint}
    return {
        "mint": mint,
        "txns": best.get("txns"),
        "pair": best.get("pairAddress"),
        "dex": best.get("dexId"),
        "network": "solana",
    }


@endpoint("/tokens/{mint}/fdv", "$0.01",
          "Fully diluted valuation and market cap of one Solana token from its deepest pair. $0.01 USDC per call.",
          "token", ["token", "fdv", "market-cap"], "token-fdv")
async def token_fdv(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    pairs = await _ds_base_pairs(mint)
    if _is_error(pairs):
        return pairs
    best = _best_pair(pairs)
    if not best:
        return {"error": "token is not the base asset of any Solana pair", "mint": mint}
    return {
        "mint": mint,
        "price_usd": best.get("priceUsd"),
        "market_cap": best.get("marketCap"),
        "fdv": best.get("fdv"),
        "pair": best.get("pairAddress"),
        "network": "solana",
    }


@endpoint("/tokens/{mint}/market", "$0.02",
          "Combined market snapshot for one Solana token: Jupiter price, on-chain supply, computed market cap, DEX liquidity, volume and FDV. $0.02 USDC per call.",
          "token", ["token", "market", "snapshot"], "token-market")
async def token_market(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    prices = await _jupiter_prices([mint])
    if _is_error(prices):
        return prices
    entry = prices.get(mint) or {}
    price = _fnum(entry.get("usdPrice"))
    supply_res = await _rpc("getTokenSupply", [mint])
    supply_ui = None
    if not _is_error(supply_res):
        supply_ui = ((supply_res or {}).get("value") or {}).get("uiAmount")
    pairs = await _ds_base_pairs(mint)
    total_liq = total_vol = None
    best = None
    if not _is_error(pairs):
        total_liq = _round(sum(_fnum((p.get("liquidity") or {}).get("usd")) or 0 for p in pairs), 2)
        total_vol = _round(sum(_fnum((p.get("volume") or {}).get("h24")) or 0 for p in pairs), 2)
        best = _best_pair(pairs)
    return {
        "mint": mint,
        "price_usd": _round(price, 10),
        "price_change_24h_percent": _round(_fnum(entry.get("priceChange24h")), 4),
        "supply": supply_ui,
        "computed_market_cap_usd": _round(price * supply_ui, 2) if price and supply_ui else None,
        "dex_total_liquidity_usd": total_liq,
        "dex_total_volume_24h_usd": total_vol,
        "dex_fdv": best.get("fdv") if best else None,
        "dex_pair_count": len(pairs) if not _is_error(pairs) else None,
        "network": "solana",
    }


@endpoint("/tokens/{mint}/links", "$0.01",
          "Websites and social links for one Solana token from its deepest pair's Dexscreener profile. $0.01 USDC per call.",
          "token", ["token", "links", "socials"], "token-links")
async def token_links(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    pairs = await _ds_base_pairs(mint)
    if _is_error(pairs):
        return pairs
    best = _best_pair(pairs)
    if not best:
        return {"error": "token is not the base asset of any Solana pair", "mint": mint}
    info = best.get("info") or {}
    return {
        "mint": mint,
        "websites": info.get("websites"),
        "socials": info.get("socials"),
        "image_url": info.get("imageUrl"),
        "pair": best.get("pairAddress"),
        "network": "solana",
    }


@endpoint("/tokens/{mint}/first-pair", "$0.01",
          "Earliest known DEX pair for one Solana token: when it first traded, on which DEX. $0.01 USDC per call.",
          "token", ["token", "dex", "age"], "token-first-pair")
async def token_first_pair(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    pairs = await _ds_base_pairs(mint)
    if _is_error(pairs):
        return pairs
    dated = [p for p in pairs if p.get("pairCreatedAt")]
    if not dated:
        return {"error": "no pair creation dates available", "mint": mint}
    first = min(dated, key=lambda p: p["pairCreatedAt"])
    return {
        "mint": mint,
        "first_pair_created_at_ms": first.get("pairCreatedAt"),
        "pair": first.get("pairAddress"),
        "dex": first.get("dexId"),
        "url": first.get("url"),
        "network": "solana",
    }


@endpoint("/tokens/{mint}/reserves", "$0.01",
          "Pool reserves of a token's deepest Solana pair: base and quote amounts held in the pool. $0.01 USDC per call.",
          "token", ["token", "liquidity", "reserves"], "token-reserves")
async def token_reserves(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    pairs = await _ds_base_pairs(mint)
    if _is_error(pairs):
        return pairs
    best = _best_pair(pairs)
    if not best:
        return {"error": "token is not the base asset of any Solana pair", "mint": mint}
    liq = best.get("liquidity") or {}
    return {
        "mint": mint,
        "pair": best.get("pairAddress"),
        "dex": best.get("dexId"),
        "liquidity_usd": liq.get("usd"),
        "base_reserve": liq.get("base"),
        "quote_reserve": liq.get("quote"),
        "base_token": (best.get("baseToken") or {}).get("symbol"),
        "quote_token": (best.get("quoteToken") or {}).get("symbol"),
        "network": "solana",
    }


@endpoint("/tokens/{mint}/pools", "$0.01",
          "GeckoTerminal pools for one Solana token, top 10 by reserve. $0.01 USDC per call.",
          "token", ["token", "pools", "geckoterminal"], "token-pools-gt")
async def token_pools_gt(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    data = await _gt(f"/networks/solana/tokens/{mint}/pools?page=1")
    if _is_error(data):
        return data
    pools = data.get("data") or []
    return {"mint": mint, "pools": [_gt_pool_trim(p) for p in pools[:10]],
            "count": len(pools), "network": "solana"}


@endpoint("/tokens/{mint}/info/geckoterminal", "$0.01",
          "GeckoTerminal token info for one Solana mint: name, symbol, decimals, supply, price, FDV, volume. $0.01 USDC per call.",
          "token", ["token", "info", "geckoterminal"], "token-info-gt")
async def token_info_gt(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    data = await _gt(f"/tokens/{mint}")
    if _is_error(data):
        # GT also exposes a network-scoped token route.
        data = await _gt(f"/networks/solana/tokens/{mint}")
        if _is_error(data):
            return data
    a = ((data.get("data") or {}).get("attributes")) or {}
    return {
        "mint": mint,
        "name": a.get("name"),
        "symbol": a.get("symbol"),
        "decimals": a.get("decimals"),
        "price_usd": _fnum(a.get("price_usd")),
        "total_supply": a.get("total_supply"),
        "fdv_usd": _round(_fnum(a.get("fdv_usd")), 2),
        "market_cap_usd": _round(_fnum(a.get("market_cap_usd")), 2),
        "volume_24h_usd": _round(_fnum((a.get("volume_usd") or {}).get("h24")), 2),
        "gt_score": a.get("gt_score"),
        "network": "solana",
    }


@endpoint("/tokens/{mint}/info/jupiter", "$0.01",
          "Jupiter token metadata for one Solana mint: name, tags, holder count, organic score, liquidity, FDV. $0.01 USDC per call.",
          "token", ["token", "info", "jupiter"], "token-info-jupiter")
async def token_info_jupiter(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    results = await _jupiter_search(mint)
    if _is_error(results):
        return results
    exact = next((t for t in results if t.get("id") == mint), None)
    if not exact:
        return {"error": "Jupiter has no metadata for this mint", "mint": mint}
    out = _jupiter_token_trim(exact)
    out["network"] = "solana"
    return out


@endpoint("/tokens/{mint}/coingecko", "$0.01",
          "CoinGecko market data for one Solana token by contract: price, market cap rank, volume, 24h range. $0.01 USDC per call.",
          "token", ["token", "coingecko", "market"], "token-coingecko")
async def token_coingecko(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    data = await _get_json(f"{COINGECKO}/coins/solana/contract/{mint}",
                           params={"localization": "false", "tickers": "false",
                                   "community_data": "false", "developer_data": "false"},
                           ttl=60)
    if _is_error(data):
        return data
    md = data.get("market_data") or {}
    return {
        "mint": mint,
        "coingecko_id": data.get("id"),
        "name": data.get("name"),
        "symbol": (data.get("symbol") or "").upper(),
        "market_cap_rank": data.get("market_cap_rank"),
        "price_usd": (md.get("current_price") or {}).get("usd"),
        "market_cap_usd": (md.get("market_cap") or {}).get("usd"),
        "total_volume_usd": (md.get("total_volume") or {}).get("usd"),
        "high_24h_usd": (md.get("high_24h") or {}).get("usd"),
        "low_24h_usd": (md.get("low_24h") or {}).get("usd"),
        "price_change_24h_percent": md.get("price_change_percentage_24h"),
        "network": "solana",
    }


@endpoint("/tokens/{mint}/ath", "$0.01",
          "All-time high and low for one Solana token from CoinGecko (by contract address). $0.01 USDC per call.",
          "token", ["token", "ath", "coingecko"], "token-ath")
async def token_ath(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    data = await _get_json(f"{COINGECKO}/coins/solana/contract/{mint}",
                           params={"localization": "false", "tickers": "false",
                                   "community_data": "false", "developer_data": "false"},
                           ttl=60)
    if _is_error(data):
        return data
    md = data.get("market_data") or {}
    return {
        "mint": mint,
        "name": data.get("name"),
        "ath_usd": (md.get("ath") or {}).get("usd"),
        "ath_date": (md.get("ath_date") or {}).get("usd"),
        "ath_change_percent": (md.get("ath_change_percentage") or {}).get("usd"),
        "atl_usd": (md.get("atl") or {}).get("usd"),
        "atl_date": (md.get("atl_date") or {}).get("usd"),
        "current_price_usd": (md.get("current_price") or {}).get("usd"),
        "network": "solana",
    }


@endpoint("/tokens/{mint}/sentiment", "$0.01",
          "CoinGecko community sentiment for one Solana token: watchlist count and up/down vote split. $0.01 USDC per call.",
          "token", ["token", "sentiment", "coingecko"], "token-sentiment")
async def token_sentiment(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    data = await _get_json(f"{COINGECKO}/coins/solana/contract/{mint}",
                           params={"localization": "false", "tickers": "false",
                                   "market_data": "false", "developer_data": "false"},
                           ttl=60)
    if _is_error(data):
        return data
    return {
        "mint": mint,
        "name": data.get("name"),
        "watchlist_portfolio_users": data.get("watchlist_portfolio_users"),
        "sentiment_votes_up_percent": data.get("sentiment_votes_up_percentage"),
        "sentiment_votes_down_percent": data.get("sentiment_votes_down_percentage"),
        "market_cap_rank": data.get("market_cap_rank"),
        "coingecko_score": data.get("coingecko_score"),
        "network": "solana",
    }


@endpoint("/tokens/{mint}/ohlcv/{timeframe}", "$0.01",
          "OHLCV candles for a Solana token's top GeckoTerminal pool (timeframe: day, hour or minute). $0.01 USDC per call.",
          "token", ["token", "ohlcv", "geckoterminal"], "token-ohlcv")
async def token_ohlcv(mint: str, timeframe: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    if timeframe not in ("day", "hour", "minute"):
        return {"error": "timeframe must be day, hour or minute"}
    pools = await _gt(f"/networks/solana/tokens/{mint}/pools?page=1")
    if _is_error(pools):
        return pools
    data_list = pools.get("data") or []
    if not data_list:
        return {"error": "no GeckoTerminal pools for token", "mint": mint}
    pool_addr = (data_list[0].get("attributes") or {}).get("address")
    data = await _gt(f"/networks/solana/pools/{pool_addr}/ohlcv/{timeframe}?limit=48")
    if _is_error(data):
        return data
    candles = (((data.get("data") or {}).get("attributes") or {}).get("ohlcv_list")) or []
    return {
        "mint": mint,
        "pool": pool_addr,
        "timeframe": timeframe,
        "candles": [{"t": c[0], "o": c[1], "h": c[2], "l": c[3], "c": c[4], "v": c[5]}
                    for c in candles if isinstance(c, list) and len(c) == 6],
        "network": "solana",
    }


@endpoint("/tokens/{mint}/top-pool/trades", "$0.01",
          "Recent trades in a Solana token's top GeckoTerminal pool (latest 20). $0.01 USDC per call.",
          "token", ["token", "trades", "geckoterminal"], "token-top-pool-trades")
async def token_top_pool_trades(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    pools = await _gt(f"/networks/solana/tokens/{mint}/pools?page=1")
    if _is_error(pools):
        return pools
    data_list = pools.get("data") or []
    if not data_list:
        return {"error": "no GeckoTerminal pools for token", "mint": mint}
    pool_addr = (data_list[0].get("attributes") or {}).get("address")
    data = await _gt(f"/networks/solana/pools/{pool_addr}/trades")
    if _is_error(data):
        return data
    trades = []
    for t in (data.get("data") or [])[:20]:
        a = t.get("attributes") or {}
        trades.append({
            "kind": a.get("kind"),
            "block_number": a.get("block_number"),
            "tx_hash": a.get("tx_hash_from") or a.get("tx_hash"),
            "from": a.get("from_address"),
            "to": a.get("to_address"),
            "volume_usd": _round(_fnum(a.get("volume_in_usd")), 2),
            "price_from_usd": _fnum(a.get("price_from_in_currency_token")),
            "price_to_usd": _fnum(a.get("price_to_in_currency_token")),
            "timestamp": a.get("block_timestamp"),
        })
    return {"mint": mint, "pool": pool_addr, "trades": trades, "network": "solana"}


@endpoint("/tokens/prices/{mints}", "$0.01",
          "Batch USD prices for up to 30 Solana mints in one Jupiter call (comma-separated mint list). $0.01 USDC per call.",
          "token", ["token", "price", "batch"], "token-prices-batch")
async def token_prices_batch(mints: str):
    mint_list = [m.strip() for m in mints.split(",") if m.strip()][:30]
    if not mint_list or not all(_is_sol_address(m) for m in mint_list):
        return {"error": "expected a comma-separated list of Solana mint addresses"}
    data = await _jupiter_prices(mint_list)
    if _is_error(data):
        return data
    out = {}
    for m in mint_list:
        entry = data.get(m) or {}
        out[m] = {
            "price_usd": _round(_fnum(entry.get("usdPrice")), 10),
            "price_change_24h_percent": _round(_fnum(entry.get("priceChange24h")), 4),
        }
    return {"prices": out, "count": len(out), "source": "jupiter", "network": "solana"}


@endpoint("/tokens/majors/prices", "$0.01",
          "USD prices and 24h changes for the Solana majors: SOL, USDC, USDT, JUP, JTO, PYTH, WIF, BONK, RAY (Jupiter). $0.01 USDC per call.",
          "token", ["token", "price", "majors"], "token-majors-prices")
async def token_majors_prices():
    data = await _jupiter_prices(list(MAJOR_MINTS.values()))
    if _is_error(data):
        return data
    out = {}
    for symbol, mint in MAJOR_MINTS.items():
        entry = data.get(mint) or {}
        out[symbol] = {
            "mint": mint,
            "price_usd": _round(_fnum(entry.get("usdPrice")), 8),
            "price_change_24h_percent": _round(_fnum(entry.get("priceChange24h")), 4),
            "liquidity_usd": _round(_fnum(entry.get("liquidity")), 2),
        }
    return {"tokens": out, "source": "jupiter", "network": "solana"}


@endpoint("/tokens/search/{query}", "$0.01",
          "Search Solana DEX pairs by token name or symbol (Dexscreener, Solana only, top 10 by liquidity). $0.01 USDC per call.",
          "token", ["token", "search", "dex"], "token-search")
async def token_search(query: str):
    pairs = await _ds_search(query)
    if _is_error(pairs):
        return pairs
    pairs = sorted(pairs, key=lambda p: -(_fnum((p.get("liquidity") or {}).get("usd")) or 0))
    return {"query": query, "pairs": [_trim_pair(p) for p in pairs[:10]],
            "count": len(pairs), "network": "solana"}


@endpoint("/tokens/search/jupiter/{query}", "$0.01",
          "Search Jupiter's Solana token index by name, symbol or mint: metadata, tags, holder counts, scores. $0.01 USDC per call.",
          "token", ["token", "search", "jupiter"], "token-search-jupiter")
async def token_search_jupiter(query: str):
    results = await _jupiter_search(query)
    if _is_error(results):
        return results
    return {"query": query,
            "tokens": [_jupiter_token_trim(t) for t in results[:10]],
            "count": len(results), "network": "solana"}


@endpoint("/tokens/top/volume", "$0.01",
          "Solana tokens with the highest 24h volume right now, from GeckoTerminal's top pools. $0.01 USDC per call.",
          "token", ["token", "volume", "top"], "token-top-volume")
async def token_top_volume():
    data = await _gt("/networks/solana/pools?page=1")
    if _is_error(data):
        return data
    pools = data.get("data") or []
    rows = []
    for p in pools:
        a = p.get("attributes") or {}
        rel = p.get("relationships") or {}
        rows.append({
            "token": ((rel.get("base_token") or {}).get("data") or {}).get("id"),
            "pool_name": a.get("name"),
            "volume_24h_usd": _round(_fnum((a.get("volume_usd") or {}).get("h24")), 2),
            "reserve_usd": _round(_fnum(a.get("reserve_in_usd")), 2),
            "price_usd": _fnum(a.get("base_token_price_usd")),
        })
    rows.sort(key=lambda r: -(r["volume_24h_usd"] or 0))
    return {"tokens": rows[:10], "network": "solana"}


# ===== WALLETS =====

async def _wallet_token_accounts(owner):
    """All SPL token accounts (both token programs) for an owner."""
    out = []
    for program in (TOKEN_PROGRAM, TOKEN_2022_PROGRAM):
        entries = await _token_accounts_by_owner(owner, program=program)
        if _is_error(entries):
            return entries
        out.extend(_trim_token_account(e, program=program) for e in entries)
    return out


@endpoint("/wallets/{address}/balance", "$0.01",
          "SOL balance of one Solana wallet (lamports and SOL). $0.01 USDC per call.",
          "wallet", ["wallet", "balance", "sol"], "wallet-balance")
async def wallet_balance(address: str):
    if not _is_sol_address(address):
        return _err_addr()
    res = await _rpc("getBalance", [address])
    if _is_error(res):
        return res
    lamports = (res or {}).get("value")
    return {"address": address, "lamports": lamports,
            "sol": _round(_sol(lamports), 9), "network": "solana"}


@endpoint("/wallets/{address}/balance/usd", "$0.01",
          "SOL balance of one wallet with its current USD value. $0.01 USDC per call.",
          "wallet", ["wallet", "balance", "usd"], "wallet-balance-usd")
async def wallet_balance_usd(address: str):
    if not _is_sol_address(address):
        return _err_addr()
    res = await _rpc("getBalance", [address])
    if _is_error(res):
        return res
    lamports = (res or {}).get("value")
    sol = _sol(lamports)
    price = await _sol_price_usd()
    return {
        "address": address,
        "sol": _round(sol, 9),
        "sol_price_usd": _round(price, 4),
        "value_usd": _round(sol * price, 2) if sol is not None and price else None,
        "network": "solana",
    }


@endpoint("/wallets/{address}/account", "$0.01",
          "Raw account info for one Solana address: lamports, owner program, executable flag, data size. $0.01 USDC per call.",
          "wallet", ["wallet", "account", "info"], "wallet-account")
async def wallet_account(address: str):
    if not _is_sol_address(address):
        return _err_addr()
    res = await _rpc("getAccountInfo", [address, {"encoding": "base64"}])
    if _is_error(res):
        return res
    value = (res or {}).get("value")
    if not value:
        return {"address": address, "exists": False, "network": "solana",
                "note": "account does not exist on-chain (never funded)"}
    data = value.get("data") or ["", ""]
    import base64 as _b64
    try:
        data_len = len(_b64.b64decode(data[0])) if data and data[0] else 0
    except Exception:
        data_len = None
    return {
        "address": address,
        "exists": True,
        "lamports": value.get("lamports"),
        "sol": _round(_sol(value.get("lamports")), 9),
        "owner_program": value.get("owner"),
        "executable": value.get("executable"),
        "rent_epoch": value.get("rentEpoch"),
        "data_bytes": data_len,
        "network": "solana",
    }


@endpoint("/wallets/{address}/tokens", "$0.01",
          "All SPL token accounts of one wallet (both token programs): mint, balance, decimals, state — sorted by balance. $0.01 USDC per call.",
          "wallet", ["wallet", "tokens", "spl"], "wallet-tokens")
async def wallet_tokens(address: str):
    if not _is_sol_address(address):
        return _err_addr()
    accounts = await _wallet_token_accounts(address)
    if _is_error(accounts):
        return accounts
    accounts.sort(key=lambda a: -(a.get("amount") or 0))
    return {"address": address, "token_accounts": accounts,
            "count": len(accounts), "network": "solana"}


@endpoint("/wallets/{address}/tokens/count", "$0.01",
          "Token account counts for one wallet: total accounts, non-zero balances, split by token program. $0.01 USDC per call.",
          "wallet", ["wallet", "tokens", "count"], "wallet-tokens-count")
async def wallet_tokens_count(address: str):
    if not _is_sol_address(address):
        return _err_addr()
    accounts = await _wallet_token_accounts(address)
    if _is_error(accounts):
        return accounts
    return {
        "address": address,
        "total_token_accounts": len(accounts),
        "nonzero_balances": sum(1 for a in accounts if (a.get("amount") or 0) > 0),
        "token_program_accounts": sum(1 for a in accounts if a.get("program") == TOKEN_PROGRAM),
        "token_2022_accounts": sum(1 for a in accounts if a.get("program") == TOKEN_2022_PROGRAM),
        "network": "solana",
    }


@endpoint("/wallets/{address}/token/{mint}", "$0.01",
          "One wallet's balance of one SPL token, summed across all its token accounts for that mint. $0.01 USDC per call.",
          "wallet", ["wallet", "token", "balance"], "wallet-token-balance")
async def wallet_token_balance(address: str, mint: str):
    if not _is_sol_address(address):
        return _err_addr()
    if not _is_sol_address(mint):
        return _err_addr("mint")
    total_raw = 0
    decimals = None
    accounts = []
    for program in (TOKEN_PROGRAM, TOKEN_2022_PROGRAM):
        entries = await _token_accounts_by_owner(address, mint=mint, program=program)
        if _is_error(entries):
            return entries
        for e in entries:
            t = _trim_token_account(e, program=program)
            accounts.append(t)
            try:
                total_raw += int(t.get("amount_raw") or 0)
            except (TypeError, ValueError):
                pass
            if decimals is None:
                decimals = t.get("decimals")
    balance = total_raw / (10 ** decimals) if decimals is not None else None
    return {
        "address": address,
        "mint": mint,
        "balance": balance,
        "balance_raw": str(total_raw),
        "decimals": decimals,
        "accounts": accounts,
        "network": "solana",
    }


@endpoint("/wallets/{address}/token/{mint}/accounts", "$0.01",
          "Every token account one wallet holds for one mint (usually one — more means fragmented holdings). $0.01 USDC per call.",
          "wallet", ["wallet", "token", "accounts"], "wallet-token-accounts")
async def wallet_token_accounts(address: str, mint: str):
    if not _is_sol_address(address):
        return _err_addr()
    if not _is_sol_address(mint):
        return _err_addr("mint")
    out = []
    for program in (TOKEN_PROGRAM, TOKEN_2022_PROGRAM):
        entries = await _token_accounts_by_owner(address, mint=mint, program=program)
        if _is_error(entries):
            return entries
        out.extend(_trim_token_account(e, program=program) for e in entries)
    return {"address": address, "mint": mint, "accounts": out,
            "count": len(out), "network": "solana"}


@endpoint("/wallets/{address}/token/{mint}/usd", "$0.01",
          "One wallet's balance of one token with its USD value at the Jupiter price. $0.01 USDC per call.",
          "wallet", ["wallet", "token", "usd"], "wallet-token-usd")
async def wallet_token_usd(address: str, mint: str):
    base = await wallet_token_balance(address, mint)
    if _is_error(base):
        return base
    prices = await _jupiter_prices([mint])
    entry = prices.get(mint) if isinstance(prices, dict) else None
    price = _fnum(entry.get("usdPrice")) if isinstance(entry, dict) else None
    bal = base.get("balance")
    return {
        "address": address,
        "mint": mint,
        "balance": bal,
        "price_usd": _round(price, 10),
        "value_usd": _round(bal * price, 2) if bal is not None and price else None,
        "network": "solana",
    }


@endpoint("/wallets/{address}/portfolio", "$0.02",
          "Full wallet portfolio: SOL plus every SPL holding priced in USD via Jupiter, with total portfolio value. $0.02 USDC per call.",
          "wallet", ["wallet", "portfolio", "usd"], "wallet-portfolio")
async def wallet_portfolio(address: str):
    if not _is_sol_address(address):
        return _err_addr()
    bal_res = await _rpc("getBalance", [address])
    if _is_error(bal_res):
        return bal_res
    sol_lamports = (bal_res or {}).get("value")
    sol_amount = _sol(sol_lamports)
    accounts = await _wallet_token_accounts(address)
    if _is_error(accounts):
        return accounts
    nonzero = [a for a in accounts if (a.get("amount") or 0) > 0]
    mints = sorted({a["mint"] for a in nonzero if a.get("mint")})
    prices = {}
    for i in range(0, len(mints), 30):
        chunk = await _jupiter_prices(mints[i:i + 30])
        if isinstance(chunk, dict) and not _is_error(chunk):
            prices.update(chunk)
    sol_price = await _sol_price_usd()
    holdings = []
    total_tokens_usd = 0.0
    for a in nonzero:
        entry = prices.get(a["mint"]) or {}
        price = _fnum(entry.get("usdPrice"))
        value = a["amount"] * price if price is not None else None
        if value is not None:
            total_tokens_usd += value
        holdings.append({
            "mint": a["mint"],
            "amount": a["amount"],
            "decimals": a["decimals"],
            "price_usd": _round(price, 10),
            "value_usd": _round(value, 2),
            "program": a["program"],
        })
    holdings.sort(key=lambda h: -(h["value_usd"] or 0))
    sol_value = sol_amount * sol_price if sol_amount is not None and sol_price else None
    return {
        "address": address,
        "sol_balance": _round(sol_amount, 9),
        "sol_price_usd": _round(sol_price, 4),
        "sol_value_usd": _round(sol_value, 2),
        "holdings": holdings,
        "priced_holdings": sum(1 for h in holdings if h["value_usd"] is not None),
        "unpriced_holdings": sum(1 for h in holdings if h["value_usd"] is None),
        "tokens_value_usd": _round(total_tokens_usd, 2),
        "total_value_usd": _round((sol_value or 0) + total_tokens_usd, 2),
        "network": "solana",
    }


@endpoint("/wallets/{address}/nfts", "$0.01",
          "NFT-style holdings of one wallet: token accounts with 0 decimals and a balance of at least 1 (the on-chain NFT shape). $0.01 USDC per call.",
          "wallet", ["wallet", "nfts", "spl"], "wallet-nfts")
async def wallet_nfts(address: str):
    if not _is_sol_address(address):
        return _err_addr()
    accounts = await _wallet_token_accounts(address)
    if _is_error(accounts):
        return accounts
    nfts = [a for a in accounts if a.get("decimals") == 0 and (a.get("amount") or 0) >= 1]
    return {
        "address": address,
        "nfts": [{"mint": a["mint"], "amount": a["amount"], "account": a["account"],
                  "program": a["program"]} for a in nfts],
        "count": len(nfts),
        "note": "identified by token shape (0 decimals); metadata lives off-chain",
        "network": "solana",
    }


@endpoint("/wallets/{address}/signatures", "$0.01",
          "Latest 25 transaction signatures for one address: slot, time, status, memo. $0.01 USDC per call.",
          "wallet", ["wallet", "signatures", "history"], "wallet-signatures")
async def wallet_signatures(address: str):
    if not _is_sol_address(address):
        return _err_addr()
    sigs = await _rpc("getSignaturesForAddress", [address, {"limit": 25}])
    if _is_error(sigs):
        return sigs
    return {"address": address, "signatures": sigs, "count": len(sigs or []),
            "network": "solana"}


@endpoint("/wallets/{address}/signatures/{limit}", "$0.01",
          "Recent transaction signatures for one address with a custom limit (capped at 100). $0.01 USDC per call.",
          "wallet", ["wallet", "signatures", "history"], "wallet-signatures-limit")
async def wallet_signatures_limit(address: str, limit: int):
    if not _is_sol_address(address):
        return _err_addr()
    limit = max(1, min(limit, 100))
    sigs = await _rpc("getSignaturesForAddress", [address, {"limit": limit}])
    if _is_error(sigs):
        return sigs
    return {"address": address, "signatures": sigs, "count": len(sigs or []),
            "network": "solana"}


@endpoint("/wallets/{address}/signatures/failed", "$0.01",
          "Failed transactions among an address's latest 100 signatures, with error summaries. $0.01 USDC per call.",
          "wallet", ["wallet", "signatures", "failed"], "wallet-signatures-failed")
async def wallet_signatures_failed(address: str):
    if not _is_sol_address(address):
        return _err_addr()
    sigs = await _rpc("getSignaturesForAddress", [address, {"limit": 100}])
    if _is_error(sigs):
        return sigs
    failed = [s for s in (sigs or []) if s.get("err")]
    return {
        "address": address,
        "sampled": len(sigs or []),
        "failed_count": len(failed),
        "failed": [{"signature": s.get("signature"), "slot": s.get("slot"),
                    "block_time": s.get("blockTime"), "err": s.get("err")} for s in failed[:25]],
        "network": "solana",
    }


@endpoint("/wallets/{address}/activity", "$0.01",
          "Activity profile of one address from its latest 100 signatures: success rate, first/last activity in window, busiest day. $0.01 USDC per call.",
          "wallet", ["wallet", "activity", "analytics"], "wallet-activity")
async def wallet_activity(address: str):
    if not _is_sol_address(address):
        return _err_addr()
    sigs = await _rpc("getSignaturesForAddress", [address, {"limit": 100}])
    if _is_error(sigs):
        return sigs
    sigs = sigs or []
    times = [s.get("blockTime") for s in sigs if s.get("blockTime")]
    per_day = {}
    for t in times:
        day = time.strftime("%Y-%m-%d", time.gmtime(t))
        per_day[day] = per_day.get(day, 0) + 1
    busiest = max(per_day.items(), key=lambda kv: kv[1]) if per_day else None
    failed = sum(1 for s in sigs if s.get("err"))
    return {
        "address": address,
        "sampled_transactions": len(sigs),
        "successful": len(sigs) - failed,
        "failed": failed,
        "success_rate_percent": round(100.0 * (len(sigs) - failed) / len(sigs), 2) if sigs else None,
        "first_in_window_unix": min(times) if times else None,
        "last_in_window_unix": max(times) if times else None,
        "busiest_day": {"date": busiest[0], "transactions": busiest[1]} if busiest else None,
        "network": "solana",
    }


async def _stake_accounts_for(owner):
    """Stake accounts where the address is staker or withdrawer."""
    found = {}
    for offset in (12, 44):  # authorized.staker, authorized.withdrawer
        res = await _rpc("getProgramAccounts", [
            STAKE_PROGRAM,
            {"encoding": "jsonParsed",
             "filters": [{"dataSize": 200},
                         {"memcmp": {"offset": offset, "bytes": owner}}]},
        ])
        if _is_error(res):
            return res
        for entry in res or []:
            found[entry.get("pubkey")] = entry
    rows = []
    for pubkey, entry in found.items():
        acct = entry.get("account") or {}
        parsed = ((acct.get("data") or {}).get("parsed") or {})
        info = parsed.get("info") or {}
        stake = (info.get("stake") or {})
        delegation = stake.get("delegation") or {}
        meta = info.get("meta") or {}
        rows.append({
            "stake_account": pubkey,
            "lamports": acct.get("lamports"),
            "sol": _round(_sol(acct.get("lamports")), 6),
            "state": parsed.get("type"),
            "staker": ((meta.get("authorized") or {}).get("staker")),
            "withdrawer": ((meta.get("authorized") or {}).get("withdrawer")),
            "delegated_stake_sol": _round(_sol(delegation.get("stake")), 6),
            "voter_pubkey": delegation.get("voterPubkey"),
            "activation_epoch": delegation.get("activationEpoch"),
            "deactivation_epoch": None if delegation.get("deactivationEpoch") == U64_MAX else delegation.get("deactivationEpoch"),
        })
    return rows


@endpoint("/wallets/{address}/stake-accounts", "$0.01",
          "Stake accounts controlled by one wallet (as staker or withdrawer): delegation, voter, epochs, balances. $0.01 USDC per call.",
          "wallet", ["wallet", "stake", "validators"], "wallet-stake-accounts")
async def wallet_stake_accounts(address: str):
    if not _is_sol_address(address):
        return _err_addr()
    rows = await _stake_accounts_for(address)
    if _is_error(rows):
        return rows
    return {"address": address, "stake_accounts": rows, "count": len(rows),
            "network": "solana"}


@endpoint("/wallets/{address}/stake/total", "$0.01",
          "Total SOL one wallet has delegated across all its stake accounts. $0.01 USDC per call.",
          "wallet", ["wallet", "stake", "total"], "wallet-stake-total")
async def wallet_stake_total(address: str):
    if not _is_sol_address(address):
        return _err_addr()
    rows = await _stake_accounts_for(address)
    if _is_error(rows):
        return rows
    total = sum(r.get("delegated_stake_sol") or 0 for r in rows)
    balance_total = sum(_sol(r.get("lamports")) or 0 for r in rows)
    return {
        "address": address,
        "stake_accounts": len(rows),
        "total_delegated_sol": _round(total, 6),
        "total_account_balance_sol": _round(balance_total, 6),
        "network": "solana",
    }


@endpoint("/wallets/{address}/validator", "$0.01",
          "Validator lookup for one address: matches it as a vote account or node identity in the live validator set. $0.01 USDC per call.",
          "wallet", ["wallet", "validator", "stake"], "wallet-validator")
async def wallet_validator(address: str):
    if not _is_sol_address(address):
        return _err_addr()
    res = await _rpc("getVoteAccounts", [])
    if _is_error(res):
        return res
    for kind in ("current", "delinquent"):
        for v in (res or {}).get(kind) or []:
            if v.get("votePubkey") == address or v.get("nodePubkey") == address:
                return {
                    "address": address,
                    "is_validator": True,
                    "delinquent": kind == "delinquent",
                    "vote_pubkey": v.get("votePubkey"),
                    "node_pubkey": v.get("nodePubkey"),
                    "stake_sol": _round(_sol(v.get("activatedStake")), 4),
                    "commission": v.get("commission"),
                    "last_vote": v.get("lastVote"),
                    "root_slot": v.get("rootSlot"),
                    "network": "solana",
                }
    return {"address": address, "is_validator": False, "network": "solana"}


@endpoint("/wallets/{address}/inflation-rewards", "$0.01",
          "Inflation reward earned by one address in the most recent completed epoch (null reward means none). $0.01 USDC per call.",
          "wallet", ["wallet", "rewards", "inflation"], "wallet-inflation-rewards")
async def wallet_inflation_rewards(address: str):
    if not _is_sol_address(address):
        return _err_addr()
    info = await _rpc("getEpochInfo", [])
    if _is_error(info):
        return info
    epoch = (info.get("epoch") or 1) - 1
    res = await _rpc("getInflationReward", [[address], {"epoch": epoch}])
    if _is_error(res):
        return res
    reward = res[0] if isinstance(res, list) and res else None
    if not reward:
        return {"address": address, "epoch": epoch, "reward_lamports": 0,
                "reward_sol": 0, "note": "no inflation reward for this epoch",
                "network": "solana"}
    return {
        "address": address,
        "epoch": reward.get("epoch", epoch),
        "reward_lamports": reward.get("amount"),
        "reward_sol": _round(_sol(reward.get("amount")), 9),
        "post_balance_lamports": reward.get("postBalance"),
        "commission": reward.get("commission"),
        "network": "solana",
    }


@endpoint("/wallets/{address}/largest-token", "$0.01",
          "A wallet's largest token holding by USD value (Jupiter-priced), with its share of the portfolio. $0.01 USDC per call.",
          "wallet", ["wallet", "token", "largest"], "wallet-largest-token")
async def wallet_largest_token(address: str):
    if not _is_sol_address(address):
        return _err_addr()
    accounts = await _wallet_token_accounts(address)
    if _is_error(accounts):
        return accounts
    nonzero = [a for a in accounts if (a.get("amount") or 0) > 0]
    if not nonzero:
        return {"address": address, "largest_token": None,
                "note": "wallet holds no SPL tokens", "network": "solana"}
    mints = sorted({a["mint"] for a in nonzero if a.get("mint")})
    prices = {}
    for i in range(0, len(mints), 30):
        chunk = await _jupiter_prices(mints[i:i + 30])
        if isinstance(chunk, dict) and not _is_error(chunk):
            prices.update(chunk)
    best = None
    total = 0.0
    for a in nonzero:
        entry = prices.get(a["mint"]) or {}
        price = _fnum(entry.get("usdPrice"))
        if price is None:
            continue
        value = a["amount"] * price
        total += value
        if best is None or value > best["value_usd"]:
            best = {"mint": a["mint"], "amount": a["amount"],
                    "price_usd": _round(price, 10), "value_usd": _round(value, 2)}
    if best is None:
        return {"address": address, "largest_token": None,
                "note": "no holding has a Jupiter price", "network": "solana"}
    best["portfolio_share_percent"] = round(100.0 * best["value_usd"] / total, 2) if total else None
    return {"address": address, "largest_token": best,
            "priced_portfolio_usd": _round(total, 2), "network": "solana"}


@endpoint("/wallets/{address}/token-rent", "$0.01",
          "SOL locked as rent inside a wallet's token accounts (sum of token-account lamports). $0.01 USDC per call.",
          "wallet", ["wallet", "rent", "tokens"], "wallet-token-rent")
async def wallet_token_rent(address: str):
    if not _is_sol_address(address):
        return _err_addr()
    accounts = await _wallet_token_accounts(address)
    if _is_error(accounts):
        return accounts
    total = sum(a.get("lamports") or 0 for a in accounts)
    return {
        "address": address,
        "token_accounts": len(accounts),
        "rent_lamports": total,
        "rent_sol": _round(_sol(total), 9),
        "network": "solana",
    }


@endpoint("/wallets/{address}/associated-token-account/{mint}", "$0.01",
          "The associated token account (ATA) address for a wallet + mint: derived PDA, whether it exists, and its balance. $0.01 USDC per call.",
          "wallet", ["wallet", "ata", "token"], "wallet-ata")
async def wallet_ata(address: str, mint: str):
    if not _is_sol_address(address):
        return _err_addr()
    if not _is_sol_address(mint):
        return _err_addr("mint")
    from solders.pubkey import Pubkey
    owner = Pubkey.from_string(address)
    mint_pk = Pubkey.from_string(mint)
    ata = None
    program_used = None
    for program in (TOKEN_PROGRAM, TOKEN_2022_PROGRAM):
        candidate, _bump = Pubkey.find_program_address(
            [bytes(owner), bytes(Pubkey.from_string(program)), bytes(mint_pk)],
            Pubkey.from_string(ATA_PROGRAM),
        )
        res = await _rpc("getAccountInfo", [str(candidate), {"encoding": "jsonParsed"}])
        if _is_error(res):
            return res
        if (res or {}).get("value"):
            ata = str(candidate)
            program_used = program
            parsed = (((res["value"].get("data") or {}).get("parsed") or {}).get("info") or {})
            amt = parsed.get("tokenAmount") or {}
            return {
                "address": address, "mint": mint, "ata": ata,
                "token_program": program_used, "exists": True,
                "balance": amt.get("uiAmount"), "balance_raw": amt.get("amount"),
                "decimals": amt.get("decimals"), "network": "solana",
            }
        if ata is None:
            ata = str(candidate)
            program_used = program
    return {"address": address, "mint": mint, "ata": ata,
            "token_program": program_used, "exists": False,
            "note": "derived for the legacy token program; account not created yet",
            "network": "solana"}


@endpoint("/wallets/{address}/summary", "$0.01",
          "One-call wallet summary: SOL balance, token and NFT counts, delegated stake, last activity, validator flag. $0.01 USDC per call.",
          "wallet", ["wallet", "summary", "profile"], "wallet-summary")
async def wallet_summary(address: str):
    if not _is_sol_address(address):
        return _err_addr()
    bal_res = await _rpc("getBalance", [address])
    if _is_error(bal_res):
        return bal_res
    accounts = await _wallet_token_accounts(address)
    if _is_error(accounts):
        return accounts
    sigs = await _rpc("getSignaturesForAddress", [address, {"limit": 1}])
    last_activity = None
    if not _is_error(sigs) and sigs:
        last_activity = sigs[0].get("blockTime")
    return {
        "address": address,
        "sol_balance": _round(_sol((bal_res or {}).get("value")), 9),
        "token_accounts": len(accounts),
        "nonzero_tokens": sum(1 for a in accounts if (a.get("amount") or 0) > 0),
        "nft_shaped_holdings": sum(1 for a in accounts if a.get("decimals") == 0 and (a.get("amount") or 0) >= 1),
        "last_activity_unix": last_activity,
        "network": "solana",
    }


# ===== TRANSACTIONS =====

@endpoint("/transactions/{signature}", "$0.01",
          "One Solana transaction: slot, time, fee, status, compute units, programs involved and SOL balance changes. $0.01 USDC per call.",
          "transaction", ["transaction", "detail"], "transaction-detail")
async def transaction_detail(signature: str):
    tx = await _rpc("getTransaction", [signature, {"encoding": "jsonParsed",
                                                    "maxSupportedTransactionVersion": 0}])
    if _is_error(tx):
        return tx
    if not tx:
        return {"error": "transaction not found", "signature": signature}
    meta = tx.get("meta") or {}
    message = ((tx.get("transaction") or {}).get("message")) or {}
    account_keys = [k.get("pubkey") if isinstance(k, dict) else k
                    for k in message.get("accountKeys") or []]
    pre = meta.get("preBalances") or []
    post = meta.get("postBalances") or []
    changes = []
    for i, key in enumerate(account_keys):
        if i < len(pre) and i < len(post) and pre[i] != post[i]:
            changes.append({"account": key, "change_sol": _round(_sol(post[i] - pre[i]), 9)})
    programs = []
    for ix in message.get("instructions") or []:
        pid = ix.get("programId") if isinstance(ix, dict) else None
        if pid and pid not in programs:
            programs.append(pid)
    return {
        "signature": signature,
        "slot": tx.get("slot"),
        "block_time": tx.get("blockTime"),
        "version": tx.get("version"),
        "fee_lamports": meta.get("fee"),
        "fee_sol": _round(_sol(meta.get("fee")), 9),
        "status": "failed" if meta.get("err") else "success",
        "err": meta.get("err"),
        "compute_units_consumed": meta.get("computeUnitsConsumed"),
        "programs": programs,
        "account_count": len(account_keys),
        "balance_changes": changes[:20],
        "log_messages": len(meta.get("logMessages") or []),
        "network": "solana",
    }


@endpoint("/transactions/{signature}/status", "$0.01",
          "Confirmation status of one Solana transaction: finalized/confirmed status, slot, error. $0.01 USDC per call.",
          "transaction", ["transaction", "status"], "transaction-status")
async def transaction_status(signature: str):
    res = await _rpc("getSignatureStatuses", [[signature], {"searchTransactionHistory": True}])
    if _is_error(res):
        return res
    value = (res or {}).get("value") or []
    status = value[0] if value else None
    if not status:
        return {"signature": signature, "found": False, "network": "solana"}
    return {
        "signature": signature,
        "found": True,
        "slot": status.get("slot"),
        "confirmation_status": status.get("confirmationStatus"),
        "confirmations": status.get("confirmations"),
        "err": status.get("err"),
        "network": "solana",
    }


@endpoint("/transactions/{signature}/balances", "$0.01",
          "Pre/post balances for one transaction: SOL changes per account and SPL token balance changes. $0.01 USDC per call.",
          "transaction", ["transaction", "balances"], "transaction-balances")
async def transaction_balances(signature: str):
    tx = await _rpc("getTransaction", [signature, {"encoding": "jsonParsed",
                                                    "maxSupportedTransactionVersion": 0}])
    if _is_error(tx):
        return tx
    if not tx:
        return {"error": "transaction not found", "signature": signature}
    meta = tx.get("meta") or {}
    message = ((tx.get("transaction") or {}).get("message")) or {}
    account_keys = [k.get("pubkey") if isinstance(k, dict) else k
                    for k in message.get("accountKeys") or []]
    pre = meta.get("preBalances") or []
    post = meta.get("postBalances") or []
    sol_changes = []
    for i, key in enumerate(account_keys):
        if i < len(pre) and i < len(post):
            sol_changes.append({"account": key,
                                "pre_sol": _round(_sol(pre[i]), 9),
                                "post_sol": _round(_sol(post[i]), 9),
                                "change_sol": _round(_sol(post[i] - pre[i]), 9)})
    def token_rows(rows):
        out = []
        for r in rows or []:
            amt = r.get("uiTokenAmount") or {}
            out.append({"account_index": r.get("accountIndex"),
                        "mint": r.get("mint"), "owner": r.get("owner"),
                        "amount": amt.get("uiAmount"), "decimals": amt.get("decimals")})
        return out
    return {
        "signature": signature,
        "sol_balances": sol_changes,
        "pre_token_balances": token_rows(meta.get("preTokenBalances")),
        "post_token_balances": token_rows(meta.get("postTokenBalances")),
        "network": "solana",
    }


@endpoint("/transactions/{signature}/fee", "$0.01",
          "Fee breakdown of one Solana transaction: fee in lamports/SOL/USD, compute units consumed and cost units. $0.01 USDC per call.",
          "transaction", ["transaction", "fee"], "transaction-fee")
async def transaction_fee(signature: str):
    tx = await _rpc("getTransaction", [signature, {"encoding": "json",
                                                    "maxSupportedTransactionVersion": 0}])
    if _is_error(tx):
        return tx
    if not tx:
        return {"error": "transaction not found", "signature": signature}
    meta = tx.get("meta") or {}
    fee = meta.get("fee")
    price = await _sol_price_usd()
    return {
        "signature": signature,
        "fee_lamports": fee,
        "fee_sol": _round(_sol(fee), 9),
        "fee_usd": _round(_sol(fee) * price, 6) if fee is not None and price else None,
        "compute_units_consumed": meta.get("computeUnitsConsumed"),
        "cost_units": meta.get("costUnits"),
        "network": "solana",
    }


@endpoint("/transactions/{signature}/programs", "$0.01",
          "Programs invoked by one Solana transaction: program ids, instruction counts and log size. $0.01 USDC per call.",
          "transaction", ["transaction", "programs"], "transaction-programs")
async def transaction_programs(signature: str):
    tx = await _rpc("getTransaction", [signature, {"encoding": "jsonParsed",
                                                    "maxSupportedTransactionVersion": 0}])
    if _is_error(tx):
        return tx
    if not tx:
        return {"error": "transaction not found", "signature": signature}
    meta = tx.get("meta") or {}
    message = ((tx.get("transaction") or {}).get("message")) or {}
    counts = {}
    for ix in message.get("instructions") or []:
        if isinstance(ix, dict):
            pid = ix.get("programId")
            if pid:
                counts[pid] = counts.get(pid, 0) + 1
    inner = meta.get("innerInstructions") or []
    inner_count = sum(len(g.get("instructions") or []) for g in inner)
    return {
        "signature": signature,
        "programs": [{"program": k, "instructions": v} for k, v in
                     sorted(counts.items(), key=lambda kv: -kv[1])],
        "top_level_instructions": sum(counts.values()),
        "inner_instructions": inner_count,
        "log_messages": len(meta.get("logMessages") or []),
        "network": "solana",
    }


@endpoint("/transactions/block/{slot}", "$0.01",
          "All transaction signatures in one Solana block (first 100 plus total count). $0.01 USDC per call.",
          "transaction", ["transaction", "block"], "transactions-block")
async def transactions_block(slot: int):
    block = await _rpc("getBlock", [slot, {"encoding": "json",
                                           "transactionDetails": "signatures",
                                           "rewards": False,
                                           "maxSupportedTransactionVersion": 0}])
    if _is_error(block):
        return block
    if not block:
        return {"error": "block not available (pruned or skipped slot)", "slot": slot}
    sigs = block.get("signatures") or []
    return {"slot": slot, "count": len(sigs), "signatures": sigs[:100],
            "network": "solana"}


# ===== DEX (Dexscreener pairs + GeckoTerminal pools) =====

@endpoint("/dex/pair/{pair_address}", "$0.01",
          "One Solana DEX pair by address (Dexscreener): tokens, price, liquidity, volume, market cap. $0.01 USDC per call.",
          "dex", ["dex", "pair", "dexscreener"], "dex-pair")
async def dex_pair(pair_address: str):
    if not _is_sol_address(pair_address):
        return _err_addr("pair address")
    data = await _get_json(f"{DEXSCREENER}/latest/dex/pairs/solana/{pair_address}")
    if _is_error(data):
        return data
    pairs = data.get("pairs") if isinstance(data, dict) else None
    if not pairs:
        return {"error": "pair not found on Solana", "pair": pair_address}
    out = _trim_pair(pairs[0])
    out["network"] = "solana"
    return out


@endpoint("/dex/pair/{pair_address}/info", "$0.01",
          "Profile info of one Solana pair: websites, socials and image from Dexscreener. $0.01 USDC per call.",
          "dex", ["dex", "pair", "info"], "dex-pair-info")
async def dex_pair_info(pair_address: str):
    if not _is_sol_address(pair_address):
        return _err_addr("pair address")
    data = await _get_json(f"{DEXSCREENER}/latest/dex/pairs/solana/{pair_address}")
    if _is_error(data):
        return data
    pairs = data.get("pairs") if isinstance(data, dict) else None
    if not pairs:
        return {"error": "pair not found on Solana", "pair": pair_address}
    p = pairs[0]
    info = p.get("info") or {}
    return {
        "pair": pair_address,
        "base_token": (p.get("baseToken") or {}).get("symbol"),
        "websites": info.get("websites"),
        "socials": info.get("socials"),
        "image_url": info.get("imageUrl"),
        "header": info.get("header"),
        "network": "solana",
    }


@endpoint("/dex/pair/{pair_address}/age", "$0.01",
          "Age of one Solana DEX pair: creation timestamp and seconds since it opened. $0.01 USDC per call.",
          "dex", ["dex", "pair", "age"], "dex-pair-age")
async def dex_pair_age(pair_address: str):
    if not _is_sol_address(pair_address):
        return _err_addr("pair address")
    data = await _get_json(f"{DEXSCREENER}/latest/dex/pairs/solana/{pair_address}")
    if _is_error(data):
        return data
    pairs = data.get("pairs") if isinstance(data, dict) else None
    if not pairs:
        return {"error": "pair not found on Solana", "pair": pair_address}
    created = pairs[0].get("pairCreatedAt")
    age = None
    if created:
        age = int(time.time()) - int(created / 1000)
    return {"pair": pair_address, "created_at_ms": created,
            "age_seconds": age, "network": "solana"}


@endpoint("/dex/trending", "$0.01",
          "Trending Solana pools on GeckoTerminal right now (top 10). $0.01 USDC per call.",
          "dex", ["dex", "trending", "geckoterminal"], "dex-trending")
async def dex_trending():
    data = await _gt("/networks/solana/trending_pools?page=1")
    if _is_error(data):
        return data
    pools = data.get("data") or []
    return {"pools": [_gt_pool_trim(p) for p in pools[:10]],
            "count": len(pools), "network": "solana"}


@endpoint("/dex/new-pools", "$0.01",
          "Newest Solana pools on GeckoTerminal (top 10 by creation). $0.01 USDC per call.",
          "dex", ["dex", "new", "pools"], "dex-new-pools")
async def dex_new_pools():
    data = await _gt("/networks/solana/new_pools?page=1")
    if _is_error(data):
        return data
    pools = data.get("data") or []
    return {"pools": [_gt_pool_trim(p) for p in pools[:10]],
            "count": len(pools), "network": "solana"}


@endpoint("/dex/pools/top", "$0.01",
          "Top Solana pools on GeckoTerminal (top 10 by their ranking: reserves and activity). $0.01 USDC per call.",
          "dex", ["dex", "pools", "top"], "dex-pools-top")
async def dex_pools_top():
    data = await _gt("/networks/solana/pools?page=1")
    if _is_error(data):
        return data
    pools = data.get("data") or []
    return {"pools": [_gt_pool_trim(p) for p in pools[:10]],
            "count": len(pools), "network": "solana"}


@endpoint("/dex/pools/{pool_address}", "$0.01",
          "One Solana pool on GeckoTerminal: prices, reserves, volume and transaction windows. $0.01 USDC per call.",
          "dex", ["dex", "pool", "geckoterminal"], "dex-pool")
async def dex_pool(pool_address: str):
    if not _is_sol_address(pool_address):
        return _err_addr("pool address")
    data = await _gt(f"/networks/solana/pools/{pool_address}")
    if _is_error(data):
        return data
    pool = data.get("data")
    if not pool:
        return {"error": "pool not found", "pool": pool_address}
    out = _gt_pool_trim(pool)
    out["network"] = "solana"
    return out


@endpoint("/dex/pools/{pool_address}/trades", "$0.01",
          "Recent trades in one Solana pool (GeckoTerminal, latest 20): kind, size in USD, price, maker. $0.01 USDC per call.",
          "dex", ["dex", "pool", "trades"], "dex-pool-trades")
async def dex_pool_trades(pool_address: str):
    if not _is_sol_address(pool_address):
        return _err_addr("pool address")
    data = await _gt(f"/networks/solana/pools/{pool_address}/trades", ttl=10)
    if _is_error(data):
        return data
    trades = []
    for t in (data.get("data") or [])[:20]:
        a = t.get("attributes") or {}
        trades.append({
            "kind": a.get("kind"),
            "block_number": a.get("block_number"),
            "timestamp": a.get("block_timestamp"),
            "from": a.get("from_address"),
            "volume_usd": _round(_fnum(a.get("volume_in_usd")), 2),
            "price_from_usd": _fnum(a.get("price_from_in_currency_token")),
            "price_to_usd": _fnum(a.get("price_to_in_currency_token")),
        })
    return {"pool": pool_address, "trades": trades, "network": "solana"}


@endpoint("/dex/pools/{pool_address}/ohlcv/{timeframe}", "$0.01",
          "OHLCV candles for one Solana pool (GeckoTerminal; timeframe: day, hour or minute; latest 48). $0.01 USDC per call.",
          "dex", ["dex", "pool", "ohlcv"], "dex-pool-ohlcv")
async def dex_pool_ohlcv(pool_address: str, timeframe: str):
    if not _is_sol_address(pool_address):
        return _err_addr("pool address")
    if timeframe not in ("day", "hour", "minute"):
        return {"error": "timeframe must be day, hour or minute"}
    data = await _gt(f"/networks/solana/pools/{pool_address}/ohlcv/{timeframe}?limit=48")
    if _is_error(data):
        return data
    candles = (((data.get("data") or {}).get("attributes") or {}).get("ohlcv_list")) or []
    return {
        "pool": pool_address,
        "timeframe": timeframe,
        "candles": [{"t": c[0], "o": c[1], "h": c[2], "l": c[3], "c": c[4], "v": c[5]}
                    for c in candles if isinstance(c, list) and len(c) == 6],
        "network": "solana",
    }


@endpoint("/dex/pools/{pool_address}/tokens", "$0.01",
          "The two tokens of one Solana pool with their GeckoTerminal token data (price, FDV, supply). $0.01 USDC per call.",
          "dex", ["dex", "pool", "tokens"], "dex-pool-tokens")
async def dex_pool_tokens(pool_address: str):
    if not _is_sol_address(pool_address):
        return _err_addr("pool address")
    data = await _gt(f"/networks/solana/pools/{pool_address}")
    if _is_error(data):
        return data
    pool = data.get("data")
    if not pool:
        return {"error": "pool not found", "pool": pool_address}
    rel = pool.get("relationships") or {}
    out = {"pool": pool_address, "network": "solana"}
    for side in ("base_token", "quote_token"):
        token_id = ((rel.get(side) or {}).get("data") or {}).get("id") or ""
        mint = token_id.replace("solana_", "")
        if not mint:
            out[side] = None
            continue
        tdata = await _gt(f"/networks/solana/tokens/{mint}")
        if _is_error(tdata):
            out[side] = {"mint": mint}
            continue
        a = ((tdata.get("data") or {}).get("attributes")) or {}
        out[side] = {
            "mint": mint, "name": a.get("name"), "symbol": a.get("symbol"),
            "decimals": a.get("decimals"), "price_usd": _fnum(a.get("price_usd")),
            "fdv_usd": _round(_fnum(a.get("fdv_usd")), 2),
            "total_supply": a.get("total_supply"),
        }
    return out


@endpoint("/dex/{dex_id}/pools", "$0.01",
          "Top pools of one Solana DEX on GeckoTerminal (dex id e.g. raydium, orca, meteora, pump-fun). $0.01 USDC per call.",
          "dex", ["dex", "pools", "geckoterminal"], "dex-pools-by-dex")
async def dex_pools_by_dex(dex_id: str):
    data = await _gt(f"/networks/solana/dexes/{dex_id}/pools?page=1")
    if _is_error(data):
        return data
    pools = data.get("data") or []
    return {"dex": dex_id, "pools": [_gt_pool_trim(p) for p in pools[:10]],
            "count": len(pools), "network": "solana"}


@endpoint("/dex/{dex_id}/new-pools", "$0.01",
          "Newest pools of one Solana DEX on GeckoTerminal. $0.01 USDC per call.",
          "dex", ["dex", "new", "pools"], "dex-new-pools-by-dex")
async def dex_new_pools_by_dex(dex_id: str):
    # GeckoTerminal has no per-dex new_pools route (it 404s) and its
    # per-dex pools route only sorts by 24h volume/tx count — so take
    # the global new-pools feed (creation-ordered) and filter by dex.
    matched = []
    scanned = 0
    for page in (1, 2, 3):
        data = await _gt(f"/networks/solana/new_pools?page={page}", ttl=20)
        if _is_error(data):
            if page == 1:
                return data
            break
        pools = data.get("data") or []
        scanned += len(pools)
        for p in pools:
            t = _gt_pool_trim(p)
            if t.get("dex") == dex_id:
                matched.append(t)
        if len(matched) >= 10:
            break
    return {"dex": dex_id, "pools": matched[:10], "count": len(matched),
            "scanned_newest_pools": scanned, "network": "solana"}


@endpoint("/dex/list", "$0.01",
          "All DEXes trading on Solana according to GeckoTerminal. $0.01 USDC per call.",
          "dex", ["dex", "list", "geckoterminal"], "dex-list")
async def dex_list():
    data = await _gt("/networks/solana/dexes?page=1", ttl=300)
    if _is_error(data):
        return data
    dexes = [{"id": d.get("id"), "name": (d.get("attributes") or {}).get("name")}
             for d in (data.get("data") or [])]
    return {"dexes": dexes, "count": len(dexes), "network": "solana"}


@endpoint("/dex/search/{query}", "$0.01",
          "Search Solana DEX pairs by name or symbol (Dexscreener, Solana only, top 10). $0.01 USDC per call.",
          "dex", ["dex", "search", "dexscreener"], "dex-search")
async def dex_search(query: str):
    pairs = await _ds_search(query)
    if _is_error(pairs):
        return pairs
    pairs = sorted(pairs, key=lambda p: -(_fnum((p.get("liquidity") or {}).get("usd")) or 0))
    return {"query": query, "pairs": [_trim_pair(p) for p in pairs[:10]],
            "count": len(pairs), "network": "solana"}


@endpoint("/dex/boosts/top", "$0.01",
          "Top boosted tokens on Dexscreener right now, Solana only. $0.01 USDC per call.",
          "dex", ["dex", "boosts", "trending"], "dex-boosts-top")
async def dex_boosts_top():
    items = await _ds_boosts("top")
    if _is_error(items):
        return items
    return {"boosts": [_trim_boost(b) for b in items[:15]],
            "count": len(items), "network": "solana"}


@endpoint("/dex/boosts/latest", "$0.01",
          "Most recently boosted tokens on Dexscreener, Solana only. $0.01 USDC per call.",
          "dex", ["dex", "boosts", "latest"], "dex-boosts-latest")
async def dex_boosts_latest():
    items = await _ds_boosts("latest")
    if _is_error(items):
        return items
    return {"boosts": [_trim_boost(b) for b in items[:15]],
            "count": len(items), "network": "solana"}


@endpoint("/dex/profiles/latest", "$0.01",
          "Newest token profiles created on Dexscreener, Solana only. $0.01 USDC per call.",
          "dex", ["dex", "profiles", "new"], "dex-profiles-latest")
async def dex_profiles_latest():
    data = await _get_json(f"{DEXSCREENER}/token-profiles/latest/v1", ttl=30)
    if _is_error(data):
        return data
    items = data if isinstance(data, list) else []
    sol_items = [i for i in items if i.get("chainId") == "solana"]
    return {
        "profiles": [{
            "token": i.get("tokenAddress"),
            "url": i.get("url"),
            "description": (i.get("description") or "")[:200] or None,
            "links": i.get("links"),
        } for i in sol_items[:15]],
        "count": len(sol_items),
        "network": "solana",
    }


@endpoint("/dex/volume/top-pools", "$0.01",
          "Aggregate 24h volume and reserves across GeckoTerminal's top Solana pools. $0.01 USDC per call.",
          "dex", ["dex", "volume", "aggregate"], "dex-volume-top-pools")
async def dex_volume_top_pools():
    data = await _gt("/networks/solana/pools?page=1")
    if _is_error(data):
        return data
    pools = data.get("data") or []
    vol = sum(_fnum((p.get("attributes") or {}).get("volume_usd", {}).get("h24")) or 0
              for p in pools)
    reserve = sum(_fnum((p.get("attributes") or {}).get("reserve_in_usd")) or 0
                  for p in pools)
    return {
        "pools_counted": len(pools),
        "total_volume_24h_usd": _round(vol, 2),
        "total_reserve_usd": _round(reserve, 2),
        "network": "solana",
    }


# ===== DEFI (DeFiLlama, Solana-filtered) =====

@endpoint("/defi/tvl", "$0.01",
          "Total value locked on Solana right now (DeFiLlama), with Solana's rank among all chains. $0.01 USDC per call.",
          "defi", ["defi", "tvl", "solana"], "defi-tvl")
async def defi_tvl():
    chains = await _llama_chains()
    if _is_error(chains):
        return chains
    ranked = sorted([c for c in chains if _fnum(c.get("tvl"))],
                    key=lambda c: -c["tvl"])
    solana = next((c for c in ranked if c.get("name") == "Solana"), None)
    if not solana:
        return {"error": "Solana not found in DeFiLlama chains"}
    return {
        "chain": "Solana",
        "tvl_usd": _round(_fnum(solana.get("tvl")), 2),
        "rank": ranked.index(solana) + 1,
        "chains_ranked": len(ranked),
        "token_symbol": solana.get("tokenSymbol"),
        "network": "solana",
    }


@endpoint("/defi/tvl/history", "$0.01",
          "Solana TVL history summary: latest value, 7d/30d-ago values, all-time high and its date. $0.01 USDC per call.",
          "defi", ["defi", "tvl", "history"], "defi-tvl-history")
async def defi_tvl_history():
    data = await _get_json(f"{LLAMA}/v2/historicalChainTvl/Solana", ttl=300)
    if _is_error(data):
        return data
    if not data:
        return {"error": "no TVL history returned"}
    latest = data[-1]
    def ago(days):
        target = latest["date"] - days * 86400
        best = None
        for row in data:
            if row["date"] <= target:
                best = row
            else:
                break
        return best
    ath = max(data, key=lambda r: r.get("tvl") or 0)
    v7 = ago(7)
    v30 = ago(30)
    return {
        "chain": "Solana",
        "latest_tvl_usd": latest.get("tvl"),
        "latest_date": latest.get("date"),
        "tvl_7d_ago_usd": v7.get("tvl") if v7 else None,
        "tvl_30d_ago_usd": v30.get("tvl") if v30 else None,
        "change_7d_percent": round(100.0 * (latest["tvl"] - v7["tvl"]) / v7["tvl"], 3) if v7 and v7.get("tvl") else None,
        "change_30d_percent": round(100.0 * (latest["tvl"] - v30["tvl"]) / v30["tvl"], 3) if v30 and v30.get("tvl") else None,
        "ath_tvl_usd": ath.get("tvl"),
        "ath_date": ath.get("date"),
        "network": "solana",
    }


@endpoint("/defi/tvl/change", "$0.01",
          "Solana TVL percent changes over 1, 7 and 30 days (DeFiLlama history). $0.01 USDC per call.",
          "defi", ["defi", "tvl", "change"], "defi-tvl-change")
async def defi_tvl_change():
    data = await _get_json(f"{LLAMA}/v2/historicalChainTvl/Solana", ttl=300)
    if _is_error(data):
        return data
    if not data:
        return {"error": "no TVL history returned"}
    latest = data[-1]
    def change(days):
        target = latest["date"] - days * 86400
        best = None
        for row in data:
            if row["date"] <= target:
                best = row
            else:
                break
        if not best or not best.get("tvl"):
            return None
        return round(100.0 * (latest["tvl"] - best["tvl"]) / best["tvl"], 3)
    return {
        "chain": "Solana",
        "tvl_usd": latest.get("tvl"),
        "change_1d_percent": change(1),
        "change_7d_percent": change(7),
        "change_30d_percent": change(30),
        "network": "solana",
    }


@endpoint("/defi/tvl/dominance", "$0.01",
          "Solana's share of total cross-chain DeFi TVL and its rank (DeFiLlama). $0.01 USDC per call.",
          "defi", ["defi", "tvl", "dominance"], "defi-tvl-dominance")
async def defi_tvl_dominance():
    chains = await _llama_chains()
    if _is_error(chains):
        return chains
    total = sum(_fnum(c.get("tvl")) or 0 for c in chains)
    ranked = sorted([c for c in chains if _fnum(c.get("tvl"))],
                    key=lambda c: -c["tvl"])
    solana = next((c for c in ranked if c.get("name") == "Solana"), None)
    if not solana or not total:
        return {"error": "Solana not found in DeFiLlama chains"}
    return {
        "chain": "Solana",
        "tvl_usd": _round(_fnum(solana.get("tvl")), 2),
        "all_chains_tvl_usd": _round(total, 2),
        "share_percent": round(100.0 * (_fnum(solana.get("tvl")) or 0) / total, 3),
        "rank": ranked.index(solana) + 1,
        "network": "solana",
    }


@endpoint("/defi/protocols", "$0.01",
          "Top 25 protocols on Solana by TVL (DeFiLlama), with category and 1d/7d change. $0.01 USDC per call.",
          "defi", ["defi", "protocols", "tvl"], "defi-protocols")
async def defi_protocols():
    protocols = await _llama_protocols()
    if _is_error(protocols):
        return protocols
    sol = _solana_protocols(protocols)
    return {"protocols": [_trim_protocol(p) for p in sol[:25]],
            "count": len(sol), "network": "solana"}


@endpoint("/defi/protocols/count", "$0.01",
          "How many DeFiLlama protocols hold TVL on Solana, and their combined Solana TVL. $0.01 USDC per call.",
          "defi", ["defi", "protocols", "count"], "defi-protocols-count")
async def defi_protocols_count():
    protocols = await _llama_protocols()
    if _is_error(protocols):
        return protocols
    sol = _solana_protocols(protocols)
    total = sum((p.get("chainTvls") or {}).get("Solana") or 0 for p in sol)
    return {"protocol_count": len(sol), "combined_solana_tvl_usd": _round(total, 2),
            "network": "solana"}


@endpoint("/defi/protocol/{slug}", "$0.01",
          "One DeFiLlama protocol (by slug): category, chains, and its Solana TVL position. $0.01 USDC per call.",
          "defi", ["defi", "protocol", "tvl"], "defi-protocol")
async def defi_protocol(slug: str):
    data = await _get_json(f"{LLAMA}/protocol/{slug}", ttl=300)
    if _is_error(data):
        return data
    current = data.get("currentChainTvls") or {}
    return {
        "name": data.get("name"),
        "slug": slug,
        "symbol": data.get("symbol"),
        "category": data.get("category"),
        "url": data.get("url"),
        "chains": data.get("chains"),
        "solana_tvl_usd": _round(_fnum(current.get("Solana")), 2),
        "total_tvl_usd": _round(_fnum(current.get("total")), 2) if current.get("total") else None,
        "network": "solana",
    }


@endpoint("/defi/protocol/{slug}/tvl", "$0.01",
          "One protocol's Solana TVL over time: current value plus 7d and 30d changes from its chain history. $0.01 USDC per call.",
          "defi", ["defi", "protocol", "tvl"], "defi-protocol-tvl")
async def defi_protocol_tvl(slug: str):
    data = await _get_json(f"{LLAMA}/protocol/{slug}", ttl=300)
    if _is_error(data):
        return data
    chain_tvls = (data.get("chainTvls") or {}).get("Solana") or {}
    history = chain_tvls.get("tvl") or []
    if not history:
        return {"slug": slug, "error": "protocol has no Solana TVL history"}
    latest = history[-1]
    def ago(days):
        target = latest["date"] - days * 86400
        best = None
        for row in history:
            if row["date"] <= target:
                best = row
            else:
                break
        return best
    v7, v30 = ago(7), ago(30)
    def pct(old):
        if not old or not old.get("totalLiquidityUSD"):
            return None
        return round(100.0 * (latest["totalLiquidityUSD"] - old["totalLiquidityUSD"]) / old["totalLiquidityUSD"], 3)
    return {
        "slug": slug,
        "name": data.get("name"),
        "solana_tvl_usd": latest.get("totalLiquidityUSD"),
        "change_7d_percent": pct(v7),
        "change_30d_percent": pct(v30),
        "network": "solana",
    }


@endpoint("/defi/categories", "$0.01",
          "Solana DeFi TVL by protocol category (lending, DEX, liquid staking, ...), top 15. $0.01 USDC per call.",
          "defi", ["defi", "categories", "tvl"], "defi-categories")
async def defi_categories():
    protocols = await _llama_protocols()
    if _is_error(protocols):
        return protocols
    sol = _solana_protocols(protocols)
    by_cat = {}
    counts = {}
    for p in sol:
        cat = p.get("category") or "Other"
        by_cat[cat] = by_cat.get(cat, 0.0) + ((p.get("chainTvls") or {}).get("Solana") or 0)
        counts[cat] = counts.get(cat, 0) + 1
    rows = [{"category": k, "solana_tvl_usd": _round(v, 2), "protocols": counts[k]}
            for k, v in by_cat.items()]
    rows.sort(key=lambda r: -r["solana_tvl_usd"])
    return {"categories": rows[:15], "network": "solana"}


@endpoint("/defi/liquid-staking", "$0.01",
          "Liquid staking protocols on Solana by TVL (DeFiLlama category filter): Sanctum, Jito, Marinade and peers. $0.01 USDC per call.",
          "defi", ["defi", "liquid-staking", "lst"], "defi-liquid-staking")
async def defi_liquid_staking():
    protocols = await _llama_protocols()
    if _is_error(protocols):
        return protocols
    sol = [p for p in _solana_protocols(protocols)
           if (p.get("category") or "") == "Liquid Staking"]
    return {"protocols": [_trim_protocol(p) for p in sol[:15]],
            "count": len(sol), "network": "solana"}


@endpoint("/defi/dex/volume", "$0.01",
          "Solana DEX volume totals (DeFiLlama): 24h, 7d, 30d volumes and day/week/month changes. $0.01 USDC per call.",
          "defi", ["defi", "dex", "volume"], "defi-dex-volume")
async def defi_dex_volume():
    data = await _get_json(f"{LLAMA}/overview/dexs/Solana", ttl=120)
    if _is_error(data):
        return data
    return {
        "chain": "Solana",
        "total_24h_usd": _round(_fnum(data.get("total24h")), 2),
        "total_7d_usd": _round(_fnum(data.get("total7d")), 2),
        "total_30d_usd": _round(_fnum(data.get("total30d")), 2),
        "change_1d_percent": _round(_fnum(data.get("change_1d")), 3),
        "change_7d_percent": _round(_fnum(data.get("change_7d")), 3),
        "change_1m_percent": _round(_fnum(data.get("change_1m")), 3),
        "network": "solana",
    }


@endpoint("/defi/dex/volume/top", "$0.01",
          "Top Solana DEXes by 24h volume (DeFiLlama overview breakdown). $0.01 USDC per call.",
          "defi", ["defi", "dex", "volume"], "defi-dex-volume-top")
async def defi_dex_volume_top():
    data = await _get_json(f"{LLAMA}/overview/dexs/Solana", ttl=120)
    if _is_error(data):
        return data
    return {"chain": "Solana", "dexes": _overview_top_protocols(data, 10),
            "network": "solana"}


@endpoint("/defi/dex/volume/history", "$0.01",
          "Solana DEX volume for the last 14 days (DeFiLlama daily totals). $0.01 USDC per call.",
          "defi", ["defi", "dex", "volume"], "defi-dex-volume-history")
async def defi_dex_volume_history():
    data = await _get_json(f"{LLAMA}/overview/dexs/Solana",
                           params={"excludeTotalDataChartBreakdown": "true"}, ttl=120)
    if _is_error(data):
        return data
    chart = data.get("totalDataChart") or []
    rows = [{"date": r[0], "volume_usd": _round(_fnum(r[1]), 2)}
            for r in chart[-14:] if isinstance(r, list) and len(r) == 2]
    return {"chain": "Solana", "days": rows, "network": "solana"}


@endpoint("/defi/fees", "$0.01",
          "Solana protocol fees totals (DeFiLlama): 24h, 7d, 30d fees and changes. $0.01 USDC per call.",
          "defi", ["defi", "fees", "solana"], "defi-fees")
async def defi_fees():
    data = await _get_json(f"{LLAMA}/overview/fees/Solana", ttl=120)
    if _is_error(data):
        return data
    return {
        "chain": "Solana",
        "total_24h_usd": _round(_fnum(data.get("total24h")), 2),
        "total_7d_usd": _round(_fnum(data.get("total7d")), 2),
        "total_30d_usd": _round(_fnum(data.get("total30d")), 2),
        "change_1d_percent": _round(_fnum(data.get("change_1d")), 3),
        "change_7d_percent": _round(_fnum(data.get("change_7d")), 3),
        "network": "solana",
    }


@endpoint("/defi/fees/top", "$0.01",
          "Top fee-earning protocols on Solana in the last 24h (DeFiLlama overview breakdown). $0.01 USDC per call.",
          "defi", ["defi", "fees", "top"], "defi-fees-top")
async def defi_fees_top():
    data = await _get_json(f"{LLAMA}/overview/fees/Solana", ttl=120)
    if _is_error(data):
        return data
    return {"chain": "Solana", "protocols": _overview_top_protocols(data, 10),
            "network": "solana"}


@endpoint("/defi/fees/history", "$0.01",
          "Solana protocol fees for the last 14 days (DeFiLlama daily totals). $0.01 USDC per call.",
          "defi", ["defi", "fees", "history"], "defi-fees-history")
async def defi_fees_history():
    data = await _get_json(f"{LLAMA}/overview/fees/Solana",
                           params={"excludeTotalDataChartBreakdown": "true"}, ttl=120)
    if _is_error(data):
        return data
    chart = data.get("totalDataChart") or []
    rows = [{"date": r[0], "fees_usd": _round(_fnum(r[1]), 2)}
            for r in chart[-14:] if isinstance(r, list) and len(r) == 2]
    return {"chain": "Solana", "days": rows, "network": "solana"}


async def _solana_yield_pools():
    pools = await _llama_yield_pools()
    if _is_error(pools):
        return pools
    return [p for p in pools if p.get("chain") == "Solana"]


@endpoint("/defi/yields", "$0.01",
          "Highest-APY yield pools on Solana right now (DeFiLlama yields, minimum $100k TVL, top 15). $0.01 USDC per call.",
          "defi", ["defi", "yields", "apy"], "defi-yields")
async def defi_yields():
    pools = await _solana_yield_pools()
    if _is_error(pools):
        return pools
    rows = [p for p in pools if (_fnum(p.get("tvlUsd")) or 0) >= 100000
            and _fnum(p.get("apy")) is not None]
    rows.sort(key=lambda p: -(p.get("apy") or 0))
    return {"pools": [_trim_yield_pool(p) for p in rows[:15]],
            "count": len(rows), "network": "solana"}


@endpoint("/defi/yields/top-tvl", "$0.01",
          "Largest yield pools on Solana by TVL (DeFiLlama yields, top 15) with their APYs. $0.01 USDC per call.",
          "defi", ["defi", "yields", "tvl"], "defi-yields-top-tvl")
async def defi_yields_top_tvl():
    pools = await _solana_yield_pools()
    if _is_error(pools):
        return pools
    rows = sorted(pools, key=lambda p: -(p.get("tvlUsd") or 0))
    return {"pools": [_trim_yield_pool(p) for p in rows[:15]],
            "count": len(rows), "network": "solana"}


@endpoint("/defi/yields/stablecoins", "$0.01",
          "Best stablecoin yields on Solana (DeFiLlama yields, stablecoin pools, minimum $100k TVL). $0.01 USDC per call.",
          "defi", ["defi", "yields", "stablecoins"], "defi-yields-stablecoins")
async def defi_yields_stablecoins():
    pools = await _solana_yield_pools()
    if _is_error(pools):
        return pools
    rows = [p for p in pools if p.get("stablecoin")
            and (_fnum(p.get("tvlUsd")) or 0) >= 100000
            and _fnum(p.get("apy")) is not None]
    rows.sort(key=lambda p: -(p.get("apy") or 0))
    return {"pools": [_trim_yield_pool(p) for p in rows[:15]],
            "count": len(rows), "network": "solana"}


@endpoint("/defi/yields/sol", "$0.01",
          "Best yields for SOL and liquid-staked SOL pools on Solana (symbol contains SOL). $0.01 USDC per call.",
          "defi", ["defi", "yields", "sol"], "defi-yields-sol")
async def defi_yields_sol():
    pools = await _solana_yield_pools()
    if _is_error(pools):
        return pools
    rows = [p for p in pools if "SOL" in (p.get("symbol") or "").upper()
            and (_fnum(p.get("tvlUsd")) or 0) >= 50000
            and _fnum(p.get("apy")) is not None]
    rows.sort(key=lambda p: -(p.get("apy") or 0))
    return {"pools": [_trim_yield_pool(p) for p in rows[:15]],
            "count": len(rows), "network": "solana"}


@endpoint("/defi/yields/usdc", "$0.01",
          "Best USDC yields on Solana (DeFiLlama yields, symbol contains USDC, minimum $50k TVL). $0.01 USDC per call.",
          "defi", ["defi", "yields", "usdc"], "defi-yields-usdc")
async def defi_yields_usdc():
    pools = await _solana_yield_pools()
    if _is_error(pools):
        return pools
    rows = [p for p in pools if "USDC" in (p.get("symbol") or "").upper()
            and (_fnum(p.get("tvlUsd")) or 0) >= 50000
            and _fnum(p.get("apy")) is not None]
    rows.sort(key=lambda p: -(p.get("apy") or 0))
    return {"pools": [_trim_yield_pool(p) for p in rows[:15]],
            "count": len(rows), "network": "solana"}


@endpoint("/defi/yields/project/{project}", "$0.01",
          "Yield pools of one project on Solana (DeFiLlama project id, e.g. kamino, jito-liquid-staking): pools, TVL, best APY. $0.01 USDC per call.",
          "defi", ["defi", "yields", "project"], "defi-yields-project")
async def defi_yields_project(project: str):
    pools = await _solana_yield_pools()
    if _is_error(pools):
        return pools
    rows = [p for p in pools if p.get("project") == project]
    if not rows:
        return {"error": "no Solana pools for project", "project": project}
    rows.sort(key=lambda p: -(p.get("tvlUsd") or 0))
    apys = [p["apy"] for p in rows if _fnum(p.get("apy")) is not None]
    return {
        "project": project,
        "pools": [_trim_yield_pool(p) for p in rows[:15]],
        "pool_count": len(rows),
        "total_tvl_usd": _round(sum(p.get("tvlUsd") or 0 for p in rows), 2),
        "best_apy": _round(max(apys), 4) if apys else None,
        "network": "solana",
    }


@endpoint("/defi/yields/pool/{pool_id}", "$0.01",
          "One DeFiLlama yield pool's recent history: latest APY/TVL and 30-day average APY. $0.01 USDC per call.",
          "defi", ["defi", "yields", "pool"], "defi-yields-pool")
async def defi_yields_pool(pool_id: str):
    data = await _get_json(f"{YIELDS}/chart/{pool_id}", ttl=300)
    if _is_error(data):
        return data
    rows = data.get("data") if isinstance(data, dict) else None
    if not rows:
        return {"error": "no chart data for pool", "pool": pool_id}
    latest = rows[-1]
    last30 = rows[-30:]
    apys = [r["apy"] for r in last30 if _fnum(r.get("apy")) is not None]
    return {
        "pool": pool_id,
        "latest_timestamp": latest.get("timestamp"),
        "latest_apy": latest.get("apy"),
        "latest_tvl_usd": latest.get("tvlUsd"),
        "avg_apy_30d": _round(sum(apys) / len(apys), 4) if apys else None,
        "network": "solana",
    }


@endpoint("/defi/solana/summary", "$0.02",
          "Solana DeFi in one call: TVL, DEX volume, protocol fees, stablecoin value on Solana, protocol count and median yield. $0.02 USDC per call.",
          "defi", ["defi", "summary", "solana"], "defi-solana-summary")
async def defi_solana_summary():
    chains = await _llama_chains()
    if _is_error(chains):
        return chains
    solana = next((c for c in chains if c.get("name") == "Solana"), None)
    dex = await _get_json(f"{LLAMA}/overview/dexs/Solana",
                          params={"excludeTotalDataChart": "true",
                                  "excludeTotalDataChartBreakdown": "true"}, ttl=120)
    fees = await _get_json(f"{LLAMA}/overview/fees/Solana",
                           params={"excludeTotalDataChart": "true",
                                   "excludeTotalDataChartBreakdown": "true"}, ttl=120)
    stable_chains = await _get_json(f"{STABLECOINS_API}/stablecoinchains", ttl=300)
    stable_solana = None
    if not _is_error(stable_chains) and isinstance(stable_chains, list):
        entry = next((c for c in stable_chains if c.get("name") == "Solana"), None)
        if entry:
            stable_solana = ((entry.get("totalCirculatingUSD") or {}).get("peggedUSD"))
    pools = await _solana_yield_pools()
    median_apy = None
    protocol_count = None
    if not _is_error(pools):
        apys = sorted(p["apy"] for p in pools
                      if _fnum(p.get("apy")) is not None and (_fnum(p.get("tvlUsd")) or 0) >= 10000)
        if apys:
            median_apy = _round(apys[len(apys) // 2], 4)
    protocols = await _llama_protocols()
    if not _is_error(protocols):
        protocol_count = len(_solana_protocols(protocols))
    return {
        "chain": "Solana",
        "tvl_usd": _round(_fnum(solana.get("tvl")), 2) if solana else None,
        "dex_volume_24h_usd": _round(_fnum(dex.get("total24h")), 2) if not _is_error(dex) else None,
        "protocol_fees_24h_usd": _round(_fnum(fees.get("total24h")), 2) if not _is_error(fees) else None,
        "stablecoin_value_on_solana_usd": _round(_fnum(stable_solana), 2),
        "protocol_count": protocol_count,
        "median_yield_apy": median_apy,
        "network": "solana",
    }


# ===== ORACLES (Pyth feed directory + cross-source price checks) =====
# Pyth's Hermes price-update endpoints now answer 401 without an API
# key (verified 2026-11-23), so no endpoint here pretends to serve
# Pyth prices. The Pyth endpoints serve the real, public feed
# directory; the price endpoints cross-check Jupiter, Dexscreener and
# CoinGecko — the sources that do answer for free.

@endpoint("/oracles/pyth/feeds", "$0.01",
          "Pyth price-feed directory (crypto): total feed count and the first 50 feeds with ids and symbols. $0.01 USDC per call.",
          "oracle", ["oracle", "pyth", "feeds"], "oracle-pyth-feeds")
async def oracle_pyth_feeds():
    feeds = await _pyth_feeds()
    if _is_error(feeds):
        return feeds
    feeds = feeds if isinstance(feeds, list) else []
    return {"feeds": [_trim_pyth_feed(f) for f in feeds[:50]],
            "count": len(feeds), "network": "solana",
            "note": "Pyth is Solana's native oracle network; live price updates require a Pyth API key, so this service cross-checks Jupiter/Dexscreener/CoinGecko for prices"}


@endpoint("/oracles/pyth/feeds/search/{query}", "$0.01",
          "Search the Pyth feed directory (e.g. SOL, BTC/USD, JUP): matching feeds with their feed ids. $0.01 USDC per call.",
          "oracle", ["oracle", "pyth", "search"], "oracle-pyth-feeds-search")
async def oracle_pyth_feeds_search(query: str):
    feeds = await _pyth_feeds(query=query)
    if _is_error(feeds):
        return feeds
    feeds = feeds if isinstance(feeds, list) else []
    return {"query": query, "feeds": [_trim_pyth_feed(f) for f in feeds[:25]],
            "count": len(feeds), "network": "solana"}


@endpoint("/oracles/pyth/feed/{symbol}", "$0.01",
          "One Pyth feed by symbol (e.g. SOL/USD): feed id, base, quote, display symbol, market hours. $0.01 USDC per call.",
          "oracle", ["oracle", "pyth", "feed"], "oracle-pyth-feed")
async def oracle_pyth_feed(symbol: str):
    feeds = await _pyth_feeds(query=symbol)
    if _is_error(feeds):
        return feeds
    feeds = feeds if isinstance(feeds, list) else []
    want = ("Crypto." + symbol).lower()
    exact = next((f for f in feeds
                  if ((f.get("attributes") or {}).get("symbol") or "").lower() == want), None)
    chosen = exact or (feeds[0] if feeds else None)
    if not chosen:
        return {"error": "no Pyth feed matches symbol", "symbol": symbol}
    out = _trim_pyth_feed(chosen)
    out["network"] = "solana"
    return out


async def _cross_source_price(mint, coingecko_id=None):
    """Jupiter vs Dexscreener (vs CoinGecko) price cross-check."""
    prices = await _jupiter_prices([mint])
    jup = None
    if isinstance(prices, dict) and not _is_error(prices):
        entry = prices.get(mint) or {}
        jup = _fnum(entry.get("usdPrice"))
    ds_price = None
    pairs = await _ds_base_pairs(mint)
    if not _is_error(pairs):
        best = _best_pair(pairs)
        if best:
            ds_price = _fnum(best.get("priceUsd"))
    cg_price = None
    if coingecko_id:
        cg = await _get_json(f"{COINGECKO}/simple/price",
                             params={"ids": coingecko_id, "vs_currencies": "usd"},
                             ttl=60)
        if not _is_error(cg):
            cg_price = _fnum((cg.get(coingecko_id) or {}).get("usd"))
    values = [v for v in (jup, ds_price, cg_price) if v]
    median = None
    max_dev = None
    if values:
        ordered = sorted(values)
        median = ordered[len(ordered) // 2]
        if median:
            max_dev = round(100.0 * max(abs(v - median) for v in values) / median, 4)
    return {
        "mint": mint,
        "jupiter_usd": _round(jup, 8),
        "dexscreener_usd": _round(ds_price, 8),
        "coingecko_usd": _round(cg_price, 8),
        "median_usd": _round(median, 8),
        "max_deviation_percent": max_dev,
        "sources_reporting": len(values),
        "network": "solana",
    }


@endpoint("/oracles/price/{mint}", "$0.01",
          "Cross-source USD price check for one Solana mint: Jupiter vs Dexscreener, median and max deviation. $0.01 USDC per call.",
          "oracle", ["oracle", "price", "cross-source"], "oracle-price")
async def oracle_price(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    return await _cross_source_price(mint)


@endpoint("/oracles/sol", "$0.02",
          "Cross-source SOL price check: Jupiter, Dexscreener and CoinGecko compared, with median and max deviation. $0.02 USDC per call.",
          "oracle", ["oracle", "sol", "price"], "oracle-sol")
async def oracle_sol():
    out = await _cross_source_price(WSOL, coingecko_id="solana")
    out["asset"] = "SOL"
    return out


@endpoint("/oracles/btc", "$0.01",
          "Cross-source BTC price check on Solana (cbBTC mint): Jupiter vs Dexscreener vs CoinGecko bitcoin. $0.01 USDC per call.",
          "oracle", ["oracle", "btc", "price"], "oracle-btc")
async def oracle_btc():
    results = await _jupiter_search("cbBTC")
    mint = None
    if not _is_error(results):
        exact = next((t for t in results if (t.get("symbol") or "") == "cbBTC"), None)
        if exact:
            mint = exact.get("id")
    if not mint:
        return {"error": "could not resolve the cbBTC mint via Jupiter"}
    out = await _cross_source_price(mint, coingecko_id="bitcoin")
    out["asset"] = "BTC (cbBTC on Solana)"
    return out


@endpoint("/oracles/eth", "$0.01",
          "Cross-source ETH price check on Solana (bridged ETH mint): Jupiter vs Dexscreener vs CoinGecko ethereum. $0.01 USDC per call.",
          "oracle", ["oracle", "eth", "price"], "oracle-eth")
async def oracle_eth():
    mint = "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs"  # Wormhole-wrapped ETH
    out = await _cross_source_price(mint, coingecko_id="ethereum")
    out["asset"] = "ETH (bridged to Solana)"
    return out


@endpoint("/oracles/usdc", "$0.01",
          "USDC peg check on Solana: Jupiter, Dexscreener and CoinGecko prices compared against the $1 peg. $0.01 USDC per call.",
          "oracle", ["oracle", "usdc", "peg"], "oracle-usdc")
async def oracle_usdc():
    out = await _cross_source_price(SOLANA_USDC, coingecko_id="usd-coin")
    median = out.get("median_usd")
    out["asset"] = "USDC"
    out["peg_deviation_percent"] = round(100.0 * abs(median - 1.0), 4) if median else None
    return out


@endpoint("/oracles/deviation/{mint}", "$0.01",
          "Price deviation between Jupiter and Dexscreener for one Solana mint (percent, with both prices). $0.01 USDC per call.",
          "oracle", ["oracle", "deviation", "price"], "oracle-deviation")
async def oracle_deviation(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    out = await _cross_source_price(mint)
    return {
        "mint": mint,
        "jupiter_usd": out["jupiter_usd"],
        "dexscreener_usd": out["dexscreener_usd"],
        "deviation_percent": out["max_deviation_percent"],
        "network": "solana",
    }


@endpoint("/oracles/jupiter/{mint}", "$0.01",
          "Jupiter's oracle-grade price detail for one mint: price, 24h change, liquidity, last-update block. $0.01 USDC per call.",
          "oracle", ["oracle", "jupiter", "price"], "oracle-jupiter")
async def oracle_jupiter(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    data = await _jupiter_prices([mint])
    if _is_error(data):
        return data
    entry = data.get(mint)
    if not entry:
        return {"error": "Jupiter has no price for this mint", "mint": mint}
    return {
        "mint": mint,
        "price_usd": _round(_fnum(entry.get("usdPrice")), 10),
        "price_change_24h_percent": _round(_fnum(entry.get("priceChange24h")), 4),
        "liquidity_usd": _round(_fnum(entry.get("liquidity")), 2),
        "decimals": entry.get("decimals"),
        "block_id": entry.get("blockId"),
        "source": "jupiter price v3",
        "network": "solana",
    }


@endpoint("/oracles/dexscreener/{mint}", "$0.01",
          "Dexscreener's price view of one Solana mint from its deepest base-asset pair. $0.01 USDC per call.",
          "oracle", ["oracle", "dexscreener", "price"], "oracle-dexscreener")
async def oracle_dexscreener(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    pairs = await _ds_base_pairs(mint)
    if _is_error(pairs):
        return pairs
    best = _best_pair(pairs)
    if not best:
        return {"error": "token is not the base asset of any Solana pair", "mint": mint}
    return {
        "mint": mint,
        "price_usd": best.get("priceUsd"),
        "liquidity_usd": (best.get("liquidity") or {}).get("usd"),
        "volume_24h_usd": (best.get("volume") or {}).get("h24"),
        "pair": best.get("pairAddress"),
        "source": "dexscreener",
        "network": "solana",
    }


@endpoint("/oracles/coingecko/{mint}", "$0.01",
          "CoinGecko's price view of one Solana mint by contract address. $0.01 USDC per call.",
          "oracle", ["oracle", "coingecko", "price"], "oracle-coingecko")
async def oracle_coingecko(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    data = await _get_json(f"{COINGECKO}/coins/solana/contract/{mint}",
                           params={"localization": "false", "tickers": "false",
                                   "market_data": "true", "community_data": "false",
                                   "developer_data": "false"}, ttl=60)
    if _is_error(data):
        return data
    md = data.get("market_data") or {}
    return {
        "mint": mint,
        "name": data.get("name"),
        "price_usd": (md.get("current_price") or {}).get("usd"),
        "price_change_24h_percent": md.get("price_change_percentage_24h"),
        "market_cap_usd": (md.get("market_cap") or {}).get("usd"),
        "source": "coingecko",
        "network": "solana",
    }


# ===== MEMECOINS (pump.fun + Dexscreener + GeckoTerminal) =====

def _trim_pump_coin(c):
    return {
        "mint": c.get("mint"),
        "name": c.get("name"),
        "symbol": c.get("symbol"),
        "description": (c.get("description") or "")[:200] or None,
        "image": c.get("image_uri"),
        "creator": c.get("creator"),
        "created_timestamp": c.get("created_timestamp"),
        "market_cap_sol": _round(_fnum(c.get("market_cap")), 4),
        "usd_market_cap": _round(_fnum(c.get("usd_market_cap")), 2),
        "virtual_sol_reserves": c.get("virtual_sol_reserves"),
        "virtual_token_reserves": c.get("virtual_token_reserves"),
        "total_supply": c.get("total_supply"),
        "complete": c.get("complete"),
        "raydium_pool": c.get("raydium_pool"),
        "website": c.get("website"),
        "twitter": c.get("twitter"),
        "telegram": c.get("telegram"),
    }


async def _pump_coins(params, ttl=20):
    return await _get_json(f"{PUMPFUN}/coins", params=params, ttl=ttl)


@endpoint("/memecoins/new", "$0.01",
          "Newest memecoins launched on pump.fun (latest 25): mint, name, market cap, creator, bonding state. $0.01 USDC per call.",
          "memecoin", ["memecoins", "pumpfun", "new"], "memecoins-new")
async def memecoins_new():
    data = await _pump_coins({"offset": 0, "limit": 25, "sort": "created_timestamp",
                              "order": "DESC", "includeNsfw": "false"})
    if _is_error(data):
        return data
    coins = data if isinstance(data, list) else []
    return {"coins": [_trim_pump_coin(c) for c in coins],
            "count": len(coins), "network": "solana"}


@endpoint("/memecoins/trending", "$0.01",
          "Biggest memecoins on pump.fun by market cap (top 25): the trending board. $0.01 USDC per call.",
          "memecoin", ["memecoins", "pumpfun", "trending"], "memecoins-trending")
async def memecoins_trending():
    data = await _pump_coins({"offset": 0, "limit": 25, "sort": "market_cap",
                              "order": "DESC", "includeNsfw": "false"})
    if _is_error(data):
        return data
    coins = data if isinstance(data, list) else []
    return {"coins": [_trim_pump_coin(c) for c in coins],
            "count": len(coins), "network": "solana"}


@endpoint("/memecoins/graduating", "$0.02",
          "pump.fun coins closest to graduation: still on the bonding curve (not complete), ranked by market cap. $0.02 USDC per call.",
          "memecoin", ["memecoins", "pumpfun", "graduating"], "memecoins-graduating")
async def memecoins_graduating():
    data = await _pump_coins({"offset": 0, "limit": 50, "sort": "market_cap",
                              "order": "DESC", "includeNsfw": "false"})
    if _is_error(data):
        return data
    coins = [c for c in (data if isinstance(data, list) else [])
             if not c.get("complete")]
    return {
        "coins": [_trim_pump_coin(c) for c in coins[:25]],
        "count": len(coins),
        "note": "graduation = bonding curve completes and the token migrates to a DEX pool; these are the largest still-bonding coins",
        "network": "solana",
    }


@endpoint("/memecoins/{mint}", "$0.01",
          "Memecoin profile for one mint: deepest pair data, price, liquidity, links (Dexscreener). $0.01 USDC per call.",
          "memecoin", ["memecoins", "profile", "dex"], "memecoin-profile")
async def memecoin_profile(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    pairs = await _ds_base_pairs(mint)
    if _is_error(pairs):
        return pairs
    best = _best_pair(pairs)
    if not best:
        return {"error": "no Solana DEX pair for this mint yet (may still be on the bonding curve)",
                "mint": mint}
    info = best.get("info") or {}
    out = _trim_pair(best)
    out.update({
        "mint": mint,
        "websites": info.get("websites"),
        "socials": info.get("socials"),
        "price_change": best.get("priceChange"),
        "txns": best.get("txns"),
        "network": "solana",
    })
    return out


@endpoint("/memecoins/{mint}/price", "$0.01",
          "Price of one memecoin: Dexscreener best pair first, GeckoTerminal top pool as fallback. $0.01 USDC per call.",
          "memecoin", ["memecoins", "price"], "memecoin-price")
async def memecoin_price(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    pairs = await _ds_base_pairs(mint)
    if not _is_error(pairs):
        best = _best_pair(pairs)
        if best:
            return {"mint": mint, "price_usd": best.get("priceUsd"),
                    "source": "dexscreener", "pair": best.get("pairAddress"),
                    "network": "solana"}
    data = await _gt(f"/networks/solana/tokens/{mint}/pools?page=1")
    if _is_error(data):
        return data
    pools = data.get("data") or []
    if not pools:
        return {"error": "no trading pool found for this mint", "mint": mint}
    a = pools[0].get("attributes") or {}
    return {"mint": mint, "price_usd": _fnum(a.get("base_token_price_usd")),
            "source": "geckoterminal", "pool": a.get("address"), "network": "solana"}


@endpoint("/memecoins/{mint}/holders", "$0.01",
          "Largest holders of one memecoin mint (getTokenLargestAccounts, top 20 with supply shares). $0.01 USDC per call.",
          "memecoin", ["memecoins", "holders"], "memecoin-holders")
async def memecoin_holders(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    res = await _rpc("getTokenLargestAccounts", [mint])
    if _is_error(res):
        return res
    value = (res or {}).get("value") or []
    supply_res = await _rpc("getTokenSupply", [mint])
    supply_raw = None
    if not _is_error(supply_res):
        supply_raw = _fnum(((supply_res or {}).get("value") or {}).get("amount"))
    rows = []
    for a in value:
        raw = _fnum(a.get("amount"))
        rows.append({
            "account": a.get("address"),
            "amount": a.get("uiAmount"),
            "share_percent": round(100.0 * raw / supply_raw, 4) if raw is not None and supply_raw else None,
        })
    return {"mint": mint, "holders": rows, "count": len(rows), "network": "solana"}


@endpoint("/memecoins/{mint}/trades", "$0.01",
          "Recent trades of one memecoin in its top GeckoTerminal pool (latest 20). $0.01 USDC per call.",
          "memecoin", ["memecoins", "trades"], "memecoin-trades")
async def memecoin_trades(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    pools = await _gt(f"/networks/solana/tokens/{mint}/pools?page=1")
    if _is_error(pools):
        return pools
    data_list = pools.get("data") or []
    if not data_list:
        return {"error": "no GeckoTerminal pools for token", "mint": mint}
    pool_addr = (data_list[0].get("attributes") or {}).get("address")
    data = await _gt(f"/networks/solana/pools/{pool_addr}/trades", ttl=10)
    if _is_error(data):
        return data
    trades = []
    for t in (data.get("data") or [])[:20]:
        a = t.get("attributes") or {}
        trades.append({
            "kind": a.get("kind"),
            "timestamp": a.get("block_timestamp"),
            "from": a.get("from_address"),
            "volume_usd": _round(_fnum(a.get("volume_in_usd")), 2),
        })
    return {"mint": mint, "pool": pool_addr, "trades": trades, "network": "solana"}


@endpoint("/memecoins/{mint}/risk", "$0.01",
          "Memecoin risk snapshot for one mint: mint/freeze authority status, top-10 holder share, deepest-pair liquidity. $0.01 USDC per call.",
          "memecoin", ["memecoins", "risk", "safety"], "memecoin-risk")
async def memecoin_risk(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    out = {"mint": mint, "network": "solana"}
    info_res = await _mint_info(mint)
    if not _is_error(info_res):
        info = info_res["info"]
        out["mint_authority_active"] = info.get("mintAuthority") is not None
        out["freeze_authority_active"] = info.get("freezeAuthority") is not None
    largest = await _rpc("getTokenLargestAccounts", [mint])
    if not _is_error(largest):
        value = (largest or {}).get("value") or []
        supply_res = await _rpc("getTokenSupply", [mint])
        supply_raw = None
        if not _is_error(supply_res):
            supply_raw = _fnum(((supply_res or {}).get("value") or {}).get("amount"))
        if supply_raw:
            top10 = sum(_fnum(a.get("amount")) or 0 for a in value[:10])
            out["top_10_holder_share_percent"] = round(100.0 * top10 / supply_raw, 4)
    pairs = await _ds_base_pairs(mint)
    if not _is_error(pairs):
        best = _best_pair(pairs)
        out["deepest_pair_liquidity_usd"] = (best.get("liquidity") or {}).get("usd") if best else None
        out["pair_count"] = len(pairs)
    flags = []
    if out.get("mint_authority_active"):
        flags.append("MINT_AUTHORITY_ACTIVE (supply can be inflated)")
    if out.get("freeze_authority_active"):
        flags.append("FREEZE_AUTHORITY_ACTIVE (balances can be frozen)")
    if (out.get("top_10_holder_share_percent") or 0) > 50:
        flags.append("TOP10_HOLDERS_OVER_50_PERCENT")
    if (out.get("deepest_pair_liquidity_usd") or 0) < 10000:
        flags.append("THIN_LIQUIDITY_UNDER_10K")
    out["risk_flags"] = flags
    return out


@endpoint("/memecoins/search/{query}", "$0.01",
          "Search Solana memecoin pairs by name or symbol (Dexscreener, top 10 by liquidity). $0.01 USDC per call.",
          "memecoin", ["memecoins", "search"], "memecoin-search")
async def memecoin_search(query: str):
    pairs = await _ds_search(query)
    if _is_error(pairs):
        return pairs
    pairs = sorted(pairs, key=lambda p: -(_fnum((p.get("liquidity") or {}).get("usd")) or 0))
    return {"query": query, "pairs": [_trim_pair(p) for p in pairs[:10]],
            "count": len(pairs), "network": "solana"}


@endpoint("/memecoins/pools", "$0.01",
          "Top pump.fun AMM pools on GeckoTerminal (graduated memecoins trade here). $0.01 USDC per call.",
          "memecoin", ["memecoins", "pools", "pumpfun"], "memecoin-pools")
async def memecoin_pools():
    data = await _gt("/networks/solana/dexes/pump-fun/pools?page=1")
    if _is_error(data):
        return data
    pools = data.get("data") or []
    return {"pools": [_gt_pool_trim(p) for p in pools[:10]],
            "count": len(pools), "network": "solana"}


@endpoint("/memecoins/trending/pools", "$0.01",
          "Trending Solana pools (GeckoTerminal) — the memecoin-heavy board, top 10 with 24h changes. $0.01 USDC per call.",
          "memecoin", ["memecoins", "trending", "pools"], "memecoin-trending-pools")
async def memecoin_trending_pools():
    data = await _gt("/networks/solana/trending_pools?page=1")
    if _is_error(data):
        return data
    pools = data.get("data") or []
    return {"pools": [_gt_pool_trim(p) for p in pools[:10]],
            "count": len(pools), "network": "solana"}


@endpoint("/memecoins/movers", "$0.01",
          "Biggest memecoin movers among trending Solana pools, ranked by absolute 24h price change. $0.01 USDC per call.",
          "memecoin", ["memecoins", "movers", "gainers"], "memecoin-movers")
async def memecoin_movers():
    rows = []
    for page in (1, 2):
        data = await _gt(f"/networks/solana/trending_pools?page={page}")
        if _is_error(data):
            if page == 1:
                return data
            break
        for p in data.get("data") or []:
            t = _gt_pool_trim(p)
            if t["price_change_24h_pct"] is not None:
                rows.append(t)
    rows.sort(key=lambda r: -abs(r["price_change_24h_pct"]))
    return {"movers": rows[:10], "count": len(rows), "network": "solana"}


@endpoint("/memecoins/stats", "$0.01",
          "pump.fun launchpad snapshot: launches in the newest 50, graduated share of the top 50, combined top-50 market cap. $0.01 USDC per call.",
          "memecoin", ["memecoins", "stats", "pumpfun"], "memecoin-stats")
async def memecoin_stats():
    newest = await _pump_coins({"offset": 0, "limit": 50, "sort": "created_timestamp",
                                "order": "DESC", "includeNsfw": "false"})
    if _is_error(newest):
        return newest
    top = await _pump_coins({"offset": 0, "limit": 50, "sort": "market_cap",
                             "order": "DESC", "includeNsfw": "false"})
    if _is_error(top):
        return top
    newest = newest if isinstance(newest, list) else []
    top = top if isinstance(top, list) else []
    now_ms = int(time.time() * 1000)
    last_hour = sum(1 for c in newest
                    if (c.get("created_timestamp") or 0) > now_ms - 3600_000)
    graduated = sum(1 for c in top if c.get("complete"))
    total_mcap = sum(_fnum(c.get("usd_market_cap")) or 0 for c in top)
    return {
        "launches_in_newest_50": len(newest),
        "launches_last_hour_in_sample": last_hour,
        "top_50_graduated": graduated,
        "top_50_still_bonding": len(top) - graduated,
        "top_50_combined_market_cap_usd": _round(total_mcap, 2),
        "network": "solana",
    }


# ===== STABLECOINS (DeFiLlama stablecoins, Solana-aware) =====

def _find_stablecoin(assets, key):
    key = key.lower()
    for a in assets or []:
        if (a.get("symbol") or "").lower() == key or (a.get("id") or "").lower() == key \
                or (a.get("name") or "").lower() == key:
            return a
    return None


def _trim_stablecoin(a):
    circ = a.get("circulating") or {}
    return {
        "id": a.get("id"),
        "name": a.get("name"),
        "symbol": a.get("symbol"),
        "peg_type": a.get("pegType"),
        "peg_mechanism": a.get("pegMechanism"),
        "price_usd": _round(_fnum(a.get("price")), 6),
        "circulating_usd": _round(_fnum(circ.get("peggedUSD")), 2),
        "chains": len(a.get("chainCirculating") or {}),
    }


def _stablecoin_chain_amount(asset, chain="Solana"):
    entry = ((asset.get("chainCirculating") or {}).get(chain)) or {}
    current = entry.get("current") or {}
    return _fnum(current.get("peggedUSD"))


@endpoint("/stablecoins/top", "$0.01",
          "Top 12 stablecoins by market cap with prices and peg types (DeFiLlama). $0.01 USDC per call.",
          "stablecoin", ["stablecoins", "top"], "stablecoins-top")
async def stablecoins_top():
    assets = await _llama_stablecoins()
    if assets is None:
        return {"error": "stablecoin data unavailable"}
    ranked = sorted(assets,
                    key=lambda a: -(_fnum((a.get("circulating") or {}).get("peggedUSD")) or 0))
    return {"stablecoins": [_trim_stablecoin(a) for a in ranked[:12]],
            "network": "solana"}


@endpoint("/stablecoins/{asset}", "$0.01",
          "One stablecoin in detail (by symbol or DeFiLlama id): supply, price, peg mechanism, and its Solana-chain amount. $0.01 USDC per call.",
          "stablecoin", ["stablecoins", "detail"], "stablecoin-detail")
async def stablecoin_detail(asset: str):
    assets = await _llama_stablecoins()
    if assets is None:
        return {"error": "stablecoin data unavailable"}
    found = _find_stablecoin(assets, asset)
    if not found:
        return {"error": "stablecoin not found", "asset": asset}
    out = _trim_stablecoin(found)
    out["solana_circulating_usd"] = _round(_stablecoin_chain_amount(found), 2)
    out["network"] = "solana"
    return out


@endpoint("/stablecoins/usdc", "$0.01",
          "USDC in detail: global supply, price, and how much USDC circulates on Solana. $0.01 USDC per call.",
          "stablecoin", ["stablecoins", "usdc"], "stablecoin-usdc")
async def stablecoin_usdc():
    assets = await _llama_stablecoins()
    if assets is None:
        return {"error": "stablecoin data unavailable"}
    found = _find_stablecoin(assets, "USDC")
    if not found:
        return {"error": "USDC not found in stablecoin data"}
    out = _trim_stablecoin(found)
    solana_amt = _stablecoin_chain_amount(found)
    total = _fnum((found.get("circulating") or {}).get("peggedUSD"))
    out["solana_circulating_usd"] = _round(solana_amt, 2)
    out["solana_share_percent"] = round(100.0 * solana_amt / total, 3) if solana_amt and total else None
    out["network"] = "solana"
    return out


@endpoint("/stablecoins/usdt", "$0.01",
          "USDT in detail: global supply, price, and how much USDT circulates on Solana. $0.01 USDC per call.",
          "stablecoin", ["stablecoins", "usdt"], "stablecoin-usdt")
async def stablecoin_usdt():
    assets = await _llama_stablecoins()
    if assets is None:
        return {"error": "stablecoin data unavailable"}
    found = _find_stablecoin(assets, "USDT")
    if not found:
        return {"error": "USDT not found in stablecoin data"}
    out = _trim_stablecoin(found)
    solana_amt = _stablecoin_chain_amount(found)
    total = _fnum((found.get("circulating") or {}).get("peggedUSD"))
    out["solana_circulating_usd"] = _round(solana_amt, 2)
    out["solana_share_percent"] = round(100.0 * solana_amt / total, 3) if solana_amt and total else None
    out["network"] = "solana"
    return out


@endpoint("/stablecoins/pyusd", "$0.01",
          "PYUSD (PayPal USD) in detail: global supply and its Solana-chain amount. $0.01 USDC per call.",
          "stablecoin", ["stablecoins", "pyusd"], "stablecoin-pyusd")
async def stablecoin_pyusd():
    assets = await _llama_stablecoins()
    if assets is None:
        return {"error": "stablecoin data unavailable"}
    found = _find_stablecoin(assets, "PYUSD")
    if not found:
        return {"error": "PYUSD not found in stablecoin data"}
    out = _trim_stablecoin(found)
    out["solana_circulating_usd"] = _round(_stablecoin_chain_amount(found), 2)
    out["network"] = "solana"
    return out


@endpoint("/stablecoins/solana", "$0.01",
          "Stablecoin value circulating on Solana: chain total plus the top stablecoins' Solana amounts. $0.01 USDC per call.",
          "stablecoin", ["stablecoins", "solana", "chain"], "stablecoins-solana")
async def stablecoins_solana():
    chains = await _get_json(f"{STABLECOINS_API}/stablecoinchains", ttl=300)
    if _is_error(chains):
        return chains
    entry = next((c for c in chains if c.get("name") == "Solana"), None)
    total = None
    if entry:
        total = ((entry.get("totalCirculatingUSD") or {}).get("peggedUSD"))
    assets = await _llama_stablecoins()
    top_assets = []
    if assets:
        rows = []
        for a in assets:
            amt = _stablecoin_chain_amount(a)
            if amt:
                rows.append({"symbol": a.get("symbol"), "name": a.get("name"),
                             "solana_circulating_usd": _round(amt, 2)})
        rows.sort(key=lambda r: -r["solana_circulating_usd"])
        top_assets = rows[:10]
    return {
        "chain": "Solana",
        "total_stablecoin_value_usd": _round(_fnum(total), 2),
        "top_assets_on_solana": top_assets,
        "network": "solana",
    }


@endpoint("/stablecoins/chains", "$0.01",
          "Stablecoin value by chain (DeFiLlama stablecoinchains), top 15 — Solana highlighted by rank. $0.01 USDC per call.",
          "stablecoin", ["stablecoins", "chains"], "stablecoins-chains")
async def stablecoins_chains():
    chains = await _get_json(f"{STABLECOINS_API}/stablecoinchains", ttl=300)
    if _is_error(chains):
        return chains
    rows = []
    for c in chains or []:
        total = ((c.get("totalCirculatingUSD") or {}).get("peggedUSD"))
        if total:
            rows.append({"chain": c.get("name"), "total_usd": _round(_fnum(total), 2)})
    rows.sort(key=lambda r: -r["total_usd"])
    solana_rank = next((i + 1 for i, r in enumerate(rows) if r["chain"] == "Solana"), None)
    return {"chains": rows[:15], "solana_rank": solana_rank, "network": "solana"}


@endpoint("/stablecoins/pegs", "$0.01",
          "Stablecoins trading furthest from their peg right now (DeFiLlama prices, top 10 by absolute deviation). $0.01 USDC per call.",
          "stablecoin", ["stablecoins", "pegs", "deviation"], "stablecoins-pegs")
async def stablecoins_pegs():
    assets = await _llama_stablecoins()
    if assets is None:
        return {"error": "stablecoin data unavailable"}
    rows = []
    for a in assets:
        price = _fnum(a.get("price"))
        circ = _fnum((a.get("circulating") or {}).get("peggedUSD"))
        if price is None or not circ or circ < 1_000_000:
            continue
        rows.append({
            "symbol": a.get("symbol"), "name": a.get("name"),
            "price_usd": _round(price, 6),
            "deviation_percent": round(100.0 * (price - 1.0), 4),
            "circulating_usd": _round(circ, 2),
        })
    rows.sort(key=lambda r: -abs(r["deviation_percent"]))
    return {"stablecoins": rows[:10], "network": "solana",
            "note": "only assets with at least $1M circulating are ranked"}


# ===== MARKETS (CoinGecko, Solana-focused + global context) =====

@endpoint("/markets/global", "$0.01",
          "Global crypto market snapshot (CoinGecko): total market cap, volume, BTC/ETH dominance, active coins. $0.01 USDC per call.",
          "market", ["markets", "global", "coingecko"], "markets-global")
async def markets_global():
    data = await _get_json(f"{COINGECKO}/global", ttl=60)
    if _is_error(data):
        return data
    g = data.get("data") or {}
    return {
        "total_market_cap_usd": (g.get("total_market_cap") or {}).get("usd"),
        "total_volume_usd": (g.get("total_volume") or {}).get("usd"),
        "market_cap_change_24h_percent": g.get("market_cap_change_percentage_24h_usd"),
        "btc_dominance": (g.get("market_cap_percentage") or {}).get("btc"),
        "eth_dominance": (g.get("market_cap_percentage") or {}).get("eth"),
        "sol_dominance": (g.get("market_cap_percentage") or {}).get("sol"),
        "active_cryptocurrencies": g.get("active_cryptocurrencies"),
        "network": "solana",
    }


@endpoint("/markets/sol", "$0.01",
          "SOL market data (CoinGecko): price, market cap and rank, volume, ATH, 24h/7d/30d changes. $0.01 USDC per call.",
          "market", ["markets", "sol", "coingecko"], "markets-sol")
async def markets_sol():
    data = await _get_json(f"{COINGECKO}/coins/solana",
                           params={"localization": "false", "tickers": "false",
                                   "community_data": "false", "developer_data": "false"},
                           ttl=60)
    if _is_error(data):
        return data
    md = data.get("market_data") or {}
    return {
        "id": "solana",
        "symbol": "SOL",
        "market_cap_rank": data.get("market_cap_rank"),
        "price_usd": (md.get("current_price") or {}).get("usd"),
        "market_cap_usd": (md.get("market_cap") or {}).get("usd"),
        "total_volume_usd": (md.get("total_volume") or {}).get("usd"),
        "ath_usd": (md.get("ath") or {}).get("usd"),
        "ath_date": (md.get("ath_date") or {}).get("usd"),
        "change_24h_percent": md.get("price_change_percentage_24h"),
        "change_7d_percent": md.get("price_change_percentage_7d"),
        "change_30d_percent": md.get("price_change_percentage_30d"),
        "circulating_supply": md.get("circulating_supply"),
        "total_supply": md.get("total_supply"),
        "network": "solana",
    }


@endpoint("/markets/sol/price", "$0.01",
          "SOL simple price (CoinGecko): USD price, market cap, 24h volume and 24h change in one light call. $0.01 USDC per call.",
          "market", ["markets", "sol", "price"], "markets-sol-price")
async def markets_sol_price():
    data = await _get_json(f"{COINGECKO}/simple/price",
                           params={"ids": "solana", "vs_currencies": "usd",
                                   "include_market_cap": "true",
                                   "include_24hr_vol": "true",
                                   "include_24hr_change": "true"}, ttl=60)
    if _is_error(data):
        return data
    s = data.get("solana") or {}
    return {
        "symbol": "SOL",
        "price_usd": s.get("usd"),
        "market_cap_usd": s.get("usd_market_cap"),
        "volume_24h_usd": s.get("usd_24h_vol"),
        "change_24h_percent": _round(_fnum(s.get("usd_24h_change")), 4),
        "network": "solana",
    }


@endpoint("/markets/sol/history/{days}", "$0.01",
          "SOL price history summary for N days (CoinGecko market chart): start/end, high, low, change. $0.01 USDC per call.",
          "market", ["markets", "sol", "history"], "markets-sol-history")
async def markets_sol_history(days: int):
    days = max(1, min(days, 365))
    data = await _get_json(f"{COINGECKO}/coins/solana/market_chart",
                           params={"vs_currency": "usd", "days": days}, ttl=120)
    if _is_error(data):
        return data
    prices = data.get("prices") or []
    if not prices:
        return {"error": "no price history returned"}
    values = [p[1] for p in prices]
    return {
        "symbol": "SOL",
        "days": days,
        "points": len(values),
        "start_price_usd": _round(values[0], 4),
        "end_price_usd": _round(values[-1], 4),
        "high_usd": _round(max(values), 4),
        "low_usd": _round(min(values), 4),
        "change_percent": round(100.0 * (values[-1] - values[0]) / values[0], 3) if values[0] else None,
        "network": "solana",
    }


@endpoint("/markets/sol/ohlc/{days}", "$0.01",
          "SOL OHLC candles from CoinGecko (days: 1, 7, 14, 30, 90, 180 or 365; latest 40 candles). $0.01 USDC per call.",
          "market", ["markets", "sol", "ohlc"], "markets-sol-ohlc")
async def markets_sol_ohlc(days: int):
    if days not in (1, 7, 14, 30, 90, 180, 365):
        return {"error": "days must be one of 1, 7, 14, 30, 90, 180, 365"}
    data = await _get_json(f"{COINGECKO}/coins/solana/ohlc",
                           params={"vs_currency": "usd", "days": days}, ttl=120)
    if _is_error(data):
        return data
    candles = data if isinstance(data, list) else []
    return {
        "symbol": "SOL",
        "days": days,
        "candles": [{"t": c[0], "o": c[1], "h": c[2], "l": c[3], "c": c[4]}
                    for c in candles[-40:] if isinstance(c, list) and len(c) == 5],
        "network": "solana",
    }


async def _cg_ecosystem_markets(ttl=120):
    return await _get_json(f"{COINGECKO}/coins/markets",
                           params={"vs_currency": "usd", "category": "solana-ecosystem",
                                   "order": "market_cap_desc", "per_page": 100,
                                   "page": 1, "price_change_percentage": "24h"},
                           ttl=ttl)


def _trim_cg_market(c):
    return {
        "id": c.get("id"),
        "symbol": (c.get("symbol") or "").upper(),
        "name": c.get("name"),
        "price_usd": c.get("current_price"),
        "market_cap_usd": c.get("market_cap"),
        "market_cap_rank": c.get("market_cap_rank"),
        "volume_24h_usd": c.get("total_volume"),
        "change_24h_percent": _round(_fnum(c.get("price_change_percentage_24h")), 3),
    }


@endpoint("/markets/ecosystem", "$0.01",
          "Top Solana-ecosystem tokens by market cap (CoinGecko category): price, volume, 24h change. $0.01 USDC per call.",
          "market", ["markets", "ecosystem", "solana"], "markets-ecosystem")
async def markets_ecosystem():
    data = await _cg_ecosystem_markets()
    if _is_error(data):
        return data
    coins = data if isinstance(data, list) else []
    return {"coins": [_trim_cg_market(c) for c in coins[:15]],
            "count": len(coins), "network": "solana"}


@endpoint("/markets/ecosystem/gainers", "$0.01",
          "Biggest 24h gainers in the Solana ecosystem category (CoinGecko, top 10). $0.01 USDC per call.",
          "market", ["markets", "ecosystem", "gainers"], "markets-ecosystem-gainers")
async def markets_ecosystem_gainers():
    data = await _cg_ecosystem_markets()
    if _is_error(data):
        return data
    coins = [c for c in (data if isinstance(data, list) else [])
             if _fnum(c.get("price_change_percentage_24h")) is not None]
    coins.sort(key=lambda c: -c["price_change_percentage_24h"])
    return {"coins": [_trim_cg_market(c) for c in coins[:10]], "network": "solana"}


@endpoint("/markets/ecosystem/losers", "$0.01",
          "Biggest 24h losers in the Solana ecosystem category (CoinGecko, top 10). $0.01 USDC per call.",
          "market", ["markets", "ecosystem", "losers"], "markets-ecosystem-losers")
async def markets_ecosystem_losers():
    data = await _cg_ecosystem_markets()
    if _is_error(data):
        return data
    coins = [c for c in (data if isinstance(data, list) else [])
             if _fnum(c.get("price_change_percentage_24h")) is not None]
    coins.sort(key=lambda c: c["price_change_percentage_24h"])
    return {"coins": [_trim_cg_market(c) for c in coins[:10]], "network": "solana"}


@endpoint("/markets/trending", "$0.01",
          "Trending coins on CoinGecko search right now (global list, Solana names included when trending). $0.01 USDC per call.",
          "market", ["markets", "trending", "coingecko"], "markets-trending")
async def markets_trending():
    data = await _get_json(f"{COINGECKO}/search/trending", ttl=120)
    if _is_error(data):
        return data
    coins = []
    for entry in (data.get("coins") or [])[:10]:
        item = entry.get("item") or {}
        coins.append({
            "id": item.get("id"),
            "name": item.get("name"),
            "symbol": (item.get("symbol") or "").upper(),
            "market_cap_rank": item.get("market_cap_rank"),
            "price_btc": item.get("price_btc"),
        })
    return {"coins": coins, "network": "solana",
            "note": "global CoinGecko trending list"}


@endpoint("/markets/fear-greed", "$0.01",
          "Crypto Fear & Greed index: current value, classification and timestamp (Alternative.me). $0.01 USDC per call.",
          "market", ["markets", "fear-greed", "sentiment"], "markets-fear-greed")
async def markets_fear_greed():
    data = await _get_json(f"{FNG}/fng/", params={"limit": 1}, ttl=120)
    if _is_error(data):
        return data
    rows = data.get("data") or []
    if not rows:
        return {"error": "no fear & greed data returned"}
    r = rows[0]
    return {"value": int(r["value"]), "classification": r.get("value_classification"),
            "timestamp": int(r["timestamp"]), "network": "solana"}


@endpoint("/markets/fear-greed/history", "$0.01",
          "Fear & Greed index for the last 7 days with the weekly average (Alternative.me). $0.01 USDC per call.",
          "market", ["markets", "fear-greed", "history"], "markets-fear-greed-history")
async def markets_fear_greed_history():
    data = await _get_json(f"{FNG}/fng/", params={"limit": 7}, ttl=120)
    if _is_error(data):
        return data
    rows = [{"value": int(r["value"]), "classification": r.get("value_classification"),
             "timestamp": int(r["timestamp"])} for r in (data.get("data") or [])]
    avg = round(sum(r["value"] for r in rows) / len(rows), 1) if rows else None
    return {"days": rows, "average": avg, "network": "solana"}


# ===== NFTS (Magic Eden public API + RPC) =====
# Magic Eden is the only free NFT source that answers reliably from
# this network (verified 2026-11-23); its stats endpoint occasionally
# drops a connection, in which case the handler returns the upstream
# error rather than invented data.

@endpoint("/nfts/collections", "$0.01",
          "Solana NFT collections on Magic Eden (first page of 20): symbol, name, image, description. $0.01 USDC per call.",
          "nft", ["nfts", "collections", "magiceden"], "nft-collections")
async def nft_collections():
    data = await _get_json(f"{MAGICEDEN}/collections",
                           params={"offset": 0, "limit": 20}, ttl=120)
    if _is_error(data):
        return data
    items = data if isinstance(data, list) else []
    return {
        "collections": [{
            "symbol": c.get("symbol"),
            "name": c.get("name"),
            "image": c.get("image"),
            "description": (c.get("description") or "")[:160] or None,
            "website": c.get("website"),
            "twitter": c.get("twitter"),
            "discord": c.get("discord"),
        } for c in items],
        "count": len(items),
        "network": "solana",
    }


@endpoint("/nfts/collections/{symbol}/stats", "$0.01",
          "Magic Eden stats for one Solana NFT collection: floor price, listed count, average sale prices, volumes. $0.01 USDC per call.",
          "nft", ["nfts", "collections", "stats"], "nft-collection-stats")
async def nft_collection_stats(symbol: str):
    data = await _get_json(f"{MAGICEDEN}/collections/{symbol}/stats", ttl=60)
    if _is_error(data):
        return data
    return {
        "symbol": data.get("symbol", symbol),
        "floor_price_sol": _round(_sol(data.get("floorPrice")), 4),
        "listed_count": data.get("listedCount"),
        "avg_price_24h_sol": _round(_sol(_fnum(data.get("avgPrice24hr"))), 4),
        "volume_24h_sol": _round(_sol(_fnum(data.get("vol24hr"))), 2),
        "volume_7d_sol": _round(_sol(_fnum(data.get("volume7d"))), 2),
        "volume_30d_sol": _round(_sol(_fnum(data.get("vol30d"))), 2),
        "volume_all_sol": _round(_sol(_fnum(data.get("volumeAll"))), 2),
        "network": "solana",
    }


@endpoint("/nfts/collections/{symbol}", "$0.01",
          "One Solana NFT collection's Magic Eden profile: name, description, image and social links. $0.01 USDC per call.",
          "nft", ["nfts", "collections", "magiceden"], "nft-collection")
async def nft_collection(symbol: str):
    data = await _get_json(f"{MAGICEDEN}/collections/{symbol}", ttl=120)
    if _is_error(data):
        return data
    c = data if isinstance(data, dict) else {}
    return {
        "symbol": c.get("symbol", symbol),
        "name": c.get("name"),
        "description": (c.get("description") or "")[:400] or None,
        "image": c.get("image"),
        "website": c.get("website"),
        "twitter": c.get("twitter"),
        "discord": c.get("discord"),
        "network": "solana",
    }


@endpoint("/nfts/{mint}/owner", "$0.01",
          "Current holder of one NFT mint: the largest token account and its owner (an NFT has exactly one holder of 1 unit). $0.01 USDC per call.",
          "nft", ["nfts", "owner", "rpc"], "nft-owner")
async def nft_owner(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    res = await _rpc("getTokenLargestAccounts", [mint])
    if _is_error(res):
        return res
    value = (res or {}).get("value") or []
    holders = [a for a in value if (_fnum(a.get("amount")) or 0) > 0]
    if not holders:
        return {"mint": mint, "owner": None, "note": "no token accounts with a balance",
                "network": "solana"}
    top = holders[0]
    owner = None
    acct = await _rpc("getAccountInfo", [top.get("address"), {"encoding": "jsonParsed"}])
    if not _is_error(acct):
        raw_data = ((acct or {}).get("value") or {}).get("data")
        parsed = (raw_data.get("parsed") or {}) if isinstance(raw_data, dict) else {}
        owner = (parsed.get("info") or {}).get("owner")
    return {
        "mint": mint,
        "token_account": top.get("address"),
        "owner": owner,
        "amount": top.get("uiAmount"),
        "network": "solana",
    }


@endpoint("/nfts/{mint}/accounts", "$0.01",
          "All token accounts holding a balance of one mint (for an NFT this is the single holder account). $0.01 USDC per call.",
          "nft", ["nfts", "accounts", "rpc"], "nft-accounts")
async def nft_accounts(mint: str):
    if not _is_sol_address(mint):
        return _err_addr("mint")
    res = await _rpc("getTokenLargestAccounts", [mint])
    if _is_error(res):
        return res
    value = (res or {}).get("value") or []
    rows = [{"account": a.get("address"), "amount": a.get("uiAmount"),
             "amount_raw": a.get("amount")} for a in value
            if (_fnum(a.get("amount")) or 0) > 0]
    return {"mint": mint, "accounts": rows, "count": len(rows), "network": "solana"}


# ===== UTILITIES (pure/local + light RPC) =====

@endpoint("/utils/convert/lamports/{amount}", "$0.01",
          "Convert lamports to SOL (and USD at the live price). $0.01 USDC per call.",
          "utils", ["utils", "convert", "lamports"], "utils-convert-lamports")
async def utils_convert_lamports(amount: int):
    sol = _sol(amount)
    price = await _sol_price_usd()
    return {"lamports": amount, "sol": sol,
            "usd": _round(sol * price, 4) if price else None, "network": "solana"}


@endpoint("/utils/convert/sol/{amount}", "$0.01",
          "Convert a SOL amount to lamports and USD at the live price. $0.01 USDC per call.",
          "utils", ["utils", "convert", "sol"], "utils-convert-sol")
async def utils_convert_sol(amount: float):
    price = await _sol_price_usd()
    return {"sol": amount, "lamports": int(amount * LAMPORTS_PER_SOL),
            "usd": _round(amount * price, 4) if price else None, "network": "solana"}


@endpoint("/utils/convert/usd-sol/{amount}", "$0.01",
          "Convert a USD amount to SOL at the live Jupiter price. $0.01 USDC per call.",
          "utils", ["utils", "convert", "usd"], "utils-convert-usd-sol")
async def utils_convert_usd_sol(amount: float):
    price = await _sol_price_usd()
    if not price:
        return {"error": "SOL price unavailable"}
    return {"usd": amount, "sol_price_usd": _round(price, 4),
            "sol": _round(amount / price, 8), "network": "solana"}


@endpoint("/utils/convert/sol-usd/{amount}", "$0.01",
          "Convert a SOL amount to USD at the live Jupiter price. $0.01 USDC per call.",
          "utils", ["utils", "convert", "usd"], "utils-convert-sol-usd")
async def utils_convert_sol_usd(amount: float):
    price = await _sol_price_usd()
    if not price:
        return {"error": "SOL price unavailable"}
    return {"sol": amount, "sol_price_usd": _round(price, 4),
            "usd": _round(amount * price, 4), "network": "solana"}


@endpoint("/utils/address/{address}/validate", "$0.01",
          "Validate a Solana address: base58 format, 32-byte length, and whether it is on the ed25519 curve (off-curve means a PDA). $0.01 USDC per call.",
          "utils", ["utils", "address", "validate"], "utils-address-validate")
async def utils_address_validate(address: str):
    valid = _is_sol_address(address)
    on_curve = _on_curve(address) if valid else None
    return {
        "address": address,
        "valid": valid,
        "on_curve": on_curve,
        "is_program_derived_address": (on_curve is False) if valid else None,
        "network": "solana",
    }


@endpoint("/utils/address/{address}/type", "$0.01",
          "Classify one Solana address by its owner program: wallet, mint, token account, stake account, vote account, program or unknown. $0.01 USDC per call.",
          "utils", ["utils", "address", "type"], "utils-address-type")
async def utils_address_type(address: str):
    if not _is_sol_address(address):
        return _err_addr()
    res = await _rpc("getAccountInfo", [address, {"encoding": "jsonParsed"}])
    if _is_error(res):
        return res
    value = (res or {}).get("value")
    if not value:
        return {"address": address, "exists": False, "type": "unfunded wallet (no account)",
                "network": "solana"}
    owner = value.get("owner")
    data = value.get("data")
    # jsonParsed falls back to the [data, "base64"] list form for
    # accounts it cannot parse (e.g. empty system accounts).
    parsed = (data.get("parsed") or {}) if isinstance(data, dict) else {}
    ptype = parsed.get("type")
    if value.get("executable"):
        kind = "program"
    elif owner == SYSTEM_PROGRAM:
        kind = "wallet (system account)"
    elif owner in (TOKEN_PROGRAM, TOKEN_2022_PROGRAM) and ptype == "mint":
        kind = "token mint"
    elif owner in (TOKEN_PROGRAM, TOKEN_2022_PROGRAM) and ptype == "account":
        kind = "token account"
    elif owner == STAKE_PROGRAM:
        kind = "stake account"
    elif owner == VOTE_PROGRAM:
        kind = "vote account"
    else:
        kind = "program-owned account"
    return {
        "address": address,
        "exists": True,
        "type": kind,
        "parsed_type": ptype,
        "owner_program": owner,
        "executable": value.get("executable"),
        "lamports": value.get("lamports"),
        "network": "solana",
    }


@endpoint("/utils/slot/time/{slot}", "$0.01",
          "Estimated wall-clock time of one slot: exact getBlockTime when the node has it, otherwise an estimate from average slot duration. $0.01 USDC per call.",
          "utils", ["utils", "slot", "time"], "utils-slot-time")
async def utils_slot_time(slot: int):
    t = await _rpc("getBlockTime", [slot])
    if not _is_error(t) and t is not None:
        return {"slot": slot, "unix_time": t, "estimated": False, "network": "solana"}
    current = await _rpc("getSlot", [{"commitment": "finalized"}])
    if _is_error(current):
        return current
    samples = await _rpc("getRecentPerformanceSamples", [12])
    slot_secs = 0.4
    if not _is_error(samples) and samples:
        tot_slots = sum(s.get("numSlots") or 0 for s in samples)
        tot_secs = sum(s.get("samplePeriodSecs") or 0 for s in samples)
        if tot_slots and tot_secs:
            slot_secs = tot_secs / tot_slots
    est = int(time.time() + (slot - current) * slot_secs)
    return {"slot": slot, "unix_time": est, "estimated": True,
            "avg_slot_seconds": _round(slot_secs, 4), "network": "solana"}


@endpoint("/utils/time/slot/{timestamp}", "$0.01",
          "Estimated slot at a given Unix timestamp, extrapolated from the current slot and average slot duration. $0.01 USDC per call.",
          "utils", ["utils", "time", "slot"], "utils-time-slot")
async def utils_time_slot(timestamp: int):
    current = await _rpc("getSlot", [{"commitment": "finalized"}])
    if _is_error(current):
        return current
    samples = await _rpc("getRecentPerformanceSamples", [12])
    slot_secs = 0.4
    if not _is_error(samples) and samples:
        tot_slots = sum(s.get("numSlots") or 0 for s in samples)
        tot_secs = sum(s.get("samplePeriodSecs") or 0 for s in samples)
        if tot_slots and tot_secs:
            slot_secs = tot_secs / tot_slots
    est_slot = int(current + (timestamp - time.time()) / slot_secs)
    return {"timestamp": timestamp, "estimated_slot": est_slot,
            "current_slot": current, "avg_slot_seconds": _round(slot_secs, 4),
            "network": "solana"}


@endpoint("/utils/epoch/{epoch}/slots", "$0.01",
          "Slot range of one Solana epoch from the epoch schedule: first slot, last slot, and estimated start/end times. $0.01 USDC per call.",
          "utils", ["utils", "epoch", "slots"], "utils-epoch-slots")
async def utils_epoch_slots(epoch: int):
    sched = await _rpc("getEpochSchedule", [])
    if _is_error(sched):
        return sched
    slots_per_epoch = sched.get("slotsPerEpoch") or 432000
    first_normal_slot = sched.get("firstNormalSlot") or 0
    first_normal_epoch = sched.get("firstNormalEpoch") or 0
    if epoch < first_normal_epoch:
        return {"error": "epoch is in the warmup period; schedule math differs",
                "epoch": epoch, "first_normal_epoch": first_normal_epoch}
    first_slot = first_normal_slot + (epoch - first_normal_epoch) * slots_per_epoch
    last_slot = first_slot + slots_per_epoch - 1
    info = await _rpc("getEpochInfo", [])
    est_start = est_end = None
    if not _is_error(info):
        samples = await _rpc("getRecentPerformanceSamples", [12])
        slot_secs = 0.4
        if not _is_error(samples) and samples:
            tot_slots = sum(s.get("numSlots") or 0 for s in samples)
            tot_secs = sum(s.get("samplePeriodSecs") or 0 for s in samples)
            if tot_slots and tot_secs:
                slot_secs = tot_secs / tot_slots
        now = time.time()
        current_slot = info.get("absoluteSlot")
        if current_slot:
            est_start = int(now + (first_slot - current_slot) * slot_secs)
            est_end = int(now + (last_slot - current_slot) * slot_secs)
    return {
        "epoch": epoch,
        "first_slot": first_slot,
        "last_slot": last_slot,
        "slots_in_epoch": slots_per_epoch,
        "estimated_start_unix": est_start,
        "estimated_end_unix": est_end,
        "network": "solana",
    }


@endpoint("/utils/fees/priority", "$0.01",
          "Current priority-fee estimate for a simple Solana transfer: median network prioritization fee plus the 5,000-lamport base fee. $0.01 USDC per call.",
          "utils", ["utils", "fees", "estimate"], "utils-fees-priority")
async def utils_fees_priority():
    fees = await _rpc("getRecentPrioritizationFees", [])
    if _is_error(fees):
        return fees
    vals = sorted(f.get("prioritizationFee") or 0 for f in (fees or []))
    if not vals:
        return {"error": "no prioritization fee data returned"}
    median = vals[len(vals) // 2]
    # A simple transfer uses ~200k compute-unit limit in most wallets;
    # priority fee total = median micro-lamports/CU * CU / 1e6 lamports.
    assumed_cu = 200_000
    priority_lamports = median * assumed_cu / 1_000_000
    base_fee = 5000
    total = base_fee + priority_lamports
    price = await _sol_price_usd()
    return {
        "median_priority_micro_lamports_per_cu": median,
        "assumed_compute_units": assumed_cu,
        "base_fee_lamports": base_fee,
        "priority_fee_lamports": _round(priority_lamports, 2),
        "estimated_total_lamports": _round(total, 2),
        "estimated_total_sol": _round(_sol(total), 9),
        "estimated_total_usd": _round(_sol(total) * price, 6) if price else None,
        "network": "solana",
    }



# ============ APP + ROUTES + X402 WIRING ============
# The REGISTRY list above is the single source of truth: the FastAPI
# routes and the x402 payment config are both built from it here, so
# they can never drift apart. Payment uses the SDK 2.9.0 FastAPI
# middleware with the x402ResourceServer configured at the top of this
# file (PayAI facilitator, exact SVM scheme, USDC on Solana mainnet).

import re

from fastapi.responses import FileResponse
from x402.http import PaymentOption
from x402.http.types import RouteConfig

app = FastAPI(title="Manteclaw x402 — Solana Data APIs", version="2.0.0")

for _entry in REGISTRY:
    app.get(_entry["path"], name=_entry["id"])(_entry["handler"])


def _route_config(entry):
    return RouteConfig(
        accepts=PaymentOption(
            scheme="exact",
            pay_to=WALLET_ADDRESS,
            price=entry["price"],
            network=SOLANA_NETWORK,
            max_timeout_seconds=300,
        ),
        description=entry["description"],
        mime_type="application/json",
    )


def _pattern(path):
    """FastAPI-style {param} -> the x402 SDK's [param] route pattern.

    The SDK compiles route patterns to regexes that understand '*',
    '[param]' and ':param' — but NOT '{param}' (braces are escaped
    literally, so a '{mint}' key would never match a real request and
    the endpoint would serve unpaid). Verified against
    x402.http.x402_http_server_base._parse_route_pattern (SDK 2.9.0).
    """
    return re.sub(r"\{(\w+)\}", r"[\1]", path)


ROUTES_CONFIG = {}
for _entry in REGISTRY:
    ROUTES_CONFIG[f"GET {_pattern(_entry['path'])}"] = _route_config(_entry)
    ROUTES_CONFIG[f"HEAD {_pattern(_entry['path'])}"] = _route_config(_entry)

_x402_gate = payment_middleware(ROUTES_CONFIG, server)


@app.middleware("http")
async def _x402_payment_gate(request, call_next):
    return await _x402_gate(request, call_next)


@app.on_event("startup")
async def _warm_facilitator():
    """Pre-sync the facilitator's supported-kinds in the background.

    The payment middleware otherwise syncs lazily on the first paid
    request; through a slow proxy that sync can time out and 500 the
    first caller (seen in local testing). Warming it here means the
    first real request usually gets its 402 immediately. This must
    NEVER block startup: it runs as a background task in a thread
    (initialize() is a blocking sync call), and failure is non-fatal —
    the middleware retries on demand.
    """
    async def _warm():
        # Retry for up to ~10 minutes, then stop — the middleware
        # remains the on-demand backstop either way.
        deadline = time.time() + 600
        delay = 2.0
        while time.time() < deadline:
            try:
                await asyncio.to_thread(server.initialize)
                return
            except Exception:
                await asyncio.sleep(delay)
                delay = min(delay * 1.5, 30.0)

    asyncio.create_task(_warm())


app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"],
    allow_headers=["*"],
)

# ============ OPENAPI (so registries can import all endpoints) ============
_default_openapi = app.openapi


def _openapi_with_inputs():
    """Same fix as the Base service: give every operation an explicit
    empty-input requestBody when it has no parameters, so importers
    (PayAPI etc.) don't flag a missing input schema."""
    schema = _default_openapi()
    for path_item in (schema.get("paths") or {}).values():
        for method, op in list(path_item.items()):
            if method not in ("get", "post", "put", "patch", "delete"):
                continue
            if not isinstance(op, dict):
                continue
            if op.get("parameters") or "requestBody" in op:
                continue
            op["requestBody"] = {
                "required": False,
                "content": {
                    "application/json": {
                        "schema": {"type": "object", "properties": {}}
                    }
                },
            }
    return schema


app.openapi = _openapi_with_inputs

# ============ FREE DISCOVERY / HEALTH / CATALOG ROUTES ============
_HERE = os.path.dirname(os.path.abspath(__file__))
BASE_URL = "https://manteclaw-x402-solana.fly.dev"
SERVICE_NAME = "Manteclaw x402 — Solana Data APIs"
SERVICE_DESCRIPTION = (
    "199 pay-per-call Solana data APIs: chain state, SPL tokens, wallets, "
    "transactions, DEX pairs, DeFi, Pyth oracle feeds, pump.fun memecoins, "
    "stablecoins, markets, NFTs and utilities. Paid in USDC on Solana "
    "via x402."
)


def _discovery_endpoints():
    """Endpoint list in the x402.json discovery shape."""
    return [
        {
            "path": e["path"],
            "price": e["price"].lstrip("$"),
            "currency": "usdc",
            "description": e["description"],
        }
        for e in REGISTRY
    ]


def _x402_document():
    return {
        "name": "Manteclaw Agent Services (Solana)",
        "description": SERVICE_DESCRIPTION,
        "version": "2.0.0",
        "wallet": WALLET_ADDRESS,
        "network": SOLANA_NETWORK,
        "asset": SOLANA_USDC,
        "scheme": "exact",
        "contact": {"github": "https://github.com/manteclaw"},
        "endpoints": _discovery_endpoints(),
    }


def _agent_catalog_document():
    return {
        "agent_id": "manteclaw-x402-solana-services",
        "agent_name": "ManteClaw x402 Services (Solana)",
        "assets": [
            {
                "category": e["category"],
                "description": e["description"],
                "endpoint": e["path"],
                "id": e["id"],
                "name": e["id"],
                "preview_endpoint": e["path"],
                "price_usdc": e["price"].lstrip("$"),
                "tags": e["tags"],
                "type": "data",
            }
            for e in REGISTRY
        ],
        "base_url": BASE_URL,
        "contact": {"github": "https://github.com/manteclaw"},
        "description": SERVICE_DESCRIPTION,
        "network": SOLANA_NETWORK,
        "protocols_supported": ["x402"],
        "schema_version": "1.0.0",
        "wallet_address": WALLET_ADDRESS,
    }


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "manteclaw-x402-solana",
        "version": "2.0.0",
        "chain": "solana",
        "network": SOLANA_NETWORK,
        "payee": WALLET_ADDRESS,
        "endpoints": len(REGISTRY) + 1,
        "paid_endpoints": len(REGISTRY),
        "facilitator": FACILITATOR_URL,
    }


@app.get("/catalog")
async def catalog():
    html_path = os.path.join(_HERE, "catalog.html")
    if os.path.exists(html_path):
        return FileResponse(html_path, media_type="text/html")
    return {"service": SERVICE_NAME, "endpoints": _discovery_endpoints()}


@app.get("/x402")
async def x402_discovery():
    return _x402_document()


@app.get("/.well-known/x402.json")
async def well_known_x402_json():
    return _x402_document()


@app.get("/.well-known/x402")
async def well_known_x402():
    return _x402_document()


@app.get("/.well-known/agent-catalog.json")
async def well_known_agent_catalog():
    return _agent_catalog_document()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
