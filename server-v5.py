# Manteclaw x402 service — v5.0 (FastAPI + x402 2.9.0, Base)
#
# 200 endpoints total: 199 paid (USDC on Base via x402) + free /health.
# The 100 v4 endpoints keep their exact paths, prices and behaviour.
# v5 adds 100 new paid endpoints (lending, stablecoins, oracles,
# governance, gas, NFTs, DeFi, DEX, wallets, memecoins, prediction
# markets, tokens, chain, utilities, ENS). The stale
# /agent/earnings-report from v3 is DELIBERATELY NOT included
# (it served stale May 2026 placeholder figures).
#
# Data sources (all free, no API keys):
#   - Base RPC (mainnet.base.org) via web3: blocks, txs, balances, code,
#     ERC20/ERC721/ Uniswap-V3 / Aave reads. NOTE: the public RPC refuses
#     eth_getLogs (429), so no endpoint here uses log queries.
#   - Dexscreener public API: token prices, pairs, liquidity, volume, txns.
#   - DeFiLlama public APIs: chain/protocol TVL, DEX volumes, yields,
#     stablecoins, fees.
#   - CoinGecko public API (no key): coin prices, global stats, trending.
#   - ENSIdeas / ensdata.net public APIs: ENS resolve / reverse.
#   - Alternative.me public API: Fear & Greed index.
# Blockscout is NOT used: its API is behind a Cloudflare challenge from
# this environment, so endpoints depending on it could not be verified.
#
# No secrets, keys, or private keys anywhere in this file. The only
# wallet address is the payee (Alex's main wallet), overridable via
# WALLET_ADDRESS env, same as v3.

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
from web3 import Web3
from eth_abi import decode as abi_decode
import httpx

# x402 v2.9.0 imports
from x402 import x402ResourceServer
from x402.http import HTTPFacilitatorClient, FacilitatorConfig
from x402.schemas import SupportedResponse
from x402.http.middleware.fastapi import payment_middleware
from x402.mechanisms.evm.exact import ExactEvmServerScheme
from x402.extensions.bazaar import OutputConfig, declare_discovery_extension

# Load environment variables from .env
load_dotenv()

# Base RPC: use the configured Alchemy URL when present, otherwise the
# public Base endpoint.
RPC_URL = os.environ.get("ALCHEMY_BASE_RPC") or "https://mainnet.base.org"
w3 = Web3(Web3.HTTPProvider(RPC_URL))

# Wallet for receiving payments — confirmed by Alex (main wallet, ...3175).
# Overridable via WALLET_ADDRESS in the environment; never put a private
# key or seed phrase here or in .env.
WALLET_ADDRESS = os.environ.get(
    "WALLET_ADDRESS", "0xea0aad6dfa33d6ea4aec842c24d2015e3a4b3175"
)

# Port: env-overridable, default 4020 (the port v3 has always used).
PORT = int(os.environ.get("PORT", "4020"))

# ===== x402 v2.9.0 SETUP =====

# Mainnet facilitator for Base (eip155:8453).
# Switched Nov 21, 2026 from facilitator.xpay.sh (returning 503 on
# /supported, which made every paid endpoint 500) to PayAI's facilitator,
# verified to support scheme "exact" on eip155:8453. Coinbase's reference
# facilitator (x402.org) only supports Base Sepolia, not Base mainnet.
facilitator = HTTPFacilitatorClient(
    FacilitatorConfig(url="https://facilitator.payai.network")
)


class DualFacilitatorClient:
    """Route verify/settle across the active facilitator clients.

    x402ResourceServer maps network -> scheme -> ONE facilitator client
    (first registered wins), so handing it two clients would silently
    shadow the second. This wrapper is the single client the resource
    server sees; internally it routes each call to exactly one real
    client:

    - Routing is deterministic per resource: sha256 of the resource
      string, modulo the number of active clients. verify and settle
      receive the same payload + requirements, so a given resource
      always lands on the same facilitator for both steps (a facilitator
      can only settle a payment it verified).
    - verify: if the chosen client RAISES (transport/HTTP error), try
      the other client once. A normal response — even is_valid=False —
      is returned as-is, no fallback.
    - settle: NO fallback, ever. If the chosen facilitator's settle
      raises, the payment may already have landed on-chain; retrying on
      the other facilitator could double-settle. Errors propagate.
    - get_supported: merged across active clients (kinds deduped by
      network+scheme+x402Version, extensions unioned, signers merged).
      A client that raises is logged and excluded; if ALL raise, the
      first error propagates — the same failure mode as a lone
      facilitator being down at startup.
    """

    def __init__(self, clients):
        # clients: list of (name, FacilitatorClient) in preference order.
        self._clients = list(clients)

    @staticmethod
    def _resource_key(payload, requirements) -> str:
        # V2 PaymentRequirements carries no resource field; the resource
        # URL rides on the payload (echoed from the 402) and, for V1, on
        # the requirements themselves. Final fallback is the canonical
        # requirements dump — identical across verify and settle.
        res = getattr(requirements, "resource", None)
        if isinstance(res, str) and res:
            return res
        if res is not None and getattr(res, "url", None):
            return res.url
        pres = getattr(payload, "resource", None)
        if pres is not None and getattr(pres, "url", None):
            return pres.url
        if isinstance(pres, str) and pres:
            return pres
        try:
            return requirements.model_dump_json(by_alias=True)
        except Exception:
            return str(requirements)

    def _pick(self, payload, requirements) -> int:
        if len(self._clients) == 1:
            return 0
        digest = hashlib.sha256(
            self._resource_key(payload, requirements).encode()
        ).hexdigest()
        return int(digest, 16) % len(self._clients)

    async def verify(self, payload, requirements):
        idx = self._pick(payload, requirements)
        name, client = self._clients[idx]
        try:
            return await client.verify(payload, requirements)
        except Exception:
            if len(self._clients) == 1:
                raise
            # Transport/HTTP failure only — a returned VerifyResponse
            # (even a negative one) never reaches this branch.
            for other_name, other in self._clients:
                if other is client:
                    continue
                print(
                    f"[facilitator] verify via {name} raised; "
                    f"falling back to {other_name}",
                    file=sys.stderr,
                )
                return await other.verify(payload, requirements)
            raise

    async def settle(self, payload, requirements):
        idx = self._pick(payload, requirements)
        # No fallback: see class docstring (double-settle risk).
        return await self._clients[idx][1].settle(payload, requirements)

    def get_supported(self):
        kinds, seen = [], set()
        extensions, signers = [], {}
        first_error = None
        answered = 0
        for name, client in self._clients:
            try:
                supported = client.get_supported()
            except Exception as exc:
                print(
                    f"[facilitator] get_supported from {name} failed: {exc}",
                    file=sys.stderr,
                )
                if first_error is None:
                    first_error = exc
                continue
            answered += 1
            for kind in supported.kinds:
                key = (str(kind.network), kind.scheme, kind.x402_version)
                if key not in seen:
                    seen.add(key)
                    kinds.append(kind)
            for ext in supported.extensions:
                if ext not in extensions:
                    extensions.append(ext)
            for family, addrs in (supported.signers or {}).items():
                bucket = signers.setdefault(family, [])
                for addr in addrs:
                    if addr not in bucket:
                        bucket.append(addr)
        if answered == 0 and first_error is not None:
            raise first_error
        return SupportedResponse(
            kinds=kinds, extensions=extensions, signers=signers
        )


# Coinbase CDP facilitator — added Nov 21, 2026 as a SECOND facilitator
# (Alex's call: add, don't switch) so the service can also settle via
# CDP and appear in Coinbase's Bazaar. Built only when both CDP keys are
# in the environment; without them the service runs on PayAI alone,
# exactly as before. Keys live in Fly secrets / .env — never in code.
# NOTE: cdp-sdk 1.53.0 names the helper create_facilitator_config (the
# "v2" is in the URL it returns: https://api.cdp.coinbase.com/platform/
# v2/x402); its return value is a plain dict {url, create_headers},
# which HTTPFacilitatorClient accepts directly.
_cdp_client = None
_cdp_key_id = os.environ.get("CDP_API_KEY_ID")
_cdp_key_secret = os.environ.get("CDP_API_KEY_SECRET")
if _cdp_key_id and _cdp_key_secret:
    try:
        from cdp.x402 import create_facilitator_config

        _cdp_client = HTTPFacilitatorClient(
            create_facilitator_config(_cdp_key_id, _cdp_key_secret)
        )
    except Exception as _exc:  # a broken CDP setup must not kill PayAI
        print(
            f"[facilitator] CDP client disabled (init failed): {_exc}",
            file=sys.stderr,
        )
        _cdp_client = None

_facilitator_clients = [("payai", facilitator)]
if _cdp_client is not None:
    _facilitator_clients.append(("cdp", _cdp_client))
print(
    f"[facilitator] active: {[name for name, _ in _facilitator_clients]}",
    file=sys.stderr,
)

server = x402ResourceServer(DualFacilitatorClient(_facilitator_clients))
server.register(network="eip155:8453", server=ExactEvmServerScheme())

# ===== shared HTTP client (free public APIs, no keys) =====

HTTP_TIMEOUT = 15.0
http_client = httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=True)

DEXSCREENER = "https://api.dexscreener.com"
LLAMA = "https://api.llama.fi"
YIELDS = "https://yields.llama.fi"
STABLECOINS = "https://stablecoins.llama.fi"
COINGECKO = "https://api.coingecko.com/api/v3"
ENSIDEAS = "https://api.ensideas.com"
ENSDATA = "https://api.ensdata.net"
FNG = "https://api.alternative.me"

# Well-known Base token contracts (verified on-chain to hold code).
USDC = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
WETH = "0x4200000000000000000000000000000000000006"
AERO = "0x3f5bF3EdAc59d650D8fDf9657a71F2f904A3a4Ee"
AAVE_POOL = "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5"

# Curated top Base tokens for wallet balance scans. Symbol/decimals are
# immutable on-chain metadata, verified against the contracts.
CURATED_TOKENS = [
    {"address": USDC, "symbol": "USDC", "decimals": 6, "name": "USD Coin"},
    {"address": WETH, "symbol": "WETH", "decimals": 18, "name": "Wrapped Ether"},
    {"address": AERO, "symbol": "AERO", "decimals": 18, "name": "Aerodrome Finance"},
    {"address": "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22", "symbol": "cbETH", "decimals": 18, "name": "Coinbase Wrapped Staked ETH"},
    {"address": "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb", "symbol": "DAI", "decimals": 18, "name": "Dai Stablecoin"},
    {"address": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA", "symbol": "USDbC", "decimals": 6, "name": "USD Base Coin"},
    {"address": "0x0578d8A44db98B23BF096A382e016e29a5Ce0FFE", "symbol": "HIGHER", "decimals": 18, "name": "Higher"},
]

# ===== helpers =====

async def _get_json(url, params=None):
    """GET a public JSON API. Returns the parsed body, or an error dict."""
    try:
        r = await http_client.get(url, params=params)
        if r.status_code != 200:
            return {"error": f"upstream HTTP {r.status_code}", "url": url}
        try:
            return r.json()
        except Exception:
            return {"error": "upstream returned non-JSON", "url": url}
    except Exception as e:
        return {"error": str(e)[:200], "url": url}


def _checksum(addr: str) -> str:
    return Web3.to_checksum_address(addr)


def _addr32(addr: str) -> bytes:
    return b"\x00" * 12 + bytes.fromhex(_checksum(addr)[2:])


def _eth_call(to: str, data_hex: str):
    return w3.eth.call({"to": _checksum(to), "data": data_hex})


def _decode_string(raw: bytes) -> str:
    try:
        return abi_decode(["string"], raw)[0]
    except Exception:
        pass
    try:
        b = abi_decode(["bytes32"], raw)[0]
        return b.split(b"\x00")[0].decode("utf-8", "replace")
    except Exception:
        return raw.hex()


def _decode_uint(raw: bytes) -> int:
    try:
        return int(abi_decode(["uint256"], raw)[0])
    except Exception:
        return int.from_bytes(raw, "big")


def _decode_bool(raw: bytes) -> bool:
    try:
        return bool(abi_decode(["bool"], raw)[0])
    except Exception:
        return raw[-1:] == b"\x01"


# ERC20 / ERC721 function selectors
SEL_NAME = "06fdde03"
SEL_SYMBOL = "95d89b41"
SEL_DECIMALS = "313ce567"
SEL_TOTAL_SUPPLY = "18160ddd"
SEL_BALANCE_OF = "70a08231"
SEL_ALLOWANCE = "dd62ed3e"
SEL_OWNER_OF = "6352211e"
SEL_TOKEN_URI = "c87b56dd"
SEL_SUPPORTS_INTERFACE = "01ffc9a7"


def _erc20_str(token: str, selector: str) -> str:
    return _decode_string(_eth_call(token, "0x" + selector))


def _erc20_uint(token: str, selector: str, args: bytes = b"") -> int:
    return _decode_uint(_eth_call(token, "0x" + selector + args.hex()))


def _erc20_balance(token: str, wallet: str) -> int:
    return _erc20_uint(token, SEL_BALANCE_OF, _addr32(wallet))


def _token_meta(token: str):
    """Best-effort ERC20 name/symbol/decimals. Returns dict; missing fields are None."""
    meta = {"name": None, "symbol": None, "decimals": None}
    for key, sel in (("name", SEL_NAME), ("symbol", SEL_SYMBOL)):
        try:
            meta[key] = _erc20_str(token, sel)
        except Exception:
            pass
    try:
        raw = _eth_call(token, "0x" + SEL_DECIMALS)
        meta["decimals"] = int.from_bytes(raw, "big") if len(raw) == 32 else None
        if meta["decimals"] is not None and meta["decimals"] > 36:
            meta["decimals"] = None
    except Exception:
        pass
    return meta


def _best_pair(pairs):
    """Pick the Dexscreener pair with the most liquidity (best price source)."""
    if not pairs:
        return None
    def liq(p):
        try:
            return float((p.get("liquidity") or {}).get("usd") or 0)
        except Exception:
            return 0.0
    return max(pairs, key=liq)


async def _dex_pairs_for_token(addr: str):
    data = await _get_json(f"{DEXSCREENER}/latest/dex/tokens/{addr}")
    if isinstance(data, dict) and "error" in data:
        return data
    pairs = data.get("pairs") if isinstance(data, dict) else None
    if not pairs:
        return {"error": "no DEX pairs found for token", "token": addr}
    return pairs


async def _eth_price_usd():
    """Live ETH price in USD from the WETH/USDC Dexscreener pair."""
    pairs = await _dex_pairs_for_token(WETH)
    if isinstance(pairs, dict):
        return None
    best = _best_pair(pairs)
    if not best:
        return None
    try:
        return float(best.get("priceUsd"))
    except Exception:
        return None


def _trim_pair(p):
    return {
        "chain": p.get("chainId"),
        "dex": p.get("dexId"),
        "pair": p.get("pairAddress"),
        "url": p.get("url"),
        "base_token": (p.get("baseToken") or {}).get("symbol"),
        "quote_token": (p.get("quoteToken") or {}).get("symbol"),
        "price_usd": p.get("priceUsd"),
        "price_native": p.get("priceNative"),
        "liquidity_usd": (p.get("liquidity") or {}).get("usd"),
        "volume_24h": (p.get("volume") or {}).get("h24"),
        "market_cap": p.get("marketCap"),
        "fdv": p.get("fdv"),
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


# ===== EXISTING v3 ENDPOINTS (same paths, same prices) =====

@endpoint("/defi/gas-tracker", "$0.01",
          "Current Base gas prices (slow/average/fast). $0.01 USDC per call.",
          "gas", ["gas", "base", "l2", "pricing"], "gas-tracker")
async def gas_tracker():
    """Current Base gas prices (slow/average/fast). $0.01 USDC per call."""
    block = w3.eth.get_block("latest")
    base_fee = block.get("baseFeePerGas", 0)

    return {
        "base_fee_gwei": round(Web3.from_wei(base_fee, "gwei"), 2),
        "slow": round(Web3.from_wei(int(base_fee * 1.0), "gwei"), 2),
        "average": round(Web3.from_wei(int(base_fee * 1.2), "gwei"), 2),
        "fast": round(Web3.from_wei(int(base_fee * 1.5), "gwei"), 2),
        "block_number": block.number,
        "timestamp": block.timestamp,
    }


@endpoint("/defi/token-price/{token_address}", "$0.01",
          "Get token price via DEX liquidity pool analysis. $0.01 USDC per call.",
          "defi", ["token", "base", "defi"], "token-price")
async def token_price(token_address: str):
    """Get token price via DEX liquidity pool analysis. $0.01 USDC per call."""
    token = _checksum(token_address)

    code = w3.eth.get_code(token)
    if len(code) == 0:
        return {"error": "Not a contract address"}

    try:
        symbol = _erc20_str(token, SEL_SYMBOL)
        decimals = _erc20_uint(token, SEL_DECIMALS)
        return {
            "token": token,
            "symbol": symbol,
            "decimals": decimals,
            "network": "base",
            "note": "Price data requires DEX pool analysis (integrate Aerodrome/Uniswap subgraph)",
        }
    except Exception as e:
        return {"error": str(e)[:200], "token": token}


@endpoint("/defi/wallet-analyzer/{address}", "$0.01",
          "Analyze wallet activity on Base. $0.01 USDC per call.",
          "analytics", ["wallet", "base", "analytics"], "wallet-analyzer")
async def wallet_analyzer(address: str):
    """Analyze wallet activity on Base. $0.01 USDC per call."""
    addr = _checksum(address)

    balance = w3.eth.get_balance(addr)
    tx_count = w3.eth.get_transaction_count(addr)

    return {
        "address": addr,
        "eth_balance": float(Web3.from_wei(balance, "ether")),
        "tx_count": tx_count,
        "network": "base",
        "is_contract": len(w3.eth.get_code(addr)) > 0,
        "analysis": "Basic wallet scan. Full token portfolio requires Alchemy API integration.",
    }


@endpoint("/defi/token-security/{token_address}", "$0.02",
          "Quick token security check on Base. $0.02 USDC per call.",
          "security", ["token", "security", "base"], "token-security")
async def token_security(token_address: str):
    """Quick token security check on Base. $0.02 USDC per call."""
    token = _checksum(token_address)

    code = w3.eth.get_code(token)
    code_len = len(code)

    checks = {
        "is_contract": code_len > 0,
        "code_size": code_len,
        "has_code": code_len > 100,
        "risk_flags": [],
    }

    if code_len == 0:
        checks["risk_flags"].append("NO_CONTRACT_CODE")
    if code_len < 200:
        checks["risk_flags"].append("VERY_SMALL_CONTRACT")

    code_hex = code.hex() if code_len > 0 else ""
    dangerous_funcs = ["selfdestruct", "delegatecall"]
    for func in dangerous_funcs:
        if func in code_hex.lower():
            checks["risk_flags"].append(f"HAS_{func.upper()}")

    risk_score = len(checks["risk_flags"]) * 25  # 0-100 scale
    checks["risk_score"] = min(risk_score, 100)
    checks["risk_level"] = "LOW" if risk_score < 25 else "MEDIUM" if risk_score < 50 else "HIGH"

    return {
        "token": token,
        "network": "base",
        "checks": checks,
        "note": "This is a basic static analysis. Full security scan requires GoPlus Security API integration.",
    }


@endpoint("/mcp/status", "$0.01",
          "MCP server status. $0.01 USDC per call.",
          "status", ["status", "mcp"], "mcp-status")
async def mcp_status():
    return {"status": "online", "version": "5.0", "endpoints": 200, "network": "base", "x402_version": "2.9.0"}


@endpoint("/mcp/tools", "$0.01",
          "MCP tools list. $0.01 USDC per call.",
          "status", ["tools", "mcp"], "mcp-tools")
async def mcp_tools():
    # Generated from the registry at request time, so it always matches
    # the live route set.
    return {
        "tools": [
            {"name": e["id"], "price": e["price"].lstrip("$"), "endpoint": e["path"]}
            for e in REGISTRY
        ],
        "pricing_model": "per_call_usdc_on_base",
        "wallet": WALLET_ADDRESS,
        "x402_version": "2.9.0",
    }

# ===== TOKEN DATA (Dexscreener + on-chain ERC20) =====

@endpoint("/tokens/{address}/price", "$0.01",
          "Live USD price of a Base token from DEX pairs. $0.01 USDC per call.",
          "token", ["token", "price", "base"], "token-live-price")
async def token_live_price(address: str):
    pairs = await _dex_pairs_for_token(_checksum(address))
    if isinstance(pairs, dict):
        return pairs
    best = _best_pair(pairs)
    return {
        "token": _checksum(address),
        "price_usd": best.get("priceUsd"),
        "price_native": best.get("priceNative"),
        "pair": best.get("pairAddress"),
        "dex": best.get("dexId"),
        "chain": best.get("chainId"),
        "url": best.get("url"),
        "price_change": best.get("priceChange"),
        "network": "base",
    }


@endpoint("/tokens/{address}/pairs", "$0.01",
          "All DEX pairs for a Base token, with liquidity and volume. $0.01 USDC per call.",
          "token", ["token", "dex", "pairs"], "token-pairs")
async def token_pairs(address: str):
    pairs = await _dex_pairs_for_token(_checksum(address))
    if isinstance(pairs, dict):
        return pairs
    return {
        "token": _checksum(address),
        "pair_count": len(pairs),
        "pairs": [_trim_pair(p) for p in pairs[:20]],
        "network": "base",
    }


@endpoint("/tokens/{address}/market", "$0.01",
          "Market cap, FDV, 24h volume and liquidity for a Base token. $0.01 USDC per call.",
          "token", ["token", "market", "mcap"], "token-market")
async def token_market(address: str):
    pairs = await _dex_pairs_for_token(_checksum(address))
    if isinstance(pairs, dict):
        return pairs
    best = _best_pair(pairs)
    return {
        "token": _checksum(address),
        "symbol": (best.get("baseToken") or {}).get("symbol"),
        "price_usd": best.get("priceUsd"),
        "market_cap": best.get("marketCap"),
        "fdv": best.get("fdv"),
        "liquidity_usd": (best.get("liquidity") or {}).get("usd"),
        "volume_24h": (best.get("volume") or {}).get("h24"),
        "volume_6h": (best.get("volume") or {}).get("h6"),
        "volume_1h": (best.get("volume") or {}).get("h1"),
        "pair": best.get("pairAddress"),
        "dex": best.get("dexId"),
        "network": "base",
    }


@endpoint("/tokens/{address}/txns", "$0.01",
          "Buy/sell counts for a Base token across 5m, 1h, 6h and 24h. $0.01 USDC per call.",
          "token", ["token", "transactions", "dex"], "token-txns")
async def token_txns(address: str):
    pairs = await _dex_pairs_for_token(_checksum(address))
    if isinstance(pairs, dict):
        return pairs
    best = _best_pair(pairs)
    return {
        "token": _checksum(address),
        "pair": best.get("pairAddress"),
        "dex": best.get("dexId"),
        "txns": best.get("txns"),
        "volume": best.get("volume"),
        "network": "base",
    }


@endpoint("/tokens/{address}/liquidity", "$0.01",
          "Liquidity per DEX pair for a Base token. $0.01 USDC per call.",
          "token", ["token", "liquidity", "dex"], "token-liquidity")
async def token_liquidity(address: str):
    pairs = await _dex_pairs_for_token(_checksum(address))
    if isinstance(pairs, dict):
        return pairs
    return {
        "token": _checksum(address),
        "pairs": [
            {
                "pair": p.get("pairAddress"),
                "dex": p.get("dexId"),
                "liquidity_usd": (p.get("liquidity") or {}).get("usd"),
                "liquidity_base": (p.get("liquidity") or {}).get("base"),
                "liquidity_quote": (p.get("liquidity") or {}).get("quote"),
            }
            for p in pairs[:20]
        ],
        "network": "base",
    }


@endpoint("/tokens/{address}/info", "$0.01",
          "On-chain ERC20 metadata: name, symbol, decimals, total supply. $0.01 USDC per call.",
          "token", ["token", "erc20", "metadata"], "token-info")
async def token_info(address: str):
    token = _checksum(address)
    code = w3.eth.get_code(token)
    if len(code) == 0:
        return {"error": "Not a contract address", "token": token}
    meta = _token_meta(token)
    try:
        supply = _erc20_uint(token, SEL_TOTAL_SUPPLY)
    except Exception:
        supply = None
    return {
        "token": token,
        "name": meta["name"],
        "symbol": meta["symbol"],
        "decimals": meta["decimals"],
        "total_supply_raw": supply,
        "total_supply": (supply / (10 ** meta["decimals"])) if (supply is not None and meta["decimals"] is not None) else None,
        "code_size": len(code),
        "network": "base",
    }


@endpoint("/tokens/{address}/supply", "$0.01",
          "ERC20 total supply for a Base token. $0.01 USDC per call.",
          "token", ["token", "supply", "erc20"], "token-supply")
async def token_supply(address: str):
    token = _checksum(address)
    try:
        supply = _erc20_uint(token, SEL_TOTAL_SUPPLY)
        meta = _token_meta(token)
        dec = meta["decimals"]
        return {
            "token": token,
            "symbol": meta["symbol"],
            "decimals": dec,
            "total_supply_raw": supply,
            "total_supply": (supply / (10 ** dec)) if dec is not None else None,
            "network": "base",
        }
    except Exception as e:
        return {"error": str(e)[:200], "token": token}


@endpoint("/tokens/{address}/name", "$0.01",
          "ERC20 name for a Base token. $0.01 USDC per call.",
          "token", ["token", "erc20"], "token-name")
async def token_name(address: str):
    token = _checksum(address)
    try:
        return {"token": token, "name": _erc20_str(token, SEL_NAME), "network": "base"}
    except Exception as e:
        return {"error": str(e)[:200], "token": token}


@endpoint("/tokens/{address}/symbol", "$0.01",
          "ERC20 symbol for a Base token. $0.01 USDC per call.",
          "token", ["token", "erc20"], "token-symbol")
async def token_symbol(address: str):
    token = _checksum(address)
    try:
        return {"token": token, "symbol": _erc20_str(token, SEL_SYMBOL), "network": "base"}
    except Exception as e:
        return {"error": str(e)[:200], "token": token}


@endpoint("/tokens/{address}/decimals", "$0.01",
          "ERC20 decimals for a Base token. $0.01 USDC per call.",
          "token", ["token", "erc20"], "token-decimals")
async def token_decimals(address: str):
    token = _checksum(address)
    try:
        return {"token": token, "decimals": _erc20_uint(token, SEL_DECIMALS), "network": "base"}
    except Exception as e:
        return {"error": str(e)[:200], "token": token}


@endpoint("/tokens/{address}/balance/{wallet}", "$0.01",
          "ERC20 balance of a wallet for a Base token. $0.01 USDC per call.",
          "token", ["token", "balance", "wallet"], "token-balance")
async def token_balance(address: str, wallet: str):
    token = _checksum(address)
    w = _checksum(wallet)
    try:
        raw = _erc20_balance(token, w)
        meta = _token_meta(token)
        dec = meta["decimals"]
        return {
            "token": token,
            "symbol": meta["symbol"],
            "wallet": w,
            "balance_raw": raw,
            "balance": (raw / (10 ** dec)) if dec is not None else None,
            "decimals": dec,
            "network": "base",
        }
    except Exception as e:
        return {"error": str(e)[:200], "token": token, "wallet": w}


@endpoint("/tokens/{address}/allowance/{owner}/{spender}", "$0.01",
          "ERC20 allowance a wallet granted to a spender. $0.01 USDC per call.",
          "token", ["token", "allowance", "erc20"], "token-allowance")
async def token_allowance(address: str, owner: str, spender: str):
    token = _checksum(address)
    o = _checksum(owner)
    s = _checksum(spender)
    try:
        args = _addr32(o) + _addr32(s)
        raw = _erc20_uint(token, SEL_ALLOWANCE, args)
        meta = _token_meta(token)
        dec = meta["decimals"]
        return {
            "token": token,
            "symbol": meta["symbol"],
            "owner": o,
            "spender": s,
            "allowance_raw": raw,
            "allowance": (raw / (10 ** dec)) if dec is not None else None,
            "decimals": dec,
            "network": "base",
        }
    except Exception as e:
        return {"error": str(e)[:200], "token": token}


@endpoint("/tokens/{address}/price-change", "$0.01",
          "Price change for a Base token over 5m, 1h, 6h and 24h. $0.01 USDC per call.",
          "token", ["token", "price", "change"], "token-price-change")
async def token_price_change(address: str):
    pairs = await _dex_pairs_for_token(_checksum(address))
    if isinstance(pairs, dict):
        return pairs
    best = _best_pair(pairs)
    return {
        "token": _checksum(address),
        "price_usd": best.get("priceUsd"),
        "price_change": best.get("priceChange"),
        "pair": best.get("pairAddress"),
        "dex": best.get("dexId"),
        "network": "base",
    }


@endpoint("/tokens/{address}/coingecko", "$0.01",
          "CoinGecko USD price for a Base token contract. $0.01 USDC per call.",
          "token", ["token", "price", "coingecko"], "token-coingecko")
async def token_coingecko(address: str):
    token = _checksum(address)
    data = await _get_json(
        f"{COINGECKO}/simple/token_price/base",
        params={"contract_addresses": token, "vs_currencies": "usd"},
    )
    if isinstance(data, dict) and "error" in data:
        return data
    entry = (data or {}).get(token.lower()) or (data or {}).get(token)
    if not entry:
        return {"error": "CoinGecko has no price for this contract", "token": token}
    return {"token": token, "price_usd": entry.get("usd"), "source": "coingecko", "network": "base"}


@endpoint("/tokens/search/{query}", "$0.01",
          "Search DEX pairs by token name, symbol or address. $0.01 USDC per call.",
          "token", ["token", "search", "dex"], "token-search")
async def token_search(query: str):
    data = await _get_json(f"{DEXSCREENER}/latest/dex/search", params={"q": query})
    if isinstance(data, dict) and "error" in data:
        return data
    pairs = (data or {}).get("pairs") or []
    return {"query": query, "pair_count": len(pairs), "pairs": [_trim_pair(p) for p in pairs[:20]]}


@endpoint("/tokens/trending", "$0.01",
          "Top boosted tokens on Dexscreener right now. $0.01 USDC per call.",
          "token", ["token", "trending", "dex"], "token-trending")
async def token_trending():
    data = await _get_json(f"{DEXSCREENER}/token-boosts/top/v1")
    if isinstance(data, dict) and "error" in data:
        return data
    items = data if isinstance(data, list) else []
    return {"count": len(items), "tokens": items[:20]}


@endpoint("/tokens/new", "$0.01",
          "Newest token profiles on Dexscreener. $0.01 USDC per call.",
          "token", ["token", "new", "dex"], "token-new")
async def token_new():
    data = await _get_json(f"{DEXSCREENER}/token-profiles/latest/v1")
    if isinstance(data, dict) and "error" in data:
        return data
    items = data if isinstance(data, list) else []
    return {"count": len(items), "tokens": items[:20]}


@endpoint("/tokens/boosts", "$0.01",
          "Latest boosted tokens on Dexscreener. $0.01 USDC per call.",
          "token", ["token", "boosts", "dex"], "token-boosts")
async def token_boosts():
    data = await _get_json(f"{DEXSCREENER}/token-boosts/latest/v1")
    if isinstance(data, dict) and "error" in data:
        return data
    items = data if isinstance(data, list) else []
    return {"count": len(items), "tokens": items[:20]}


# ===== WALLET ANALYTICS (on-chain + Dexscreener) =====

@endpoint("/wallets/{address}/balance", "$0.01",
          "ETH balance of a Base wallet. $0.01 USDC per call.",
          "wallet", ["wallet", "balance", "base"], "wallet-balance")
async def wallet_balance(address: str):
    addr = _checksum(address)
    bal = w3.eth.get_balance(addr)
    return {
        "address": addr,
        "eth_balance": float(Web3.from_wei(bal, "ether")),
        "eth_balance_wei": bal,
        "network": "base",
    }


@endpoint("/wallets/{address}/nonce", "$0.01",
          "Transaction count (nonce) of a Base wallet. $0.01 USDC per call.",
          "wallet", ["wallet", "nonce", "base"], "wallet-nonce")
async def wallet_nonce(address: str):
    addr = _checksum(address)
    return {
        "address": addr,
        "tx_count": w3.eth.get_transaction_count(addr),
        "network": "base",
    }


@endpoint("/wallets/{address}/is-contract", "$0.01",
          "Whether a Base address is a smart contract. $0.01 USDC per call.",
          "wallet", ["wallet", "contract", "base"], "wallet-is-contract")
async def wallet_is_contract(address: str):
    addr = _checksum(address)
    code = w3.eth.get_code(addr)
    return {"address": addr, "is_contract": len(code) > 0, "code_size": len(code), "network": "base"}


@endpoint("/wallets/{address}/code", "$0.01",
          "Bytecode size and hash for a Base address. $0.01 USDC per call.",
          "wallet", ["wallet", "code", "base"], "wallet-code")
async def wallet_code(address: str):
    addr = _checksum(address)
    code = w3.eth.get_code(addr)
    return {
        "address": addr,
        "code_size": len(code),
        "code_keccak": Web3.keccak(bytes(code)).hex() if len(code) > 0 else None,
        "is_contract": len(code) > 0,
        "network": "base",
    }


@endpoint("/wallets/{address}/tokens", "$0.02",
          "Balances of the top Base tokens for a wallet. $0.02 USDC per call.",
          "wallet", ["wallet", "tokens", "portfolio"], "wallet-tokens")
async def wallet_tokens(address: str):
    addr = _checksum(address)
    out = []
    for t in CURATED_TOKENS:
        try:
            raw = _erc20_balance(t["address"], addr)
        except Exception:
            continue
        out.append({
            "token": t["address"],
            "symbol": t["symbol"],
            "name": t["name"],
            "decimals": t["decimals"],
            "balance_raw": raw,
            "balance": raw / (10 ** t["decimals"]),
        })
    return {"address": addr, "tokens": out, "token_count": len(out), "network": "base"}


@endpoint("/wallets/{address}/portfolio", "$0.02",
          "USD portfolio value of a Base wallet (ETH + top tokens). $0.02 USDC per call.",
          "wallet", ["wallet", "portfolio", "usd"], "wallet-portfolio")
async def wallet_portfolio(address: str):
    addr = _checksum(address)
    eth_bal = float(Web3.from_wei(w3.eth.get_balance(addr), "ether"))
    eth_price = await _eth_price_usd()
    holdings = []
    total_usd = (eth_bal * eth_price) if eth_price else 0.0
    holdings.append({
        "token": "ETH", "symbol": "ETH", "balance": eth_bal,
        "price_usd": eth_price, "value_usd": (eth_bal * eth_price) if eth_price else None,
    })
    # Prices for the curated tokens, fetched concurrently.
    async def _price_for(t):
        pairs = await _dex_pairs_for_token(t["address"])
        if isinstance(pairs, dict):
            return t, None
        best = _best_pair(pairs)
        try:
            return t, float(best.get("priceUsd"))
        except Exception:
            return t, None
    priced = await asyncio.gather(*[_price_for(t) for t in CURATED_TOKENS])
    for t, price in priced:
        try:
            raw = _erc20_balance(t["address"], addr)
        except Exception:
            continue
        bal = raw / (10 ** t["decimals"])
        value = (bal * price) if (price is not None) else None
        if value:
            total_usd += value
        holdings.append({
            "token": t["address"], "symbol": t["symbol"], "balance": bal,
            "price_usd": price, "value_usd": value,
        })
    return {
        "address": addr,
        "eth_price_usd": eth_price,
        "total_value_usd": round(total_usd, 2),
        "holdings": holdings,
        "network": "base",
    }


@endpoint("/wallets/{address}/ens", "$0.01",
          "Reverse ENS name for a Base wallet address. $0.01 USDC per call.",
          "wallet", ["wallet", "ens", "name"], "wallet-ens")
async def wallet_ens(address: str):
    addr = _checksum(address)
    data = await _get_json(f"{ENSDATA}/{addr}")
    if isinstance(data, dict) and "upstream HTTP 404" in str(data.get("error", "")):
        # No ENS name for this address: a valid answer, not an error.
        return {"address": addr, "ens_name": None, "avatar": None, "network": "base"}
    if isinstance(data, dict) and "error" in data:
        return data
    name = (data or {}).get("name") or (data or {}).get("ens") or (data or {}).get("displayName")
    return {"address": addr, "ens_name": name, "avatar": (data or {}).get("avatar"), "network": "base"}


@endpoint("/wallets/{address}/eth-value", "$0.01",
          "USD value of a Base wallet's ETH balance. $0.01 USDC per call.",
          "wallet", ["wallet", "eth", "usd"], "wallet-eth-value")
async def wallet_eth_value(address: str):
    addr = _checksum(address)
    eth_bal = float(Web3.from_wei(w3.eth.get_balance(addr), "ether"))
    eth_price = await _eth_price_usd()
    return {
        "address": addr,
        "eth_balance": eth_bal,
        "eth_price_usd": eth_price,
        "value_usd": (round(eth_bal * eth_price, 2) if eth_price else None),
        "network": "base",
    }


@endpoint("/wallets/{address}/token-count", "$0.01",
          "How many of the top Base tokens a wallet holds. $0.01 USDC per call.",
          "wallet", ["wallet", "tokens", "count"], "wallet-token-count")
async def wallet_token_count(address: str):
    addr = _checksum(address)
    held = []
    for t in CURATED_TOKENS:
        try:
            raw = _erc20_balance(t["address"], addr)
        except Exception:
            continue
        if raw > 0:
            held.append(t["symbol"])
    return {"address": addr, "tokens_held": len(held), "symbols": held, "network": "base"}


@endpoint("/wallets/{address}/usdc-balance", "$0.01",
          "USDC balance of a Base wallet. $0.01 USDC per call.",
          "wallet", ["wallet", "usdc", "balance"], "wallet-usdc-balance")
async def wallet_usdc_balance(address: str):
    addr = _checksum(address)
    raw = _erc20_balance(USDC, addr)
    return {"address": addr, "symbol": "USDC", "balance": raw / 1e6, "balance_raw": raw, "network": "base"}


@endpoint("/wallets/{address}/weth-balance", "$0.01",
          "WETH balance of a Base wallet. $0.01 USDC per call.",
          "wallet", ["wallet", "weth", "balance"], "wallet-weth-balance")
async def wallet_weth_balance(address: str):
    addr = _checksum(address)
    raw = _erc20_balance(WETH, addr)
    return {"address": addr, "symbol": "WETH", "balance": raw / 1e18, "balance_raw": raw, "network": "base"}


@endpoint("/wallets/{address}/aero-balance", "$0.01",
          "AERO balance of a Base wallet. $0.01 USDC per call.",
          "wallet", ["wallet", "aero", "balance"], "wallet-aero-balance")
async def wallet_aero_balance(address: str):
    addr = _checksum(address)
    raw = _erc20_balance(AERO, addr)
    return {"address": addr, "symbol": "AERO", "balance": raw / 1e18, "balance_raw": raw, "network": "base"}

# ===== DEX DATA (Dexscreener + Uniswap V3 on-chain + DeFiLlama) =====

@endpoint("/dex/pairs/{chain}/{pair}", "$0.01",
          "Detail for one DEX pair by chain and pair address. $0.01 USDC per call.",
          "dex", ["dex", "pair"], "dex-pair")
async def dex_pair(chain: str, pair: str):
    data = await _get_json(f"{DEXSCREENER}/latest/dex/pairs/{chain}/{_checksum(pair)}")
    if isinstance(data, dict) and "error" in data:
        return data
    pairs = (data or {}).get("pairs") or []
    if not pairs:
        return {"error": "pair not found", "chain": chain, "pair": pair}
    return {"chain": chain, "pair": _trim_pair(pairs[0]), "network": "base"}


@endpoint("/dex/search/{query}", "$0.01",
          "Search DEX pairs by token name, symbol or address. $0.01 USDC per call.",
          "dex", ["dex", "search", "pairs"], "dex-search")
async def dex_search(query: str):
    data = await _get_json(f"{DEXSCREENER}/latest/dex/search", params={"q": query})
    if isinstance(data, dict) and "error" in data:
        return data
    pairs = (data or {}).get("pairs") or []
    return {"query": query, "pair_count": len(pairs), "pairs": [_trim_pair(p) for p in pairs[:20]]}


@endpoint("/dex/trending", "$0.01",
          "Top boosted DEX pairs right now. $0.01 USDC per call.",
          "dex", ["dex", "trending"], "dex-trending")
async def dex_trending():
    data = await _get_json(f"{DEXSCREENER}/token-boosts/top/v1")
    if isinstance(data, dict) and "error" in data:
        return data
    items = data if isinstance(data, list) else []
    return {"count": len(items), "tokens": items[:20]}


@endpoint("/dex/latest-profiles", "$0.01",
          "Newest token profiles on Dexscreener. $0.01 USDC per call.",
          "dex", ["dex", "profiles", "new"], "dex-latest-profiles")
async def dex_latest_profiles():
    data = await _get_json(f"{DEXSCREENER}/token-profiles/latest/v1")
    if isinstance(data, dict) and "error" in data:
        return data
    items = data if isinstance(data, list) else []
    return {"count": len(items), "tokens": items[:20]}


@endpoint("/dex/latest-boosts", "$0.01",
          "Latest boosted tokens on Dexscreener. $0.01 USDC per call.",
          "dex", ["dex", "boosts"], "dex-latest-boosts")
async def dex_latest_boosts():
    data = await _get_json(f"{DEXSCREENER}/token-boosts/latest/v1")
    if isinstance(data, dict) and "error" in data:
        return data
    items = data if isinstance(data, list) else []
    return {"count": len(items), "tokens": items[:20]}


_UNI_POOL_ABI = [
    {"inputs": [], "name": "slot0", "outputs": [
        {"name": "sqrtPriceX96", "type": "uint160"}, {"name": "tick", "type": "int24"},
        {"name": "observationIndex", "type": "uint16"}, {"name": "observationCardinality", "type": "uint16"},
        {"name": "observationCardinalityNext", "type": "uint16"}, {"name": "feeProtocol", "type": "uint8"},
        {"name": "unlocked", "type": "bool"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "liquidity", "outputs": [{"name": "", "type": "uint128"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "token0", "outputs": [{"name": "", "type": "address"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "token1", "outputs": [{"name": "", "type": "address"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "fee", "outputs": [{"name": "", "type": "uint24"}], "stateMutability": "view", "type": "function"},
]


def _uni_pool_contract(pool: str):
    return w3.eth.contract(address=_checksum(pool), abi=_UNI_POOL_ABI)


@endpoint("/dex/pool/{pool}/info", "$0.01",
          "Uniswap V3 pool state: tokens, fee, liquidity, tick. $0.01 USDC per call.",
          "dex", ["dex", "uniswap", "pool"], "dex-pool-info")
async def dex_pool_info(pool: str):
    p = _uni_pool_contract(pool)
    try:
        slot0 = p.functions.slot0().call()
        liq = p.functions.liquidity().call()
        t0 = p.functions.token0().call()
        t1 = p.functions.token1().call()
        fee = p.functions.fee().call()
        m0 = _token_meta(t0)
        m1 = _token_meta(t1)
        return {
            "pool": _checksum(pool),
            "token0": {"address": t0, "symbol": m0["symbol"], "decimals": m0["decimals"]},
            "token1": {"address": t1, "symbol": m1["symbol"], "decimals": m1["decimals"]},
            "fee": fee,
            "fee_percent": fee / 10000,
            "liquidity": liq,
            "tick": slot0[1],
            "sqrt_price_x96": str(slot0[0]),
            "unlocked": slot0[6],
            "network": "base",
        }
    except Exception as e:
        return {"error": str(e)[:200], "pool": pool}


@endpoint("/dex/pool/{pool}/price", "$0.01",
          "Implied price from a Uniswap V3 pool, both directions. $0.01 USDC per call.",
          "dex", ["dex", "uniswap", "price"], "dex-pool-price")
async def dex_pool_price(pool: str):
    p = _uni_pool_contract(pool)
    try:
        slot0 = p.functions.slot0().call()
        t0 = p.functions.token0().call()
        t1 = p.functions.token1().call()
        m0 = _token_meta(t0)
        m1 = _token_meta(t1)
        d0 = m0["decimals"] if m0["decimals"] is not None else 18
        d1 = m1["decimals"] if m1["decimals"] is not None else 18
        ratio = (slot0[0] / 2**96) ** 2
        price0_in_1 = ratio * 10 ** (d0 - d1)
        price1_in_0 = (1.0 / price0_in_1) if price0_in_1 else None
        return {
            "pool": _checksum(pool),
            "token0": {"address": t0, "symbol": m0["symbol"], "decimals": d0},
            "token1": {"address": t1, "symbol": m1["symbol"], "decimals": d1},
            "tick": slot0[1],
            "price_token0_in_token1": price0_in_1,
            "price_token1_in_token0": price1_in_0,
            "network": "base",
        }
    except Exception as e:
        return {"error": str(e)[:200], "pool": pool}


@endpoint("/dex/volume", "$0.01",
          "24h trading volume across tracked DEXs. $0.01 USDC per call.",
          "dex", ["dex", "volume"], "dex-volume")
async def dex_volume():
    data = await _get_json(f"{LLAMA}/overview/dexs")
    if isinstance(data, dict) and "error" in data:
        return data
    protocols = (data or {}).get("protocols") or []
    top = sorted(protocols, key=lambda x: float(x.get("total24h") or 0), reverse=True)[:10]
    total = sum(float(x.get("total24h") or 0) for x in protocols)
    return {
        "total_24h_usd": total,
        "top_dexs": [
            {"name": x.get("name"), "volume_24h_usd": x.get("total24h"), "volume_7d_usd": x.get("total7d")}
            for x in top
        ],
    }


@endpoint("/dex/volume/base", "$0.01",
          "24h DEX trading volume on Base. $0.01 USDC per call.",
          "dex", ["dex", "volume", "base"], "dex-volume-base")
async def dex_volume_base():
    data = await _get_json(f"{LLAMA}/overview/dexs", params={"chain": "Base"})
    if isinstance(data, dict) and "error" in data:
        return data
    protocols = (data or {}).get("protocols") or []
    top = sorted(protocols, key=lambda x: float(x.get("total24h") or 0), reverse=True)[:10]
    total = sum(float(x.get("total24h") or 0) for x in protocols)
    return {
        "chain": "Base",
        "total_24h_usd": total,
        "top_dexs": [
            {"name": x.get("name"), "volume_24h_usd": x.get("total24h"), "volume_7d_usd": x.get("total7d")}
            for x in top
        ],
    }


@endpoint("/dex/volume/{protocol}", "$0.01",
          "Volume summary for one DEX protocol. $0.01 USDC per call.",
          "dex", ["dex", "volume", "protocol"], "dex-volume-protocol")
async def dex_volume_protocol(protocol: str):
    data = await _get_json(f"{LLAMA}/summary/dexs/{protocol}")
    if isinstance(data, dict) and "error" in data:
        return data
    if not isinstance(data, dict) or not data:
        return {"error": "protocol not found", "protocol": protocol}
    return {
        "protocol": protocol,
        "name": data.get("name"),
        "volume_24h_usd": data.get("total24h"),
        "volume_7d_usd": data.get("total7d"),
        "volume_30d_usd": data.get("total30d"),
        "chains": data.get("chains"),
    }


@endpoint("/dex/fees/{protocol}", "$0.01",
          "Fee and revenue summary for one protocol. $0.01 USDC per call.",
          "dex", ["dex", "fees", "protocol"], "dex-fees-protocol")
async def dex_fees_protocol(protocol: str):
    data = await _get_json(f"{LLAMA}/summary/fees/{protocol}")
    if isinstance(data, dict) and "error" in data:
        return data
    if not isinstance(data, dict) or not data:
        return {"error": "protocol not found", "protocol": protocol}
    return {
        "protocol": protocol,
        "name": data.get("name"),
        "fees_24h_usd": data.get("total24h"),
        "fees_7d_usd": data.get("total7d"),
        "revenue_24h_usd": data.get("totalRevenue24h"),
        "chains": data.get("chains"),
    }


@endpoint("/dex/top", "$0.01",
          "Top 10 DEXs by 24h volume. $0.01 USDC per call.",
          "dex", ["dex", "top", "volume"], "dex-top")
async def dex_top():
    data = await _get_json(f"{LLAMA}/overview/dexs")
    if isinstance(data, dict) and "error" in data:
        return data
    protocols = (data or {}).get("protocols") or []
    top = sorted(protocols, key=lambda x: float(x.get("total24h") or 0), reverse=True)[:10]
    return {
        "top_dexs": [
            {"rank": i + 1, "name": x.get("name"), "volume_24h_usd": x.get("total24h")}
            for i, x in enumerate(top)
        ],
    }


# ===== CHAIN / BLOCKS / TRANSACTIONS / GAS =====

def _block_dict(block):
    return {
        "number": block.number,
        "hash": block.hash.hex(),
        "parent_hash": block.parentHash.hex(),
        "timestamp": block.timestamp,
        "miner": block.miner,
        "gas_used": block.gasUsed,
        "gas_limit": block.gasLimit,
        "base_fee_per_gas": block.get("baseFeePerGas"),
        "tx_count": len(block.transactions),
        "size": block.get("size"),
    }


@endpoint("/chain/block/latest", "$0.01",
          "Latest Base block with gas and transaction count. $0.01 USDC per call.",
          "chain", ["chain", "block", "latest"], "chain-block-latest")
async def chain_block_latest():
    return _block_dict(w3.eth.get_block("latest"))


@endpoint("/chain/block/{number}", "$0.01",
          "Base block by number. $0.01 USDC per call.",
          "chain", ["chain", "block"], "chain-block")
async def chain_block(number: str):
    try:
        n = int(number)
    except ValueError:
        return {"error": "block number must be an integer", "number": number}
    try:
        return _block_dict(w3.eth.get_block(n))
    except Exception as e:
        return {"error": str(e)[:200], "number": n}


@endpoint("/chain/block/{number}/transactions", "$0.01",
          "Transaction hashes in a Base block. $0.01 USDC per call.",
          "chain", ["chain", "block", "transactions"], "chain-block-txs")
async def chain_block_txs(number: str):
    try:
        n = int(number)
    except ValueError:
        return {"error": "block number must be an integer", "number": number}
    try:
        block = w3.eth.get_block(n, full_transactions=False)
        txs = [t.hex() if hasattr(t, "hex") else str(t) for t in block.transactions]
        return {"block": n, "tx_count": len(txs), "transactions": txs[:100]}
    except Exception as e:
        return {"error": str(e)[:200], "number": n}


def _tx_dict(tx):
    return {
        "hash": tx.hash.hex(),
        "from": tx["from"],
        "to": tx.get("to"),
        "value_eth": float(Web3.from_wei(tx.value, "ether")),
        "value_wei": tx.value,
        "gas": tx.gas,
        "gas_price_gwei": round(Web3.from_wei(tx.gasPrice, "gwei"), 4) if tx.get("gasPrice") else None,
        "nonce": tx.nonce,
        "block_number": tx.get("blockNumber"),
        "transaction_index": tx.get("transactionIndex"),
        "input_length_bytes": (len(tx.input) - 2) // 2 if tx.get("input") else 0,
    }


@endpoint("/chain/transaction/{hash}", "$0.01",
          "Detail for a Base transaction. $0.01 USDC per call.",
          "chain", ["chain", "transaction"], "chain-transaction")
async def chain_transaction(hash: str):
    try:
        return _tx_dict(w3.eth.get_transaction(hash))
    except Exception as e:
        return {"error": str(e)[:200], "hash": hash}


@endpoint("/chain/transaction/{hash}/receipt", "$0.01",
          "Receipt for a Base transaction: status, gas used, logs. $0.01 USDC per call.",
          "chain", ["chain", "transaction", "receipt"], "chain-tx-receipt")
async def chain_tx_receipt(hash: str):
    try:
        r = w3.eth.get_transaction_receipt(hash)
        return {
            "hash": r.transactionHash.hex(),
            "block_number": r.blockNumber,
            "status": r.status,
            "gas_used": r.gasUsed,
            "contract_address": r.get("contractAddress"),
            "log_count": len(r.logs),
            "logs": [
                {
                    "address": l.address,
                    "topics": [t.hex() for t in l.topics],
                    "data": l.data.hex() if hasattr(l.data, "hex") else str(l.data),
                    "log_index": l.logIndex,
                }
                for l in r.logs[:20]
            ],
        }
    except Exception as e:
        return {"error": str(e)[:200], "hash": hash}


@endpoint("/chain/transaction/{hash}/logs", "$0.01",
          "Event logs for a Base transaction (from its receipt). $0.01 USDC per call.",
          "chain", ["chain", "transaction", "logs"], "chain-tx-logs")
async def chain_tx_logs(hash: str):
    try:
        r = w3.eth.get_transaction_receipt(hash)
        return {
            "hash": hash,
            "log_count": len(r.logs),
            "logs": [
                {
                    "address": l.address,
                    "topics": [t.hex() for t in l.topics],
                    "data": l.data.hex() if hasattr(l.data, "hex") else str(l.data),
                    "block_number": l.blockNumber,
                    "log_index": l.logIndex,
                }
                for l in r.logs
            ],
        }
    except Exception as e:
        return {"error": str(e)[:200], "hash": hash}


@endpoint("/chain/transaction/{hash}/input", "$0.01",
          "Decoded input data for a Base transaction. $0.01 USDC per call.",
          "chain", ["chain", "transaction", "input"], "chain-tx-input")
async def chain_tx_input(hash: str):
    try:
        tx = w3.eth.get_transaction(hash)
        data = tx.input.hex() if hasattr(tx.input, "hex") else str(tx.input)
        return {
            "hash": hash,
            "function_selector": data[:10] if len(data) >= 10 else None,
            "input": data,
            "input_length_bytes": (len(data) - 2) // 2 if data.startswith("0x") else len(data) // 2,
            "value_eth": float(Web3.from_wei(tx.value, "ether")),
            "from": tx["from"],
            "to": tx.get("to"),
        }
    except Exception as e:
        return {"error": str(e)[:200], "hash": hash}


@endpoint("/chain/gas/fee-history", "$0.01",
          "Base fee history for the last 10 blocks. $0.01 USDC per call.",
          "chain", ["chain", "gas", "fees"], "chain-fee-history")
async def chain_fee_history():
    fh = w3.eth.fee_history(10, "latest", [25, 50, 75])
    return {
        "oldest_block": fh["oldestBlock"],
        "base_fee_per_gas_gwei": [round(Web3.from_wei(int(x), "gwei"), 4) for x in fh["baseFeePerGas"]],
        "gas_used_ratio": fh.get("gasUsedRatio"),
        "reward_gwei": [
            [round(Web3.from_wei(int(v), "gwei"), 4) for v in row]
            for row in (fh.get("reward") or [])
        ],
    }


@endpoint("/chain/gas/priority-fee", "$0.01",
          "Current max priority fee on Base. $0.01 USDC per call.",
          "chain", ["chain", "gas", "priority"], "chain-priority-fee")
async def chain_priority_fee():
    fee = w3.eth.max_priority_fee
    return {
        "max_priority_fee_wei": fee,
        "max_priority_fee_gwei": round(Web3.from_wei(fee, "gwei"), 4),
        "network": "base",
    }


@endpoint("/chain/gas/estimate-transfer/{to}", "$0.01",
          "Estimated gas for a plain ETH transfer on Base. $0.01 USDC per call.",
          "chain", ["chain", "gas", "estimate"], "chain-estimate-transfer")
async def chain_estimate_transfer(to: str):
    dest = _checksum(to)
    try:
        # Value 0: gas for a plain transfer is 21000 regardless of value,
        # and 0 keeps the estimate independent of the sender's balance.
        gas = w3.eth.estimate_gas({
            "from": _checksum(WALLET_ADDRESS),
            "to": dest,
            "value": 0,
        })
        gas_price = w3.eth.gas_price
        return {
            "to": dest,
            "value_eth": 0,
            "estimated_gas": gas,
            "gas_price_gwei": round(Web3.from_wei(gas_price, "gwei"), 4),
            "estimated_cost_eth": float(Web3.from_wei(gas * gas_price, "ether")),
            "note": "Live estimate for this recipient; a plain transfer to an EOA is 21000 gas, a contract wallet costs more.",
            "network": "base",
        }
    except Exception as e:
        return {"error": str(e)[:200], "to": dest}


@endpoint("/chain/gas/oracle", "$0.01",
          "Combined gas oracle: gas price, priority fee, fee history. $0.01 USDC per call.",
          "chain", ["chain", "gas", "oracle"], "chain-gas-oracle")
async def chain_gas_oracle():
    gas_price = w3.eth.gas_price
    priority = w3.eth.max_priority_fee
    fh = w3.eth.fee_history(5, "latest", [50])
    return {
        "gas_price_gwei": round(Web3.from_wei(gas_price, "gwei"), 4),
        "max_priority_fee_gwei": round(Web3.from_wei(priority, "gwei"), 4),
        "base_fee_gwei": round(Web3.from_wei(int(fh["baseFeePerGas"][-1]), "gwei"), 4),
        "block_number": w3.eth.block_number,
        "network": "base",
    }


def _sync_status():
    """Whether the node is at the chain head, inferred from latest block age.
    The public RPC filters eth_syncing, so block recency is the live signal:
    Base produces a block every ~2s, so a block older than 60s means the
    node is behind."""
    import time
    block = w3.eth.get_block("latest")
    age = int(time.time()) - int(block.timestamp)
    return {"syncing": age > 60, "latest_block": block.number, "block_age_seconds": age}


@endpoint("/chain/stats", "$0.01",
          "Live Base chain stats: block, gas, chain id, client. $0.01 USDC per call.",
          "chain", ["chain", "stats"], "chain-stats")
async def chain_stats():
    sync = _sync_status()
    return {
        "chain_id": w3.eth.chain_id,
        "block_number": sync["latest_block"],
        "block_age_seconds": sync["block_age_seconds"],
        "gas_price_gwei": round(Web3.from_wei(w3.eth.gas_price, "gwei"), 4),
        "client_version": w3.client_version,
        "syncing": sync["syncing"],
        "network": "base",
    }


@endpoint("/chain/syncing", "$0.01",
          "Whether the Base node is syncing (from latest block age). $0.01 USDC per call.",
          "chain", ["chain", "syncing"], "chain-syncing")
async def chain_syncing():
    sync = _sync_status()
    return {
        "syncing": sync["syncing"],
        "latest_block": sync["latest_block"],
        "block_age_seconds": sync["block_age_seconds"],
        "note": "eth_syncing is filtered by the public RPC; sync state is inferred from block recency (Base makes a block every ~2s).",
        "network": "base",
    }


@endpoint("/chain/client", "$0.01",
          "Client version of the Base RPC node. $0.01 USDC per call.",
          "chain", ["chain", "client"], "chain-client")
async def chain_client():
    return {"client_version": w3.client_version, "network": "base"}


@endpoint("/chain/network", "$0.01",
          "Chain id and latest block for Base. $0.01 USDC per call.",
          "chain", ["chain", "network"], "chain-network")
async def chain_network():
    return {"chain_id": w3.eth.chain_id, "network": "base", "block_number": w3.eth.block_number}


@endpoint("/chain/address/{address}/code", "$0.01",
          "Bytecode for a Base address, with size and hash. $0.01 USDC per call.",
          "chain", ["chain", "address", "code"], "chain-address-code")
async def chain_address_code(address: str):
    addr = _checksum(address)
    code = w3.eth.get_code(addr)
    return {
        "address": addr,
        "code_size": len(code),
        "code_keccak": Web3.keccak(bytes(code)).hex() if len(code) > 0 else None,
        "is_contract": len(code) > 0,
        "network": "base",
    }

# ===== NFT DATA (on-chain ERC721) =====

def _ipfs_to_http(uri: str):
    """Convert an ipfs:// URI to a public gateway URL."""
    if uri.startswith("ipfs://"):
        cid = uri[len("ipfs://"):]
        return f"https://ipfs.io/ipfs/{cid}"
    return uri


async def _fetch_nft_metadata(uri: str):
    """Fetch NFT metadata JSON from an http(s) or ipfs:// tokenURI."""
    url = _ipfs_to_http(uri)
    if not url.startswith("http"):
        return None
    try:
        r = await http_client.get(url, timeout=8.0)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    # Fallback gateway for ipfs URIs.
    if uri.startswith("ipfs://"):
        try:
            r = await http_client.get(
                f"https://cloudflare-ipfs.com/ipfs/{uri[len('ipfs://'):]}", timeout=8.0
            )
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
    return None


@endpoint("/nfts/{collection}/info", "$0.01",
          "ERC721 collection name and symbol. $0.01 USDC per call.",
          "nft", ["nft", "collection", "erc721"], "nft-info")
async def nft_info(collection: str):
    c = _checksum(collection)
    code = w3.eth.get_code(c)
    if len(code) == 0:
        return {"error": "Not a contract address", "collection": c}
    meta = _token_meta(c)
    return {
        "collection": c,
        "name": meta["name"],
        "symbol": meta["symbol"],
        "code_size": len(code),
        "network": "base",
    }


@endpoint("/nfts/{collection}/owner/{tokenId}", "$0.01",
          "Owner of one NFT by token id. $0.01 USDC per call.",
          "nft", ["nft", "owner", "erc721"], "nft-owner")
async def nft_owner(collection: str, tokenId: str):
    c = _checksum(collection)
    try:
        tid = int(tokenId)
    except ValueError:
        return {"error": "tokenId must be an integer", "collection": c}
    try:
        raw = _eth_call(c, "0x" + SEL_OWNER_OF + int(tid).to_bytes(32, "big").hex())
        owner = "0x" + raw[-20:].hex()
        return {"collection": c, "token_id": tid, "owner": Web3.to_checksum_address(owner), "network": "base"}
    except Exception as e:
        return {"error": str(e)[:200], "collection": c, "token_id": tid}


@endpoint("/nfts/{collection}/token/{tokenId}", "$0.01",
          "NFT tokenURI plus fetched metadata. $0.01 USDC per call.",
          "nft", ["nft", "metadata", "tokenuri"], "nft-token")
async def nft_token(collection: str, tokenId: str):
    c = _checksum(collection)
    try:
        tid = int(tokenId)
    except ValueError:
        return {"error": "tokenId must be an integer", "collection": c}
    try:
        raw = _eth_call(c, "0x" + SEL_TOKEN_URI + int(tid).to_bytes(32, "big").hex())
        uri = _decode_string(raw)
    except Exception as e:
        return {"error": str(e)[:200], "collection": c, "token_id": tid}
    metadata = await _fetch_nft_metadata(uri) if uri else None
    return {
        "collection": c,
        "token_id": tid,
        "token_uri": uri,
        "metadata": metadata,
        "network": "base",
    }


@endpoint("/nfts/{collection}/balance/{wallet}", "$0.01",
          "How many NFTs of a collection a wallet holds. $0.01 USDC per call.",
          "nft", ["nft", "balance", "wallet"], "nft-balance")
async def nft_balance(collection: str, wallet: str):
    c = _checksum(collection)
    w = _checksum(wallet)
    try:
        bal = _erc20_balance(c, w)
        return {"collection": c, "wallet": w, "balance": bal, "network": "base"}
    except Exception as e:
        return {"error": str(e)[:200], "collection": c, "wallet": w}


@endpoint("/nfts/{collection}/interfaces", "$0.01",
          "Which token standards an NFT contract supports (ERC721/1155/Enumerable). $0.01 USDC per call.",
          "nft", ["nft", "erc165", "standards"], "nft-interfaces")
async def nft_interfaces(collection: str):
    c = _checksum(collection)
    # ERC165 interface ids.
    interfaces = {
        "ERC165": "0x01ffc9a7",
        "ERC721": "0x80ac58cd",
        "ERC721Metadata": "0x5b5e139f",
        "ERC721Enumerable": "0x780e9d63",
        "ERC1155": "0xd9b67a26",
    }
    out = {}
    for name, iid in interfaces.items():
        try:
            # supportsInterface(bytes4): 4-byte id left-aligned in a 32-byte word.
            data = "0x" + SEL_SUPPORTS_INTERFACE + iid[2:] + "00" * 28
            raw = _eth_call(c, data)
            out[name] = _decode_bool(raw)
        except Exception:
            out[name] = None
    return {"collection": c, "interfaces": out, "network": "base"}


@endpoint("/nfts/{collection}/supply", "$0.01",
          "Total supply of an enumerable NFT collection. $0.01 USDC per call.",
          "nft", ["nft", "supply", "erc721"], "nft-supply")
async def nft_supply(collection: str):
    c = _checksum(collection)
    try:
        supply = _erc20_uint(c, SEL_TOTAL_SUPPLY)
        return {"collection": c, "total_supply": supply, "enumerable": True, "network": "base"}
    except Exception:
        return {
            "collection": c,
            "total_supply": None,
            "enumerable": False,
            "note": "Contract does not expose totalSupply (not ERC721Enumerable)",
            "network": "base",
        }


@endpoint("/nfts/{collection}/exists/{tokenId}", "$0.01",
          "Whether an NFT token id exists. $0.01 USDC per call.",
          "nft", ["nft", "exists", "erc721"], "nft-exists")
async def nft_exists(collection: str, tokenId: str):
    c = _checksum(collection)
    try:
        tid = int(tokenId)
    except ValueError:
        return {"error": "tokenId must be an integer", "collection": c}
    try:
        raw = _eth_call(c, "0x" + SEL_OWNER_OF + int(tid).to_bytes(32, "big").hex())
        owner = "0x" + raw[-20:].hex()
        exists = owner != "0x0000000000000000000000000000000000000000"
        return {"collection": c, "token_id": tid, "exists": exists, "owner": Web3.to_checksum_address(owner) if exists else None, "network": "base"}
    except Exception:
        return {"collection": c, "token_id": tid, "exists": False, "network": "base"}


# ===== DEFI (DeFiLlama + Aave on-chain) =====

@endpoint("/defi/tvl", "$0.01",
          "Total value locked on Base. $0.01 USDC per call.",
          "defi", ["defi", "tvl", "base"], "defi-tvl")
async def defi_tvl():
    data = await _get_json(f"{LLAMA}/v2/chains")
    if isinstance(data, dict) and "error" in data:
        return data
    chains = data if isinstance(data, list) else []
    base = next((c for c in chains if str(c.get("name", "")).lower() == "base"), None)
    if not base:
        return {"error": "Base chain not found in DeFiLlama response"}
    return {"chain": "Base", "tvl_usd": base.get("tvl"), "network": "base"}


@endpoint("/defi/tvl/{protocol}", "$0.01",
          "Total value locked for one protocol. $0.01 USDC per call.",
          "defi", ["defi", "tvl", "protocol"], "defi-tvl-protocol")
async def defi_tvl_protocol(protocol: str):
    try:
        r = await http_client.get(f"{LLAMA}/tvl/{protocol}")
        if r.status_code != 200:
            return {"error": f"upstream HTTP {r.status_code}", "protocol": protocol}
        try:
            tvl = r.json()
        except Exception:
            return {"error": "protocol not found", "protocol": protocol}
        if not isinstance(tvl, (int, float)):
            return {"error": "protocol not found", "protocol": protocol}
        return {"protocol": protocol, "tvl_usd": tvl}
    except Exception as e:
        return {"error": str(e)[:200], "protocol": protocol}


@endpoint("/defi/protocols", "$0.01",
          "Top protocols by TVL. $0.01 USDC per call.",
          "defi", ["defi", "protocols", "tvl"], "defi-protocols")
async def defi_protocols():
    data = await _get_json(f"{LLAMA}/protocols")
    if isinstance(data, dict) and "error" in data:
        return data
    protos = data if isinstance(data, list) else []
    top = sorted(protos, key=lambda x: float(x.get("tvl") or 0), reverse=True)[:20]
    return {
        "count": len(top),
        "protocols": [
            {"name": x.get("name"), "symbol": x.get("symbol"), "tvl_usd": x.get("tvl"),
             "chains": x.get("chains"), "url": x.get("url")}
            for x in top
        ],
    }


@endpoint("/defi/protocols/base", "$0.01",
          "Top protocols on Base by TVL. $0.01 USDC per call.",
          "defi", ["defi", "protocols", "base"], "defi-protocols-base")
async def defi_protocols_base():
    data = await _get_json(f"{LLAMA}/protocols")
    if isinstance(data, dict) and "error" in data:
        return data
    protos = data if isinstance(data, list) else []
    on_base = [x for x in protos if "Base" in (x.get("chains") or [])]
    top = sorted(on_base, key=lambda x: float(x.get("tvl") or 0), reverse=True)[:20]
    return {
        "chain": "Base",
        "count": len(top),
        "protocols": [
            {"name": x.get("name"), "symbol": x.get("symbol"), "tvl_usd": x.get("tvl"), "url": x.get("url")}
            for x in top
        ],
    }


def _trim_pool(p):
    return {
        "pool": p.get("pool"),
        "chain": p.get("chain"),
        "project": p.get("project"),
        "symbol": p.get("symbol"),
        "tvl_usd": p.get("tvlUsd"),
        "apy": p.get("apy"),
        "apy_base": p.get("apyBase"),
        "apy_reward": p.get("apyReward"),
        "reward_tokens": p.get("rewardTokens"),
    }


async def _yields_pools():
    data = await _get_json(f"{YIELDS}/pools")
    if isinstance(data, dict) and "error" in data:
        return data
    pools = (data or {}).get("data") or []
    return pools


@endpoint("/defi/yields/top", "$0.01",
          "Highest-APY yield pools across chains. $0.01 USDC per call.",
          "defi", ["defi", "yields", "apy"], "defi-yields-top")
async def defi_yields_top():
    pools = await _yields_pools()
    if isinstance(pools, dict):
        return pools
    top = sorted(pools, key=lambda x: float(x.get("apy") or 0), reverse=True)[:10]
    return {"count": len(top), "pools": [_trim_pool(p) for p in top]}


@endpoint("/defi/yields/base", "$0.01",
          "Highest-APY yield pools on Base. $0.01 USDC per call.",
          "defi", ["defi", "yields", "base"], "defi-yields-base")
async def defi_yields_base():
    pools = await _yields_pools()
    if isinstance(pools, dict):
        return pools
    on_base = [p for p in pools if p.get("chain") == "Base"]
    top = sorted(on_base, key=lambda x: float(x.get("apy") or 0), reverse=True)[:10]
    return {"chain": "Base", "count": len(top), "pools": [_trim_pool(p) for p in top]}


@endpoint("/defi/yields/stable", "$0.01",
          "Highest-APY stablecoin yield pools. $0.01 USDC per call.",
          "defi", ["defi", "yields", "stablecoin"], "defi-yields-stable")
async def defi_yields_stable():
    pools = await _yields_pools()
    if isinstance(pools, dict):
        return pools
    stables = [p for p in pools if (p.get("stablecoin") is True)]
    top = sorted(stables, key=lambda x: float(x.get("apy") or 0), reverse=True)[:10]
    return {"count": len(top), "pools": [_trim_pool(p) for p in top]}


@endpoint("/defi/yields/pool/{pool}", "$0.01",
          "Yield details for one pool by id. $0.01 USDC per call.",
          "defi", ["defi", "yields", "pool"], "defi-yields-pool")
async def defi_yields_pool(pool: str):
    pools = await _yields_pools()
    if isinstance(pools, dict):
        return pools
    match = next((p for p in pools if p.get("pool") == pool), None)
    if not match:
        return {"error": "pool not found", "pool": pool}
    return _trim_pool(match)


@endpoint("/defi/stablecoins", "$0.01",
          "Largest stablecoins by circulating supply. $0.01 USDC per call.",
          "defi", ["defi", "stablecoins"], "defi-stablecoins")
async def defi_stablecoins():
    data = await _get_json(f"{STABLECOINS}/stablecoins")
    if isinstance(data, dict) and "error" in data:
        return data
    assets = (data or {}).get("peggedAssets") or []
    top = sorted(
        assets,
        key=lambda x: float(((x.get("circulating") or {}).get("peggedUSD")) or 0),
        reverse=True,
    )[:15]
    return {
        "count": len(top),
        "stablecoins": [
            {
                "name": x.get("name"), "symbol": x.get("symbol"),
                "peg_type": x.get("pegType"), "peg_mechanism": x.get("pegMechanism"),
                "circulating_usd": (x.get("circulating") or {}).get("peggedUSD"),
            }
            for x in top
        ],
    }


_AAVE_ABI = [
    {"inputs": [], "name": "getReservesList", "outputs": [{"name": "", "type": "address[]"}],
     "stateMutability": "view", "type": "function"},
]


@endpoint("/defi/aave/reserves", "$0.01",
          "Aave V3 reserve assets on Base. $0.01 USDC per call.",
          "defi", ["defi", "aave", "lending"], "defi-aave-reserves")
async def defi_aave_reserves():
    aave = w3.eth.contract(address=_checksum(AAVE_POOL), abi=_AAVE_ABI)
    try:
        reserves = aave.functions.getReservesList().call()
    except Exception as e:
        return {"error": str(e)[:200], "pool": AAVE_POOL}
    out = []
    for r in reserves:
        meta = _token_meta(r)
        out.append({"address": r, "symbol": meta["symbol"], "name": meta["name"]})
    return {"pool": AAVE_POOL, "count": len(out), "reserves": out, "network": "base"}


# ===== NAMING / ENS =====

@endpoint("/ens/resolve/{name}", "$0.01",
          "Resolve an ENS name to its address. $0.01 USDC per call.",
          "naming", ["ens", "resolve"], "ens-resolve")
async def ens_resolve(name: str):
    data = await _get_json(f"{ENSIDEAS}/ens/resolve/{name}")
    if isinstance(data, dict) and "error" in data:
        return data
    if not isinstance(data, dict) or not data.get("address"):
        return {"error": "ENS name not found", "name": name}
    return {
        "name": name,
        "address": data.get("address"),
        "display_name": data.get("displayName"),
        "avatar": data.get("avatar"),
    }


@endpoint("/ens/reverse/{address}", "$0.01",
          "Reverse-resolve a wallet address to its ENS name. $0.01 USDC per call.",
          "naming", ["ens", "reverse"], "ens-reverse")
async def ens_reverse(address: str):
    addr = _checksum(address)
    data = await _get_json(f"{ENSDATA}/{addr}")
    if isinstance(data, dict) and "upstream HTTP 404" in str(data.get("error", "")):
        # No ENS name for this address: a valid answer, not an error.
        return {"address": addr, "ens_name": None, "avatar": None}
    if isinstance(data, dict) and "error" in data:
        return data
    name = (data or {}).get("name") or (data or {}).get("ens") or (data or {}).get("displayName")
    return {"address": addr, "ens_name": name, "avatar": (data or {}).get("avatar")}


@endpoint("/ens/avatar/{name}", "$0.01",
          "Avatar URL for an ENS name. $0.01 USDC per call.",
          "naming", ["ens", "avatar"], "ens-avatar")
async def ens_avatar(name: str):
    data = await _get_json(f"{ENSIDEAS}/ens/resolve/{name}")
    if isinstance(data, dict) and "error" in data:
        return data
    if not isinstance(data, dict) or not data.get("address"):
        return {"error": "ENS name not found", "name": name}
    return {"name": name, "address": data.get("address"), "avatar": data.get("avatar")}


@endpoint("/ens/profile/{name}", "$0.01",
          "Full public profile for an ENS name. $0.01 USDC per call.",
          "naming", ["ens", "profile"], "ens-profile")
async def ens_profile(name: str):
    data = await _get_json(f"{ENSDATA}/{name}")
    if isinstance(data, dict) and "error" in data:
        return data
    if not isinstance(data, dict) or not data.get("address"):
        return {"error": "ENS name not found", "name": name}
    return {
        "name": name,
        "address": data.get("address"),
        "avatar": data.get("avatar"),
        "description": data.get("description"),
        "url": data.get("url"),
        "socials": {k: v for k, v in (data or {}).items() if k in ("twitter", "github", "discord", "telegram")},
    }


# ===== UTILITIES =====

@endpoint("/utils/eth-usd", "$0.01",
          "Live ETH price in USD. $0.01 USDC per call.",
          "utils", ["utils", "eth", "price"], "utils-eth-usd")
async def utils_eth_usd():
    price = await _eth_price_usd()
    if price is None:
        return {"error": "could not fetch ETH price"}
    return {"asset": "ETH", "price_usd": price}


@endpoint("/utils/convert/eth-usd/{amount}", "$0.01",
          "Convert an ETH amount to USD at the live price. $0.01 USDC per call.",
          "utils", ["utils", "convert", "eth"], "utils-convert-eth-usd")
async def utils_convert_eth_usd(amount: str):
    try:
        eth = float(amount)
    except ValueError:
        return {"error": "amount must be a number", "amount": amount}
    price = await _eth_price_usd()
    if price is None:
        return {"error": "could not fetch ETH price"}
    return {"eth": eth, "eth_price_usd": price, "usd": round(eth * price, 2)}


@endpoint("/utils/convert/usd-eth/{amount}", "$0.01",
          "Convert a USD amount to ETH at the live price. $0.01 USDC per call.",
          "utils", ["utils", "convert", "usd"], "utils-convert-usd-eth")
async def utils_convert_usd_eth(amount: str):
    try:
        usd = float(amount)
    except ValueError:
        return {"error": "amount must be a number", "amount": amount}
    price = await _eth_price_usd()
    if price is None:
        return {"error": "could not fetch ETH price"}
    return {"usd": usd, "eth_price_usd": price, "eth": round(usd / price, 6) if price else None}


@endpoint("/utils/convert/wei/{amount}", "$0.01",
          "Convert wei to ether and gwei. $0.01 USDC per call.",
          "utils", ["utils", "convert", "wei"], "utils-convert-wei")
async def utils_convert_wei(amount: str):
    try:
        wei = int(amount)
    except ValueError:
        return {"error": "amount must be an integer number of wei", "amount": amount}
    return {
        "wei": wei,
        "ether": float(Web3.from_wei(wei, "ether")),
        "gwei": float(Web3.from_wei(wei, "gwei")),
    }


@endpoint("/utils/convert/ether-wei/{amount}", "$0.01",
          "Convert ether to wei. $0.01 USDC per call.",
          "utils", ["utils", "convert", "ether"], "utils-convert-ether-wei")
async def utils_convert_ether_wei(amount: str):
    try:
        eth = float(amount)
    except ValueError:
        return {"error": "amount must be a number", "amount": amount}
    return {"ether": eth, "wei": Web3.to_wei(eth, "ether")}


@endpoint("/utils/address/validate/{address}", "$0.01",
          "Validate an Ethereum address and return its checksum form. $0.01 USDC per call.",
          "utils", ["utils", "address", "validate"], "utils-address-validate")
async def utils_address_validate(address: str):
    valid = Web3.is_address(address)
    result = {"address": address, "is_valid": valid}
    if valid:
        result["checksum_address"] = Web3.to_checksum_address(address)
        result["is_checksum"] = (address == result["checksum_address"])
    return result


@endpoint("/utils/address/keccak/{text}", "$0.01",
          "Keccak-256 hash of text. $0.01 USDC per call.",
          "utils", ["utils", "hash", "keccak"], "utils-keccak")
async def utils_keccak(text: str):
    return {"input": text, "keccak256": Web3.keccak(text=text).hex()}


@endpoint("/utils/gas-cost/{gwei}/{gas}", "$0.01",
          "Cost of a transaction in ETH and USD at the live ETH price. $0.01 USDC per call.",
          "utils", ["utils", "gas", "cost"], "utils-gas-cost")
async def utils_gas_cost(gwei: str, gas: str):
    try:
        g = float(gwei)
        units = int(gas)
    except ValueError:
        return {"error": "gwei and gas must be numbers", "gwei": gwei, "gas": gas}
    cost_eth = (g * units) / 1e9
    price = await _eth_price_usd()
    return {
        "gas_price_gwei": g,
        "gas_units": units,
        "cost_eth": cost_eth,
        "eth_price_usd": price,
        "cost_usd": (round(cost_eth * price, 4) if price else None),
    }


@endpoint("/utils/coin-price/{coin}", "$0.01",
          "Live USD price for a coin by CoinGecko id. $0.01 USDC per call.",
          "utils", ["utils", "coin", "price"], "utils-coin-price")
async def utils_coin_price(coin: str):
    data = await _get_json(
        f"{COINGECKO}/simple/price", params={"ids": coin, "vs_currencies": "usd"}
    )
    if isinstance(data, dict) and "error" in data:
        return data
    entry = (data or {}).get(coin)
    if not entry:
        return {"error": "coin not found on CoinGecko", "coin": coin}
    return {"coin": coin, "price_usd": entry.get("usd"), "source": "coingecko"}


@endpoint("/utils/market/global", "$0.01",
          "Global crypto market stats from CoinGecko. $0.01 USDC per call.",
          "utils", ["utils", "market", "global"], "utils-market-global")
async def utils_market_global():
    data = await _get_json(f"{COINGECKO}/global")
    if isinstance(data, dict) and "error" in data:
        return data
    d = (data or {}).get("data") or {}
    return {
        "active_cryptocurrencies": d.get("active_cryptocurrencies"),
        "markets": d.get("markets"),
        "total_market_cap_usd": (d.get("total_market_cap") or {}).get("usd"),
        "total_volume_24h_usd": (d.get("total_volume") or {}).get("usd"),
        "btc_dominance_pct": (d.get("market_cap_percentage") or {}).get("btc"),
        "eth_dominance_pct": (d.get("market_cap_percentage") or {}).get("eth"),
        "market_cap_change_24h_pct": d.get("market_cap_change_percentage_24h_usd"),
    }


@endpoint("/utils/market/trending", "$0.01",
          "Trending coins on CoinGecko. $0.01 USDC per call.",
          "utils", ["utils", "market", "trending"], "utils-market-trending")
async def utils_market_trending():
    data = await _get_json(f"{COINGECKO}/search/trending")
    if isinstance(data, dict) and "error" in data:
        return data
    coins = (data or {}).get("coins") or []
    return {
        "coins": [
            {
                "id": c.get("item", {}).get("id"),
                "name": c.get("item", {}).get("name"),
                "symbol": c.get("item", {}).get("symbol"),
                "market_cap_rank": c.get("item", {}).get("market_cap_rank"),
                "price_btc": c.get("item", {}).get("price_btc"),
            }
            for c in coins[:10]
        ],
    }


@endpoint("/utils/market/top", "$0.01",
          "Top 10 cryptocurrencies by market cap. $0.01 USDC per call.",
          "utils", ["utils", "market", "top"], "utils-market-top")
async def utils_market_top():
    data = await _get_json(
        f"{COINGECKO}/coins/markets",
        params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": 10, "page": 1},
    )
    if isinstance(data, dict) and "error" in data:
        return data
    coins = data if isinstance(data, list) else []
    return {
        "coins": [
            {
                "rank": c.get("market_cap_rank"), "id": c.get("id"), "symbol": c.get("symbol"),
                "name": c.get("name"), "price_usd": c.get("current_price"),
                "market_cap_usd": c.get("market_cap"), "volume_24h_usd": c.get("total_volume"),
                "change_24h_pct": c.get("price_change_percentage_24h"),
            }
            for c in coins
        ],
    }


@endpoint("/utils/fear-greed", "$0.01",
          "Crypto Fear & Greed Index. $0.01 USDC per call.",
          "utils", ["utils", "fear-greed", "sentiment"], "utils-fear-greed")
async def utils_fear_greed():
    data = await _get_json(f"{FNG}/fng/", params={"limit": 1})
    if isinstance(data, dict) and "error" in data:
        return data
    items = (data or {}).get("data") or []
    if not items:
        return {"error": "no Fear & Greed data returned"}
    it = items[0]
    return {
        "value": it.get("value"),
        "classification": it.get("value_classification"),
        "timestamp": it.get("timestamp"),
        "time_until_update": it.get("time_until_update"),
    }


@endpoint("/utils/coin/{coin}", "$0.01",
          "Detail for a coin by CoinGecko id. $0.01 USDC per call.",
          "utils", ["utils", "coin", "detail"], "utils-coin")
async def utils_coin(coin: str):
    data = await _get_json(f"{COINGECKO}/coins/{coin}")
    if isinstance(data, dict) and "error" in data:
        return data
    if not isinstance(data, dict) or not data.get("id"):
        return {"error": "coin not found on CoinGecko", "coin": coin}
    md = data.get("market_data") or {}
    return {
        "id": data.get("id"),
        "symbol": data.get("symbol"),
        "name": data.get("name"),
        "price_usd": (md.get("current_price") or {}).get("usd"),
        "market_cap_usd": (md.get("market_cap") or {}).get("usd"),
        "market_cap_rank": data.get("market_cap_rank"),
        "volume_24h_usd": (md.get("total_volume") or {}).get("usd"),
        "change_24h_pct": (md.get("price_change_percentage_24h") or {}),
        "change_7d_pct": (md.get("price_change_percentage_7d") or {}),
        "circulating_supply": md.get("circulating_supply"),
        "total_supply": md.get("total_supply"),
        "max_supply": md.get("max_supply"),
        "homepage": (data.get("links") or {}).get("homepage", [None])[0],
    }

# ===== v5 ADDITIONS (100 new paid endpoints) =====
# New data sources — all free, no keys, verified reachable from this VM
# Nov 21, 2026:
#   - DeFiLlama stablecoins API (stablecoins.llama.fi): /stablecoins list,
#     /stablecoinchains, /stablecoinprices. NOTE: there is NO per-asset
#     path — /stablecoins/{id} 404s, so per-asset handlers filter the
#     full list client-side.
#   - DeFiLlama yields /chart/{pool}: per-pool history of tvl/apy.
#   - GoPlus Security API (api.gopluslabs.io, no key): token_security/8453
#     returns holder counts, top holders, audit flags (honeypot, mintable,
#     taxes, proxy, owner/creator).
#   - Polymarket Gamma API (gamma-api.polymarket.com, no key): /markets,
#     /events, /markets/{slug}.
#   - Polymarket CLOB price API (clob.polymarket.com) is DEAD for our use:
#     it rejects every Gamma clobTokenId as "Invalid token id" (verified
#     Nov 21, 2026 across /price, /book, /midpoint, /prices) — do not use.
#   - Snapshot Hub GraphQL (hub.snapshot.org/graphql, no key): spaces,
#     proposals, votes.
# Dead ends (verified Nov 21, 2026 — do NOT use):
#   - bridges.llama.fi returns 402 (paid plan required).
#   - DeFiLlama /summary/revenue/* returns 500.
#   - Farcaster hub is unreachable from this environment.
#   - eth_blobBaseFee is filtered by the public Base RPC (401).
#   - Compound V3 Comet getSupplyRate/getBorrowRate revert on Base;
#     use DeFiLlama yields for Compound APYs instead.
#   - DeFiLlama /protocol/{slug} returns ~30MB and truncates; use
#     /protocols for protocol detail instead.
#   - DeFiLlama /overview/dexs?chain=X IGNORES the chain param (returns
#     global totals for every chain); use the path form /overview/dexs/{chain}.
#   - Public IPFS gateways are all blocked from this VM (ipfs.io 403,
#     cloudflare-ipfs.com unreachable, dweb.link 403, w3s.link blocked,
#     pinata unreachable, nftstorage 403); NFT metadata handlers decode
#     data: URIs locally and treat gateway fetches as best-effort.
#   - Compound V3 Comet on Base has no totalSupply(uint256) per-token
#     supply for ERC1155s in Zora1155 either; those handlers return an
#     explicit "not supported" note (same pattern as v4 nft-supply).

GOPLUS = "https://api.gopluslabs.io"
SNAPSHOT = "https://hub.snapshot.org/graphql"
GAMMA = "https://gamma-api.polymarket.com"

# Chainlink price feeds on Base (verified on-chain Nov 21, 2026 — all
# 8-decimal aggregators; prices at verification: ETH $1680.44,
# BTC $60152.69, USDC $0.99987).
CHAINLINK_FEEDS = {
    "eth": {"pair": "ETH / USD", "address": "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70"},
    "btc": {"pair": "BTC / USD", "address": "0x64c911996D3c6aC71f9b455B1E8E7266BcbD848F"},
    "usdc": {"pair": "USDC / USD", "address": "0x7e860098F58bBFC8648a4311b374B1D669a2bc6B"},
}

# Compound V3 Comet USDC market on Base (verified on-chain Nov 21, 2026:
# utilization 85.45%, totalSupply/totalBorrow/balanceOf/borrowBalanceOf
# work; getSupplyRate/getBorrowRate revert).
COMET_USDC = "0xb125E6687d4313864e53df431d5425969c15Eb2F"

# Token spenders queried by the wallet-approvals endpoints (verified
# on-chain Nov 21, 2026).
SPENDERS = [
    {"name": "Permit2", "address": "0x000000000022D473030F116dDEE9F6B43aC78BA3"},
    {"name": "Uniswap SwapRouter02", "address": "0x2626664c2603336E57B271c5C0b26F421741e481"},
    {"name": "Aave V3 Pool", "address": AAVE_POOL},
    {"name": "Aerodrome Router", "address": "0x9Ee35Bf1bd69a03c6F0393582030fCe8Bb93D418"},
]

_CHAINLINK_ABI = [
    {"name": "latestAnswer", "outputs": [{"type": "int256"}], "inputs": [], "stateMutability": "view", "type": "function"},
    {"name": "decimals", "outputs": [{"type": "uint8"}], "inputs": [], "stateMutability": "view", "type": "function"},
    {"name": "description", "outputs": [{"type": "string"}], "inputs": [], "stateMutability": "view", "type": "function"},
]

_AAVE_RESERVE_ABI = [
    {"name": "getReservesList", "outputs": [{"type": "address[]"}], "inputs": [], "stateMutability": "view", "type": "function"},
    {"name": "getReserveData", "outputs": [{"type": "tuple", "components": [
        {"name": "configuration", "type": "tuple", "components": [{"name": "data", "type": "uint256"}]},
        {"name": "liquidityIndex", "type": "uint128"},
        {"name": "currentLiquidityRate", "type": "uint128"},
        {"name": "variableBorrowIndex", "type": "uint128"},
        {"name": "currentVariableBorrowRate", "type": "uint128"},
        {"name": "currentStableBorrowRate", "type": "uint128"},
        {"name": "lastUpdateTimestamp", "type": "uint40"},
        {"name": "id", "type": "uint16"},
        {"name": "aTokenAddress", "type": "address"},
        {"name": "stableDebtTokenAddress", "type": "address"},
        {"name": "variableDebtTokenAddress", "type": "address"},
        {"name": "interestRateStrategyAddress", "type": "address"},
        {"name": "accruedToTreasury", "type": "uint128"},
        {"name": "unbacked", "type": "uint128"},
        {"name": "isolationModeTotalDebt", "type": "uint128"},
    ]}], "inputs": [{"name": "asset", "type": "address"}], "stateMutability": "view", "type": "function"},
    {"name": "getUserAccountData", "outputs": [
        {"type": "uint256"}, {"type": "uint256"}, {"type": "uint256"},
        {"type": "uint256"}, {"type": "uint256"}, {"type": "uint256"},
    ], "inputs": [{"name": "user", "type": "address"}], "stateMutability": "view", "type": "function"},
]

_COMET_ABI = [
    {"name": "getUtilization", "outputs": [{"type": "uint256"}], "inputs": [], "stateMutability": "view", "type": "function"},
    {"name": "totalSupply", "outputs": [{"type": "uint256"}], "inputs": [], "stateMutability": "view", "type": "function"},
    {"name": "totalBorrow", "outputs": [{"type": "uint256"}], "inputs": [], "stateMutability": "view", "type": "function"},
    {"name": "balanceOf", "outputs": [{"type": "uint256"}], "inputs": [{"name": "account", "type": "address"}], "stateMutability": "view", "type": "function"},
    {"name": "borrowBalanceOf", "outputs": [{"type": "uint256"}], "inputs": [{"name": "account", "type": "address"}], "stateMutability": "view", "type": "function"},
    {"name": "baseToken", "outputs": [{"type": "address"}], "inputs": [], "stateMutability": "view", "type": "function"},
]

# ERC1155 selectors (balanceOf(address,uint256)=0x00fdd58e,
# uri(uint256)=0x0e89341c, totalSupply(uint256)=0xbd85b039).
SEL_BALANCE_1155 = "00fdd58e"
SEL_URI_1155 = "0e89341c"
SEL_TOTALSUPPLY_1155 = "bd85b039"


async def _post_json(url, payload):
    """POST a JSON body to a public API. Returns parsed body or error dict."""
    try:
        r = await http_client.post(url, json=payload)
        if r.status_code != 200:
            return {"error": f"upstream HTTP {r.status_code}", "url": url}
        try:
            return r.json()
        except Exception:
            return {"error": "upstream returned non-JSON", "url": url}
    except Exception as e:
        return {"error": str(e)[:200], "url": url}


def _ray_to_pct(ray):
    """Aave RAY (1e27) rate -> percent per year."""
    return round((ray / 1e27) * 100, 4)


def _chainlink_read(feed_addr):
    """Read a Chainlink aggregator: price, decimals, description."""
    try:
        c = w3.eth.contract(address=_checksum(feed_addr), abi=_CHAINLINK_ABI)
        dec = c.functions.decimals().call()
        ans = c.functions.latestAnswer().call()
        desc = c.functions.description().call()
        return {"feed": _checksum(feed_addr), "pair": desc, "decimals": dec,
                "price": ans / (10 ** dec), "answer_raw": ans,
                "source": "chainlink", "network": "base"}
    except Exception as e:
        return {"error": str(e)[:200], "feed": feed_addr}


def _aave_reserve_data(asset):
    """Aave V3 getReserveData for one asset (rates + token addresses)."""
    aave = w3.eth.contract(address=_checksum(AAVE_POOL), abi=_AAVE_RESERVE_ABI)
    rd = aave.functions.getReserveData(_checksum(asset)).call()
    return {
        "liquidity_index": rd[1],
        "supply_rate_ray": rd[2],
        "supply_apy_pct": _ray_to_pct(rd[2]),
        "variable_borrow_index": rd[3],
        "variable_borrow_rate_ray": rd[4],
        "variable_borrow_apy_pct": _ray_to_pct(rd[4]),
        "stable_borrow_rate_ray": rd[5],
        "stable_borrow_apy_pct": _ray_to_pct(rd[5]),
        "last_update": rd[6],
        "a_token": rd[8],
        "stable_debt_token": rd[9],
        "variable_debt_token": rd[10],
    }


async def _stablecoin_list():
    data = await _get_json(f"{STABLECOINS}/stablecoins")
    if isinstance(data, dict) and "error" in data:
        return data
    return (data or {}).get("peggedAssets") or []


def _stablecoin_find(assets, asset):
    """Find one stablecoin by id, symbol, or gecko_id (case-insensitive)."""
    key = asset.lower()
    for a in assets:
        for field in ("id", "symbol", "gecko_id"):
            if str(a.get(field, "")).lower() == key:
                return a
    return None


async def _yields_by_project(project, chain=None):
    """Yield pools for one project (optionally one chain), from /pools."""
    pools = await _yields_pools()
    if isinstance(pools, dict):
        return pools
    out = [p for p in pools if str(p.get("project", "")).lower() == project.lower()]
    if chain:
        out = [p for p in out if str(p.get("chain", "")).lower() == chain.lower()]
    return out


async def _goplus_token(addr):
    """GoPlus token_security entry for one Base token, or error dict."""
    data = await _get_json(f"{GOPLUS}/api/v1/token_security/8453",
                           params={"contract_addresses": _checksum(addr)})
    if isinstance(data, dict) and "error" in data:
        return data
    result = (data or {}).get("result") or {}
    entry = result.get(_checksum(addr).lower()) or result.get(_checksum(addr))
    if not entry:
        return {"error": "GoPlus has no data for this token", "token": _checksum(addr)}
    return entry


async def _snapshot(query, variables=None):
    """Run a Snapshot Hub GraphQL query. Returns data dict or error dict."""
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    data = await _post_json(SNAPSHOT, payload)
    if isinstance(data, dict) and "error" in data:
        return data
    if isinstance(data, dict) and data.get("errors"):
        return {"error": str(data["errors"])[:300]}
    return (data or {}).get("data") or {}


def _trim_market(m):
    return {
        "id": m.get("id"), "slug": m.get("slug"), "question": m.get("question"),
        "liquidity": m.get("liquidity"), "volume_24h": m.get("volume24hr"),
        "volume": m.get("volume"), "end_date": m.get("endDate"),
        "active": m.get("active"), "closed": m.get("closed"),
        "outcomes": m.get("outcomes"), "outcome_prices": m.get("outcomePrices"),
        "url": f"https://polymarket.com/event/{m.get('slug')}" if m.get("slug") else None,
    }


def _trim_stable(a):
    return {
        "name": a.get("name"), "symbol": a.get("symbol"), "id": a.get("id"),
        "peg_type": a.get("pegType"), "peg_mechanism": a.get("pegMechanism"),
        "price": a.get("price"),
        "circulating_usd": (a.get("circulating") or {}).get("peggedUSD"),
        "chains": list((a.get("chainCirculating") or {}).keys()),
    }


def _decode_data_uri(uri):
    """Decode a data:application/json;base64,... token URI to a dict, or None.
    Uniswap V3 position NFTs (and other fully on-chain collections) embed
    their metadata this way — no gateway needed, so it works even where
    public IPFS gateways are blocked (as they are from this VM)."""
    if not uri or not uri.startswith("data:application/json;base64,"):
        return None
    try:
        return json.loads(base64.b64decode(uri.split(",", 1)[1]))
    except Exception:
        return None


async def _coin_price(coin):
    """CoinGecko simple price for one coin id. Returns {"usd": ...} or error dict."""
    data = await _get_json(f"{COINGECKO}/simple/price", params={"ids": coin, "vs_currencies": "usd"})
    if isinstance(data, dict) and "error" in data:
        return data
    entry = (data or {}).get(coin)
    if not entry:
        return {"error": "coin not found on CoinGecko", "coin": coin}
    return {"usd": entry.get("usd")}


async def _ens_lookup(name):
    """Full public ENS profile for one name (ensdata.net)."""
    return await _get_json(f"{ENSDATA}/{name}")


def _curated(symbol):
    """Curated Base token entry by symbol (case-insensitive), or None."""
    for t in CURATED_TOKENS:
        if t["symbol"].lower() == symbol.lower():
            return t
    return None


def _allowance(token, owner, spender):
    """ERC20 allowance a wallet granted to a spender (raw + human)."""
    raw = _erc20_uint(token, SEL_ALLOWANCE, _addr32(owner) + _addr32(spender))
    meta = _token_meta(token)
    dec = meta["decimals"]
    return {"allowance_raw": raw,
            "allowance": (raw / (10 ** dec)) if dec is not None else None,
            "decimals": dec}


# Meme tokens used by /memes/trending (addresses verified on Dexscreener
# Nov 21, 2026; DEGEN corrected — the widely-quoted address ending
# "Ef58" returns no pairs, the live one ends "Efefed").
MEME_TOKENS = [
    {"symbol": "DEGEN", "address": "0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed"},
    {"symbol": "BRETT", "address": "0x532f27101965dd16442E59d40670FaF5eBB142E4"},
    {"symbol": "TOSHI", "address": "0xAC1Bd2486aAf3B5C0fc3Fd868558b082a531B2b4"},
]

# ---------- v5 LENDING (14) ----------

async def lending_aave_markets():
    """All Aave V3 markets on Base with on-chain supply/borrow APY."""
    try:
        aave = w3.eth.contract(address=_checksum(AAVE_POOL), abi=_AAVE_RESERVE_ABI)
        reserves = aave.functions.getReservesList().call()
        markets = []
        for r in reserves:
            try:
                meta = _token_meta(r)
            except Exception:
                meta = {"symbol": None, "name": None, "decimals": None}
            try:
                rd = _aave_reserve_data(r)
            except Exception as e:
                rd = {"error": str(e)[:200]}
            markets.append({"asset": _checksum(r), "symbol": meta.get("symbol"),
                            "name": meta.get("name"), "decimals": meta.get("decimals"), **rd})
        return {"network": "base", "protocol": "aave-v3", "pool": AAVE_POOL,
                "count": len(markets), "markets": markets}
    except Exception as e:
        return {"error": str(e)[:200]}


async def lending_aave_market_asset(asset: str):
    """Aave V3 market for one asset: on-chain rates + aToken/debt tokens."""
    try:
        addr = _checksum(asset)
    except Exception:
        return {"error": "invalid address", "asset": asset}
    try:
        meta = _token_meta(addr)
    except Exception:
        meta = {"symbol": None, "name": None, "decimals": None}
    try:
        rd = _aave_reserve_data(addr)
    except Exception as e:
        return {"error": str(e)[:200], "asset": addr}
    return {"network": "base", "protocol": "aave-v3", "asset": addr,
            "symbol": meta.get("symbol"), "name": meta.get("name"),
            "decimals": meta.get("decimals"), **rd}


async def lending_aave_user(wallet: str):
    """Aave V3 account data for one wallet: collateral/debt in ETH."""
    try:
        addr = _checksum(wallet)
    except Exception:
        return {"error": "invalid address", "wallet": wallet}
    try:
        aave = w3.eth.contract(address=_checksum(AAVE_POOL), abi=_AAVE_RESERVE_ABI)
        d = aave.functions.getUserAccountData(addr).call()
        return {"network": "base", "protocol": "aave-v3", "wallet": addr,
                "total_collateral_eth": d[0] / 1e18, "total_debt_eth": d[1] / 1e18,
                "available_borrows_eth": d[2] / 1e18,
                "current_liquidation_threshold_bps": d[3],
                "ltv_bps": d[4], "health_factor": d[5] / 1e18 if d[5] < 10 ** 30 else None,
                "health_factor_raw": d[5]}
    except Exception as e:
        return {"error": str(e)[:200], "wallet": addr}


async def lending_aave_tvl():
    """Aave V3 TVL on Base (DeFiLlama /protocols)."""
    data = await _get_json(f"{LLAMA}/protocols")
    if isinstance(data, dict) and "error" in data:
        return data
    for p in data:
        if p.get("slug") == "aave-v3":
            chain_tvls = p.get("chainTvls") or {}
            return {"protocol": "aave-v3", "name": p.get("name"),
                    "chain": "Base", "tvl_usd": chain_tvls.get("Base"),
                    "total_tvl_usd": p.get("tvl"), "url": p.get("url"),
                    "category": p.get("category")}
    return {"error": "aave-v3 not found in DeFiLlama protocols"}


async def lending_compound_markets():
    """Compound V3 yield pools on Base, top 10 by TVL."""
    pools = await _yields_by_project("compound-v3", "Base")
    if isinstance(pools, dict):
        return pools
    pools = sorted(pools, key=lambda p: p.get("tvlUsd") or 0, reverse=True)[:10]
    return {"protocol": "compound-v3", "chain": "Base", "count": len(pools),
            "markets": [_trim_pool(p) for p in pools]}


async def lending_compound_usdc():
    """Compound V3 Comet USDC market on Base: utilization, totals, APY."""
    try:
        comet = w3.eth.contract(address=_checksum(COMET_USDC), abi=_COMET_ABI)
        util = comet.functions.getUtilization().call()
        total_supply = comet.functions.totalSupply().call()
        total_borrow = comet.functions.totalBorrow().call()
        base_token = comet.functions.baseToken().call()
        apy = None
        pools = await _yields_by_project("compound-v3", "Base")
        if isinstance(pools, list):
            usdc = [p for p in pools if "USDC" in str(p.get("symbol", "")).upper()]
            if usdc:
                usdc = sorted(usdc, key=lambda p: p.get("tvlUsd") or 0, reverse=True)[0]
                apy = {"apy_pct": usdc.get("apy"), "apy_base_pct": usdc.get("apyBase"),
                       "apy_reward_pct": usdc.get("apyReward"), "pool": usdc.get("pool")}
        return {"network": "base", "protocol": "compound-v3", "market": COMET_USDC,
                "base_token": base_token, "base_token_decimals": 6,
                "utilization_pct": round(util / 1e18 * 100, 2),
                "total_supplied_usdc": total_supply / 1e6,
                "total_borrowed_usdc": total_borrow / 1e6, "apy": apy}
    except Exception as e:
        return {"error": str(e)[:200]}


async def lending_compound_user(wallet: str):
    """Compound V3 Comet USDC position for one wallet: supplied/borrowed."""
    try:
        addr = _checksum(wallet)
    except Exception:
        return {"error": "invalid address", "wallet": wallet}
    try:
        comet = w3.eth.contract(address=_checksum(COMET_USDC), abi=_COMET_ABI)
        supplied = comet.functions.balanceOf(addr).call()
        borrowed = comet.functions.borrowBalanceOf(addr).call()
        return {"network": "base", "protocol": "compound-v3", "market": COMET_USDC,
                "wallet": addr, "supplied_usdc": supplied / 1e6,
                "borrowed_usdc": borrowed / 1e6}
    except Exception as e:
        return {"error": str(e)[:200], "wallet": addr}


async def lending_morpho_pools():
    """Morpho Blue yield pools on Base, top 10 by TVL."""
    pools = await _yields_by_project("morpho-blue", "Base")
    if isinstance(pools, dict):
        return pools
    pools = sorted(pools, key=lambda p: p.get("tvlUsd") or 0, reverse=True)[:10]
    return {"protocol": "morpho-blue", "chain": "Base", "count": len(pools),
            "markets": [_trim_pool(p) for p in pools]}


async def lending_morpho_stable():
    """Highest-APY Morpho Blue stablecoin pools on Base."""
    pools = await _yields_by_project("morpho-blue", "Base")
    if isinstance(pools, dict):
        return pools
    pools = [p for p in pools if p.get("stablecoin")]
    pools = sorted(pools, key=lambda p: p.get("apy") or 0, reverse=True)[:10]
    return {"protocol": "morpho-blue", "chain": "Base", "count": len(pools),
            "markets": [_trim_pool(p) for p in pools]}


async def lending_rates(asset: str):
    """Lending rates for one asset: Aave on-chain + yield pools (Aave/Morpho/Compound)."""
    try:
        meta = _token_meta(asset)
    except Exception:
        return {"error": "invalid address or token", "asset": asset}
    addr = _checksum(asset)
    aave = None
    try:
        aave = _aave_reserve_data(addr)
    except Exception as e:
        aave = {"error": str(e)[:200]}
    pools = await _yields_pools()
    if isinstance(pools, dict):
        return pools
    sym = str(meta.get("symbol") or "").upper()
    mine = [p for p in pools if p.get("chain") == "Base"
            and str(p.get("project", "")).lower() in ("aave-v3", "morpho-blue", "compound-v3")
            and sym and sym in str(p.get("symbol", "")).upper()]
    mine = sorted(mine, key=lambda p: p.get("apy") or 0, reverse=True)[:5]
    return {"asset": addr, "symbol": meta.get("symbol"), "name": meta.get("name"),
            "aave_on_chain": aave,
            "yield_pools": [_trim_pool(p) for p in mine]}


async def lending_top_apy():
    """Top 10 lending APY pools on Base (all lending projects)."""
    pools = await _yields_pools()
    if isinstance(pools, dict):
        return pools
    lending = {"aave-v3", "morpho-blue", "compound-v3", "aave-v2", "spark",
               "moonwell", "seamless-protocol", "fluid-lending", "maple"}
    mine = [p for p in pools if p.get("chain") == "Base"
            and str(p.get("project", "")).lower() in lending]
    mine = sorted(mine, key=lambda p: p.get("apy") or 0, reverse=True)[:10]
    return {"chain": "Base", "count": len(mine),
            "pools": [_trim_pool(p) for p in mine]}


async def lending_stablecoin_rates():
    """Aave V3 supply/borrow APY for USDC, DAI and USDbC on Base."""
    out = []
    for sym in ("USDC", "DAI", "USDBC"):
        entry = _curated(sym)
        if not entry:
            continue
        addr = entry["address"]
        try:
            rd = _aave_reserve_data(addr)
        except Exception as e:
            rd = {"error": str(e)[:200]}
        out.append({"symbol": sym, "asset": addr, **rd})
    return {"network": "base", "protocol": "aave-v3", "markets": out}


async def lending_usdc_rate():
    """Aave V3 USDC market rates on Base."""
    addr = USDC
    try:
        rd = _aave_reserve_data(addr)
    except Exception as e:
        return {"error": str(e)[:200]}
    return {"network": "base", "protocol": "aave-v3", "asset": addr,
            "symbol": "USDC", **rd}


async def lending_weth_rate():
    """Aave V3 WETH market rates on Base."""
    addr = WETH
    try:
        rd = _aave_reserve_data(addr)
    except Exception as e:
        return {"error": str(e)[:200]}
    return {"network": "base", "protocol": "aave-v3", "asset": addr,
            "symbol": "WETH", **rd}

# ---------- v5 STABLECOINS (6) ----------

async def stablecoins_top():
    """Top 10 stablecoins by circulating supply (DeFiLlama)."""
    assets = await _stablecoin_list()
    if isinstance(assets, dict):
        return assets
    assets = sorted(assets, key=lambda a: (a.get("circulating") or {}).get("peggedUSD") or 0, reverse=True)[:10]
    return {"count": len(assets), "stablecoins": [_trim_stable(a) for a in assets]}


async def stablecoins_asset(asset: str):
    """One stablecoin by id, symbol, or gecko_id, with per-chain supply."""
    assets = await _stablecoin_list()
    if isinstance(assets, dict):
        return assets
    a = _stablecoin_find(assets, asset)
    if not a:
        return {"error": f"stablecoin not found: {asset}"}
    chains = {}
    cc = a.get("chainCirculating") or {}
    for ch, v in cc.items():
        cur = (v or {}).get("current") or {}
        chains[ch] = cur.get("peggedUSD")
    return {"id": a.get("id"), "name": a.get("name"), "symbol": a.get("symbol"),
            "gecko_id": a.get("gecko_id"), "peg_type": a.get("pegType"),
            "peg_mechanism": a.get("pegMechanism"), "price": a.get("price"),
            "circulating_usd": (a.get("circulating") or {}).get("peggedUSD"),
            "chains": chains, "price_source": a.get("priceSource")}


async def stablecoins_chains():
    """Stablecoin supply by chain, top 15."""
    data = await _get_json(f"{STABLECOINS}/stablecoinchains")
    if isinstance(data, dict) and "error" in data:
        return data
    rows = data if isinstance(data, list) else []
    def _amt(r):
        for k in ("totalCirculatingUSD", "totalCirculating", "circulating"):
            v = r.get(k)
            if isinstance(v, (int, float)):
                return v
        return 0
    rows = sorted(rows, key=_amt, reverse=True)[:15]
    return {"count": len(rows), "chains": [
        {"chain": r.get("name"), "total_circulating_usd": _amt(r),
         "stablecoin_count": r.get("stablecoinCount")} for r in rows]}


async def stablecoins_pegs():
    """Stablecoins furthest from their $1 peg right now, top 15."""
    data = await _get_json(f"{STABLECOINS}/stablecoinprices")
    if isinstance(data, dict) and "error" in data:
        return data
    # Shape: [{"date": 0, "prices": {stablecoin_id: price, ...}}] — keyed
    # by stablecoin id, not by contract address.
    prices = {}
    for r in (data if isinstance(data, list) else []):
        if isinstance(r, dict) and isinstance(r.get("prices"), dict):
            prices = r["prices"]
            break
    out = []
    for k, v in prices.items():
        if isinstance(v, (int, float)):
            out.append({"id": k, "price": v,
                        "deviation_pct": round((v - 1) * 100, 3)})
    out = sorted(out, key=lambda x: abs(x["deviation_pct"]), reverse=True)[:15]
    return {"count": len(out), "pegs": out}


async def stablecoins_chain(chain: str):
    """Top 15 stablecoins by circulating supply on one chain."""
    assets = await _stablecoin_list()
    if isinstance(assets, dict):
        return assets
    out = []
    for a in assets:
        cc = a.get("chainCirculating") or {}
        for ch, v in cc.items():
            if ch.lower() == chain.lower():
                cur = (v or {}).get("current") or {}
                out.append({"symbol": a.get("symbol"), "id": a.get("id"),
                            "name": a.get("name"),
                            "circulating_usd": cur.get("peggedUSD")})
    out = sorted(out, key=lambda x: x.get("circulating_usd") or 0, reverse=True)[:15]
    return {"chain": chain, "count": len(out), "stablecoins": out}


async def stablecoins_total():
    """Total circulating supply of all stablecoins (DeFiLlama)."""
    assets = await _stablecoin_list()
    if isinstance(assets, dict):
        return assets
    total = sum((a.get("circulating") or {}).get("peggedUSD") or 0 for a in assets)
    return {"total_circulating_usd": round(total, 2), "stablecoin_count": len(assets)}


# ---------- v5 ORACLES (6) ----------

async def oracles_feeds():
    """Chainlink price feeds on Base: ETH/USD, BTC/USD, USDC/USD."""
    feeds = []
    for key, f in CHAINLINK_FEEDS.items():
        r = _chainlink_read(f["address"])
        r["key"] = key
        feeds.append(r)
    return {"network": "base", "source": "chainlink", "feeds": feeds}


async def oracles_price(symbol: str):
    """Chainlink price for one symbol (eth, btc, usdc)."""
    f = CHAINLINK_FEEDS.get(symbol.lower())
    if not f:
        return {"error": f"no Chainlink feed for symbol: {symbol}",
                "available": list(CHAINLINK_FEEDS.keys())}
    return _chainlink_read(f["address"])


async def oracles_eth():
    """Chainlink ETH/USD on Base."""
    return _chainlink_read(CHAINLINK_FEEDS["eth"]["address"])


async def oracles_btc():
    """Chainlink BTC/USD on Base."""
    return _chainlink_read(CHAINLINK_FEEDS["btc"]["address"])


async def oracles_usdc():
    """Chainlink USDC/USD on Base."""
    return _chainlink_read(CHAINLINK_FEEDS["usdc"]["address"])


async def oracles_vs_coingecko():
    """Chainlink ETH/USD vs CoinGecko ETH price (oracle deviation check)."""
    chainlink = _chainlink_read(CHAINLINK_FEEDS["eth"]["address"])
    cg = await _coin_price("ethereum")
    if isinstance(cg, dict) and "error" in cg:
        return {"chainlink": chainlink, "coingecko": cg}
    cg_price = (cg or {}).get("usd")
    out = {"chainlink": chainlink, "coingecko_usd": cg_price}
    if "price" in chainlink and isinstance(cg_price, (int, float)) and cg_price:
        out["deviation_pct"] = round((chainlink["price"] - cg_price) / cg_price * 100, 3)
    return out


# ---------- v5 GOVERNANCE (5) ----------

_GOV_PROPOSALS_Q = """
query { proposals(first: 10, orderBy: "created", orderDirection: desc) {
  id title choices scores scores_total state start end
  space { id name } author } }
"""

_GOV_SPACE_PROPOSALS_Q = """
query($space: String!) { proposals(first: 10, where: {space: $space},
  orderBy: "created", orderDirection: desc) {
  id title choices scores scores_total state start end
  space { id name } author } }
"""

_GOV_PROPOSAL_Q = """
query($id: String!) { proposal(id: $id) {
  id title body choices scores scores_total scores_state state start end
  space { id name } author } }
"""

_GOV_VOTES_Q = """
query($id: String!) { votes(first: 20, where: {proposal: $id},
  orderBy: "vp", orderDirection: desc) { voter choice vp created } }
"""

_GOV_SPACE_Q = """
query($id: String!) { space(id: $id) {
  id name about proposalsCount followersCount } }
"""


async def gov_proposals():
    """Latest 10 Snapshot proposals across all DAOs."""
    data = await _snapshot(_GOV_PROPOSALS_Q)
    if isinstance(data, dict) and "error" in data:
        return data
    return {"count": len(data.get("proposals") or []),
            "proposals": data.get("proposals") or []}


async def gov_space_proposals(space: str):
    """Latest 10 Snapshot proposals for one space (e.g. 'aave.eth')."""
    data = await _snapshot(_GOV_SPACE_PROPOSALS_Q, {"space": space})
    if isinstance(data, dict) and "error" in data:
        return data
    return {"space": space, "count": len(data.get("proposals") or []),
            "proposals": data.get("proposals") or []}


async def gov_proposal(proposal_id: str):
    """One Snapshot proposal by id, with scores."""
    data = await _snapshot(_GOV_PROPOSAL_Q, {"id": proposal_id})
    if isinstance(data, dict) and "error" in data:
        return data
    p = data.get("proposal")
    if not p:
        return {"error": f"proposal not found: {proposal_id}"}
    return p


async def gov_votes(proposal_id: str):
    """Top 20 votes on one Snapshot proposal, by voting power."""
    data = await _snapshot(_GOV_VOTES_Q, {"id": proposal_id})
    if isinstance(data, dict) and "error" in data:
        return data
    return {"proposal": proposal_id, "count": len(data.get("votes") or []),
            "votes": data.get("votes") or []}


async def gov_space(space: str):
    """One Snapshot space: name, about, proposal/follower counts."""
    data = await _snapshot(_GOV_SPACE_Q, {"id": space})
    if isinstance(data, dict) and "error" in data:
        return data
    s = data.get("space")
    if not s:
        return {"error": f"space not found: {space}"}
    return s

# ---------- v5 GAS (5) ----------

async def chain_gas_base_fee():
    """Current base fee + predicted next-block base fee (EIP-1559 formula)."""
    blk = w3.eth.get_block("latest")
    base = blk["baseFeePerGas"]
    gas_used = blk["gasUsed"]
    gas_limit = blk["gasLimit"]
    target = gas_limit // 2
    if gas_used > target:
        nxt = base * (1 + 0.125 * (gas_used - target) / target)
    else:
        nxt = base * (1 - 0.125 * (target - gas_used) / target)
    return {"network": "base", "block": blk["number"],
            "base_fee_gwei": base / 1e9,
            "predicted_next_base_fee_gwei": round(nxt / 1e9, 4),
            "gas_used": gas_used, "gas_limit": gas_limit,
            "gas_used_pct": round(gas_used / gas_limit * 100, 2)}


async def chain_gas_fee_history(blocks: int):
    """Fee history for the last N blocks (1-20): base fee + priority percentiles."""
    if blocks < 1 or blocks > 20:
        return {"error": "blocks must be 1-20", "blocks": blocks}
    fh = w3.eth.fee_history(blocks, "latest", [50])
    return {"network": "base", "blocks": blocks,
            "oldest_block": fh["oldestBlock"],
            "base_fee_per_gas_gwei": [round(b / 1e9, 4) for b in fh["baseFeePerGas"]],
            "gas_used_ratio": [round(r, 3) for r in fh["gasUsedRatio"]],
            "priority_fee_p50_gwei": [[round(v / 1e9, 4) for v in r] for r in fh["reward"]]}


async def chain_gas_transfer_cost():
    """Cost of a plain 21,000-gas ETH transfer on Base right now."""
    gp = w3.eth.gas_price
    cost_wei = gp * 21000
    return {"network": "base", "gas_price_gwei": gp / 1e9,
            "gas_limit": 21000, "cost_wei": cost_wei,
            "cost_eth": cost_wei / 1e18}


async def chain_gas_transfer_cost_usd():
    """Cost of a plain ETH transfer on Base in USD."""
    gp = w3.eth.gas_price
    cost_eth = gp * 21000 / 1e18
    eth = await _coin_price("ethereum")
    if isinstance(eth, dict) and "error" in eth:
        return {"network": "base", "gas_price_gwei": gp / 1e9, "cost_eth": cost_eth,
                "cost_usd": None, "error": eth["error"]}
    return {"network": "base", "gas_price_gwei": gp / 1e9,
            "cost_eth": cost_eth, "eth_usd": eth.get("usd"),
            "cost_usd": round(cost_eth * eth["usd"], 6)}


async def chain_gas_max_fee():
    """Current base fee + max priority fee (fast-lane fee estimate)."""
    try:
        mp = w3.eth.max_priority_fee
    except Exception:
        mp = None
    base = w3.eth.get_block("latest")["baseFeePerGas"]
    return {"network": "base", "base_fee_gwei": base / 1e9,
            "max_priority_fee_gwei": (mp / 1e9) if mp is not None else None,
            "max_fee_gwei": ((base + mp) / 1e9) if mp is not None else None}


# ---------- v5 NFTS (6) ----------

async def nft_token_uri(collection: str, token_id: str):
    """Raw tokenURI for one NFT token."""
    try:
        c = _checksum(collection)
    except Exception:
        return {"error": "invalid collection address", "collection": collection}
    try:
        tid = int(token_id)
    except ValueError:
        return {"error": "token_id must be an integer", "collection": c}
    try:
        raw = _eth_call(c, "0x" + SEL_TOKEN_URI + int(tid).to_bytes(32, "big").hex())
        uri = _decode_string(raw)
        return {"collection": c, "token_id": tid, "token_uri": uri, "network": "base"}
    except Exception as e:
        return {"error": str(e)[:200], "collection": c, "token_id": tid}


async def nft_token_image(collection: str, token_id: str):
    """Image URL from one NFT token's metadata."""
    try:
        c = _checksum(collection)
    except Exception:
        return {"error": "invalid collection address", "collection": collection}
    try:
        tid = int(token_id)
    except ValueError:
        return {"error": "token_id must be an integer", "collection": c}
    try:
        raw = _eth_call(c, "0x" + SEL_TOKEN_URI + int(tid).to_bytes(32, "big").hex())
        uri = _decode_string(raw)
    except Exception as e:
        return {"error": str(e)[:200], "collection": c, "token_id": tid}
    meta = _decode_data_uri(uri)
    if meta is None:
        meta = await _fetch_nft_metadata(uri) if uri else None
    if not meta:
        return {"collection": c, "token_id": tid, "token_uri": uri,
                "image": None, "error": "could not fetch metadata"}
    return {"collection": c, "token_id": tid, "token_uri": uri,
            "name": meta.get("name"),
            "image": _ipfs_to_http(meta.get("image")) if meta.get("image") else None,
            "network": "base"}


async def nft_token_attributes(collection: str, token_id: str):
    """Traits/attributes from one NFT token's metadata."""
    try:
        c = _checksum(collection)
    except Exception:
        return {"error": "invalid collection address", "collection": collection}
    try:
        tid = int(token_id)
    except ValueError:
        return {"error": "token_id must be an integer", "collection": c}
    try:
        raw = _eth_call(c, "0x" + SEL_TOKEN_URI + int(tid).to_bytes(32, "big").hex())
        uri = _decode_string(raw)
    except Exception as e:
        return {"error": str(e)[:200], "collection": c, "token_id": tid}
    meta = _decode_data_uri(uri)
    if meta is None:
        meta = await _fetch_nft_metadata(uri) if uri else None
    if not meta:
        return {"collection": c, "token_id": tid, "token_uri": uri,
                "attributes": None, "error": "could not fetch metadata"}
    return {"collection": c, "token_id": tid, "token_uri": uri,
            "name": meta.get("name"),
            "attributes": meta.get("attributes") or meta.get("traits") or [],
            "network": "base"}


async def nft1155_balance(collection: str, wallet: str, token_id: str):
    """ERC1155 balance of one wallet for one token id."""
    try:
        addr = _checksum(collection)
        holder = _checksum(wallet)
    except Exception:
        return {"error": "invalid address", "collection": collection, "wallet": wallet}
    try:
        tid = int(token_id)
    except ValueError:
        return {"error": "token_id must be an integer", "collection": addr, "wallet": holder}
    try:
        data = "0x" + SEL_BALANCE_1155 + (b"\x00" * 12 + bytes.fromhex(holder[2:])).hex() + int(tid).to_bytes(32, "big").hex()
        raw = _eth_call(addr, data)
        return {"collection": addr, "wallet": holder, "token_id": tid,
                "balance": int.from_bytes(raw, "big"), "standard": "erc1155", "network": "base"}
    except Exception as e:
        return {"error": str(e)[:200], "collection": addr, "wallet": holder,
                "token_id": tid}


async def nft1155_uri(collection: str, token_id: str):
    """ERC1155 metadata URI for one token id."""
    try:
        addr = _checksum(collection)
    except Exception:
        return {"error": "invalid collection address", "collection": collection}
    try:
        tid = int(token_id)
    except ValueError:
        return {"error": "token_id must be an integer", "collection": addr}
    try:
        data = "0x" + SEL_URI_1155 + int(tid).to_bytes(32, "big").hex()
        raw = _eth_call(addr, data)
        uri = _decode_string(raw)
        return {"collection": addr, "token_id": tid, "uri": uri,
                "standard": "erc1155", "network": "base"}
    except Exception as e:
        return {"error": str(e)[:200], "collection": addr, "token_id": tid}


async def nft1155_supply(collection: str, token_id: str):
    """ERC1155 total supply for one token id (if the contract tracks it)."""
    try:
        addr = _checksum(collection)
    except Exception:
        return {"error": "invalid collection address", "collection": collection}
    try:
        tid = int(token_id)
    except ValueError:
        return {"error": "token_id must be an integer", "collection": addr}
    try:
        data = "0x" + SEL_TOTALSUPPLY_1155 + int(tid).to_bytes(32, "big").hex()
        raw = _eth_call(addr, data)
        return {"collection": addr, "token_id": tid,
                "total_supply": int.from_bytes(raw, "big"), "standard": "erc1155",
                "network": "base"}
    except Exception:
        # Many ERC1155s (incl. Zora1155) do not implement totalSupply(uint256).
        return {"collection": addr, "token_id": tid, "total_supply": None,
                "enumerable": False,
                "note": "contract does not implement ERC1155Supply totalSupply(uint256)"}


# ---------- v5 DEFI (12) ----------

async def defi_protocol(slug: str):
    """One protocol's TVL detail by slug (DeFiLlama /protocols)."""
    data = await _get_json(f"{LLAMA}/protocols")
    if isinstance(data, dict) and "error" in data:
        return data
    for p in data:
        if str(p.get("slug", "")).lower() == slug.lower():
            chain_tvls = p.get("chainTvls") or {}
            return {"slug": p.get("slug"), "name": p.get("name"),
                    "category": p.get("category"), "tvl_usd": p.get("tvl"),
                    "chains": p.get("chains"), "chain_tvls": chain_tvls,
                    "base_tvl_usd": chain_tvls.get("Base"),
                    "url": p.get("url"), "description": p.get("description")}
    return {"error": f"protocol not found: {slug}"}


async def defi_chains():
    """TVL by chain, top 20."""
    data = await _get_json(f"{LLAMA}/v2/chains")
    if isinstance(data, dict) and "error" in data:
        return data
    rows = sorted(data, key=lambda x: x.get("tvl") or 0, reverse=True)[:20]
    return {"count": len(rows), "chains": [
        {"chain": r.get("name"), "tvl_usd": r.get("tvl"),
         "token_symbol": r.get("tokenSymbol")} for r in rows]}


async def defi_chain(chain: str):
    """TVL for one chain."""
    data = await _get_json(f"{LLAMA}/v2/chains")
    if isinstance(data, dict) and "error" in data:
        return data
    for r in data:
        if str(r.get("name", "")).lower() == chain.lower():
            return {"chain": r.get("name"), "tvl_usd": r.get("tvl"),
                    "token_symbol": r.get("tokenSymbol")}
    return {"error": f"chain not found: {chain}"}


async def defi_chain_history(chain: str):
    """TVL history for one chain: first, latest, change over 30 days."""
    data = await _get_json(f"{LLAMA}/v2/historicalChainTvl/{chain}")
    if isinstance(data, dict) and "error" in data:
        return data
    pts = data if isinstance(data, list) else []
    if not pts:
        return {"error": f"no TVL history for chain: {chain}"}
    last30 = pts[-30:]
    first, last = pts[0], pts[-1]
    return {"chain": chain, "points": len(pts),
            "first": first, "latest": last,
            "change_30d_pct": round((last30[-1]["tvl"] - last30[0]["tvl"]) / last30[0]["tvl"] * 100, 2) if len(last30) > 1 and last30[0].get("tvl") else None,
            "last_30_days": last30}


async def defi_fees_overview():
    """Top 10 protocols by 24h fees (all chains)."""
    data = await _get_json(f"{LLAMA}/overview/fees")
    if isinstance(data, dict) and "error" in data:
        return data
    protos = (data or {}).get("protocols") or []
    protos = sorted(protos, key=lambda p: p.get("total24h") or 0, reverse=True)[:10]
    return {"total_24h_usd": (data or {}).get("total24h"),
            "count": len(protos),
            "protocols": [{"name": p.get("name"), "slug": p.get("slug"),
                           "fees_24h_usd": p.get("total24h"),
                           "fees_7d_usd": p.get("total7d")} for p in protos]}


async def defi_yields_chart(pool: str):
    """TVL/APY history for one yield pool (last 30 points)."""
    data = await _get_json(f"{YIELDS}/chart/{pool}")
    if isinstance(data, dict) and "error" in data:
        return data
    pts = (data or {}).get("data") or []
    if not pts:
        return {"error": f"no chart data for pool: {pool}", "pool": pool}
    return {"pool": pool, "points": len(pts),
            "latest": pts[-1], "last_30": pts[-30:]}


async def defi_yields_il(pool: str):
    """Impermanent-loss risk + APY for one yield pool."""
    pools = await _yields_pools()
    if isinstance(pools, dict):
        return pools
    for p in pools:
        if p.get("pool") == pool:
            return {"pool": pool, "symbol": p.get("symbol"),
                    "project": p.get("project"), "chain": p.get("chain"),
                    "apy_pct": p.get("apy"), "apy_base_pct": p.get("apyBase"),
                    "apy_reward_pct": p.get("apyReward"),
                    "tvl_usd": p.get("tvlUsd"),
                    "il_7d_pct": p.get("il7d"), "il_risk": p.get("ilRisk"),
                    "stablecoin": p.get("stablecoin")}
    return {"error": f"pool not found: {pool}"}


async def defi_yields_project(project: str):
    """Top 10 yield pools for one project, by APY."""
    pools = await _yields_pools()
    if isinstance(pools, dict):
        return pools
    mine = [p for p in pools if str(p.get("project", "")).lower() == project.lower()]
    mine = sorted(mine, key=lambda p: p.get("apy") or 0, reverse=True)[:10]
    return {"project": project, "count": len(mine),
            "pools": [_trim_pool(p) for p in mine]}


async def defi_yields_token(symbol: str):
    """Top 10 yield pools for one token symbol, by APY (all chains)."""
    pools = await _yields_pools()
    if isinstance(pools, dict):
        return pools
    key = symbol.upper()
    mine = [p for p in pools if key in str(p.get("symbol", "")).upper()]
    mine = sorted(mine, key=lambda p: p.get("apy") or 0, reverse=True)[:10]
    return {"symbol": symbol, "count": len(mine),
            "pools": [_trim_pool(p) for p in mine]}


async def defi_yields_stable(chain: str):
    """Top 10 stablecoin yield pools on one chain, by APY."""
    pools = await _yields_pools()
    if isinstance(pools, dict):
        return pools
    mine = [p for p in pools if p.get("stablecoin")
            and str(p.get("chain", "")).lower() == chain.lower()]
    mine = sorted(mine, key=lambda p: p.get("apy") or 0, reverse=True)[:10]
    return {"chain": chain, "count": len(mine),
            "pools": [_trim_pool(p) for p in mine]}


async def defi_dex_volume_7d():
    """Total DEX volume, last 7 days (all chains)."""
    data = await _get_json(f"{LLAMA}/overview/dexs")
    if isinstance(data, dict) and "error" in data:
        return data
    protos = (data or {}).get("protocols") or []
    return {"total_7d_usd": (data or {}).get("total7d"),
            "total_24h_usd": (data or {}).get("total24h"),
            "dex_count": len(protos)}


async def defi_dex_volume_base_7d():
    """Total DEX volume on Base, last 7 days."""
    data = await _get_json(f"{LLAMA}/overview/dexs/Base")
    if isinstance(data, dict) and "error" in data:
        return data
    protos = (data or {}).get("protocols") or []
    return {"chain": "Base", "total_7d_usd": (data or {}).get("total7d"),
            "total_24h_usd": (data or {}).get("total24h"),
            "dex_count": len(protos)}

# ---------- v5 DEX (8) ----------

async def dex_top_volume(n: int):
    """Top N DEXs by 24h volume (1-25, all chains)."""
    if n < 1 or n > 25:
        return {"error": "n must be 1-25", "n": n}
    data = await _get_json(f"{LLAMA}/overview/dexs")
    if isinstance(data, dict) and "error" in data:
        return data
    protos = (data or {}).get("protocols") or []
    protos = sorted(protos, key=lambda p: p.get("total24h") or 0, reverse=True)[:n]
    return {"count": len(protos), "dexs": [
        {"name": p.get("name"), "slug": p.get("slug"),
         "volume_24h_usd": p.get("total24h"), "volume_7d_usd": p.get("total7d"),
         "chains": p.get("chains")} for p in protos]}


async def dex_volume_chain(chain: str):
    """Total DEX volume for one chain (24h + 7d)."""
    data = await _get_json(f"{LLAMA}/overview/dexs/{chain}")
    if isinstance(data, dict) and "error" in data:
        return data
    protos = (data or {}).get("protocols") or []
    return {"chain": chain, "total_24h_usd": (data or {}).get("total24h"),
            "total_7d_usd": (data or {}).get("total7d"),
            "dex_count": len(protos)}


async def dex_chains():
    """DEX volume by chain for the major chains."""
    chains = ["Base", "Ethereum", "Arbitrum", "Solana", "BSC", "Polygon",
              "Optimism", "Avalanche"]
    out = []
    for ch in chains:
        data = await _get_json(f"{LLAMA}/overview/dexs/{ch}")
        if isinstance(data, dict) and "error" in data:
            out.append({"chain": ch, "error": data["error"]})
        else:
            out.append({"chain": ch, "total_24h_usd": (data or {}).get("total24h"),
                        "total_7d_usd": (data or {}).get("total7d")})
    out = sorted(out, key=lambda x: x.get("total_24h_usd") or 0, reverse=True)
    return {"count": len(out), "chains": out}


async def dex_top_fees():
    """Top 10 DEXs by 24h fees."""
    data = await _get_json(f"{LLAMA}/overview/fees")
    if isinstance(data, dict) and "error" in data:
        return data
    protos = (data or {}).get("protocols") or []
    dexs = [p for p in protos if str(p.get("category", "")).lower() in ("dexes", "dex")]
    if not dexs:
        dexs = protos
    dexs = sorted(dexs, key=lambda p: p.get("total24h") or 0, reverse=True)[:10]
    return {"count": len(dexs), "dexs": [
        {"name": p.get("name"), "slug": p.get("slug"),
         "fees_24h_usd": p.get("total24h"), "fees_7d_usd": p.get("total7d")}
        for p in dexs]}


async def dex_fees_chains(protocol: str):
    """Fee breakdown by chain for one protocol (DeFiLlama fees summary)."""
    data = await _get_json(f"{LLAMA}/summary/fees/{protocol}")
    if isinstance(data, dict) and "error" in data:
        return data
    return {"protocol": protocol,
            "total_24h_usd": (data or {}).get("total24h"),
            "total_7d_usd": (data or {}).get("total7d"),
            "total_all_time_usd": (data or {}).get("totalAllTime"),
            "chains": (data or {}).get("chains"),
            "breakdown_24h": (data or {}).get("breakdown24h")}


async def dex_pair_txns(chain: str, pair: str):
    """Transactions + volume for one DEX pair."""
    data = await _get_json(f"{DEXSCREENER}/latest/dex/pairs/{chain}/{pair}")
    if isinstance(data, dict) and "error" in data:
        return data
    pairs = (data or {}).get("pairs") or []
    if not pairs:
        return {"error": f"pair not found: {chain}/{pair}"}
    p = pairs[0]
    return {"chain": chain, "pair": pair, "dex": p.get("dexId"),
            "base_token": p.get("baseToken"), "quote_token": p.get("quoteToken"),
            "price_usd": p.get("priceUsd"), "liquidity_usd": (p.get("liquidity") or {}).get("usd"),
            "volume_24h_usd": (p.get("volume") or {}).get("h24"),
            "txns": p.get("txns"), "url": p.get("url")}


async def dex_pairs_token(token: str):
    """Top 10 DEX pairs for one token, by liquidity."""
    try:
        addr = _checksum(token)
    except Exception:
        return {"error": "invalid address", "token": token}
    pairs = await _dex_pairs_for_token(addr)
    if isinstance(pairs, dict):
        return pairs
    pairs = sorted(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0, reverse=True)[:10]
    return {"token": token, "count": len(pairs),
            "pairs": [_trim_pair(p) for p in pairs]}


async def dex_pair_chain(token: str, chain: str):
    """Best DEX pair for one token on one chain, by liquidity."""
    try:
        addr = _checksum(token)
    except Exception:
        return {"error": "invalid address", "token": token}
    pairs = await _dex_pairs_for_token(addr)
    if isinstance(pairs, dict):
        return pairs
    mine = [p for p in pairs if str(p.get("chainId", "")).lower() == chain.lower()]
    if not mine:
        return {"error": f"no pair for {token} on chain {chain}"}
    mine = sorted(mine, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0, reverse=True)
    return {"token": token, "chain": chain, "pair": _trim_pair(mine[0])}


# ---------- v5 WALLETS (7) ----------

async def wallet_approvals(address: str):
    """Token allowances a wallet has granted to Permit2, routers, Aave."""
    try:
        holder = _checksum(address)
    except Exception:
        return {"error": "invalid address", "address": address}
    out = []
    for sym in ("USDC", "WETH"):
        entry = _curated(sym)
        for sp in SPENDERS:
            try:
                al = _allowance(entry["address"], holder, sp["address"])
            except Exception as e:
                al = {"error": str(e)[:200]}
            out.append({"token": sym, "spender": sp["name"],
                        "spender_address": sp["address"], **al})
    return {"wallet": holder, "approvals": out}


async def wallet_approvals_token(address: str, token: str):
    """Allowances one wallet granted for one token (by address or symbol)."""
    try:
        holder = _checksum(address)
    except Exception:
        return {"error": "invalid address", "address": address}
    try:
        taddr = _checksum(token)
    except Exception:
        t = _curated(token)
        if not t:
            return {"error": f"token not found: {token}"}
        taddr = t["address"]
    out = []
    for sp in SPENDERS:
        try:
            al = _allowance(taddr, holder, sp["address"])
        except Exception as e:
            al = {"error": str(e)[:200]}
        out.append({"spender": sp["name"], "spender_address": sp["address"], **al})
    return {"wallet": holder, "token": taddr, "approvals": out}


async def wallet_token_value(address: str, token: str):
    """One wallet's balance of one token + USD value."""
    try:
        holder = _checksum(address)
    except Exception:
        return {"error": "invalid address", "address": address}
    try:
        taddr = _checksum(token)
    except Exception:
        t = _curated(token)
        if not t:
            return {"error": f"token not found: {token}"}
        taddr = t["address"]
    try:
        meta = _token_meta(taddr)
    except Exception as e:
        return {"error": str(e)[:200], "token": taddr}
    raw = _erc20_balance(taddr, holder)
    bal = raw / (10 ** meta["decimals"])
    price = None
    pairs = await _dex_pairs_for_token(taddr)
    if isinstance(pairs, list) and pairs:
        best = _best_pair(pairs)
        if best:
            try:
                price = float(best.get("priceUsd"))
            except Exception:
                price = None
    out = {"wallet": holder, "token": taddr, "symbol": meta.get("symbol"),
           "name": meta.get("name"), "decimals": meta["decimals"],
           "balance": bal, "price_usd": price}
    if price is not None:
        out["value_usd"] = round(bal * price, 2)
    return out


async def wallet_cbeth_balance(address: str):
    """Wallet cbETH balance."""
    try:
        holder = _checksum(address)
    except Exception:
        return {"error": "invalid address", "address": address}
    meta = _curated("cbETH")
    raw = _erc20_balance(meta["address"], holder)
    return {"wallet": holder, "token": meta["address"], "symbol": "cbETH",
            "decimals": meta["decimals"], "balance": raw / 10 ** meta["decimals"]}


async def wallet_dai_balance(address: str):
    """Wallet DAI balance."""
    try:
        holder = _checksum(address)
    except Exception:
        return {"error": "invalid address", "address": address}
    meta = _curated("DAI")
    raw = _erc20_balance(meta["address"], holder)
    return {"wallet": holder, "token": meta["address"], "symbol": "DAI",
            "decimals": meta["decimals"], "balance": raw / 10 ** meta["decimals"]}


async def wallet_usdbc_balance(address: str):
    """Wallet USDbC (bridged USDC) balance."""
    try:
        holder = _checksum(address)
    except Exception:
        return {"error": "invalid address", "address": address}
    meta = _curated("USDbC")
    raw = _erc20_balance(meta["address"], holder)
    return {"wallet": holder, "token": meta["address"], "symbol": "USDbC",
            "decimals": meta["decimals"], "balance": raw / 10 ** meta["decimals"]}


async def wallet_higher_balance(address: str):
    """Wallet HIGHER balance."""
    try:
        holder = _checksum(address)
    except Exception:
        return {"error": "invalid address", "address": address}
    meta = _curated("HIGHER")
    raw = _erc20_balance(meta["address"], holder)
    return {"wallet": holder, "token": meta["address"], "symbol": "HIGHER",
            "decimals": meta["decimals"], "balance": raw / 10 ** meta["decimals"]}


# ---------- v5 MEMECOINS (5) ----------

async def memes_new_pairs():
    """Newest Base pairs on Dexscreener (latest token profiles)."""
    data = await _get_json(f"{DEXSCREENER}/token-profiles/latest/v1")
    if isinstance(data, dict) and "error" in data:
        return data
    rows = [r for r in (data if isinstance(data, list) else [])
            if str(r.get("chainId", "")).lower() == "base"]
    return {"chain": "base", "count": len(rows[:20]),
            "pairs": [{"token_address": r.get("tokenAddress"),
                       "chain_id": r.get("chainId"),
                       "url": r.get("url"), "description": r.get("description")}
                      for r in rows[:20]]}


async def memes_trending():
    """Top 15 Base pairs by 24h volume across the main Base tokens."""
    seen = {}
    addrs = []
    for sym in ("USDC", "WETH", "CBETH", "HIGHER"):
        t = _curated(sym)
        if t:
            addrs.append(t["address"])
    addrs += [m["address"] for m in MEME_TOKENS]
    for addr in addrs:
        data = await _get_json(f"{DEXSCREENER}/latest/dex/tokens/{addr}")
        if isinstance(data, dict) and "error" in data:
            continue
        for p in (data or {}).get("pairs") or []:
            if str(p.get("chainId", "")).lower() != "base":
                continue
            seen[p.get("pairAddress")] = p
    pairs = sorted(seen.values(),
                   key=lambda p: (p.get("volume") or {}).get("h24") or 0,
                   reverse=True)[:15]
    return {"chain": "base", "count": len(pairs),
            "pairs": [_trim_pair(p) for p in pairs]}


async def memes_boosted():
    """Latest boosted Base tokens on Dexscreener."""
    data = await _get_json(f"{DEXSCREENER}/token-boosts/latest/v1")
    if isinstance(data, dict) and "error" in data:
        return data
    rows = [r for r in (data if isinstance(data, list) else [])
            if str(r.get("chainId", "")).lower() == "base"]
    return {"chain": "base", "count": len(rows[:20]),
            "boosted": [{"token_address": r.get("tokenAddress"),
                         "total_amount": r.get("totalAmount"),
                         "amount": r.get("amount"),
                         "description": r.get("description"),
                         "url": r.get("url")} for r in rows[:20]]}


async def memes_price_change(token: str):
    """Price changes (5m/1h/6h/24h) for one token's best pair."""
    try:
        addr = _checksum(token)
    except Exception:
        t = _curated(token)
        if not t:
            return {"error": "token not found", "token": token}
        addr = t["address"]
    pairs = await _dex_pairs_for_token(addr)
    if isinstance(pairs, dict):
        return pairs
    if not pairs:
        return {"error": f"no pairs for token: {token}"}
    best = sorted(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0, reverse=True)[0]
    return {"token": token, "pair": _trim_pair(best),
            "price_change": best.get("priceChange")}


async def memes_holders(token: str):
    """Holder count + top holders for one token (GoPlus)."""
    try:
        addr = _checksum(token)
    except Exception:
        t = _curated(token)
        if not t:
            return {"error": f"token not found: {token}"}
        addr = t["address"]
    entry = await _goplus_token(addr)
    if isinstance(entry, dict) and "error" in entry:
        return entry
    holders = entry.get("holders") or []
    return {"token": addr, "holder_count": entry.get("holder_count"),
            "top_holders": holders[:10],
            "is_honeypot": entry.get("is_honeypot"),
            "is_mintable": entry.get("is_mintable")}

# ---------- v5 PREDICTION (6) ----------

async def predict_markets():
    """Top 15 active Polymarket markets by liquidity."""
    data = await _get_json(f"{GAMMA}/markets", params={"limit": 50, "active": "true"})
    if isinstance(data, dict) and "error" in data:
        return data
    markets = data if isinstance(data, list) else []
    def _liq(m):
        try:
            return float(m.get("liquidity") or 0)
        except Exception:
            return 0
    markets = sorted(markets, key=_liq, reverse=True)[:15]
    return {"count": len(markets), "markets": [_trim_market(m) for m in markets]}


async def predict_trending():
    """Top 15 active Polymarket markets by 24h volume."""
    data = await _get_json(f"{GAMMA}/markets", params={"limit": 50, "active": "true"})
    if isinstance(data, dict) and "error" in data:
        return data
    markets = data if isinstance(data, list) else []
    def _vol(m):
        try:
            return float(m.get("volume24hr") or 0)
        except Exception:
            return 0
    markets = sorted(markets, key=_vol, reverse=True)[:15]
    return {"count": len(markets), "markets": [_trim_market(m) for m in markets]}


async def predict_search(query: str):
    """Search active Polymarket markets by keyword in the question."""
    data = await _get_json(f"{GAMMA}/markets", params={"limit": 100, "active": "true"})
    if isinstance(data, dict) and "error" in data:
        return data
    markets = data if isinstance(data, list) else []
    q = query.lower()
    mine = [m for m in markets if q in str(m.get("question", "")).lower()]
    mine = sorted(mine, key=lambda m: float(m.get("liquidity") or 0), reverse=True)[:15]
    return {"query": query, "count": len(mine),
            "markets": [_trim_market(m) for m in mine]}


async def predict_events():
    """Latest 20 active Polymarket events."""
    data = await _get_json(f"{GAMMA}/events", params={"limit": 20, "active": "true"})
    if isinstance(data, dict) and "error" in data:
        return data
    events = data if isinstance(data, list) else []
    return {"count": len(events), "events": [
        {"id": e.get("id"), "slug": e.get("slug"), "title": e.get("title"),
         "volume_24h": e.get("volume24hr"), "volume": e.get("volume"),
         "market_count": len(e.get("markets") or []),
         "url": f"https://polymarket.com/event/{e.get('slug')}" if e.get("slug") else None}
        for e in events]}


async def predict_market(slug: str):
    """One Polymarket market by slug, with outcomes and prices."""
    data = await _get_json(f"{GAMMA}/markets/{slug}")
    if isinstance(data, dict) and "error" in data:
        # Fall back to filtering the list endpoint.
        data2 = await _get_json(f"{GAMMA}/markets", params={"limit": 100, "active": "true"})
        if isinstance(data2, dict) and "error" in data2:
            return data
        for m in (data2 if isinstance(data2, list) else []):
            if str(m.get("slug", "")).lower() == slug.lower():
                return _trim_market(m)
        return {"error": f"market not found: {slug}"}
    if isinstance(data, dict) and data.get("slug"):
        return _trim_market(data)
    return {"error": f"market not found: {slug}"}


async def predict_closing():
    """Active Polymarket markets ending soonest (next 15 by end date)."""
    data = await _get_json(f"{GAMMA}/markets", params={"limit": 100, "active": "true"})
    if isinstance(data, dict) and "error" in data:
        return data
    markets = data if isinstance(data, list) else []
    mine = [m for m in markets if m.get("endDate")]
    mine = sorted(mine, key=lambda m: m.get("endDate") or "")[:15]
    return {"count": len(mine), "markets": [_trim_market(m) for m in mine]}


# ---------- v5 TOKENS (6) ----------

async def token_history(address: str, days: int):
    """Price history for one Base token (1-365 days, CoinGecko)."""
    if days < 1 or days > 365:
        return {"error": "days must be 1-365", "days": days}
    try:
        addr = _checksum(address)
    except Exception:
        return {"error": "invalid address", "address": address}
    data = await _get_json(
        f"{COINGECKO}/coins/base/contract/{addr}/market_chart",
        params={"vs_currency": "usd", "days": days})
    if isinstance(data, dict) and "error" in data:
        return data
    prices = (data or {}).get("prices") or []
    return {"token": addr, "days": days, "points": len(prices),
            "prices": [{"timestamp": p[0], "price_usd": p[1]} for p in prices],
            "first_price_usd": prices[0][1] if prices else None,
            "last_price_usd": prices[-1][1] if prices else None}


async def token_audit(address: str):
    """Security audit flags for one Base token (GoPlus)."""
    try:
        addr = _checksum(address)
    except Exception:
        return {"error": "invalid address", "address": address}
    entry = await _goplus_token(addr)
    if isinstance(entry, dict) and "error" in entry:
        return entry
    return {"token": addr,
            "is_honeypot": entry.get("is_honeypot"),
            "is_mintable": entry.get("is_mintable"),
            "is_proxy": entry.get("is_proxy"),
            "is_open_source": entry.get("is_open_source"),
            "is_in_dex": entry.get("is_in_dex"),
            "is_anti_whale": entry.get("is_anti_whale"),
            "buy_tax": entry.get("buy_tax"), "sell_tax": entry.get("sell_tax"),
            "cannot_buy": entry.get("cannot_buy"),
            "cannot_sell_all": entry.get("cannot_sell_all"),
            "hidden_owner": entry.get("hidden_owner"),
            "owner_address": entry.get("owner_address"),
            "creator_address": entry.get("creator_address"),
            "holder_count": entry.get("holder_count"),
            "trading_cooldown": entry.get("trading_cooldown"),
            "personal_slippage_modifiable": entry.get("personal_slippage_modifiable")}


async def token_holders(address: str):
    """Holder count + top 10 holders for one Base token (GoPlus)."""
    try:
        addr = _checksum(address)
    except Exception:
        return {"error": "invalid address", "address": address}
    entry = await _goplus_token(addr)
    if isinstance(entry, dict) and "error" in entry:
        return entry
    holders = entry.get("holders") or []
    return {"token": addr, "holder_count": entry.get("holder_count"),
            "top_holders": holders[:10]}


async def token_top_holder(address: str):
    """Largest holder of one Base token (GoPlus)."""
    try:
        addr = _checksum(address)
    except Exception:
        return {"error": "invalid address", "address": address}
    entry = await _goplus_token(addr)
    if isinstance(entry, dict) and "error" in entry:
        return entry
    holders = entry.get("holders") or []
    if not holders:
        return {"token": addr, "holder_count": entry.get("holder_count"),
                "top_holder": None}
    return {"token": addr, "holder_count": entry.get("holder_count"),
            "top_holder": holders[0]}


async def token_age(address: str):
    """Age of a token's oldest DEX pair (first listing date)."""
    try:
        addr = _checksum(address)
    except Exception:
        return {"error": "invalid address", "address": address}
    pairs = await _dex_pairs_for_token(addr)
    if isinstance(pairs, dict):
        return pairs
    if not pairs:
        return {"error": f"no pairs for token: {addr}"}
    created = [p.get("pairCreatedAt") for p in pairs if p.get("pairCreatedAt")]
    if not created:
        return {"error": "no pair creation date available", "token": addr}
    first = min(created)
    age_days = (time.time() * 1000 - first) / 86400000
    return {"token": addr, "first_pair_created_at_ms": first,
            "first_pair_created": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(first / 1000)),
            "age_days": round(age_days, 1), "pair_count": len(pairs)}


async def token_liquidity_usd(address: str):
    """Total DEX liquidity (USD) across all pairs for one token."""
    try:
        addr = _checksum(address)
    except Exception:
        return {"error": "invalid address", "address": address}
    pairs = await _dex_pairs_for_token(addr)
    if isinstance(pairs, dict):
        return pairs
    total = sum((p.get("liquidity") or {}).get("usd") or 0 for p in pairs)
    return {"token": addr, "pair_count": len(pairs),
            "total_liquidity_usd": round(total, 2)}


# ---------- v5 CHAIN (5) ----------

async def chain_blocks(count: int):
    """Summaries of the last N blocks (1-10)."""
    if count < 1 or count > 10:
        return {"error": "count must be 1-10", "count": count}
    latest = w3.eth.block_number
    out = []
    for n in range(latest, latest - count, -1):
        b = w3.eth.get_block(n)
        out.append({"number": b["number"],
                    "timestamp": b["timestamp"],
                    "tx_count": len(b["transactions"]),
                    "gas_used": b["gasUsed"], "gas_limit": b["gasLimit"],
                    "base_fee_gwei": round(b["baseFeePerGas"] / 1e9, 4)})
    return {"network": "base", "blocks": out}


async def chain_pending_count():
    """Pending-block transaction count (live mempool snapshot)."""
    blk = w3.eth.get_block("pending")
    return {"network": "base", "pending_block_number": blk["number"],
            "pending_tx_count": len(blk["transactions"])}


async def chain_tx_status(tx_hash: str):
    """Transaction receipt: status + confirmations."""
    try:
        receipt = w3.eth.get_transaction_receipt(tx_hash)
    except Exception:
        return {"tx_hash": tx_hash, "status": "pending",
                "confirmations": 0, "note": "not yet mined or unknown hash"}
    current = w3.eth.block_number
    return {"tx_hash": tx_hash, "status": "success" if receipt["status"] == 1 else "failed",
            "block_number": receipt["blockNumber"],
            "confirmations": current - receipt["blockNumber"],
            "gas_used": receipt["gasUsed"],
            "contract_address": receipt.get("contractAddress")}


async def chain_tx_cost(tx_hash: str):
    """Transaction cost in ETH and USD."""
    try:
        tx = w3.eth.get_transaction(tx_hash)
        receipt = w3.eth.get_transaction_receipt(tx_hash)
    except Exception:
        return {"tx_hash": tx_hash, "error": "transaction not found or not yet mined"}
    # NB: explicit None checks, not `or` — deposit (type 126) txs have
    # gasPrice 0, which is falsy but a real value (cost 0).
    gas_price = receipt.get("effectiveGasPrice")
    if gas_price is None:
        gas_price = tx.get("gasPrice")
    if gas_price is None:
        gas_price = tx.get("maxFeePerGas")
    if gas_price is None:
        gas_price = 0
    cost_eth = receipt["gasUsed"] * gas_price / 1e18
    eth = await _coin_price("ethereum")
    usd = None
    if isinstance(eth, dict) and not eth.get("error") and eth.get("usd"):
        usd = round(cost_eth * eth["usd"], 4)
    return {"tx_hash": tx_hash, "gas_used": receipt["gasUsed"],
            "gas_price_gwei": gas_price / 1e9, "cost_eth": cost_eth,
            "eth_usd": (eth or {}).get("usd"), "cost_usd": usd}


async def chain_block_tx_count(number: int):
    """Transaction count + gas for one block number."""
    try:
        b = w3.eth.get_block(number)
    except Exception as e:
        return {"error": str(e)[:200], "block": number}
    return {"network": "base", "block": b["number"],
            "timestamp": b["timestamp"], "tx_count": len(b["transactions"]),
            "gas_used": b["gasUsed"], "gas_limit": b["gasLimit"],
            "base_fee_gwei": round(b["baseFeePerGas"] / 1e9, 4)}


# ---------- v5 UTILS (5) ----------

async def utils_coin_history(coin: str, days: int):
    """Price history for one CoinGecko coin (1-365 days)."""
    if days < 1 or days > 365:
        return {"error": "days must be 1-365", "days": days}
    data = await _get_json(f"{COINGECKO}/coins/{coin}/market_chart",
                           params={"vs_currency": "usd", "days": days})
    if isinstance(data, dict) and "error" in data:
        return data
    prices = (data or {}).get("prices") or []
    return {"coin": coin, "days": days, "points": len(prices),
            "prices": [{"timestamp": p[0], "price_usd": p[1]} for p in prices],
            "first_price_usd": prices[0][1] if prices else None,
            "last_price_usd": prices[-1][1] if prices else None}


async def utils_convert_usdc_eth(amount: float):
    """Convert a USDC amount to ETH at the current price."""
    eth = await _coin_price("ethereum")
    if isinstance(eth, dict) and "error" in eth:
        return eth
    price = eth.get("usd")
    if not price:
        return {"error": "no ETH price available"}
    return {"usdc": amount, "eth_usd": price, "eth": round(amount / price, 6)}


async def utils_fear_greed_history():
    """Fear & Greed index, last 7 days."""
    data = await _get_json(f"{FNG}/fng/", params={"limit": 7})
    if isinstance(data, dict) and "error" in data:
        return data
    rows = (data or {}).get("data") or []
    return {"count": len(rows), "history": [
        {"value": r.get("value"), "classification": r.get("value_classification"),
         "timestamp": r.get("timestamp")} for r in rows]}


async def utils_coin_ath(coin: str):
    """All-time high/low for one CoinGecko coin (USD)."""
    data = await _get_json(f"{COINGECKO}/coins/{coin}",
                           params={"localization": "false", "tickers": "false",
                                   "market_data": "true", "community_data": "false",
                                   "developer_data": "false", "sparkline": "false"})
    if isinstance(data, dict) and "error" in data:
        return data
    md = (data or {}).get("market_data") or {}
    return {"coin": coin, "name": data.get("name"), "symbol": data.get("symbol"),
            "current_price_usd": (md.get("current_price") or {}).get("usd"),
            "ath_usd": (md.get("ath") or {}).get("usd"),
            "ath_date": (md.get("ath_date") or {}).get("usd"),
            "ath_change_pct": (md.get("ath_change_percentage") or {}).get("usd"),
            "atl_usd": (md.get("atl") or {}).get("usd"),
            "atl_date": (md.get("atl_date") or {}).get("usd"),
            "market_cap_usd": md.get("market_cap", {}).get("usd")}


async def utils_convert_gwei_eth(amount: float):
    """Convert gwei to ETH."""
    return {"gwei": amount, "eth": amount / 1e9}


# ---------- v5 ENS (2) ----------

async def ens_records(name: str):
    """ENS text records / socials for one name (ensdata.net)."""
    data = await _ens_lookup(name)
    if isinstance(data, dict) and "error" in data:
        return data
    return {"name": name, "address": data.get("address"),
            "avatar": data.get("avatar"), "url": data.get("url"),
            "description": data.get("description"),
            "twitter": data.get("twitter"), "github": data.get("github"),
            "discord": data.get("discord"), "telegram": data.get("telegram"),
            "content_hash": data.get("contentHash"),
            "header": data.get("header"), "pgp": data.get("pgp")}


async def ens_content(name: str):
    """ENS content hash + avatar for one name (ensdata.net)."""
    data = await _ens_lookup(name)
    if isinstance(data, dict) and "error" in data:
        return data
    return {"name": name, "address": data.get("address"),
            "content_hash": data.get("contentHash"),
            "avatar": data.get("avatar")}

# ---------- v5 TOKENS (extra) ----------

async def token_fdv(address: str):
    """Fully diluted valuation + market cap for one Base token."""
    try:
        addr = _checksum(address)
    except Exception:
        return {"error": "invalid address", "address": address}
    pairs = await _dex_pairs_for_token(addr)
    if isinstance(pairs, dict):
        return pairs
    best = _best_pair(pairs)
    if not best:
        return {"error": "no pairs for token", "token": addr}
    return {"token": addr, "pair": best.get("pairAddress"), "dex": best.get("dexId"),
            "chain": best.get("chainId"), "price_usd": best.get("priceUsd"),
            "fdv": best.get("fdv"), "market_cap": best.get("marketCap"),
            "liquidity_usd": (best.get("liquidity") or {}).get("usd"),
            "network": "base"}


# ---------- v5 UTILS (extra) ----------

async def utils_coin_change(coin: str):
    """Price change (24h/7d/30d/1y) for one CoinGecko coin."""
    data = await _get_json(f"{COINGECKO}/coins/{coin}",
                           params={"localization": "false", "tickers": "false",
                                   "market_data": "true", "community_data": "false",
                                   "developer_data": "false", "sparkline": "false"})
    if isinstance(data, dict) and "error" in data:
        return data
    md = (data or {}).get("market_data") or {}
    return {"coin": coin, "name": data.get("name"), "symbol": data.get("symbol"),
            "current_price_usd": (md.get("current_price") or {}).get("usd"),
            "change_24h_pct": md.get("price_change_percentage_24h"),
            "change_7d_pct": md.get("price_change_percentage_7d"),
            "change_30d_pct": md.get("price_change_percentage_30d"),
            "change_1y_pct": md.get("price_change_percentage_1y"),
            "market_cap_usd": (md.get("market_cap") or {}).get("usd")}

# ---------- v5 REGISTRATION ----------
# Applied here (rather than as decorators above) so the handler
# definitions stay readable; calling endpoint(...)(fn) is identical
# to decorating fn with @endpoint(...).
# Price mix: $0.01 x97, $0.02 x3 = 100 new.

# Lending (14)
endpoint("/lending/aave/markets", "$0.02", "All Aave V3 markets on Base with on-chain supply/borrow APY. $0.02 USDC per call.", "lending", ["lending", "aave", "markets"], "lending-aave-markets")(lending_aave_markets)
endpoint("/lending/aave/market/{asset}", "$0.01", "Aave V3 market for one asset: on-chain rates + aToken/debt tokens. $0.01 USDC per call.", "lending", ["lending", "aave", "market"], "lending-aave-market")(lending_aave_market_asset)
endpoint("/lending/aave/user/{wallet}", "$0.01", "Aave V3 account data for one wallet: collateral, debt, health factor. $0.01 USDC per call.", "lending", ["lending", "aave", "account"], "lending-aave-user")(lending_aave_user)
endpoint("/lending/aave/tvl", "$0.01", "Aave V3 TVL on Base. $0.01 USDC per call.", "lending", ["lending", "aave", "tvl"], "lending-aave-tvl")(lending_aave_tvl)
endpoint("/lending/compound/markets", "$0.01", "Compound V3 yield pools on Base, top 10 by TVL. $0.01 USDC per call.", "lending", ["lending", "compound", "markets"], "lending-compound-markets")(lending_compound_markets)
endpoint("/lending/compound/usdc", "$0.01", "Compound V3 Comet USDC market on Base: utilization, totals, APY. $0.01 USDC per call.", "lending", ["lending", "compound", "usdc"], "lending-compound-usdc")(lending_compound_usdc)
endpoint("/lending/compound/user/{wallet}", "$0.01", "Compound V3 Comet USDC position for one wallet: supplied/borrowed. $0.01 USDC per call.", "lending", ["lending", "compound", "account"], "lending-compound-user")(lending_compound_user)
endpoint("/lending/morpho/pools", "$0.01", "Morpho Blue yield pools on Base, top 10 by TVL. $0.01 USDC per call.", "lending", ["lending", "morpho", "pools"], "lending-morpho-pools")(lending_morpho_pools)
endpoint("/lending/morpho/stable", "$0.01", "Highest-APY Morpho Blue stablecoin pools on Base. $0.01 USDC per call.", "lending", ["lending", "morpho", "stablecoin"], "lending-morpho-stable")(lending_morpho_stable)
endpoint("/lending/rates/{asset}", "$0.01", "Lending rates for one asset: Aave on-chain + yield pools. $0.01 USDC per call.", "lending", ["lending", "rates", "aave"], "lending-rates")(lending_rates)
endpoint("/lending/top-apy", "$0.01", "Top 10 lending APY pools on Base. $0.01 USDC per call.", "lending", ["lending", "apy", "top"], "lending-top-apy")(lending_top_apy)
endpoint("/lending/stablecoin-rates", "$0.01", "Aave V3 supply/borrow APY for USDC, DAI and USDbC on Base. $0.01 USDC per call.", "lending", ["lending", "stablecoin", "rates"], "lending-stablecoin-rates")(lending_stablecoin_rates)
endpoint("/lending/usdc-rate", "$0.01", "Aave V3 USDC market rates on Base. $0.01 USDC per call.", "lending", ["lending", "aave", "usdc"], "lending-usdc-rate")(lending_usdc_rate)
endpoint("/lending/weth-rate", "$0.01", "Aave V3 WETH market rates on Base. $0.01 USDC per call.", "lending", ["lending", "aave", "weth"], "lending-weth-rate")(lending_weth_rate)

# Stablecoins (6)
endpoint("/stablecoins/top", "$0.01", "Top 10 stablecoins by circulating supply. $0.01 USDC per call.", "stablecoins", ["stablecoins", "top", "supply"], "stablecoins-top")(stablecoins_top)
endpoint("/stablecoins/{asset}", "$0.01", "One stablecoin by id, symbol, or gecko_id, with per-chain supply. $0.01 USDC per call.", "stablecoins", ["stablecoins", "asset"], "stablecoins-asset")(stablecoins_asset)
endpoint("/stablecoins/chains", "$0.01", "Stablecoin supply by chain, top 15. $0.01 USDC per call.", "stablecoins", ["stablecoins", "chains"], "stablecoins-chains")(stablecoins_chains)
endpoint("/stablecoins/pegs", "$0.01", "Stablecoins furthest from their $1 peg right now. $0.01 USDC per call.", "stablecoins", ["stablecoins", "peg", "depeg"], "stablecoins-pegs")(stablecoins_pegs)
endpoint("/stablecoins/chain/{chain}", "$0.01", "Top 15 stablecoins by circulating supply on one chain. $0.01 USDC per call.", "stablecoins", ["stablecoins", "chain"], "stablecoins-chain")(stablecoins_chain)
endpoint("/stablecoins/total", "$0.01", "Total circulating supply of all stablecoins. $0.01 USDC per call.", "stablecoins", ["stablecoins", "total"], "stablecoins-total")(stablecoins_total)

# Oracles (6)
endpoint("/oracles/feeds", "$0.01", "Chainlink price feeds on Base: ETH/USD, BTC/USD, USDC/USD. $0.01 USDC per call.", "oracles", ["oracles", "chainlink", "feeds"], "oracles-feeds")(oracles_feeds)
endpoint("/oracles/price/{symbol}", "$0.01", "Chainlink price for one symbol (eth, btc, usdc). $0.01 USDC per call.", "oracles", ["oracles", "chainlink", "price"], "oracles-price")(oracles_price)
endpoint("/oracles/eth", "$0.01", "Chainlink ETH/USD on Base. $0.01 USDC per call.", "oracles", ["oracles", "chainlink", "eth"], "oracles-eth")(oracles_eth)
endpoint("/oracles/btc", "$0.01", "Chainlink BTC/USD on Base. $0.01 USDC per call.", "oracles", ["oracles", "chainlink", "btc"], "oracles-btc")(oracles_btc)
endpoint("/oracles/usdc", "$0.01", "Chainlink USDC/USD on Base. $0.01 USDC per call.", "oracles", ["oracles", "chainlink", "usdc"], "oracles-usdc")(oracles_usdc)
endpoint("/oracles/vs-coingecko", "$0.01", "Chainlink ETH/USD vs CoinGecko ETH price (oracle deviation check). $0.01 USDC per call.", "oracles", ["oracles", "chainlink", "coingecko"], "oracles-vs-coingecko")(oracles_vs_coingecko)

# Governance (5)
endpoint("/gov/proposals", "$0.01", "Latest 10 Snapshot proposals across all DAOs. $0.01 USDC per call.", "governance", ["governance", "snapshot", "proposals"], "gov-proposals")(gov_proposals)
endpoint("/gov/space/{space}/proposals", "$0.01", "Latest 10 Snapshot proposals for one space. $0.01 USDC per call.", "governance", ["governance", "snapshot", "space"], "gov-space-proposals")(gov_space_proposals)
endpoint("/gov/proposal/{proposal_id}", "$0.01", "One Snapshot proposal by id, with scores. $0.01 USDC per call.", "governance", ["governance", "snapshot", "proposal"], "gov-proposal")(gov_proposal)
endpoint("/gov/proposal/{proposal_id}/votes", "$0.01", "Top 20 votes on one Snapshot proposal, by voting power. $0.01 USDC per call.", "governance", ["governance", "snapshot", "votes"], "gov-votes")(gov_votes)
endpoint("/gov/space/{space}", "$0.01", "One Snapshot space: name, about, proposal/follower counts. $0.01 USDC per call.", "governance", ["governance", "snapshot", "space"], "gov-space")(gov_space)

# Gas (5)
endpoint("/chain/gas/base-fee", "$0.01", "Current base fee + predicted next-block base fee. $0.01 USDC per call.", "gas", ["gas", "base-fee", "eip1559"], "chain-gas-base-fee")(chain_gas_base_fee)
endpoint("/chain/gas/fee-history/{blocks}", "$0.01", "Fee history for the last N blocks (1-20). $0.01 USDC per call.", "gas", ["gas", "fee-history"], "chain-gas-fee-history")(chain_gas_fee_history)
endpoint("/chain/gas/transfer-cost", "$0.01", "Cost of a plain 21,000-gas ETH transfer on Base. $0.01 USDC per call.", "gas", ["gas", "transfer", "cost"], "chain-gas-transfer-cost")(chain_gas_transfer_cost)
endpoint("/chain/gas/transfer-cost-usd", "$0.01", "Cost of a plain ETH transfer on Base in USD. $0.01 USDC per call.", "gas", ["gas", "transfer", "usd"], "chain-gas-transfer-cost-usd")(chain_gas_transfer_cost_usd)
endpoint("/chain/gas/max-fee", "$0.01", "Current base fee + max priority fee. $0.01 USDC per call.", "gas", ["gas", "max-fee", "priority"], "chain-gas-max-fee")(chain_gas_max_fee)

# NFTs (6)
endpoint("/nfts/{collection}/token/{token_id}/uri", "$0.01", "Raw tokenURI for one NFT token. $0.01 USDC per call.", "nft", ["nft", "tokenuri"], "nft-token-uri")(nft_token_uri)
endpoint("/nfts/{collection}/token/{token_id}/image", "$0.01", "Image URL from one NFT token's metadata. $0.01 USDC per call.", "nft", ["nft", "image", "metadata"], "nft-token-image")(nft_token_image)
endpoint("/nfts/{collection}/token/{token_id}/attributes", "$0.01", "Traits/attributes from one NFT token's metadata. $0.01 USDC per call.", "nft", ["nft", "traits", "metadata"], "nft-token-attributes")(nft_token_attributes)
endpoint("/nfts1155/{collection}/balance/{wallet}/{token_id}", "$0.01", "ERC1155 balance of one wallet for one token id. $0.01 USDC per call.", "nft", ["nft", "erc1155", "balance"], "nft1155-balance")(nft1155_balance)
endpoint("/nfts1155/{collection}/uri/{token_id}", "$0.01", "ERC1155 metadata URI for one token id. $0.01 USDC per call.", "nft", ["nft", "erc1155", "uri"], "nft1155-uri")(nft1155_uri)
endpoint("/nfts1155/{collection}/supply/{token_id}", "$0.01", "ERC1155 total supply for one token id (if the contract tracks it). $0.01 USDC per call.", "nft", ["nft", "erc1155", "supply"], "nft1155-supply")(nft1155_supply)

# DeFi (12)
endpoint("/defi/protocol/{slug}", "$0.01", "One protocol's TVL detail by slug. $0.01 USDC per call.", "defi", ["defi", "protocol", "tvl"], "defi-protocol")(defi_protocol)
endpoint("/defi/chains", "$0.01", "TVL by chain, top 20. $0.01 USDC per call.", "defi", ["defi", "chains", "tvl"], "defi-chains")(defi_chains)
endpoint("/defi/chains/{chain}", "$0.01", "TVL for one chain. $0.01 USDC per call.", "defi", ["defi", "chain", "tvl"], "defi-chain")(defi_chain)
endpoint("/defi/chains/{chain}/history", "$0.01", "TVL history for one chain: first, latest, 30-day change. $0.01 USDC per call.", "defi", ["defi", "chain", "history"], "defi-chain-history")(defi_chain_history)
endpoint("/defi/fees/overview", "$0.01", "Top 10 protocols by 24h fees (all chains). $0.01 USDC per call.", "defi", ["defi", "fees", "overview"], "defi-fees-overview")(defi_fees_overview)
endpoint("/defi/yields/chart/{pool}", "$0.01", "TVL/APY history for one yield pool (last 30 points). $0.01 USDC per call.", "defi", ["defi", "yields", "chart"], "defi-yields-chart")(defi_yields_chart)
endpoint("/defi/yields/il/{pool}", "$0.01", "Impermanent-loss risk + APY for one yield pool. $0.01 USDC per call.", "defi", ["defi", "yields", "il"], "defi-yields-il")(defi_yields_il)
endpoint("/defi/yields/project/{project}", "$0.01", "Top 10 yield pools for one project, by APY. $0.01 USDC per call.", "defi", ["defi", "yields", "project"], "defi-yields-project")(defi_yields_project)
endpoint("/defi/yields/token/{symbol}", "$0.01", "Top 10 yield pools for one token symbol, by APY. $0.01 USDC per call.", "defi", ["defi", "yields", "token"], "defi-yields-token")(defi_yields_token)
endpoint("/defi/yields/stable/{chain}", "$0.01", "Top 10 stablecoin yield pools on one chain, by APY. $0.01 USDC per call.", "defi", ["defi", "yields", "stablecoin"], "defi-yields-stable-chain")(defi_yields_stable)
endpoint("/defi/dex/volume/7d", "$0.01", "Total DEX volume, last 7 days (all chains). $0.01 USDC per call.", "defi", ["defi", "dex", "volume"], "defi-dex-volume-7d")(defi_dex_volume_7d)
endpoint("/defi/dex/volume/base/7d", "$0.01", "Total DEX volume on Base, last 7 days. $0.01 USDC per call.", "defi", ["defi", "dex", "volume", "base"], "defi-dex-volume-base-7d")(defi_dex_volume_base_7d)

# DEX (8)
endpoint("/dex/top/volume/{n}", "$0.01", "Top N DEXs by 24h volume (1-25). $0.01 USDC per call.", "dex", ["dex", "top", "volume"], "dex-top-volume")(dex_top_volume)
endpoint("/dex/volume/{chain}", "$0.01", "Total DEX volume for one chain (24h + 7d). $0.01 USDC per call.", "dex", ["dex", "volume", "chain"], "dex-volume-chain")(dex_volume_chain)
endpoint("/dex/chains", "$0.02", "DEX volume by chain for the major chains. $0.02 USDC per call.", "dex", ["dex", "chains", "volume"], "dex-chains")(dex_chains)
endpoint("/dex/top/fees", "$0.01", "Top 10 DEXs by 24h fees. $0.01 USDC per call.", "dex", ["dex", "top", "fees"], "dex-top-fees")(dex_top_fees)
endpoint("/dex/fees/{protocol}/chains", "$0.01", "Fee breakdown by chain for one protocol. $0.01 USDC per call.", "dex", ["dex", "fees", "chains"], "dex-fees-chains")(dex_fees_chains)
endpoint("/dex/pair/{chain}/{pair}/txns", "$0.01", "Transactions + volume for one DEX pair. $0.01 USDC per call.", "dex", ["dex", "pair", "txns"], "dex-pair-txns")(dex_pair_txns)
endpoint("/dex/pairs/{token}", "$0.01", "Top 10 DEX pairs for one token, by liquidity. $0.01 USDC per call.", "dex", ["dex", "pairs", "token"], "dex-pairs-token")(dex_pairs_token)
endpoint("/dex/pair/{token}/{chain}", "$0.01", "Best DEX pair for one token on one chain, by liquidity. $0.01 USDC per call.", "dex", ["dex", "pair", "chain"], "dex-pair-chain")(dex_pair_chain)

# Wallets (7)
endpoint("/wallets/{address}/approvals", "$0.01", "Token allowances a wallet granted to Permit2, routers, Aave. $0.01 USDC per call.", "wallet", ["wallet", "approvals", "allowance"], "wallet-approvals")(wallet_approvals)
endpoint("/wallets/{address}/approvals/{token}", "$0.01", "Allowances one wallet granted for one token. $0.01 USDC per call.", "wallet", ["wallet", "approvals", "token"], "wallet-approvals-token")(wallet_approvals_token)
endpoint("/wallets/{address}/token/{token}/value", "$0.01", "One wallet's balance of one token + USD value. $0.01 USDC per call.", "wallet", ["wallet", "token", "value"], "wallet-token-value")(wallet_token_value)
endpoint("/wallets/{address}/cbeth-balance", "$0.01", "Wallet cbETH balance. $0.01 USDC per call.", "wallet", ["wallet", "cbeth", "balance"], "wallet-cbeth-balance")(wallet_cbeth_balance)
endpoint("/wallets/{address}/dai-balance", "$0.01", "Wallet DAI balance. $0.01 USDC per call.", "wallet", ["wallet", "dai", "balance"], "wallet-dai-balance")(wallet_dai_balance)
endpoint("/wallets/{address}/usdbc-balance", "$0.01", "Wallet USDbC balance. $0.01 USDC per call.", "wallet", ["wallet", "usdbc", "balance"], "wallet-usdbc-balance")(wallet_usdbc_balance)
endpoint("/wallets/{address}/higher-balance", "$0.01", "Wallet HIGHER balance. $0.01 USDC per call.", "wallet", ["wallet", "higher", "balance"], "wallet-higher-balance")(wallet_higher_balance)

# Memecoins (5)
endpoint("/memes/new-pairs", "$0.01", "Newest Base token profiles on Dexscreener. $0.01 USDC per call.", "memes", ["memes", "new", "pairs"], "memes-new-pairs")(memes_new_pairs)
endpoint("/memes/trending", "$0.02", "Top 15 Base pairs by 24h volume across the main Base tokens. $0.02 USDC per call.", "memes", ["memes", "trending", "volume"], "memes-trending")(memes_trending)
endpoint("/memes/boosted", "$0.01", "Latest boosted Base tokens on Dexscreener. $0.01 USDC per call.", "memes", ["memes", "boosted"], "memes-boosted")(memes_boosted)
endpoint("/memes/{token}/price-change", "$0.01", "Price changes (5m/1h/6h/24h) for one token's best pair. $0.01 USDC per call.", "memes", ["memes", "price-change"], "memes-price-change")(memes_price_change)
endpoint("/memes/{token}/holders", "$0.01", "Holder count + top holders for one token. $0.01 USDC per call.", "memes", ["memes", "holders"], "memes-holders")(memes_holders)

# Prediction (6)
endpoint("/predict/markets", "$0.01", "Top 15 active Polymarket markets by liquidity. $0.01 USDC per call.", "prediction", ["prediction", "polymarket", "markets"], "predict-markets")(predict_markets)
endpoint("/predict/trending", "$0.01", "Top 15 active Polymarket markets by 24h volume. $0.01 USDC per call.", "prediction", ["prediction", "polymarket", "trending"], "predict-trending")(predict_trending)
endpoint("/predict/search/{query}", "$0.01", "Search active Polymarket markets by keyword. $0.01 USDC per call.", "prediction", ["prediction", "polymarket", "search"], "predict-search")(predict_search)
endpoint("/predict/events", "$0.01", "Latest 20 active Polymarket events. $0.01 USDC per call.", "prediction", ["prediction", "polymarket", "events"], "predict-events")(predict_events)
endpoint("/predict/market/{slug}", "$0.01", "One Polymarket market by slug, with outcomes and prices. $0.01 USDC per call.", "prediction", ["prediction", "polymarket", "market"], "predict-market")(predict_market)
endpoint("/predict/markets/closing", "$0.01", "Active Polymarket markets ending soonest (next 15 by end date). $0.01 USDC per call.", "prediction", ["prediction", "polymarket", "closing"], "predict-closing")(predict_closing)

# Tokens (6)
endpoint("/tokens/{address}/history/{days}", "$0.01", "Price history for one Base token (1-365 days). $0.01 USDC per call.", "token", ["token", "history", "price"], "token-history")(token_history)
endpoint("/tokens/{address}/audit", "$0.01", "Security audit flags for one Base token (GoPlus). $0.01 USDC per call.", "token", ["token", "audit", "security"], "token-audit")(token_audit)
endpoint("/tokens/{address}/holders", "$0.01", "Holder count + top 10 holders for one Base token (GoPlus). $0.01 USDC per call.", "token", ["token", "holders"], "token-holders")(token_holders)
endpoint("/tokens/{address}/top-holder", "$0.01", "Largest holder of one Base token (GoPlus). $0.01 USDC per call.", "token", ["token", "top-holder"], "token-top-holder")(token_top_holder)
endpoint("/tokens/{address}/age", "$0.01", "Age of a token's oldest DEX pair (first listing date). $0.01 USDC per call.", "token", ["token", "age"], "token-age")(token_age)
endpoint("/tokens/{address}/liquidity-usd", "$0.01", "Total DEX liquidity (USD) across all pairs for one token. $0.01 USDC per call.", "token", ["token", "liquidity"], "token-liquidity-usd")(token_liquidity_usd)
endpoint("/tokens/{address}/fdv", "$0.01", "Fully diluted valuation + market cap for one Base token. $0.01 USDC per call.", "token", ["token", "fdv", "market-cap"], "token-fdv")(token_fdv)

# Chain (5)
endpoint("/chain/blocks/{count}", "$0.01", "Summaries of the last N blocks (1-10). $0.01 USDC per call.", "chain", ["chain", "blocks"], "chain-blocks")(chain_blocks)
endpoint("/chain/pending/count", "$0.01", "Pending-block transaction count (live mempool snapshot). $0.01 USDC per call.", "chain", ["chain", "pending", "mempool"], "chain-pending-count")(chain_pending_count)
endpoint("/chain/transaction/{tx_hash}/status", "$0.01", "Transaction receipt: status + confirmations. $0.01 USDC per call.", "chain", ["chain", "transaction", "status"], "chain-tx-status")(chain_tx_status)
endpoint("/chain/transaction/{tx_hash}/cost", "$0.01", "Transaction cost in ETH and USD. $0.01 USDC per call.", "chain", ["chain", "transaction", "cost"], "chain-tx-cost")(chain_tx_cost)
endpoint("/chain/block/{number}/tx-count", "$0.01", "Transaction count + gas for one block number. $0.01 USDC per call.", "chain", ["chain", "block", "tx-count"], "chain-block-tx-count")(chain_block_tx_count)

# Utils (5)
endpoint("/utils/coin/{coin}/history/{days}", "$0.01", "Price history for one CoinGecko coin (1-365 days). $0.01 USDC per call.", "utils", ["utils", "coin", "history"], "utils-coin-history")(utils_coin_history)
endpoint("/utils/convert/usdc-eth/{amount}", "$0.01", "Convert a USDC amount to ETH at the current price. $0.01 USDC per call.", "utils", ["utils", "convert", "usdc", "eth"], "utils-convert-usdc-eth")(utils_convert_usdc_eth)
endpoint("/utils/market/fear-greed/history", "$0.01", "Fear & Greed index, last 7 days. $0.01 USDC per call.", "utils", ["utils", "fear-greed", "history"], "utils-fear-greed-history")(utils_fear_greed_history)
endpoint("/utils/coin/{coin}/ath", "$0.01", "All-time high/low for one CoinGecko coin (USD). $0.01 USDC per call.", "utils", ["utils", "coin", "ath"], "utils-coin-ath")(utils_coin_ath)
endpoint("/utils/convert/gwei-eth/{amount}", "$0.01", "Convert gwei to ETH. $0.01 USDC per call.", "utils", ["utils", "convert", "gwei", "eth"], "utils-convert-gwei-eth")(utils_convert_gwei_eth)
endpoint("/utils/coin/{coin}/change", "$0.01", "Price change (24h/7d/30d/1y) for one CoinGecko coin. $0.01 USDC per call.", "utils", ["utils", "coin", "change"], "utils-coin-change")(utils_coin_change)

# ENS (2)
endpoint("/ens/{name}/records", "$0.01", "ENS text records / socials for one name. $0.01 USDC per call.", "naming", ["ens", "records", "socials"], "ens-records")(ens_records)
endpoint("/ens/{name}/content", "$0.01", "ENS content hash + avatar for one name. $0.01 USDC per call.", "naming", ["ens", "content", "avatar"], "ens-content")(ens_content)


# ===== APP CONSTRUCTION (routes + paywall built from the registry) =====

# Static routes first, parameterized routes after. This matters twice:
#  - FastAPI matches in registration order, so /chain/block/latest must
#    come before /chain/block/{number}.
#  - the x402 middleware returns the FIRST matching route config, so an
#    exact static route must be checked before a :param pattern that
#    could also match its path (e.g. /defi/yields/top vs :pool).
ORDERED = sorted(REGISTRY, key=lambda e: ("{" in e["path"],))


def _middleware_pattern(path: str) -> str:
    """FastAPI '{param}' -> x402 ':param' (single-segment match)."""
    import re
    return re.sub(r"\{(\w+)\}", r":\1", path)


# Public origin of the deployed service. Route configs advertise the
# resource as an absolute URL: Coinbase CDP's Bazaar validator rejects
# bare paths in its discovery simulation ("resource must start with
# 'https://' when protocol type is http").
PUBLIC_ORIGIN = "https://manteclaw-x402.fly.dev"


def _resource_for(path: str) -> str:
    """v3 convention: the resource is the path up to the first parameter,
    made absolute with PUBLIC_ORIGIN (CDP Bazaar requires https://)."""
    idx = path.find("/{")
    return PUBLIC_ORIGIN + (path if idx == -1 else path[:idx])


# ===== BAZAAR DISCOVERY DECLARATIONS (PayAI catalog) =====
# PayAI's Bazaar catalogs a resource from the payments its facilitator
# processes — but only when the payment payload carries the discovery
# declaration this server put in its 402 response (the buyer's client
# echoes `extensions` from the 402 into the payload). Without a
# declaration, settled payments stay invisible to the catalog: this
# service took ~31 settled calls on Nov 21, 2026 and appeared nowhere
# in /discovery/resources. So every route config below carries one
# declaration, built with the SDK's declare_discovery_extension. The
# FastAPI payment middleware auto-registers
# bazaar_resource_server_extension when a route config declares a
# "bazaar" extension; that extension enriches each declaration at
# request time with the HTTP method and, for dynamic routes, the
# concrete path params plus a :param routeTemplate.
#
# Dynamic routes: PayAI rejects a templated resource
# (resource_path_template) unless EVERY path parameter declares an
# example value — the catalog lists the route under a real URL built
# from the examples and probes that URL. Path params are strings on
# the wire (the SDK extracts them from the URL as strings), so their
# schema type is "string" even when a handler annotates int/float;
# the query-input schema (no route takes query params today) maps
# annotations to JSON types properly.

_BAZAAR_JSON_TYPES = {str: "string", int: "integer", float: "number", bool: "boolean"}

# Example values for path parameters, by parameter name. Address-shaped
# parameters (see _BAZAAR_ADDRESS_PARAMS) use the USDC contract: a real
# Base contract that returns useful data from any address-taking
# endpoint, so the catalog's probe of the example URL and any agent
# calling the route as listed both get a sensible result.
_BAZAAR_EXAMPLES = {
    "symbol": "ETH",
    "chain": "base",
    "number": "19000000",
    "name": "vitalik.eth",
    "space": "aave.eth",
    "protocol": "aave",
    "slug": "aave",
    "project": "aave-v3",
    "coin": "ethereum",
    "query": "ethereum",
    "text": "hello",
    "gwei": "20",
    "gas": "21000",
    "amount": "1",
    "days": "30",
    "count": "5",
    "n": "10",
    "blocks": "10",
    "tokenId": "1",
    "token_id": "1",
    "proposal_id": "0x" + "0123456789abcdef" * 4,
    "hash": "0x" + "0123456789abcdef" * 4,
    "tx_hash": "0x" + "0123456789abcdef" * 4,
}

_BAZAAR_ADDRESS_PARAMS = {
    "address", "token_address", "wallet", "owner", "spender", "to",
    "token", "collection", "pool", "pair", "asset",
}


def _bazaar_example(entry, name, annotation):
    """One example value for a parameter: by route/name, then by type."""
    # /stablecoins/{asset} looks up by id/symbol, not by contract address
    # (the lending {asset} routes do want an address — handled below).
    if name == "asset" and entry["path"].startswith("/stablecoins"):
        return "USDC"
    if name in _BAZAAR_EXAMPLES:
        return _BAZAAR_EXAMPLES[name]
    if name in _BAZAAR_ADDRESS_PARAMS:
        return USDC
    if annotation is int:
        return 1
    if annotation is float:
        return 1.0
    if annotation is bool:
        return True
    return "example"


def _bazaar_extension(entry):
    """Build the {"bazaar": ...} discovery declaration for one registry entry.

    All routes are GET, so handler params not in the path are query
    params: they go into the declaration's example input (required ones
    only) and input schema, with JSON types mapped from the annotations.
    Path params go into path_params_schema with an example for each.
    """
    import re

    path_params = re.findall(r"\{(\w+)\}", entry["path"])
    query_props, query_required, query_example = {}, [], {}
    path_props, path_required = {}, []
    for pname, param in inspect.signature(entry["handler"]).parameters.items():
        if pname in path_params:
            path_props[pname] = {
                "type": "string",
                "examples": [str(_bazaar_example(entry, pname, param.annotation))],
            }
            path_required.append(pname)
        else:
            query_props[pname] = {
                "type": _BAZAAR_JSON_TYPES.get(param.annotation, "string")
            }
            if param.default is inspect.Parameter.empty:
                query_required.append(pname)
                query_example[pname] = _bazaar_example(entry, pname, param.annotation)
    return declare_discovery_extension(
        input=query_example or None,
        input_schema={"properties": query_props, "required": query_required},
        path_params_schema=(
            {"properties": path_props, "required": path_required} if path_props else None
        ),
        output=OutputConfig(example={"status": "ok"}),
    )


# x402 payment config, built from the same registry as the routes.
routes = {}
for _e in ORDERED:
    _cfg = {
        "accepts": {
            "scheme": "exact",
            "payTo": WALLET_ADDRESS,
            "price": _e["price"],
            "network": "eip155:8453",
        },
        "resource": _resource_for(_e["path"]),
        "description": _e["description"],
        "mimeType": "application/json",
        "extensions": _bazaar_extension(_e),
    }
    routes[f"GET {_middleware_pattern(_e['path'])}"] = _cfg
    # HEAD carries the same payment config (Alex approved 2026-11-21):
    # directory probers (AgentPay, Arispay) probe with HEAD and must see
    # the 402 payment terms instead of a bare 405. A HEAD response has
    # no body, so nothing leaks unpaid; a paid HEAD runs the handler
    # exactly like GET (the server strips the body).
    routes[f"HEAD {_middleware_pattern(_e['path'])}"] = _cfg

# Create FastAPI app
app = FastAPI(title="Manteclaw x402 service", version="5.0")

# Extensionless x402 discovery document. Agent402 probes /.well-known/x402
# (no extension) and expects JSON. StaticFiles serves an extensionless
# file as application/octet-stream, which the probe rejects, so serve it
# explicitly as JSON. This route is registered BEFORE the StaticFiles
# mount below so it takes precedence for this one path.
_WELL_KNOWN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".well-known")


@app.get("/.well-known/x402", include_in_schema=False)
async def well_known_x402():
    """Serve the x402 discovery document at the extensionless path."""
    from fastapi.responses import FileResponse

    return FileResponse(
        os.path.join(_WELL_KNOWN_DIR, "x402.json"),
        media_type="application/json",
    )


# Serve the discovery + verification files for free (no payment):
# /.well-known/x402.json, /.well-known/agent-catalog.json, and
# /.well-known/aipaygen-verify.txt (AiPayGen site verification).
# These MUST be public — agents and directories read them unpaid.
if os.path.isdir(_WELL_KNOWN_DIR):
    app.mount("/.well-known", StaticFiles(directory=_WELL_KNOWN_DIR), name="well-known")

# Build x402 middleware once and register it
x402_mw = payment_middleware(routes, server)

# Paths that must answer without payment. /health is the liveness probe
# used by the service manager and monitors: it must never return 402,
# or the manager will keep restarting a healthy service.
# /.well-known/* is also free: discovery files (x402.json,
# agent-catalog.json) and the AiPayGen site-verification file must be
# readable unpaid, or directories cannot index or verify the service.
# / is the landing page: directories (e.g. Agent402) probe the bare root
# and require a 200, not a 404.
# /catalog is the public shareable catalog page (LinkedIn): it must load
# free, or nobody can open the link.
HEALTH_PATHS = {"/health", "/", "/catalog"}


@app.middleware("http")
async def x402_middleware(request: Request, call_next):
    # AWS Marketplace Agentic Commerce: a request whose AWS payment
    # token was already verified by the AWS middleware (which runs
    # outside this one) carries the scope flag; let it straight
    # through to the handler. The flag is only ever set by
    # aws_agentic_commerce after cryptographic token verification.
    if request.scope.get("aws_payment_verified"):
        return await call_next(request)
    if request.url.path in HEALTH_PATHS or request.url.path.startswith("/.well-known/"):
        return await call_next(request)
    response = await x402_mw(request, call_next)
    # The SDK returns 402s with an EMPTY {} JSON body: the payment terms
    # live only in the base64 `payment-required` header. Body-based
    # validators (e.g. Market402) read the body and score an empty one
    # 5/10, so mirror the header's decoded PaymentRequired JSON into the
    # body — identical terms in both places, header and status untouched.
    # Only an empty body is replaced (a richer SDK error body is kept),
    # and HEAD is skipped: a HEAD response must not carry a body.
    if response.status_code == 402 and request.method != "HEAD":
        pr_header = response.headers.get("payment-required")
        body = getattr(response, "body", b"") or b""
        if pr_header and body.strip() in (b"", b"{}"):
            try:
                terms = json.loads(base64.b64decode(pr_header))
            except Exception:
                terms = None
            if isinstance(terms, dict) and terms.get("accepts"):
                from fastapi.responses import JSONResponse

                headers = {
                    k: v
                    for k, v in response.headers.items()
                    if k.lower() not in ("content-length", "content-type")
                }
                return JSONResponse(content=terms, status_code=402, headers=headers)
    return response


# CORS: the public catalog page (and any browser client) calls the API
# cross-origin. Browsers block reading a cross-origin response unless
# the server sends CORS headers, and the x402 client must be able to
# read the `payment-required` header on the 402 and send `X-PAYMENT`.
# Allow any origin (the endpoints are public, pay-per-call) and expose
# the headers the x402 browser flow needs.
# NOTE: this MUST be registered AFTER the x402 middleware above.
# Starlette runs the most-recently-added middleware outermost, so CORS
# has to be added last to wrap the 402 responses the x402 middleware
# returns without calling through.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["payment-required", "x-payment-response"],
)

# AWS Marketplace Agentic Commerce — added Nov 23, 2026 as a SECOND
# payment scheme (Alex's call), mirroring the parked CDP facilitator
# pattern above: built only when AWS_SCHEME_ENABLED is set in the
# environment; without it this block never runs, the module is never
# imported, and the crypto x402 surface is exactly as before. All of
# the scheme logic lives in aws_agentic_commerce.py (local-only, like
# the CDP code — never pushed with the repo copy). A broken AWS setup
# must not kill the crypto service, hence the broad guard.
if os.environ.get("AWS_SCHEME_ENABLED"):
    try:
        from aws_agentic_commerce import install_aws_scheme

        install_aws_scheme(
            app,
            public_origin=PUBLIC_ORIGIN,
            well_known_dir=_WELL_KNOWN_DIR,
            resource_server=server,
        )
    except Exception as _exc:  # a broken AWS setup must not kill x402
        print(
            f"[aws] Agentic Commerce disabled (init failed): {_exc}",
            file=sys.stderr,
        )


# ===== HEALTH CHECK (no payment required) =====

@app.get("/health")
async def health():
    """Liveness probe. No payment, no on-chain call, no facilitator call."""
    return {
        "status": "ok",
        "service": "x402-service",
        "version": "5.0",
        "network": "base",
        "payee": WALLET_ADDRESS,
        "rpc_connected": w3.is_connected(),
        "endpoints": len(ORDERED) + 1,  # 199 paid + /health = 200
    }


# ===== ROOT LANDING PAGE (no payment required) =====

@app.get("/")
async def root():
    """Service landing/discovery page. Free, no payment.
    Directories probe the bare root and require a 200."""
    return {
        "service": "Manteclaw x402 service",
        "version": "5.0",
        "network": "base",
        "payment": "x402 v2, USDC on Base, per call ($0.01-$0.02)",
        "payee": WALLET_ADDRESS,
        "endpoints": len(ORDERED) + 1,  # 199 paid + /health = 200
        "health": "/health",
        "catalog": "/.well-known/agent-catalog.json",
        "discovery": "/.well-known/x402.json",
        "tools": "/mcp/tools",
        "docs": "https://manteclaw-x402.fly.dev/.well-known/agent-catalog.json",
    }


@app.head("/")
async def root_head():
    """Explicit HEAD handler for the root. Some directory/indexer probes
    send HEAD (not GET); Starlette does not auto-answer HEAD here once
    the x402 middleware is in the stack, so answer it directly."""
    from fastapi.responses import Response

    return Response(status_code=200)


# ===== PUBLIC CATALOG PAGE (no payment required) =====

@app.get("/catalog")
async def catalog_page():
    """The shareable catalog page (LinkedIn). Served from the same origin
    as the API, so the page's Pay & Call buttons are same-origin calls.
    Free, no payment. The HTML is a static export of the catalog
    artifact; keep catalog.html next to this file in the repo."""
    from fastapi.responses import FileResponse

    _catalog = os.path.join(os.path.dirname(os.path.abspath(__file__)), "catalog.html")
    return FileResponse(_catalog, media_type="text/html")


# Register every paid route from the registry.
for _e in ORDERED:
    app.add_api_route(_e["path"], _e["handler"], methods=["GET"])
    # HEAD twin for the middleware config above (Alex approved
    # 2026-11-21). include_in_schema=False keeps /openapi.json exactly
    # as the GET-only spec directories already consume.
    app.add_api_route(
        _e["path"], _e["handler"], methods=["HEAD"], include_in_schema=False
    )


# ===== OPENAPI INPUT-SCHEMA FIX (x402scan) =====
# Directories that import this service from its OpenAPI document
# (x402scan's registerFromOrigin) reject any operation that has neither
# `parameters` nor a `requestBody` — "Missing input schema — add a
# requestBody or parameter schema to your OpenAPI spec". The no-input GET
# endpoints (e.g. /defi/gas-tracker) have neither, so they failed to
# register (72 of the 199 paid endpoints in v5). Give every such operation
# an explicit, optional JSON request body describing an empty input
# object. This changes only the served /openapi.json document — the
# handlers take no body and runtime behaviour is unchanged.
from fastapi.openapi.utils import get_openapi


def _custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=app.title,
        version=app.version,
        openapi_version=app.openapi_version,
        description=app.description,
        routes=app.routes,
    )
    _empty_input_body = {
        "required": False,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": True,
                    "title": "EmptyInput",
                    "description": "This endpoint takes no input.",
                }
            }
        },
    }
    for _path_item in schema.get("paths", {}).values():
        for _op in _path_item.values():
            if (
                isinstance(_op, dict)
                and "parameters" not in _op
                and "requestBody" not in _op
            ):
                _op["requestBody"] = _empty_input_body
    app.openapi_schema = schema
    return schema


app.openapi = _custom_openapi

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
