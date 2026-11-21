# Manteclaw x402 service — v4.0 (FastAPI + x402 2.9.0, Base)
#
# 100 endpoints total: 99 paid (USDC on Base via x402) + free /health.
# The 6 paid endpoints from v3 keep their exact paths and prices.
# The stale /agent/earnings-report from v3 is DELIBERATELY NOT included
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
import math

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
from web3 import Web3
from eth_abi import decode as abi_decode
import httpx

# x402 v2.9.0 imports
from x402 import x402ResourceServer
from x402.http import HTTPFacilitatorClient, FacilitatorConfig
from x402.http.middleware.fastapi import payment_middleware
from x402.mechanisms.evm.exact import ExactEvmServerScheme

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

# Mainnet facilitator for Base (eip155:8453)
facilitator = HTTPFacilitatorClient(
    FacilitatorConfig(url="https://facilitator.xpay.sh")
)

server = x402ResourceServer(facilitator)
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

@endpoint("/defi/gas-tracker", "$0.005",
          "Current Base gas prices (slow/average/fast). $0.005 USDC per call.",
          "gas", ["gas", "base", "l2", "pricing"], "gas-tracker")
async def gas_tracker():
    """Current Base gas prices (slow/average/fast). $0.005 USDC per call."""
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


@endpoint("/mcp/status", "$0.001",
          "MCP server status. $0.001 USDC per call.",
          "status", ["status", "mcp"], "mcp-status")
async def mcp_status():
    return {"status": "online", "version": "4.0", "endpoints": 100, "network": "base", "x402_version": "2.9.0"}


@endpoint("/mcp/tools", "$0.001",
          "MCP tools list. $0.001 USDC per call.",
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


@endpoint("/tokens/{address}/supply", "$0.005",
          "ERC20 total supply for a Base token. $0.005 USDC per call.",
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


@endpoint("/tokens/{address}/name", "$0.005",
          "ERC20 name for a Base token. $0.005 USDC per call.",
          "token", ["token", "erc20"], "token-name")
async def token_name(address: str):
    token = _checksum(address)
    try:
        return {"token": token, "name": _erc20_str(token, SEL_NAME), "network": "base"}
    except Exception as e:
        return {"error": str(e)[:200], "token": token}


@endpoint("/tokens/{address}/symbol", "$0.005",
          "ERC20 symbol for a Base token. $0.005 USDC per call.",
          "token", ["token", "erc20"], "token-symbol")
async def token_symbol(address: str):
    token = _checksum(address)
    try:
        return {"token": token, "symbol": _erc20_str(token, SEL_SYMBOL), "network": "base"}
    except Exception as e:
        return {"error": str(e)[:200], "token": token}


@endpoint("/tokens/{address}/decimals", "$0.005",
          "ERC20 decimals for a Base token. $0.005 USDC per call.",
          "token", ["token", "erc20"], "token-decimals")
async def token_decimals(address: str):
    token = _checksum(address)
    try:
        return {"token": token, "decimals": _erc20_uint(token, SEL_DECIMALS), "network": "base"}
    except Exception as e:
        return {"error": str(e)[:200], "token": token}


@endpoint("/tokens/{address}/balance/{wallet}", "$0.005",
          "ERC20 balance of a wallet for a Base token. $0.005 USDC per call.",
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


@endpoint("/tokens/{address}/allowance/{owner}/{spender}", "$0.005",
          "ERC20 allowance a wallet granted to a spender. $0.005 USDC per call.",
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

@endpoint("/wallets/{address}/balance", "$0.005",
          "ETH balance of a Base wallet. $0.005 USDC per call.",
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


@endpoint("/wallets/{address}/nonce", "$0.005",
          "Transaction count (nonce) of a Base wallet. $0.005 USDC per call.",
          "wallet", ["wallet", "nonce", "base"], "wallet-nonce")
async def wallet_nonce(address: str):
    addr = _checksum(address)
    return {
        "address": addr,
        "tx_count": w3.eth.get_transaction_count(addr),
        "network": "base",
    }


@endpoint("/wallets/{address}/is-contract", "$0.005",
          "Whether a Base address is a smart contract. $0.005 USDC per call.",
          "wallet", ["wallet", "contract", "base"], "wallet-is-contract")
async def wallet_is_contract(address: str):
    addr = _checksum(address)
    code = w3.eth.get_code(addr)
    return {"address": addr, "is_contract": len(code) > 0, "code_size": len(code), "network": "base"}


@endpoint("/wallets/{address}/code", "$0.005",
          "Bytecode size and hash for a Base address. $0.005 USDC per call.",
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


@endpoint("/wallets/{address}/ens", "$0.005",
          "Reverse ENS name for a Base wallet address. $0.005 USDC per call.",
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


@endpoint("/wallets/{address}/eth-value", "$0.005",
          "USD value of a Base wallet's ETH balance. $0.005 USDC per call.",
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


@endpoint("/wallets/{address}/usdc-balance", "$0.005",
          "USDC balance of a Base wallet. $0.005 USDC per call.",
          "wallet", ["wallet", "usdc", "balance"], "wallet-usdc-balance")
async def wallet_usdc_balance(address: str):
    addr = _checksum(address)
    raw = _erc20_balance(USDC, addr)
    return {"address": addr, "symbol": "USDC", "balance": raw / 1e6, "balance_raw": raw, "network": "base"}


@endpoint("/wallets/{address}/weth-balance", "$0.005",
          "WETH balance of a Base wallet. $0.005 USDC per call.",
          "wallet", ["wallet", "weth", "balance"], "wallet-weth-balance")
async def wallet_weth_balance(address: str):
    addr = _checksum(address)
    raw = _erc20_balance(WETH, addr)
    return {"address": addr, "symbol": "WETH", "balance": raw / 1e18, "balance_raw": raw, "network": "base"}


@endpoint("/wallets/{address}/aero-balance", "$0.005",
          "AERO balance of a Base wallet. $0.005 USDC per call.",
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


@endpoint("/chain/block/latest", "$0.005",
          "Latest Base block with gas and transaction count. $0.005 USDC per call.",
          "chain", ["chain", "block", "latest"], "chain-block-latest")
async def chain_block_latest():
    return _block_dict(w3.eth.get_block("latest"))


@endpoint("/chain/block/{number}", "$0.005",
          "Base block by number. $0.005 USDC per call.",
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


@endpoint("/chain/transaction/{hash}/input", "$0.005",
          "Decoded input data for a Base transaction. $0.005 USDC per call.",
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


@endpoint("/chain/gas/fee-history", "$0.005",
          "Base fee history for the last 10 blocks. $0.005 USDC per call.",
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


@endpoint("/chain/gas/priority-fee", "$0.005",
          "Current max priority fee on Base. $0.005 USDC per call.",
          "chain", ["chain", "gas", "priority"], "chain-priority-fee")
async def chain_priority_fee():
    fee = w3.eth.max_priority_fee
    return {
        "max_priority_fee_wei": fee,
        "max_priority_fee_gwei": round(Web3.from_wei(fee, "gwei"), 4),
        "network": "base",
    }


@endpoint("/chain/gas/estimate-transfer/{to}", "$0.005",
          "Estimated gas for a plain ETH transfer on Base. $0.005 USDC per call.",
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


@endpoint("/chain/gas/oracle", "$0.005",
          "Combined gas oracle: gas price, priority fee, fee history. $0.005 USDC per call.",
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


@endpoint("/chain/stats", "$0.005",
          "Live Base chain stats: block, gas, chain id, client. $0.005 USDC per call.",
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


@endpoint("/chain/syncing", "$0.001",
          "Whether the Base node is syncing (from latest block age). $0.001 USDC per call.",
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


@endpoint("/chain/client", "$0.001",
          "Client version of the Base RPC node. $0.001 USDC per call.",
          "chain", ["chain", "client"], "chain-client")
async def chain_client():
    return {"client_version": w3.client_version, "network": "base"}


@endpoint("/chain/network", "$0.001",
          "Chain id and latest block for Base. $0.001 USDC per call.",
          "chain", ["chain", "network"], "chain-network")
async def chain_network():
    return {"chain_id": w3.eth.chain_id, "network": "base", "block_number": w3.eth.block_number}


@endpoint("/chain/address/{address}/code", "$0.005",
          "Bytecode for a Base address, with size and hash. $0.005 USDC per call.",
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


@endpoint("/nfts/{collection}/owner/{tokenId}", "$0.005",
          "Owner of one NFT by token id. $0.005 USDC per call.",
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


@endpoint("/nfts/{collection}/balance/{wallet}", "$0.005",
          "How many NFTs of a collection a wallet holds. $0.005 USDC per call.",
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


@endpoint("/nfts/{collection}/supply", "$0.005",
          "Total supply of an enumerable NFT collection. $0.005 USDC per call.",
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


@endpoint("/nfts/{collection}/exists/{tokenId}", "$0.005",
          "Whether an NFT token id exists. $0.005 USDC per call.",
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

@endpoint("/defi/tvl", "$0.005",
          "Total value locked on Base. $0.005 USDC per call.",
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

@endpoint("/ens/resolve/{name}", "$0.005",
          "Resolve an ENS name to its address. $0.005 USDC per call.",
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


@endpoint("/ens/reverse/{address}", "$0.005",
          "Reverse-resolve a wallet address to its ENS name. $0.005 USDC per call.",
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


@endpoint("/ens/avatar/{name}", "$0.005",
          "Avatar URL for an ENS name. $0.005 USDC per call.",
          "naming", ["ens", "avatar"], "ens-avatar")
async def ens_avatar(name: str):
    data = await _get_json(f"{ENSIDEAS}/ens/resolve/{name}")
    if isinstance(data, dict) and "error" in data:
        return data
    if not isinstance(data, dict) or not data.get("address"):
        return {"error": "ENS name not found", "name": name}
    return {"name": name, "address": data.get("address"), "avatar": data.get("avatar")}


@endpoint("/ens/profile/{name}", "$0.005",
          "Full public profile for an ENS name. $0.005 USDC per call.",
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

@endpoint("/utils/eth-usd", "$0.005",
          "Live ETH price in USD. $0.005 USDC per call.",
          "utils", ["utils", "eth", "price"], "utils-eth-usd")
async def utils_eth_usd():
    price = await _eth_price_usd()
    if price is None:
        return {"error": "could not fetch ETH price"}
    return {"asset": "ETH", "price_usd": price}


@endpoint("/utils/convert/eth-usd/{amount}", "$0.005",
          "Convert an ETH amount to USD at the live price. $0.005 USDC per call.",
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


@endpoint("/utils/convert/usd-eth/{amount}", "$0.005",
          "Convert a USD amount to ETH at the live price. $0.005 USDC per call.",
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


@endpoint("/utils/convert/wei/{amount}", "$0.001",
          "Convert wei to ether and gwei. $0.001 USDC per call.",
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


@endpoint("/utils/convert/ether-wei/{amount}", "$0.001",
          "Convert ether to wei. $0.001 USDC per call.",
          "utils", ["utils", "convert", "ether"], "utils-convert-ether-wei")
async def utils_convert_ether_wei(amount: str):
    try:
        eth = float(amount)
    except ValueError:
        return {"error": "amount must be a number", "amount": amount}
    return {"ether": eth, "wei": Web3.to_wei(eth, "ether")}


@endpoint("/utils/address/validate/{address}", "$0.001",
          "Validate an Ethereum address and return its checksum form. $0.001 USDC per call.",
          "utils", ["utils", "address", "validate"], "utils-address-validate")
async def utils_address_validate(address: str):
    valid = Web3.is_address(address)
    result = {"address": address, "is_valid": valid}
    if valid:
        result["checksum_address"] = Web3.to_checksum_address(address)
        result["is_checksum"] = (address == result["checksum_address"])
    return result


@endpoint("/utils/address/keccak/{text}", "$0.001",
          "Keccak-256 hash of text. $0.001 USDC per call.",
          "utils", ["utils", "hash", "keccak"], "utils-keccak")
async def utils_keccak(text: str):
    return {"input": text, "keccak256": Web3.keccak(text=text).hex()}


@endpoint("/utils/gas-cost/{gwei}/{gas}", "$0.005",
          "Cost of a transaction in ETH and USD at the live ETH price. $0.005 USDC per call.",
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


@endpoint("/utils/coin-price/{coin}", "$0.005",
          "Live USD price for a coin by CoinGecko id. $0.005 USDC per call.",
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


@endpoint("/utils/market/global", "$0.005",
          "Global crypto market stats from CoinGecko. $0.005 USDC per call.",
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


@endpoint("/utils/market/trending", "$0.005",
          "Trending coins on CoinGecko. $0.005 USDC per call.",
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


@endpoint("/utils/market/top", "$0.005",
          "Top 10 cryptocurrencies by market cap. $0.005 USDC per call.",
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


@endpoint("/utils/fear-greed", "$0.005",
          "Crypto Fear & Greed Index. $0.005 USDC per call.",
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


def _resource_for(path: str) -> str:
    """v3 convention: the resource is the path up to the first parameter."""
    idx = path.find("/{")
    return path if idx == -1 else path[:idx]


# x402 payment config, built from the same registry as the routes.
routes = {}
for _e in ORDERED:
    routes[f"GET {_middleware_pattern(_e['path'])}"] = {
        "accepts": {
            "scheme": "exact",
            "payTo": WALLET_ADDRESS,
            "price": _e["price"],
            "network": "eip155:8453",
        },
        "resource": _resource_for(_e["path"]),
        "description": _e["description"],
        "mimeType": "application/json",
    }

# Create FastAPI app
app = FastAPI(title="Manteclaw x402 service", version="4.0")

# Serve the discovery + verification files for free (no payment):
# /.well-known/x402.json, /.well-known/agent-catalog.json, and
# /.well-known/aipaygen-verify.txt (AiPayGen site verification).
# These MUST be public — agents and directories read them unpaid.
_WELL_KNOWN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".well-known")
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
HEALTH_PATHS = {"/health"}


@app.middleware("http")
async def x402_middleware(request: Request, call_next):
    if request.url.path in HEALTH_PATHS or request.url.path.startswith("/.well-known/"):
        return await call_next(request)
    return await x402_mw(request, call_next)


# ===== HEALTH CHECK (no payment required) =====

@app.get("/health")
async def health():
    """Liveness probe. No payment, no on-chain call, no facilitator call."""
    return {
        "status": "ok",
        "service": "x402-service",
        "version": "4.0",
        "network": "base",
        "payee": WALLET_ADDRESS,
        "rpc_connected": w3.is_connected(),
        "endpoints": len(ORDERED) + 1,  # 99 paid + /health = 100
    }


# Register every paid route from the registry.
for _e in ORDERED:
    app.add_api_route(_e["path"], _e["handler"], methods=["GET"])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
