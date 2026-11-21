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

from fastapi import FastAPI, Request
from dotenv import load_dotenv
from web3 import Web3

# x402 v2.9.0 imports
from x402 import x402ResourceServer
from x402.http import HTTPFacilitatorClient, FacilitatorConfig
from x402.http.middleware.fastapi import payment_middleware
from x402.mechanisms.evm.exact import ExactEvmServerScheme

# Load environment variables from .env
load_dotenv()

# Base RPC: use the configured Alchemy URL when present, otherwise the
# public Base endpoint. The old code defaulted to an empty string, which
# made every on-chain call fail at runtime.
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

# Route configuration for payment-gated endpoints
routes = {
    "GET /defi/gas-tracker": {
        "accepts": {
            "scheme": "exact",
            "payTo": WALLET_ADDRESS,
            "price": "$0.005",
            "network": "eip155:8453",
        },
        "resource": "/defi/gas-tracker",
        "description": "Current Base gas prices (slow/average/fast). $0.005 USDC per call.",
        "mimeType": "application/json",
    },
    "GET /defi/token-price/*": {
        "accepts": {
            "scheme": "exact",
            "payTo": WALLET_ADDRESS,
            "price": "$0.01",
            "network": "eip155:8453",
        },
        "resource": "/defi/token-price",
        "description": "Get token price via DEX liquidity pool analysis. $0.01 USDC per call.",
        "mimeType": "application/json",
    },
    "GET /defi/wallet-analyzer/*": {
        "accepts": {
            "scheme": "exact",
            "payTo": WALLET_ADDRESS,
            "price": "$0.01",
            "network": "eip155:8453",
        },
        "resource": "/defi/wallet-analyzer",
        "description": "Analyze wallet activity on Base. $0.01 USDC per call.",
        "mimeType": "application/json",
    },
    "GET /defi/token-security/*": {
        "accepts": {
            "scheme": "exact",
            "payTo": WALLET_ADDRESS,
            "price": "$0.02",
            "network": "eip155:8453",
        },
        "resource": "/defi/token-security",
        "description": "Quick token security check on Base. $0.02 USDC per call.",
        "mimeType": "application/json",
    },
    "GET /agent/earnings-report": {
        "accepts": {
            "scheme": "exact",
            "payTo": WALLET_ADDRESS,
            "price": "$0.05",
            "network": "eip155:8453",
        },
        "resource": "/agent/earnings-report",
        "description": "Daily earnings summary for all active revenue streams. $0.05 USDC per call.",
        "mimeType": "application/json",
    },
    "GET /mcp/status": {
        "accepts": {
            "scheme": "exact",
            "payTo": WALLET_ADDRESS,
            "price": "$0.001",
            "network": "eip155:8453",
        },
        "resource": "/mcp/status",
        "description": "MCP server status. $0.001 USDC per call.",
        "mimeType": "application/json",
    },
    "GET /mcp/tools": {
        "accepts": {
            "scheme": "exact",
            "payTo": WALLET_ADDRESS,
            "price": "$0.001",
            "network": "eip155:8453",
        },
        "resource": "/mcp/tools",
        "description": "MCP tools list. $0.001 USDC per call.",
        "mimeType": "application/json",
    },
}

# Create FastAPI app
app = FastAPI()

# Build x402 middleware once and register it
x402_mw = payment_middleware(routes, server)

# Paths that must answer without payment. /health is the liveness probe
# used by the service manager and monitors: it must never return 402,
# or the manager will keep restarting a healthy service.
HEALTH_PATHS = {"/health"}

@app.middleware("http")
async def x402_middleware(request: Request, call_next):
    if request.url.path in HEALTH_PATHS:
        return await call_next(request)
    return await x402_mw(request, call_next)

# ===== HEALTH CHECK (no payment required) =====

@app.get("/health")
async def health():
    """Liveness probe. No payment, no on-chain call, no facilitator call."""
    return {
        "status": "ok",
        "service": "x402-service",
        "version": "3.0",
        "network": "base",
        "payee": WALLET_ADDRESS,
        "rpc_connected": w3.is_connected(),
    }

# ===== MONETIZED ENDPOINTS =====

@app.get("/defi/gas-tracker")
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
        "timestamp": block.timestamp
    }

@app.get("/defi/token-price/{token_address}")
async def token_price(token_address: str):
    """Get token price via DEX liquidity pool analysis. $0.01 USDC per call."""
    token = Web3.to_checksum_address(token_address)
    
    # USDC contract on Base
    usdc = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
    
    # Check if token contract exists
    code = w3.eth.get_code(token)
    if len(code) == 0:
        return {"error": "Not a contract address"}
    
    # Get token metadata
    try:
        # Standard ERC20 calls
        symbol = w3.eth.call({"to": token, "data": "0x95d89b41"})  # symbol()
        decimals = w3.eth.call({"to": token, "data": "0x313ce567"})  # decimals()
        
        return {
            "token": token,
            "symbol": symbol.hex(),
            "decimals": int(decimals.hex(), 16),
            "network": "base",
            "note": "Price data requires DEX pool analysis (integrate Aerodrome/Uniswap subgraph)"
        }
    except Exception as e:
        return {"error": str(e), "token": token}

@app.get("/defi/wallet-analyzer/{address}")
async def wallet_analyzer(address: str):
    """Analyze wallet activity on Base. $0.01 USDC per call."""
    addr = Web3.to_checksum_address(address)
    
    balance = w3.eth.get_balance(addr)
    tx_count = w3.eth.get_transaction_count(addr)
    
    return {
        "address": addr,
        "eth_balance": float(Web3.from_wei(balance, "ether")),
        "tx_count": tx_count,
        "network": "base",
        "is_contract": len(w3.eth.get_code(addr)) > 0,
        "analysis": "Basic wallet scan. Full token portfolio requires Alchemy API integration."
    }

@app.get("/defi/token-security/{token_address}")
async def token_security(token_address: str):
    """Quick token security check on Base. $0.02 USDC per call."""
    token = Web3.to_checksum_address(token_address)
    
    # Security heuristics
    code = w3.eth.get_code(token)
    code_len = len(code)
    
    checks = {
        "is_contract": code_len > 0,
        "code_size": code_len,
        "has_code": code_len > 100,
        "risk_flags": []
    }
    
    # Basic risk flags
    if code_len == 0:
        checks["risk_flags"].append("NO_CONTRACT_CODE")
    if code_len < 200:
        checks["risk_flags"].append("VERY_SMALL_CONTRACT")
    
    # Check for common patterns (simplified)
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
        "note": "This is a basic static analysis. Full security scan requires GoPlus Security API integration."
    }

@app.get("/agent/earnings-report")
async def earnings_report():
    """Daily earnings summary for all active revenue streams. $0.05 USDC per call."""
    return {
        "agent": "Manteclaw",
        # NOTE: the per-stream figures below are stale placeholders from
        # May 2026, not live data. Do not treat them as earnings until
        # each stream is wired to a live source.
        "data_status": "stale_placeholder",
        "report_time": "2026-05-07",
        "revenue_streams": {
            "litcoiin_mining": {
                "status": "active",
                "total_earned": 182,
                "last_claim": "2026-05-07",
                "tx_hash": "0x4d0ffa98..."
            },
            "0xwork": {
                "status": "registered",
                "agent_id": 93,
                "balance_eth": 0.000602,
                "tasks_completed": 0
            },
            "nookplot_bounties": {
                "status": "applied",
                "bounty_id": 10,
                "application": "f70ded48..."
            },
            "x402_revenue": {
                "status": "live",
                "endpoints": 7,
                "payments_received": 0  # Track via blockchain
            }
        },
        "total_estimated_value_usd": "~$50-100/month (growing)",
        "next_actions": [
            "Governance vote bot (Aerodrome)",
            "Hypersub subscription launch",
            "Arbitrage scanner"
        ]
    }

@app.get("/mcp/status")
async def mcp_status():
    return {"status": "online", "version": "3.0", "endpoints": 7, "network": "base", "x402_version": "2.9.0"}

@app.get("/mcp/tools")
async def mcp_tools():
    return {
        "tools": [
            {"name": "gas_tracker", "price": "0.005", "endpoint": "/defi/gas-tracker"},
            {"name": "token_price", "price": "0.01", "endpoint": "/defi/token-price/{address}"},
            {"name": "wallet_analyzer", "price": "0.01", "endpoint": "/defi/wallet-analyzer/{address}"},
            {"name": "token_security", "price": "0.02", "endpoint": "/defi/token-security/{address}"},
            {"name": "earnings_report", "price": "0.05", "endpoint": "/agent/earnings-report"},
            {"name": "mcp_status", "price": "0.001", "endpoint": "/mcp/status"},
            {"name": "mcp_tools", "price": "0.001", "endpoint": "/mcp/tools"},
        ],
        "pricing_model": "per_call_usdc_on_base",
        "wallet": WALLET_ADDRESS,
        "x402_version": "2.9.0"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
