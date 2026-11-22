# How to call the Manteclaw x402 API

Base URL: `https://manteclaw-x402.fly.dev`
200 endpoints (199 paid + free `/health`). Full list with prices:
`https://manteclaw-x402.fly.dev/.well-known/agent-catalog.json`
(Cheapest call to test with: `GET /mcp/status` — $0.001 USDC.)

## How it works (x402 v2)

1. You `GET` an endpoint. It answers **HTTP 402 Payment Required**.
2. The `payment-required` header carries the price (base64 JSON): the amount in
   USDC, the network (`eip155:8453` = Base), the token
   (`0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913` = USDC on Base), and the
   `payTo` wallet. The same JSON is also the response body, unencoded.
   Example — `/defi/gas-tracker` costs `5000` = **$0.005 USDC**.
3. Your client signs an EIP-3009 `transferWithAuthorization` for that amount
   and retries the same request with the payment in the `PAYMENT-SIGNATURE`
   header.
4. The server verifies the payment and returns the data as JSON.

You need **USDC on Base** in a wallet you control. The payment signature is
free (off-chain) — you do not need ETH for gas; the facilitator submits the
transfer. Prices are $0.001–$0.02 per call.

## TypeScript (recommended — the x402 SDK does steps 2–4 for you)

```bash
npm install x402-fetch viem
```

```ts
import { createWalletClient, http } from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { base } from "viem/chains";
import { wrapFetchWithPayment } from "x402-fetch";

// Your wallet — needs USDC on Base. Never share the private key.
const account = privateKeyToAccount("0xYOUR_PRIVATE_KEY");
const wallet = createWalletClient({ account, chain: base, transport: http() });
const fetchWithPayment = wrapFetchWithPayment(fetch, wallet);

const res = await fetchWithPayment(
  "https://manteclaw-x402.fly.dev/defi/gas-tracker"
);
console.log(await res.json());
```

## Python (manual EIP-3009)

```bash
pip install eth-account requests
```

```python
import base64, json, secrets, time, requests
from eth_account import Account

BASE = "https://manteclaw-x402.fly.dev"
URL = BASE + "/defi/gas-tracker"
USDC = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"

acct = Account.from_key("0xYOUR_PRIVATE_KEY")

# 1. Unpaid request -> 402 with the payment terms in the header
r = requests.get(URL)
terms = json.loads(base64.b64decode(r.headers["payment-required"]))
a = terms["accepts"][0]  # the PaymentRequirements you are fulfilling

# 2. Sign an EIP-3009 transferWithAuthorization (USDC, version 2)
nonce = secrets.token_bytes(32)
valid_before = int(time.time()) + 300
auth = {
    "from": acct.address, "to": a["payTo"],
    "value": int(a["amount"]), "validAfter": 0,
    "validBefore": valid_before, "nonce": nonce,
}
domain = {"name": "USD Coin", "version": "2",
          "chainId": 8453, "verifyingContract": USDC}
types = {"TransferWithAuthorization": [
    {"name": "from", "type": "address"}, {"name": "to", "type": "address"},
    {"name": "value", "type": "uint256"}, {"name": "validAfter", "type": "uint256"},
    {"name": "validBefore", "type": "uint256"}, {"name": "nonce", "type": "bytes32"}]}
sig = acct.sign_typed_data(domain, types, auth)
sig_hex = sig.signature.hex()
if not sig_hex.startswith("0x"):
    sig_hex = "0x" + sig_hex

# 3. Retry with the payment in the PAYMENT-SIGNATURE header.
# x402 v2 payload: echo the accepted requirements back in "accepted"
# (there are no top-level "scheme"/"network" fields in v2), and send
# the authorization amounts/timestamps as strings.
payment = {"x402Version": 2, "accepted": a,
           "payload": {"signature": sig_hex,
                       "authorization": {
                           "from": acct.address, "to": a["payTo"],
                           "value": a["amount"], "validAfter": "0",
                           "validBefore": str(valid_before),
                           "nonce": "0x" + nonce.hex()}}}
r = requests.get(URL, headers={"PAYMENT-SIGNATURE":
    base64.b64encode(json.dumps(payment).encode()).decode()})
print(r.status_code, r.json())
```

## Notes

- Every endpoint is a `GET`. Parameterised ones take the value in the path,
  e.g. `GET /tokens/<address>/price` or `GET /wallets/<address>/balance`.
- `GET /health` is free (no payment). `GET /mcp/tools` ($0.001) lists all 199
  paid tools with their paths and prices.
- The payment is only valid for 300 seconds (`maxTimeoutSeconds`), so sign
  and send promptly. A payment is single-use — each call needs a fresh one.
- If you get a 402 again after sending payment, the signature was invalid or
  expired; sign a fresh one and retry.
