# Manteclaw x402 service — Solana settlement variant

The Solana-settled twin of the Base x402 service (`~/workspace/x402-service/`,
`server-v5.py`). Built 2026-11-22 by copying the Base build and changing
only the payment settlement; not yet deployed (the Fly app
`manteclaw-x402-solana` exists, empty).

## What is the same

- The full API surface: 199 paid endpoints + free `/health`, `/catalog`,
  `/` and `/.well-known/*` discovery files. Paths, handlers and data are
  byte-for-byte the Base service's — the endpoints serve Base/EVM data
  (Base RPC, Dexscreener, DeFiLlama, CoinGecko, ENS APIs).
- The price map, mirrored exactly from the Base REGISTRY: 193 endpoints
  at $0.01 and 6 at $0.02.
- The facilitator: PayAI, `https://facilitator.payai.network`.
- Stack: FastAPI + x402 python SDK 2.9.0, Fly.io sizing identical to the
  Base app (iad, shared-cpu-1x, 256MB, always-on).

## What differs

| Aspect | Base service | This variant |
| --- | --- | --- |
| Payment network | Base (`eip155:8453`) | Solana mainnet (`solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp`) |
| Scheme class | `ExactEvmServerScheme` | `ExactSvmServerScheme` (x402 `mechanisms.svm.exact`) |
| Payment asset | Base USDC `0x833589…02913` | Solana USDC mint `EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v` (6 decimals) |
| payTo | Alex's main EVM wallet (env `WALLET_ADDRESS`) | Alex's Solana payout wallet `Bkd6W9THp8qKgCYcamEkYx4Rk6TTHkUnRsegMaDZXwz2` (env `SOLANA_WALLET_ADDRESS`, receive-only) |
| Facilitator URL | hardcoded PayAI | env `FACILITATOR_URL`, default PayAI |
| Facilitator routing | PayAI + parked CDP dual-facilitator experiment | PayAI only — the CDP experiment is deliberately excluded |
| Origin | `https://manteclaw-x402.fly.dev` | `https://manteclaw-x402-solana.fly.dev` |
| Catalog page | in-page EVM wallet Pay & Call | Pay & Call shows the endpoint's live Solana 402 terms (browser EVM wallets cannot sign Solana payments) |
| Discovery files | Base wallet/network; stale pre-v5 prices in places | regenerated from the live REGISTRY: Solana wallet/network/mint, true $0.01/$0.02 prices |

Settling on Solana while serving Base data is normal for x402: the 402
terms name the payment network, asset and payTo; the payload is
independent of the payment rail.

## Verified before handoff (2026-11-22)

- **SDK**: x402 2.9.0 (the Base pin) ships the SVM exact server scheme.
  Its built-in USDC address for the Solana mainnet network id is exactly
  the mint above, and `parse_price("$0.01")` → `10000`. One trap:
  `x402[svm]` declares only `solana>=0.36.0`; a fresh resolve picks
  solana 0.42.0, which removed `solana.rpc.api` and breaks the SVM
  import, while solana 0.36.7 conflicts with the extra's `solders>=0.27`.
  `requirements.txt` therefore pins `solana==0.39.0` (inside the SDK's
  declared range; solders resolves to 0.27.x). No stack upgrade.
- **PayAI**: `GET https://facilitator.payai.network/supported` lists
  `{x402Version: 2, scheme: "exact", network:
  "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp", extra: {feePayer:
  "CjNFTjvBhbJJd2B5ePPMHRLx1ELZpa8dwQgGL727eKww", …}}`, and its Solana
  batch-settlement entry names the USDC mint above as the asset.
- **Local tests** (`test-solana.py`, log in `test-output.log`): server
  starts; `/health` 200 with `"network": "solana"` and the Solana payee;
  `/catalog` 200 and says USDC on Solana; unpaid calls to a $0.01 and a
  $0.02 endpoint return 402s whose decoded `payment-required` header
  names the Solana network, the USDC mint, the Solana payTo, amounts
  `10000`/`20000`, and PayAI's real feePayer in `extra`.

## Deploy (when approved — not done here)

Push this directory as its own repo, add the `FLY_API_TOKEN` secret, and
the GitHub Actions workflow (`.github/workflows/deploy.yml`) runs
`flyctl deploy --remote-only` against the existing empty Fly app
`manteclaw-x402-solana`. No Fly secrets are required: the payTo and
facilitator defaults are in code, overridable via `SOLANA_WALLET_ADDRESS`
/ `FACILITATOR_URL` / `ALCHEMY_BASE_RPC` / `PORT` (see `.env.example`).
