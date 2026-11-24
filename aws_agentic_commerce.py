"""AWS Marketplace Agentic Commerce scheme for the Manteclaw x402 service.

Added Nov 23, 2026, mirroring the parked Coinbase CDP build pattern in
server-v5.py: everything here is DORMANT unless server-v5.py calls
install_aws_scheme(), which it does only when the AWS_SCHEME_ENABLED
environment variable is set. With the flag unset this module is never
imported and the crypto x402 surface is byte-for-byte untouched.

What it does when enabled (AWS Marketplace Agentic Commerce, scope doc:
~/workspace/goals/increase-my-income/files/aws-agentic-commerce-scope.md):

* Serves our EC P-256 quote-signing public key as JWKS at
  /.well-known/jwks.json (written into the .well-known directory the app
  already mounts, plus an explicit route for good measure).
* For routes with a configured product_id (see aws-products.json), merges
  an AWS payment option into the crypto 402: an accepts entry on network
  aws:base, scheme exact, asset iso4217:USD, carrying
  extra.quoteToken = a JWT quote signed with our key (iss = this
  endpoint, aud = aws:marketplace, settlement.product_id = the route's
  product, price in USD from the live price map, short expiration).
  Routes whose product_id is still null are NEVER quoted (clean refusal:
  the crypto 402 goes out unchanged).
* Verifies AWS Marketplace payment tokens locally on the retry
  (PAYMENT-SIGNATURE header), records each payment in a persistent
  on-disk replay store (accept each payment only once, restarts do not
  reopen replay), checks DPoP nonces when a DPoP header is present, then
  flags the request scope so the crypto middleware lets it through.
  Settlement on our side is a no-op - AWS captured the payment.
* On success, sets the PAYMENT-RESPONSE header to the AWS Marketplace
  payment ID and logs it for disbursement reconciliation.
* An unpaid HEAD request to a configured route gets the 402 quote and
  never executes the paid handler.

Package integration: the fiat-extension-for-x402 PyPI package
(ExactAgenticCommerceServerScheme / facilitator-side verifier) is wired
through the two adapter functions register_aws_scheme() and
_package_verifier() below - the ONLY places that touch the package - so
the rest of the module is package-independent and unit-testable offline.
If the package is not importable, token verification falls back to local
JWKS verification against a configured key set (env
AWS_MARKETPLACE_JWKS_FILE, a JWKS JSON file with the token issuer's
public keys); in production that key set is AWS Marketplace's, in tests
it is a locally minted test key. Verification is always cryptographic -
there is no mode that accepts an unverifiable token.
"""

import base64
import hashlib
import json
import os
import re
import sys
import threading
import time

import jwt  # PyJWT
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

AWS_NETWORK = "aws:base"
AWS_SCHEME = "exact"
AWS_ASSET = "iso4217:USD"
AWS_AUDIENCE = "aws:marketplace"
QUOTE_TTL_SECONDS = int(os.environ.get("AWS_QUOTE_TTL_SECONDS", "900"))
MAX_QUOTE_TTL_SECONDS = 30 * 24 * 3600  # AWS cap: quote validity <= 30 days

_HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_KEY_PATH = os.environ.get(
    "AWS_QUOTE_KEY_PATH", os.path.join(_HERE, "aws-quote-key.pem"))
DEFAULT_MAP_PATH = os.environ.get(
    "AWS_PRODUCTS_MAP", os.path.join(_HERE, "aws-products.json"))
DEFAULT_REPLAY_PATH = os.environ.get(
    "AWS_REPLAY_STORE_PATH", os.path.join(_HERE, "aws-replay-store.json"))


def _log(msg):
    print(f"[aws] {msg}", file=sys.stderr)


def _b64u(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


class ProductNotConfigured(Exception):
    """Raised when a quote is requested for a route with no product_id."""


# ---------------------------------------------------------------------------
# Quote signing key + JWKS
# ---------------------------------------------------------------------------

class QuoteSigner:
    """EC P-256 quote-signing key. The private key never leaves the
    key file (chmod 600); only the public JWK is exposed."""

    def __init__(self, key_path=DEFAULT_KEY_PATH):
        with open(key_path, "rb") as fh:
            self._key = serialization.load_pem_private_key(
                fh.read(), password=None)
        if not isinstance(self._key, ec.EllipticCurvePrivateKey):
            raise ValueError("quote key is not an EC key")
        if self._key.curve.name != "secp256r1":
            raise ValueError("quote key is not P-256")
        pub = self._key.public_key().public_numbers()
        self._jwk = {
            "kty": "EC",
            "crv": "P-256",
            "x": _b64u(pub.x.to_bytes(32, "big")),
            "y": _b64u(pub.y.to_bytes(32, "big")),
            "use": "sig",
            "alg": "ES256",
        }
        # Stable kid: RFC 7638 JWK thumbprint of the public key.
        thumb_input = json.dumps(
            {k: self._jwk[k] for k in ("crv", "kty", "x", "y")},
            separators=(",", ":"), sort_keys=True).encode()
        self.kid = _b64u(hashlib.sha256(thumb_input).digest())
        self._jwk["kid"] = self.kid

    def jwks_document(self):
        return {"keys": [dict(self._jwk)]}

    def sign_quote(self, claims):
        return jwt.encode(
            claims, self._key, algorithm="ES256",
            headers={"kid": self.kid})


# ---------------------------------------------------------------------------
# Route -> product_id map (config-driven; fill the map, no code change)
# ---------------------------------------------------------------------------

class ProductMap:
    """Loads aws-products.json: {"routes": {"GET /path/{param}":
    {"product_id": str|null, "price_usd": number}}}."""

    def __init__(self, map_path=DEFAULT_MAP_PATH):
        with open(map_path) as fh:
            doc = json.load(fh)
        self.routes = doc.get("routes", {})
        self._matchers = []
        for key, entry in self.routes.items():
            method, _, template = key.partition(" ")
            pattern = ""
            for part in template.strip("/").split("/"):
                if part.startswith("{") and part.endswith("}"):
                    pattern += r"/[^/]+"
                else:
                    pattern += "/" + re.escape(part)
            self._matchers.append(
                (method.upper(), re.compile("^" + pattern + "$"), entry))

    def lookup(self, method, path):
        """Return the map entry for a concrete request path, or None."""
        for m, rx, entry in self._matchers:
            if m == method.upper() and rx.match(path):
                return entry
        return None

    def __len__(self):
        return len(self.routes)


# ---------------------------------------------------------------------------
# Persistent replay store (payments + DPoP nonces), on disk, atomic writes
# ---------------------------------------------------------------------------

class ReplayStore:
    """Accept-each-payment-only-once store. JSON file on disk so a
    restart does not reopen replay; writes are tmp-file + rename."""

    def __init__(self, path=DEFAULT_REPLAY_PATH):
        self._path = path
        self._lock = threading.Lock()
        self._data = {"payments": {}, "nonces": {}}
        if os.path.exists(path):
            try:
                with open(path) as fh:
                    loaded = json.load(fh)
                if isinstance(loaded, dict):
                    self._data.update(loaded)
            except Exception as exc:  # corrupt store must not kill service
                _log(f"replay store unreadable, starting empty: {exc}")

    def _save_locked(self):
        tmp = self._path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(self._data, fh)
        os.replace(tmp, self._path)

    def seen_payment(self, payment_id):
        with self._lock:
            return payment_id in self._data["payments"]

    def record_payment(self, payment_id, meta=None):
        """Record a payment. Returns False if it was already recorded
        (replay), True if this is the first acceptance."""
        with self._lock:
            if payment_id in self._data["payments"]:
                return False
            record = {"ts": int(time.time())}
            if meta:
                record.update(meta)
            self._data["payments"][payment_id] = record
            self._save_locked()
            return True

    def check_and_record_nonce(self, nonce):
        """DPoP nonce: True if fresh (and now recorded), False if seen."""
        with self._lock:
            if nonce in self._data["nonces"]:
                return False
            self._data["nonces"][nonce] = int(time.time())
            self._save_locked()
            return True


# ---------------------------------------------------------------------------
# Quote construction
# ---------------------------------------------------------------------------

def build_quote_claims(entry, resource_url, issuer, now=None):
    product_id = entry.get("product_id")
    if not product_id:
        raise ProductNotConfigured(
            "route has no product_id configured; refusing to quote")
    now = int(now if now is not None else time.time())
    ttl = min(QUOTE_TTL_SECONDS, MAX_QUOTE_TTL_SECONDS)
    return {
        "iss": issuer,
        "aud": AWS_AUDIENCE,
        "iat": now,
        "exp": now + ttl,
        "scheme": AWS_SCHEME,
        "network": AWS_NETWORK,
        "asset": AWS_ASSET,
        "currency": "USD",
        "amount": f"{float(entry['price_usd']):.2f}",
        "resource": resource_url,
        "settlement": {"product_id": product_id},
    }


def build_accepts_entry(entry, resource_url, issuer, signer):
    """The aws:base entry for a 402 accepts array, with the signed
    quote in extra.quoteToken. Raises ProductNotConfigured for a route
    whose product_id is null."""
    claims = build_quote_claims(entry, resource_url, issuer)
    return {
        "scheme": AWS_SCHEME,
        "network": AWS_NETWORK,
        "asset": AWS_ASSET,
        "amount": claims["amount"],
        "payTo": claims["settlement"]["product_id"],
        "maxTimeoutSeconds": claims["exp"] - claims["iat"],
        "extra": {"quoteToken": signer.sign_quote(claims)},
    }


# ---------------------------------------------------------------------------
# Payment-token verification (package adapter + local JWKS verification)
# ---------------------------------------------------------------------------

def _package_verifier():
    """Return the fiat-extension-for-x402 verifier if the package is
    importable, else None. One of only two package touchpoints."""
    try:
        import fiat_extension_for_x402 as pkg
    except Exception:
        return None
    for name in ("AgenticCommerceVerifier",
                 "ExactAgenticCommerceFacilitatorScheme"):
        cls = getattr(pkg, name, None)
        if cls is not None:
            try:
                return cls()
            except Exception as exc:
                _log(f"package verifier {name} not constructible: {exc}")
    return None


def _jwk_to_ec_key(jwk):
    from cryptography.hazmat.primitives.asymmetric.ec import (
        SECP256R1, SECP384R1, SECP521R1, EllipticCurvePublicNumbers)

    def dec(value):
        return int.from_bytes(
            base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)),
            "big")

    curve = {"P-256": SECP256R1(), "P-384": SECP384R1(),
             "P-521": SECP521R1()}[jwk["crv"]]
    return EllipticCurvePublicNumbers(
        dec(jwk["x"]), dec(jwk["y"]), curve).public_key()


class PaymentTokenVerifier:
    """Verifies AWS Marketplace payment tokens.

    Order: (1) the fiat-extension-for-x402 package verifier when the
    package is installed; (2) local JWKS verification against the key
    set in AWS_MARKETPLACE_JWKS_FILE. Both paths are cryptographic.
    """

    def __init__(self, jwks_file=None, package_verifier=None):
        self._package = (package_verifier if package_verifier is not None
                         else _package_verifier())
        self._keys = []
        jwks_file = jwks_file or os.environ.get("AWS_MARKETPLACE_JWKS_FILE")
        if jwks_file and os.path.exists(jwks_file):
            with open(jwks_file) as fh:
                doc = json.load(fh)
            for jwk in doc.get("keys", []):
                self._keys.append((jwk.get("kid"), _jwk_to_ec_key(jwk)))

    @property
    def package_backed(self):
        return self._package is not None

    def verify(self, token):
        """Return the token's claims dict, with the payment id under
        '_payment_id'. Raises ValueError on any verification failure."""
        if self._package is not None:
            result = self._package_verify(token)
            if result is not None:
                return result
        if not self._keys:
            raise ValueError(
                "no payment-token verification keys configured "
                "(fiat-extension-for-x402 not installed and "
                "AWS_MARKETPLACE_JWKS_FILE not set)")
        last_exc = None
        for _kid, key in self._keys:
            try:
                claims = jwt.decode(
                    token, key, algorithms=["ES256", "ES384", "ES512"],
                    options={"require": ["exp"]})
                pid = (claims.get("payment_id") or claims.get("paymentId")
                       or claims.get("jti"))
                if not pid:
                    raise ValueError("payment token carries no payment id")
                claims["_payment_id"] = pid
                return claims
            except Exception as exc:
                last_exc = exc
        raise ValueError(f"payment token verification failed: {last_exc}")

    def _package_verify(self, token):
        verify = getattr(self._package, "verify", None)
        if verify is None:
            return None
        try:
            result = verify(token)
        except TypeError:
            try:
                result = verify(payload=token)
            except Exception:
                return None
        except Exception:
            return None
        if isinstance(result, dict):
            pid = (result.get("payment_id") or result.get("paymentId")
                   or result.get("jti"))
            if pid:
                result = dict(result)
                result["_payment_id"] = pid
                return result
        return None


# ---------------------------------------------------------------------------
# Package scheme registration (second and last package touchpoint)
# ---------------------------------------------------------------------------

def register_aws_scheme(resource_server):
    """Register ExactAgenticCommerceServerScheme on aws:base with the
    x402 resource server, when the fiat package is available. Returns
    True on success; logs and returns False otherwise (never raises -
    a broken AWS setup must not kill the crypto service)."""
    try:
        import fiat_extension_for_x402 as pkg
        scheme_cls = getattr(pkg, "ExactAgenticCommerceServerScheme", None)
        if scheme_cls is None:
            _log("fiat-extension-for-x402 has no "
                 "ExactAgenticCommerceServerScheme; scheme not registered")
            return False
        resource_server.register(network=AWS_NETWORK, server=scheme_cls())
        _log("registered ExactAgenticCommerceServerScheme on aws:base")
        return True
    except Exception as exc:
        _log(f"AWS scheme registration skipped: {exc}")
        return False


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

def _json_402(terms, error=None):
    from fastapi.responses import JSONResponse

    body = dict(terms)
    if error:
        body["error"] = error
    header = base64.b64encode(json.dumps(terms).encode()).decode()
    return JSONResponse(
        content=body, status_code=402,
        headers={"payment-required": header})


class AwsAgenticCommerceMiddleware:
    """Pure-ASGI middleware: merges AWS quotes into 402s for
    configured routes and verifies AWS payment tokens before the
    crypto middleware runs."""

    def __init__(self, app, *, product_map, signer, verifier,
                 replay_store, public_origin):
        self.app = app
        self.product_map = product_map
        self.signer = signer
        self.verifier = verifier
        self.replay_store = replay_store
        self.public_origin = public_origin.rstrip("/")
        self._rate_lock = threading.Lock()
        self._rate_window = []  # timestamps of AWS-paid requests (1 min)

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        from starlette.requests import Request

        request = Request(scope, receive)
        # HEAD carries the payment config of its GET route (server-v5
        # registers HEAD twins of every paid GET), so match the map
        # under GET for HEAD requests.
        lookup_method = ("GET" if request.method == "HEAD"
                         else request.method)
        entry = self.product_map.lookup(lookup_method,
                                        request.url.path)
        if entry is None:
            await self.app(scope, receive, send)
            return

        resource_url = self.public_origin + request.url.path
        payment_token = request.headers.get("payment-signature")

        if payment_token:
            await self._handle_paid(scope, receive, send, request,
                                    entry, payment_token, resource_url)
            return

        if request.method == "HEAD":
            # An unpaid HEAD must never execute a paid route: answer
            # with the quote (or, for an unconfigured route, a bare
            # 402) without calling the app.
            terms = self._base_terms(request)
            if entry.get("product_id"):
                terms["accepts"].append(build_accepts_entry(
                    entry, resource_url, self.public_origin,
                    self.signer))
            response = _json_402(terms)
            await response(scope, receive, send)
            return

        # Unpaid: run the app (crypto middleware answers 402), then
        # merge the AWS accepts entry into that 402 for configured
        # routes. Unconfigured routes pass through untouched.
        if not entry.get("product_id"):
            await self.app(scope, receive, send)
            return
        captured = {}

        async def send_capture(message):
            if message["type"] == "http.response.start":
                captured["start"] = message
                return
            if message["type"] == "http.response.body":
                captured.setdefault("body", b"")
                captured["body"] += message.get("body", b"")
                if not message.get("more_body", False):
                    await self._send_merged(captured, entry,
                                            resource_url, send)
                return
            await send(message)

        await self.app(scope, receive, send_capture)

    def _base_terms(self, request):
        return {
            "x402Version": 2,
            "resource": {
                "url": self.public_origin + request.url.path,
                "mimeType": "application/json",
            },
            "accepts": [],
        }

    async def _send_merged(self, captured, entry, resource_url, send):
        start = captured["start"]
        body = captured.get("body", b"")
        if start["status"] != 402:
            await send(start)
            await send({"type": "http.response.body", "body": body})
            return
        headers = {k.decode().lower(): v.decode()
                   for k, v in start.get("headers", [])}
        terms = None
        if "payment-required" in headers:
            try:
                terms = json.loads(base64.b64decode(
                    headers["payment-required"]))
            except Exception:
                terms = None
        if terms is None and body.strip() not in (b"", b"{}"):
            try:
                terms = json.loads(body)
            except Exception:
                terms = None
        if not isinstance(terms, dict) or "accepts" not in terms:
            await send(start)
            await send({"type": "http.response.body", "body": body})
            return
        terms = dict(terms)
        terms["accepts"] = list(terms["accepts"]) + [
            build_accepts_entry(entry, resource_url, self.public_origin,
                                self.signer)]
        new_body = json.dumps(terms).encode()
        new_header = base64.b64encode(new_body).decode()
        raw_headers = [
            (k, v) for k, v in start.get("headers", [])
            if k.decode().lower() not in (
                "content-length", "payment-required", "content-type")
        ]
        raw_headers.append((b"payment-required", new_header.encode()))
        raw_headers.append((b"content-type", b"application/json"))
        raw_headers.append((b"content-length",
                            str(len(new_body)).encode()))
        start = dict(start)
        start["headers"] = raw_headers
        await send(start)
        await send({"type": "http.response.body", "body": new_body})

    async def _handle_paid(self, scope, receive, send, request, entry,
                           payment_token, resource_url):
        terms = self._base_terms(request)
        if entry.get("product_id"):
            terms["accepts"].append(build_accepts_entry(
                entry, resource_url, self.public_origin, self.signer))

        async def reject(error):
            response = _json_402(terms, error=error)
            await response(scope, receive, send)

        # DPoP: if the buyer sent a DPoP proof, its nonce must be fresh.
        dpop = request.headers.get("dpop")
        if dpop:
            try:
                proof = jwt.decode(
                    dpop, options={"verify_signature": False})
            except Exception as exc:
                await reject(f"invalid dpop proof: {exc}")
                return
            nonce = proof.get("nonce")
            if not nonce:
                await reject("dpop proof carries no nonce")
                return
            if not self.replay_store.check_and_record_nonce(nonce):
                await reject("dpop nonce replay")
                return

        try:
            claims = self.verifier.verify(payment_token)
        except Exception as exc:
            await reject(f"payment verification failed: {exc}")
            return

        payment_id = claims["_payment_id"]
        if not self._throttle_ok():
            await reject("rate limited")
            return
        if not self.replay_store.record_payment(
                payment_id, {"path": request.url.path}):
            await reject("payment already used")
            return

        # Flag the scope: server-v5's x402 middleware checks this and
        # lets the request through to the handler (settlement is a
        # no-op on our side; AWS captured the payment).
        scope["aws_payment_verified"] = True
        scope["aws_payment_id"] = payment_id

        async def send_with_payment_response(message):
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.append((b"payment-response",
                                str(payment_id).encode()))
                message = dict(message)
                message["headers"] = headers
            await send(message)

        _log(f"AWS payment accepted: id={payment_id} "
             f"path={request.url.path}")
        await self.app(scope, receive, send_with_payment_response)

    def _throttle_ok(self):
        now = time.time()
        with self._rate_lock:
            self._rate_window = [
                t for t in self._rate_window if now - t < 60]
            if len(self._rate_window) >= 300:
                return False
            self._rate_window.append(now)
            return True


# ---------------------------------------------------------------------------
# Install (called from server-v5.py only when AWS_SCHEME_ENABLED is set)
# ---------------------------------------------------------------------------

def install_aws_scheme(app, *, public_origin, well_known_dir=None,
                       resource_server=None, key_path=None,
                       map_path=None, replay_path=None):
    """Wire the AWS scheme into the FastAPI app. Returns True on
    success; on any failure logs and returns False - a broken AWS
    setup must never take the crypto service down (CDP precedent)."""
    try:
        signer = QuoteSigner(key_path or DEFAULT_KEY_PATH)
        product_map = ProductMap(map_path or DEFAULT_MAP_PATH)
        replay_store = ReplayStore(replay_path or DEFAULT_REPLAY_PATH)
        verifier = PaymentTokenVerifier()

        jwks = signer.jwks_document()
        jwks_json = json.dumps(jwks)
        if len(jwks_json.encode()) > 5 * 1024:
            raise ValueError("JWKS document exceeds the 5 KB AWS limit")

        # Serve the JWKS two ways: an explicit route (registered here)
        # and the static file the app's existing /.well-known mount
        # serves (the mount shadows later routes, so the file is the
        # reliable path in the real app).
        from fastapi.responses import JSONResponse

        @app.get("/.well-known/jwks.json", include_in_schema=False)
        async def jwks_route():
            return JSONResponse(jwks)

        if well_known_dir and os.path.isdir(well_known_dir):
            with open(os.path.join(well_known_dir, "jwks.json"),
                      "w") as fh:
                fh.write(jwks_json)

        app.add_middleware(
            AwsAgenticCommerceMiddleware,
            product_map=product_map, signer=signer, verifier=verifier,
            replay_store=replay_store, public_origin=public_origin)

        if resource_server is not None:
            register_aws_scheme(resource_server)

        configured = sum(
            1 for e in product_map.routes.values() if e.get("product_id"))
        _log(f"Agentic Commerce scheme enabled: "
             f"{len(product_map)} routes in map, {configured} with a "
             f"product_id, kid={signer.kid}, token verification "
             f"{'package-backed' if verifier.package_backed else 'local JWKS'}")
        return True
    except Exception as exc:
        _log(f"Agentic Commerce disabled (init failed): {exc}")
        return False
