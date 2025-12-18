# security_basic_db.py
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from jose import jwt
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from pathlib import Path

import os
import logging

from dotenv import load_dotenv

log = logging.getLogger("auth")

ENV_PATH = Path.cwd() / ".env"
load_dotenv(dotenv_path=ENV_PATH)

# ====== CONFIG ======
KEYCLOAK_HOST = os.getenv("KEYCLOAK_HOST", "http://192.168.1.144:8080")
REALM = os.getenv("REALM", "Kafka_VL")

MYSQL_HOST = os.getenv("MYSQL_HOST", "192.168.1.144")
MYSQL_USER = os.getenv("MYSQL_USER", "homestead")
MYSQL_PASS = os.getenv("MYSQL_PASS", "secret")
MYSQL_DB   = os.getenv("MYSQL_DB",   "openhie_vl_20250312")

DATABASE_URL = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:3306/{MYSQL_DB}"


TLS_VERIFY = os.getenv("TLS_VERIFY", "false").lower() == "true"  # dev default False

# ====== CONSTANTS ======
ISSUER = "http://kafkahie.cphl.go.ug:8500/auth/realms/Kafka_VL"
TokenEndpoint_URL   = "http://127.0.0.1:8500/auth/realms/Kafka_VL/protocol/openid-connect/token"
JsonWebKeySet_URL   = "http://127.0.0.1:8500/auth/realms/Kafka_VL/protocol/openid-connect/certs"

basicAuth = HTTPBasic()

# ---- DB ----
engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

_cachedJsonWebKeySet = None
def _getJsonWebKeySet():
    """Fetch & cache the JSON Web Key Set (public keys for token verification)."""
    global _cachedJsonWebKeySet
    if _cachedJsonWebKeySet is None:
        _cachedJsonWebKeySet = requests.get(JsonWebKeySet_URL, timeout=5, verify=TLS_VERIFY).json()
    return _cachedJsonWebKeySet

def _decodeAndVerifyAccessToken(accessToken: str, expectedClientId: str):
    header = jwt.get_unverified_header(accessToken)
    jwks = _getJsonWebKeySet()
    key = next((k for k in jwks["keys"] if k.get("kid") == header.get("kid")), None)
    if not key:
        raise HTTPException(status_code=401, detail="Signing key not found (kid mismatch)")

    # Prefer x5c if present (Keycloak provides it). It’s a cert chain; index 0 is leaf cert.
    if "x5c" in key and key["x5c"]:
        cert_pem = "-----BEGIN CERTIFICATE-----\n" + key["x5c"][0] + "\n-----END CERTIFICATE-----"
        verification_key = cert_pem
    else:
        # Fallback: build JWK dict (n/e/kty) for python-jose
        required = ("n", "e", "kty")
        if not all(k in key for k in required):
            raise HTTPException(status_code=401, detail="No usable key material in JWKS")
        verification_key = {
            "kty": key["kty"],
            "n": key["n"],
            "e": key["e"],
            "kid": key.get("kid"),
            "alg": key.get("alg", "RS256"),
            "use": key.get("use", "sig"),
        }

    # Now verify
    claims = jwt.decode(
        accessToken,
        verification_key,            # <- certificate PEM or JWK
        algorithms=["RS256"],
        issuer=ISSUER,
        options={"verify_aud": False, "verify_exp": True},
    )

    # audience / azp check
    audience = claims.get("aud", [])
    if isinstance(audience, str):
        audience = [audience]
    authorizedParty = claims.get("azp")
    if expectedClientId not in audience and authorizedParty != expectedClientId:
        raise HTTPException(status_code=401, detail="Token audience/azp mismatch")

    return claims

def createBasicAuthWithApiTokenDependency(expectedClientId: str, clientSecret: str):
    """
    Dependency factory:
      1) Reads Basic auth (username/password) + api_token (header X-API-Token or ?api_token=)
      2) Validates credentials via Keycloak password grant (server-side)
      3) Looks up api_token in your MySQL users table by username (email OR name)
      4) Compares api_token and allows only on exact match
    """
    def _dependency(
        creds: HTTPBasicCredentials = Depends(basicAuth),
        request: Request = None,
    ):
        # 1) Extract api_token
        print (f".....1.....")
        t = request.headers.get("api_token")

        print(f"...api_token.....: {t}")

        t1 = request.headers.get("x-api-token")
        print(f"....x-api-token.....: {t1}")
        incomingApiToken = request.headers.get("api_token") if request else None
        if not incomingApiToken and request:
            incomingApiToken = request.query_params.get("api_token")
        if not incomingApiToken:
            raise HTTPException(status_code=400, detail="Missing api_token")
        if len(incomingApiToken) > 60:  # matches your varchar(60)
            raise HTTPException(status_code=400, detail="api_token length invalid")

        # 2) Validate username/password with Keycloak
        form = {
            "grant_type": "password",
            "client_id": expectedClientId,
            "client_secret": clientSecret,
            "username": creds.username,
            "password": creds.password,
        }
        try:
            kcResponse = requests.post(TokenEndpoint_URL, data=form, timeout=8, verify=TLS_VERIFY)
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Keycloak unreachable: {e}")

        if kcResponse.status_code != 200:
            raise HTTPException(status_code=401, detail="Invalid credentials")

        accessToken = kcResponse.json().get("access_token")
        if not accessToken:
            raise HTTPException(status_code=401, detail="Token not issued")

        claims = _decodeAndVerifyAccessToken(accessToken, expectedClientId)

        # 3) Look up api_token by email or name (your schema)
        with SessionLocal() as db:
            row = db.execute(
                text("""
                    SELECT id as id_of_token
                    FROM users
                    WHERE api_token = :u 
                    LIMIT 1
                """),
                {"u": incomingApiToken},
            ).mappings().first()

        if not row or not row["id_of_token"]:
            raise HTTPException(status_code=401, detail="api_token not registered")

        return claims
    return _dependency
