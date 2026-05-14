"""Fetch IMD district-warning data via the official authenticated API.

Local-only fetcher. The IMD API key is IP-bound, so this script MUST run from
the registered IP (typically the developer's home IP) — it will not work from
GitHub Actions runners. Use scripts/fetch_imd_districtwarning.py (WFS, no auth)
for any environment where the IP isn't allowlisted.

Reads credentials from .env:
    IMD_API_KEY    — long-lived API key (X-API-KEY header)
    IMD_EMAIL      — registered account email
    IMD_PASSWORD   — account password (used only to fetch a JWT)

JWT auth flow:
    POST https://api.imd.gov.in/api/oauth/token.php  → { access_token, expires_in }
    Token cached to .imd_jwt_cache.json for reuse until ~5 min before expiry.

Endpoint:
    GET https://api.imd.gov.in/api/v1/districtwarning
    GET https://api.imd.gov.in/api/v1/districtwarning?id=<district_id>

Outputs:
    reference_history/imd_districtwarning_api_latest.json
    reference_history/imd_districtwarning_api/<stamp>.json   (archive)

Both files are sibling to the WFS-based outputs so you can compare side-by-side.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DASHBOARD_ROOT = Path(__file__).resolve().parent.parent
REF_DIR = DASHBOARD_ROOT / "reference_history"
ARCHIVE_DIR = REF_DIR / "imd_districtwarning_api"
LATEST_PATH = REF_DIR / "imd_districtwarning_api_latest.json"
JWT_CACHE = DASHBOARD_ROOT / ".imd_jwt_cache.json"
ENV_FILE = DASHBOARD_ROOT / ".env"

AUTH_URL = "https://api.imd.gov.in/api/oauth/token.php"
DATA_URL = "https://api.imd.gov.in/api/v1/districtwarning"
REQUEST_TIMEOUT = 60

# Refresh JWT this many seconds before its stated expiry — gives us a safety
# margin in case the server clock drifts.
TOKEN_SAFETY_MARGIN_S = 300


def load_env() -> dict[str, str]:
    """Read .env without adding python-dotenv as a dep. Simple KEY=VALUE format."""
    env = {}
    if not ENV_FILE.exists():
        return env
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def get_jwt(email: str, password: str) -> str:
    """Return a valid JWT — from cache if still fresh, otherwise re-auth."""
    now = time.time()

    if JWT_CACHE.exists():
        try:
            cached = json.loads(JWT_CACHE.read_text())
            if cached.get("token") and cached.get("expires_at", 0) - TOKEN_SAFETY_MARGIN_S > now:
                logger.info("using cached JWT (valid for %ds more)",
                            int(cached["expires_at"] - now))
                return cached["token"]
        except Exception as e:
            logger.warning("cache read failed (%s), re-authenticating", e)

    logger.info("authenticating to %s", AUTH_URL)
    resp = requests.post(
        AUTH_URL,
        json={"email": email, "password": password},
        timeout=REQUEST_TIMEOUT,
        verify=False,
    )
    resp.raise_for_status()
    body = resp.json()
    token = body.get("access_token")
    expires_in = int(body.get("expires_in", 3600))
    if not token:
        raise RuntimeError(f"auth response missing access_token: {body}")
    expires_at = time.time() + expires_in
    JWT_CACHE.write_text(json.dumps({"token": token, "expires_at": expires_at}))
    logger.info("got JWT (expires_in=%ds)", expires_in)
    return token


def fetch_district_warnings(api_key: str, jwt: str,
                            district_id: int | None = None) -> dict:
    headers = {
        "X-API-KEY": api_key,
        "Authorization": f"Bearer {jwt}",
    }
    url = DATA_URL
    params = {"id": district_id} if district_id is not None else None
    logger.info("GET %s%s", url, f"?id={district_id}" if district_id else "")
    resp = requests.get(url, headers=headers, params=params,
                        timeout=REQUEST_TIMEOUT, verify=False)
    resp.raise_for_status()
    return resp.json()


def main() -> int:
    env = load_env()
    api_key = env.get("IMD_API_KEY") or os.environ.get("IMD_API_KEY")
    email = env.get("IMD_EMAIL") or os.environ.get("IMD_EMAIL")
    password = env.get("IMD_PASSWORD") or os.environ.get("IMD_PASSWORD")

    missing = [name for name, val in [
        ("IMD_API_KEY", api_key),
        ("IMD_EMAIL", email),
        ("IMD_PASSWORD", password),
    ] if not val]
    if missing:
        logger.error("missing credentials in .env: %s", ", ".join(missing))
        logger.error("expected file: %s", ENV_FILE)
        return 1

    try:
        jwt = get_jwt(email, password)
    except requests.HTTPError as e:
        logger.error("auth failed: HTTP %s — body=%s", e.response.status_code,
                     e.response.text[:300])
        return 2
    except Exception as e:
        logger.error("auth failed: %s", e)
        return 2

    try:
        data = fetch_district_warnings(api_key, jwt)
    except requests.HTTPError as e:
        # API key is IP-bound. If the IP doesn't match, IMD typically returns
        # 401/403 with a clear message — surface that.
        logger.error("fetch failed: HTTP %s — body=%s", e.response.status_code,
                     e.response.text[:300])
        if e.response.status_code in (401, 403):
            logger.error("this looks like an auth or IP-mismatch error.")
            logger.error("your API key may only work from the IP registered "
                         "with IMD. If you're behind a VPN or new network, "
                         "your IP has changed.")
        return 3

    payload = {
        "label": "IMD district warnings (official API)",
        "source_url": DATA_URL,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "raw": data,
    }

    # Try to surface a top-line count if the shape allows
    try:
        if isinstance(data, list):
            payload["n_records"] = len(data)
        elif isinstance(data, dict):
            payload["top_keys"] = list(data.keys())[:20]
    except Exception:
        pass

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive_path = ARCHIVE_DIR / f"imd_districtwarning_api_{stamp}.json"
    with archive_path.open("w") as f:
        json.dump(payload, f, indent=2)
    logger.info("archived → %s", archive_path)

    with LATEST_PATH.open("w") as f:
        json.dump(payload, f, indent=2)
    logger.info("wrote latest → %s", LATEST_PATH)

    return 0


if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    sys.exit(main())
