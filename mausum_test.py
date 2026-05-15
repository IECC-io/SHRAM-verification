import json, os, requests, urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Read creds from env vars first (GitHub Secrets), then fall back to .env locally.
env = {}
if os.path.exists(".env"):
    env = dict(l.strip().split("=", 1) for l in open(".env") if "=" in l and not l.startswith("#"))
for k in ("IMD_API_KEY", "IMD_EMAIL", "IMD_PASSWORD"):
    if os.environ.get(k):
        env[k] = os.environ[k]

# Get JWT
token_resp = requests.post(
    "https://api.imd.gov.in/api/oauth/token.php",
    json={"email": env["IMD_EMAIL"], "password": env["IMD_PASSWORD"]},
    verify=False,
)
print(f"TOKEN HTTP {token_resp.status_code} | type={type(token_resp.json()).__name__}")
print(json.dumps(token_resp.json(), indent=2)[:1000])

token_body = token_resp.json()
if "access_token" not in token_body:
    raise SystemExit("No access_token in token response — auth failed.")
jwt = token_body["access_token"]
print(f"Got JWT (length {len(jwt)}, prefix {jwt[:20]}...)")

# Call API
headers = {"X-API-KEY": env["IMD_API_KEY"], "Authorization": f"Bearer {jwt}"}
resp = requests.get(
    "https://api.imd.gov.in/api/v1/districtwarning",
    headers=headers, verify=False,
)
data = resp.json()

print(f"HTTP {resp.status_code} | type={type(data).__name__} | len={len(data)}")
if isinstance(data, list) and data:
    print(json.dumps(data[0], indent=2))
else:
    print(json.dumps(data, indent=2)[:2000])
