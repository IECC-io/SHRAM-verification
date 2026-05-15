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
jwt = requests.post(
    "https://api.imd.gov.in/api/oauth/token.php",
    json={"email": env["IMD_EMAIL"], "password": env["IMD_PASSWORD"]},
    verify=False,
).json()["access_token"]

# Call API
headers = {"X-API-KEY": env["IMD_API_KEY"], "Authorization": f"Bearer {jwt}"}
data = requests.get(
    "https://api.imd.gov.in/api/v1/districtwarning",
    headers=headers, verify=False,
).json()

print(f"{len(data)} districts")
print(json.dumps(data[0], indent=2))
