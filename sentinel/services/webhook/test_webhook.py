import hmac
import hashlib 
import json
import urllib.request
SECRET = "sentinel_secret"
URL = "http://localhost:8000/webhook"
def make_signature(payload: bytes) -> str:
    return "sha256=" + hmac.new(SECRET.encode(), payload, hashlib.sha256).hexdigest()

def send(payload: dict, sign: bool = True, fake: bool = False):
    body = json.dumps(payload).encode()
    if fake:
        sig = "sha256=fakesignature"
    elif sign:
        sig = make_signature(body)
    else:
        sig = ""

    req = urllib.request.Request(URL, data=body, method="POST", headers={
        "Content-Type": "application/json",
        "X-GitHub-Event": "push",
        "X-Hub-Signature-256": sig,
    })
    try:
        with urllib.request.urlopen(req) as r:
            print(f"  ✓ {r.status} {json.loads(r.read())}")
    except urllib.error.HTTPError as e:
        print(f"  ✗ {e.code} {e.reason}")

payload = {
    "repository": {"full_name": "parishachauhan/sentinel-test"},
    "after": "abc123def456",
}

print("Test 1 — valid signature:")
send(payload, sign=True)

print("Test 2 — fake signature (should 401):")
send(payload, fake=True)

print("Test 3 — no signature (should 401):")
send(payload, sign=False)

