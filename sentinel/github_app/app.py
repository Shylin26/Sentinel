"""
SENTINEL GitHub App
Handles OAuth flow + automatic webhook registration.

Run from sentinel/ directory:
  python3 github_app/app.py
"""
import json
import os
import hmac
import hashlib
import urllib.request
import urllib.parse
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from dotenv import load_dotenv

load_dotenv()

GITHUB_APP_ID         = os.environ.get("GITHUB_APP_ID", "")
GITHUB_APP_SECRET     = os.environ.get("GITHUB_APP_CLIENT_SECRET", "")
GITHUB_APP_CLIENT_ID  = os.environ.get("GITHUB_APP_CLIENT_ID", "")
WEBHOOK_SECRET        = os.environ.get("GITHUB_WEBHOOK_SECRET", "sentinel_secret")
PUBLIC_URL            = "https://nickname-agency-ninja.ngrok-free.app"

app = FastAPI()

# Store installations in memory (use DB in production)
installations = {}


@app.get("/", response_class=HTMLResponse)
async def home():
    """Landing page with one-click install button."""
    manifest = {
        "name": "SENTINEL Code Review",
        "url": PUBLIC_URL,
        "hook_attributes": {
            "url": f"{PUBLIC_URL}/webhook",
            "active": True,
        },
        "redirect_url": f"{PUBLIC_URL}/github/callback",
        "callback_urls": [f"{PUBLIC_URL}/github/callback"],
        "setup_url": f"{PUBLIC_URL}/github/setup",
        "description": "Real-time ML-powered code review on every push. Under 2 seconds.",
        "public": True,
        "default_events": ["push", "pull_request"],
        "default_permissions": {
            "contents": "read",
            "pull_requests": "write",
            "statuses": "write",
        },
    }
    manifest_json = json.dumps(manifest)
    return HTMLResponse(f"""
<!DOCTYPE html>
<html>
<head>
  <title>SENTINEL — Real-time ML Code Review</title>
  <style>
    body {{ font-family: -apple-system, sans-serif; max-width: 640px; margin: 80px auto; padding: 0 20px; }}
    h1 {{ font-size: 2em; margin-bottom: 8px; }}
    p  {{ color: #666; margin-bottom: 32px; }}
    .install-btn {{
      background: #238636; color: white; border: none;
      padding: 14px 28px; font-size: 16px; border-radius: 6px;
      cursor: pointer; font-weight: 600;
    }}
    .install-btn:hover {{ background: #2ea043; }}
    ul {{ color: #444; line-height: 2; }}
  </style>
</head>
<body>
  <h1>⚔️ SENTINEL</h1>
  <p>Real-time ML-powered code review. Every git push. Under 2 seconds.</p>
  <ul>
    <li>🤖 Transformer model trained from scratch on 5,001 PR review comments</li>
    <li>⚡ gRPC inference — p99 under 2 seconds end-to-end</li>
    <li>🔁 Kafka-backed — zero reviews dropped, 24hr replay window</li>
    <li>📊 Live Grafana dashboard — watch reviews flow in real time</li>
  </ul>
  <br/>
  <form action="https://github.com/settings/apps/new" method="post">
    <input type="hidden" name="manifest" value='{manifest_json}'/>
    <button class="install-btn" type="submit">Install SENTINEL on GitHub →</button>
  </form>
</body>
</html>
""")


@app.get("/github/callback")
async def github_callback(request: Request):
    """
    GitHub redirects here after app creation with a one-time code.
    Exchange it for app credentials.
    """
    code = request.query_params.get("code")
    if not code:
        return JSONResponse({"error": "no code"}, status_code=400)

    # Exchange code for app credentials
    url = f"https://api.github.com/app-manifests/{code}/conversions"
    req = urllib.request.Request(
        url,
        data=b"{}",
        headers={
            "Accept": "application/vnd.github.v3+json",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

    app_id     = data.get("id")
    client_id  = data.get("client_id")
    webhook_secret = data.get("webhook_secret")

    print(f"GitHub App created — id={app_id}")
    print(f"Add to .env:")
    print(f"  GITHUB_APP_ID={app_id}")
    print(f"  GITHUB_APP_CLIENT_ID={client_id}")
    print(f"  GITHUB_WEBHOOK_SECRET={webhook_secret}")

    return HTMLResponse(f"""
<!DOCTYPE html>
<html>
<head>
  <title>SENTINEL — Installed</title>
  <style>
    body {{ font-family: -apple-system, sans-serif; max-width: 640px; margin: 80px auto; padding: 0 20px; }}
    code {{ background: #f0f0f0; padding: 2px 6px; border-radius: 4px; }}
    pre  {{ background: #f6f8fa; padding: 16px; border-radius: 6px; overflow-x: auto; }}
  </style>
</head>
<body>
  <h1>✅ SENTINEL installed</h1>
  <p>App created successfully. Add these to your <code>.env</code>:</p>
  <pre>
GITHUB_APP_ID={app_id}
GITHUB_APP_CLIENT_ID={client_id}
GITHUB_WEBHOOK_SECRET={webhook_secret}
  </pre>
  <p>SENTINEL will now review every push to your repositories automatically.</p>
</body>
</html>
""")


@app.get("/github/setup")
async def github_setup(request: Request):
    installation_id = request.query_params.get("installation_id")
    if installation_id:
        installations[installation_id] = True
        print(f"New installation: {installation_id}")
    return HTMLResponse("""
<!DOCTYPE html>
<html>
<body style="font-family:-apple-system,sans-serif;max-width:640px;margin:80px auto;padding:0 20px">
  <h1>⚔️ SENTINEL is watching</h1>
  <p>Push any code to your repo — SENTINEL will review it within 2 seconds.</p>
</body>
</html>
""")


@app.get("/health")
async def health():
    return {"status": "ok", "installations": len(installations)}


if __name__ == "__main__":
    import uvicorn
    print(f"GitHub App server running — visit {PUBLIC_URL} to install")
    uvicorn.run(app, host="0.0.0.0", port=8080)
