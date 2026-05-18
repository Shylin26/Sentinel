import hmac
import hashlib
import json
import os
from fastapi import FastAPI,Request,HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
load_dotenv()

app=FastAPI()

GITHUB_SECRET=os.environ.get("GITHUB_WEBHOOK_SECRET", "sentinel_secret")
def verify_signature(payload: bytes, signature: str) -> bool:
    expected = "sha256=" + hmac.new(
        GITHUB_SECRET.encode(),
        payload,
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)

@app.post("/webhook")
async def webhook(request: Request):
    signature = request.headers.get("X-Hub-Signature-256", "")
    payload = await request.body()

    if not verify_signature(payload, signature):
        raise HTTPException(status_code=401, detail="Invalid signature")

    event = request.headers.get("X-GitHub-Event", "")
    data = json.loads(payload)

    if event == "ping":
        return JSONResponse({"msg": "pong"})

    if event not in ("push", "pull_request"):
        return JSONResponse({"msg": "ignored"})

    repo = data.get("repository", {}).get("full_name", "unknown")
    commit = data.get("after", data.get("pull_request", {}).get("head", {}).get("sha", ""))

    print(f"Event: {event} | Repo: {repo} | Commit: {commit[:7]}")

    return JSONResponse({
        "status": "queued",
        "repo": repo,
        "commit": commit,
        "event": event,
    })

@app.get("/health")
async def health():
    return {"status": "ok"}
