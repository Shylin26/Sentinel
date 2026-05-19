import json
import os
import re
import time
import urllib.request
from pathlib import Path

TOKEN = os.environ.get("GITHUB_TOKEN", "")
if not TOKEN:
    raise EnvironmentError("Set GITHUB_TOKEN env var before running this script.")
OUTPUT = Path("data/raw")
OUTPUT.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "Authorization": f"token {TOKEN}",
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "SENTINEL-miner",
}

SEVERITY_KEYWORDS = {
    3: ["security", "vulnerability", "critical", "crash", "exploit"],
    2: ["bug", "incorrect", "wrong", "broken", "fails"],
    1: ["consider", "suggestion", "maybe", "could"],
    0: ["nit", "minor", "style", "rename", "typo"],
}

# Patterns that match real secrets — any example containing these gets dropped
SECRET_PATTERNS = re.compile(
    r'ghp_[A-Za-z0-9]{36}'                  # GitHub personal access token
    r'|gho_[A-Za-z0-9]{36}'                 # GitHub OAuth token
    r'|AIza[0-9A-Za-z\-_]{35}'             # Google API key
    r'|ya29\.[0-9A-Za-z\-_]+'              # Google OAuth access token
    r'|[0-9]+-[0-9A-Za-z_]{32}\.apps\.googleusercontent\.com'  # Google OAuth client ID
    r'|sk_live_[0-9a-zA-Z]{24,}'           # Stripe live secret key
    r'|rk_live_[0-9a-zA-Z]{24,}'           # Stripe restricted key
    r'|AC[a-z0-9]{32}'                      # Twilio Account SID
    r'|SK[a-z0-9]{32}'                      # Twilio API key
    r'|sk-or-v1-[A-Za-z0-9]{64}'           # OpenRouter API key
    r'|-----BEGIN (RSA |EC )?PRIVATE KEY'   # Private keys
)

def contains_secret(text: str) -> bool:
    return bool(SECRET_PATTERNS.search(text))

def infer_severity(comment):
    c = comment.lower()
    for severity in [3, 2, 1, 0]:
        if any(kw in c for kw in SEVERITY_KEYWORDS[severity]):
            return severity
    return 1

def infer_language(path):
    ext_map = {".py": "python", ".js": "javascript", ".ts": "typescript",
               ".go": "go", ".rs": "rust", ".java": "java", ".cpp": "cpp"}
    for ext, lang in ext_map.items():
        if path.endswith(ext):
            return lang
    return "unknown"

def fetch(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())

def get_repos(page=1):
    url = f"https://api.github.com/search/repositories?q=stars:%3E500&sort=updated&per_page=30&page={page}"
    return fetch(url)["items"]

def get_review_comments(owner, repo):
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/comments?per_page=100"
    return fetch(url)

def mine(target=5000):
    examples = []
    out_path = OUTPUT / "pr_review_comments.jsonl"
    page = 1

    while len(examples) < target:
        print(f"\nFetching repo page {page}...")
        repos = get_repos(page)
        if not repos:
            break

        for repo in repos:
            owner = repo["owner"]["login"]
            name = repo["name"]
            print(f"  {owner}/{name}...", end=" ")
            try:
                comments = get_review_comments(owner, name)
                count = 0
                for c in comments:
                    body = c.get("body", "").strip()
                    diff_hunk = c.get("diff_hunk", "").strip()
                    if not body or not diff_hunk or len(body) < 15:
                        continue
                    # Skip any example that contains a real secret pattern
                    if contains_secret(body) or contains_secret(diff_hunk):
                        continue
                    examples.append({
                        "diff_hunk": diff_hunk,
                        "comment": body,
                        "severity": infer_severity(body),
                        "path": c.get("path", ""),
                        "language": infer_language(c.get("path", "")),
                        "repo": f"{owner}/{name}",
                    })
                    count += 1
                print(f"+{count} (total: {len(examples)})")
                time.sleep(0.5)
            except Exception as e:
                print(f"skipped: {e}")
                time.sleep(1)

            if len(examples) >= target:
                break
        page += 1

    with open(out_path, "w") as f:
        for ex in examples:
            f.write(json.dumps(ex) + "\n")
    print(f"\nDone. {len(examples)} examples → {out_path}")

if __name__ == "__main__":
    mine(target=50000)