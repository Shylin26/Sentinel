import json
import os
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