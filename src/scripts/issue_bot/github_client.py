import logging
import os
import requests

logger = logging.getLogger("issue_bot")


class GitHubClient:
    def __init__(self, cfg):
        self._token = cfg.github_token
        self._repo = cfg.repo
        self._timeout = cfg.github_api_timeout
        self._dry_run = cfg.dry_run
        self._repo_root = os.getenv("GITHUB_WORKSPACE", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
        self._headers = {
            "Authorization": f"token {self._token}",
            "Accept": "application/vnd.github.v3+json",
        }

    def get_issue(self, number):
        return self._get(f"/repos/{self._repo}/issues/{number}")

    def get_comments(self, number):
        comments = []
        page = 1
        while True:
            batch = self._get(f"/repos/{self._repo}/issues/{number}/comments?per_page=100&page={page}")
            if not batch:
                break
            comments.extend(batch)
            if len(batch) < 100:
                break
            page += 1
        return comments

    def get_pr(self, number):
        return self._get(f"/repos/{self._repo}/pulls/{number}")

    def get_pr_diff(self, number):
        headers = {**self._headers, "Accept": "application/vnd.github.v3.diff"}
        try:
            resp = requests.get(
                f"https://api.github.com/repos/{self._repo}/pulls/{number}",
                headers=headers, timeout=self._timeout,
            )
            return resp.text if resp.status_code == 200 else ""
        except Exception as e:
            logger.error(f"PR diff fetch failed: {e}")
            return ""

    def get_pr_files(self, number):
        return self._get(f"/repos/{self._repo}/pulls/{number}/files") or []

    def get_pr_review_comments(self, number):
        """Get all existing review comments on a PR."""
        comments = []
        page = 1
        while True:
            batch = self._get(f"/repos/{self._repo}/pulls/{number}/comments?per_page=100&page={page}")
            if not batch:
                break
            comments.extend(batch)
            if len(batch) < 100:
                break
            page += 1
        return comments

    def has_bot_commented(self, number):
        for c in self.get_comments(number):
            if c.get("user", {}).get("login") == "github-actions[bot]":
                return True
        return False

    def get_codebase_map(self, src_dir="src/main/scala"):
        """List all Scala source files (excluding examples) as relative paths."""
        import subprocess
        full_dir = os.path.join(self._repo_root, src_dir)
        prefix = self._repo_root.rstrip("/") + "/"
        try:
            proc = subprocess.run(
                ["find", full_dir, "-name", "*.scala", "-not", "-path", "*/examples/*"],
                capture_output=True, text=True, timeout=10,
            )
            paths = sorted(
                p[len(prefix):] if p.startswith(prefix) else p
                for p in proc.stdout.strip().split("\n") if p
            )
            return "\n".join(paths)
        except Exception as e:
            logger.error(f"Codebase map failed: {e}")
            return ""

    def read_local_file(self, path):
        if not path.startswith("src/"):
            logger.error(f"Blocked read outside src/: {path}")
            return ""
        full_path = os.path.join(self._repo_root, path)
        try:
            with open(full_path, "r", errors="replace") as f:
                return f.read()
        except Exception:
            return ""

    def get_file_content(self, path, repo=None, ref=None):
        target = repo or self._repo
        url = f"https://api.github.com/repos/{target}/contents/{path}"
        if ref:
            url += f"?ref={ref}"
        headers = {**self._headers, "Accept": "application/vnd.github.v3.raw"}
        try:
            resp = requests.get(url, headers=headers, timeout=self._timeout)
            return resp.text if resp.status_code == 200 else ""
        except Exception as e:
            logger.error(f"File fetch failed ({path}): {e}")
            return ""

    def post_comment(self, number, body):
        if self._dry_run:
            logger.info(f"[DRY RUN] Comment on #{number}: {body[:80]}...")
            return True
        return self._post(f"/repos/{self._repo}/issues/{number}/comments", {"body": body})

    def post_pr_review(self, number, summary, inline_comments):
        if self._dry_run:
            logger.info(f"[DRY RUN] PR review on #{number}: {len(inline_comments)} inline comments")
            return True
        comments = []
        for ic in inline_comments:
            comment = {"path": ic["file"], "body": ic["comment"]}
            if ic.get("line"):
                comment["line"] = ic["line"]
                comment["side"] = "RIGHT"
            comments.append(comment)
        if comments:
            payload = {"body": summary, "event": "REQUEST_CHANGES", "comments": comments}
            try:
                resp = requests.post(
                    f"https://api.github.com/repos/{self._repo}/pulls/{number}/reviews",
                    headers=self._headers, json=payload, timeout=self._timeout,
                )
                if resp.status_code in (200, 201):
                    return True
                logger.error(f"PR review API failed: {resp.status_code}, falling back to comment")
            except Exception as e:
                logger.error(f"PR review API failed: {e}, falling back to comment")
        # Fallback: post as regular comment with inline feedback in the body
        body = summary
        if inline_comments:
            body += "\n\n**Inline feedback:**\n"
            for ic in inline_comments:
                line_ref = f":{ic['line']}" if ic.get('line') else ""
                body += f"\n`{ic['file']}{line_ref}` — {ic['comment']}\n"
        return self._post(f"/repos/{self._repo}/issues/{number}/comments", {"body": body})

    def add_labels(self, number, labels):
        if not labels:
            return True
        if self._dry_run:
            logger.info(f"[DRY RUN] Labels on #{number}: {labels}")
            return True
        return self._post(f"/repos/{self._repo}/issues/{number}/labels", {"labels": labels})

    def _get(self, path):
        try:
            resp = requests.get(f"https://api.github.com{path}", headers=self._headers, timeout=self._timeout)
            if resp.status_code == 200:
                return resp.json()
            logger.error(f"GET {path}: {resp.status_code}")
        except Exception as e:
            logger.error(f"GET {path}: {e}")
        return None

    def _post(self, path, payload):
        try:
            resp = requests.post(f"https://api.github.com{path}", headers=self._headers, json=payload, timeout=self._timeout)
            if resp.status_code in (200, 201):
                return True
            logger.error(f"POST {path}: {resp.status_code}")
        except Exception as e:
            logger.error(f"POST {path}: {e}")
        return False
