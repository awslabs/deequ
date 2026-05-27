"""
Tools for the agentic PR review pipeline.

Each tool is a pure function that takes structured args and returns a string
the model reads as text. Tools never raise — they return error messages as
text so the model can self-correct and continue investigating.

Path safety: every tool that accepts a path normalizes via realpath and
rejects anything outside the repo root.
"""
import logging
import os
import re

logger = logging.getLogger("issue_bot")

# Per-tool output caps. These bound a single tool call's response size; the
# loop separately bounds total tool output across the agent run.
_MAX_RESULT_CHARS = 50_000
_MAX_FILE_LINES_PER_CALL = 200
_MAX_GREP_RESULTS = 50

# Default per-tool call budgets, co-located with TOOL_SPECS so a tool rename
# requires updating both in one place. Tools omitted from this dict have no
# per-tool cap and are bounded only by AgentCaps.max_tool_calls.
DEFAULT_PER_TOOL_CALL_CAPS = {
    "grep_codebase": 50,
    "read_file": 30,
    "list_dir": 20,
    "find_callers": 20,
    "find_tests_for": 10,
}

# JSON Schema definitions for Bedrock toolConfig. These describe each tool
# in the format Bedrock Converse expects.
TOOL_SPECS = [
    {
        "toolSpec": {
            "name": "grep_codebase",
            "description": (
                "Search for a regex pattern across the codebase. Returns matching "
                "lines as 'path:line:text'. Use to find references to a symbol, "
                "method, type parameter, or any specific identifier across files."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "pattern": {
                            "type": "string",
                            "description": "Regex pattern to search for. Use \\b for word boundaries on identifiers.",
                        },
                        "path_glob": {
                            "type": "string",
                            "description": "File extension filter (e.g., '.scala', '.py'). Defaults to the codebase's primary extension.",
                        },
                    },
                    "required": ["pattern"],
                },
            },
        },
    },
    {
        "toolSpec": {
            "name": "read_file",
            "description": (
                "Read a file (or a slice of it) from the repository at the PR head "
                "SHA. Returns content with line numbers prefixed. Max 200 lines per "
                "call — request a specific range for large files."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Repo-relative path to the file (e.g., 'src/main/scala/com/amazon/deequ/X.scala').",
                        },
                        "start_line": {
                            "type": "integer",
                            "description": "First line to read (1-indexed). Defaults to 1.",
                        },
                        "end_line": {
                            "type": "integer",
                            "description": "Last line to read (inclusive). Use -1 for end of file. Defaults to start_line + 199.",
                        },
                    },
                    "required": ["path"],
                },
            },
        },
    },
    {
        "toolSpec": {
            "name": "list_dir",
            "description": (
                "List files and subdirectories in a directory. Use to discover "
                "what files exist before reading."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Repo-relative path to a directory.",
                        },
                    },
                    "required": ["path"],
                },
            },
        },
    },
    {
        "toolSpec": {
            "name": "find_callers",
            "description": (
                "Find files that reference a class, trait, object, or method "
                "name. Returns a list of files ranked by reference count. Use "
                "when changing a type signature to discover impacted callers."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "symbol": {
                            "type": "string",
                            "description": "Symbol name (case-sensitive, no namespace prefix).",
                        },
                    },
                    "required": ["symbol"],
                },
            },
        },
    },
    {
        "toolSpec": {
            "name": "find_tests_for",
            "description": (
                "Find test files that exercise a given source file or symbol. "
                "Returns paths to test files in src/test/."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "target": {
                            "type": "string",
                            "description": "Either a source path (e.g., 'src/main/scala/com/X.scala') or a symbol name.",
                        },
                    },
                    "required": ["target"],
                },
            },
        },
    },
]


def _truncate(text):
    if len(text) <= _MAX_RESULT_CHARS:
        return text
    return text[:_MAX_RESULT_CHARS] + (
        f"\n... [truncated at {_MAX_RESULT_CHARS} chars; "
        "ask for specific path or line range to see more]"
    )


def _safe_repo_path(repo_root, rel_path):
    """Resolve rel_path against repo_root and reject paths outside.

    Returns (absolute_path, error_message). On error, absolute_path is None.
    """
    if not rel_path or not isinstance(rel_path, str):
        return None, "ERROR: path must be a non-empty string"
    if "\x00" in rel_path:
        return None, "ERROR: path contains NUL byte"
    if rel_path.startswith("/") or ".." in rel_path.split("/"):
        return None, f"ERROR: path '{rel_path}' must be repo-relative without '..' segments"
    real_root = os.path.realpath(repo_root)
    candidate = os.path.realpath(os.path.join(real_root, rel_path))
    if not (candidate == real_root or candidate.startswith(real_root + os.sep)):
        return None, f"ERROR: path '{rel_path}' resolves outside repo root"
    return candidate, ""


def grep_codebase(pattern, path_glob, repo_root, src_dir, default_ext):
    """Search for a regex across the codebase. Returns 'path:line:text' lines.

    The pattern is model-supplied; pathological patterns can ReDoS-pin a
    worker. Acceptable today (we own the model and the diffs it sees);
    revisit when this runs in less-trusted contexts.
    """
    if not pattern or not isinstance(pattern, str):
        return "ERROR: pattern must be a non-empty string"
    try:
        regex = re.compile(pattern)
    except re.error as e:
        return f"ERROR: invalid regex: {e}"
    ext = path_glob if (path_glob and path_glob.startswith(".")) else default_ext
    search_root = os.path.join(repo_root, src_dir)
    if not os.path.isdir(search_root):
        return f"ERROR: search root '{src_dir}' does not exist in repo"

    matches = list(_iter_matches(regex, search_root, repo_root, ext, _MAX_GREP_RESULTS))
    if not matches:
        return f"No matches for pattern '{pattern}' in {src_dir}/**/*{ext}"
    header = f"Found {len(matches)} match(es) (capped at {_MAX_GREP_RESULTS}):\n"
    return _truncate(header + "\n".join(matches))


def _iter_matches(regex, search_root, repo_root, ext, limit):
    """Yield up to `limit` regex matches as 'path:line:text' strings."""
    count = 0
    for dirpath, _, files in os.walk(search_root):
        for fn in files:
            if not fn.endswith(ext):
                continue
            full = os.path.join(dirpath, fn)
            try:
                with open(full, encoding="utf-8", errors="ignore") as f:
                    rel = os.path.relpath(full, repo_root)
                    for lineno, line in enumerate(f, 1):
                        if regex.search(line):
                            yield f"{rel}:{lineno}:{line.rstrip()}"
                            count += 1
                            if count >= limit:
                                return
            except OSError:
                continue


def read_file(path, start_line, end_line, repo_root):
    """Read a file slice from the repo with line numbers.

    end_line semantics:
      None or omitted: read up to start_line + MAX_FILE_LINES_PER_CALL - 1
      -1: read to end of file (still capped at MAX_FILE_LINES_PER_CALL)
      any other negative or non-int: same as None (use default range)
      positive int: read up to that line (capped at total)
    """
    abs_path, err = _safe_repo_path(repo_root, path)
    if err:
        return err
    if not os.path.isfile(abs_path):
        return f"ERROR: file '{path}' not found"
    try:
        with open(abs_path, encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
    except OSError as e:
        return f"ERROR: cannot read '{path}': {e}"
    total = len(lines)
    start = start_line if isinstance(start_line, int) and start_line >= 1 else 1
    if isinstance(end_line, int) and end_line == -1:
        end = total
    elif isinstance(end_line, int) and end_line >= 1:
        end = min(end_line, total)
    else:
        end = min(start + _MAX_FILE_LINES_PER_CALL - 1, total)
    if start > total:
        return f"ERROR: start_line {start} exceeds file length {total}"
    if end - start + 1 > _MAX_FILE_LINES_PER_CALL:
        end = start + _MAX_FILE_LINES_PER_CALL - 1
    out = []
    width = len(str(end))
    for i in range(start - 1, end):
        out.append(f"{str(i + 1).rjust(width)}│ {lines[i].rstrip()}")
    header = f"`{path}` lines {start}-{end} of {total}:\n"
    if end < total:
        out.append(f"... [{total - end} more lines below; request a higher range to see them]")
    return _truncate(header + "\n".join(out))


def list_dir(path, repo_root):
    """List directory contents."""
    abs_path, err = _safe_repo_path(repo_root, path)
    if err:
        return err
    if not os.path.isdir(abs_path):
        return f"ERROR: '{path}' is not a directory"
    try:
        entries = sorted(os.listdir(abs_path))
    except OSError as e:
        return f"ERROR: cannot list '{path}': {e}"
    rows = []
    for name in entries:
        full = os.path.join(abs_path, name)
        if os.path.isdir(full):
            rows.append(f"  {name}/")
        else:
            rows.append(f"  {name}")
    if not rows:
        return f"`{path}` is empty"
    return _truncate(f"`{path}`:\n" + "\n".join(rows))


def find_callers(symbol, repo_root, src_dir, default_ext):
    """Find files that reference a symbol, ranked by count."""
    if not symbol or not isinstance(symbol, str):
        return "ERROR: symbol must be a non-empty string"
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", symbol):
        return f"ERROR: symbol '{symbol}' is not a valid identifier"
    pattern = re.compile(rf"\b{re.escape(symbol)}\b")
    search_root = os.path.join(repo_root, src_dir)
    if not os.path.isdir(search_root):
        return f"ERROR: search root '{src_dir}' does not exist in repo"
    counts = {}
    for dirpath, _, files in os.walk(search_root):
        for fn in files:
            if not fn.endswith(default_ext):
                continue
            full = os.path.join(dirpath, fn)
            try:
                with open(full, encoding="utf-8", errors="ignore") as f:
                    text = f.read()
            except OSError:
                continue
            n = len(pattern.findall(text))
            if n > 0:
                rel = os.path.relpath(full, repo_root)
                counts[rel] = n
    if not counts:
        return f"No files reference '{symbol}' in {src_dir}"
    ranked = sorted(counts.items(), key=lambda kv: -kv[1])
    out = [f"Files referencing '{symbol}' (top {min(20, len(ranked))} by count):"]
    for rel, n in ranked[:20]:
        out.append(f"  {rel} ({n} reference{'s' if n != 1 else ''})")
    return _truncate("\n".join(out))


def find_tests_for(target, repo_root, src_dir, default_ext):
    """Find test files for a source file or symbol.

    First looks for naming-convention matches (X.scala → XTest.scala /
    XSpec.scala) under the test directory mirroring src_dir, then grep-matches
    the symbol name in remaining test files. Returns up to 20 candidates.
    """
    if not target or not isinstance(target, str):
        return "ERROR: target must be a non-empty string"
    test_root = os.path.join(repo_root, src_dir.replace("src/main", "src/test", 1))
    if not os.path.isdir(test_root):
        test_root = os.path.join(repo_root, "src", "test")
        if not os.path.isdir(test_root):
            return "ERROR: no test directory found at src/test"

    name = os.path.basename(target)
    if name.endswith(default_ext):
        name = name[:-len(default_ext)]
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
        return f"ERROR: target '{target}' did not yield a valid identifier"

    convention_names = {f"{name}{suffix}{default_ext}" for suffix in ("Test", "Spec")}
    grep_pattern = re.compile(rf"\b{re.escape(name)}\b")
    candidates = []
    seen = set()
    limit = 20

    for dirpath, _, files in os.walk(test_root):
        for fn in files:
            if not fn.endswith(default_ext):
                continue
            full = os.path.join(dirpath, fn)
            rel = os.path.relpath(full, repo_root)
            if rel in seen:
                continue
            if fn in convention_names:
                candidates.append(rel)
                seen.add(rel)
            else:
                try:
                    with open(full, encoding="utf-8", errors="ignore") as f:
                        if grep_pattern.search(f.read()):
                            candidates.append(rel)
                            seen.add(rel)
                except OSError:
                    continue
            if len(candidates) >= limit:
                break
        if len(candidates) >= limit:
            break

    if not candidates:
        return f"No tests found for '{target}' under {os.path.relpath(test_root, repo_root)}"
    out = [f"Tests for '{target}':"] + [f"  {rel}" for rel in candidates]
    return _truncate("\n".join(out))


class ToolRunner:
    """Dispatches tool calls to the right implementation. Stateless across calls."""

    def __init__(self, cfg, repo_root):
        self._cfg = cfg
        self._repo_root = repo_root

    def run(self, name, args):
        """Execute a tool call. Returns a string. Never raises."""
        if not isinstance(args, dict):
            return f"ERROR: tool args must be an object, got {type(args).__name__}"
        try:
            if name == "grep_codebase":
                return grep_codebase(
                    pattern=args.get("pattern", ""),
                    path_glob=args.get("path_glob", ""),
                    repo_root=self._repo_root,
                    src_dir=self._cfg.codebase_src_dir,
                    default_ext=self._cfg.codebase_file_ext,
                )
            if name == "read_file":
                return read_file(
                    path=args.get("path", ""),
                    start_line=args.get("start_line", 1),
                    end_line=args.get("end_line", None),
                    repo_root=self._repo_root,
                )
            if name == "list_dir":
                return list_dir(
                    path=args.get("path", ""),
                    repo_root=self._repo_root,
                )
            if name == "find_callers":
                return find_callers(
                    symbol=args.get("symbol", ""),
                    repo_root=self._repo_root,
                    src_dir=self._cfg.codebase_src_dir,
                    default_ext=self._cfg.codebase_file_ext,
                )
            if name == "find_tests_for":
                return find_tests_for(
                    target=args.get("target", ""),
                    repo_root=self._repo_root,
                    src_dir=self._cfg.codebase_src_dir,
                    default_ext=self._cfg.codebase_file_ext,
                )
            return f"ERROR: unknown tool '{name}'"
        except Exception as e:
            logger.error("Tool %s raised: %s", name, type(e).__name__)
            return f"ERROR: tool {name} failed: {type(e).__name__}: {e}"
