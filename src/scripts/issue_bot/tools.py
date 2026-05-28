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
_MAX_FILES_FOR_CALLERS = 20
_MAX_FILES_LINE_SNIPPETS = 10
_MAX_LINES_PER_FILE = 5
_MAX_SNIPPET_CHARS = 200

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
                            "description": (
                                "Optional scope. Accepts: empty (search all files of the "
                                "codebase's primary extension); an extension like '.scala' "
                                "or '.py'; or a repo-relative path to a single file or "
                                "directory INSIDE the configured source directory. Paths "
                                "outside the source dir (tests, docs, build artifacts) are "
                                "rejected — use empty scope plus a more specific pattern instead."
                            ),
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
                "name. Returns files ranked by reference count, each with the "
                "specific line numbers and matching-line snippets. Use when "
                "changing a type signature to discover impacted callers; the "
                "inlined snippets often surface enough context to decide "
                "whether a follow-up read_file is needed."
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

    See _resolve_grep_scope for the path_glob shapes accepted.

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

    search_root, ext, scope_label, single_file, err = _resolve_grep_scope(
        path_glob, repo_root, src_dir, default_ext,
    )
    if err:
        return err

    if single_file:
        matches = list(_iter_matches_in_file(regex, single_file, repo_root, _MAX_GREP_RESULTS))
    else:
        matches = list(_iter_matches(regex, search_root, repo_root, ext, _MAX_GREP_RESULTS))
    if not matches:
        return f"No matches for pattern '{pattern}' in {scope_label}"
    header = f"Found {len(matches)} match(es) (capped at {_MAX_GREP_RESULTS}) in {scope_label}:\n"
    return _truncate(header + "\n".join(matches))


def _resolve_grep_scope(path_glob, repo_root, src_dir, default_ext):
    """Resolve path_glob → (search_root, ext, scope_label, single_file, error).

    Accepted path_glob shapes:
      empty   → walk src_dir for default_ext.
      ".X"    → walk src_dir for that extension.
      path    → repo-relative file or directory; must resolve inside src_dir.

    Path values that resolve outside src_dir are rejected so a path scope
    can only narrow the default search, not broaden it.
    """
    real_root = os.path.realpath(repo_root)
    default_root = os.path.realpath(os.path.join(repo_root, src_dir))
    if not os.path.isdir(default_root):
        return None, None, None, None, f"ERROR: search root '{src_dir}' does not exist in repo"
    # Guard against operator misconfig where src_dir resolves outside the
    # repo (e.g., a symlinked checkout). Silent-empty would mislead the
    # model into "no callers" conclusions.
    if not (default_root == real_root or default_root.startswith(real_root + os.sep)):
        return None, None, None, None, (
            f"ERROR: configured source dir '{src_dir}' resolves outside the repo root"
        )

    if not path_glob or not isinstance(path_glob, str):
        return default_root, default_ext, f"{src_dir}/**/*{default_ext}", None, ""

    # Extension filter: configured default (handles multi-dot like '.d.ts')
    # or '.' + alphanumerics. Tighter than startswith('.') so '../...'
    # falls through to path resolution and gets safety-checked.
    if path_glob == default_ext or re.fullmatch(r"\.[A-Za-z0-9]+", path_glob):
        return default_root, path_glob, f"{src_dir}/**/*{path_glob}", None, ""

    abs_path, err = _safe_repo_path(repo_root, path_glob)
    if err:
        return None, None, None, None, err
    # Inside src_dir, not just inside repo_root: 'src' would otherwise
    # broaden scope to include tests/.
    if not (abs_path == default_root or abs_path.startswith(default_root + os.sep)):
        return None, None, None, None, (
            f"ERROR: path_glob '{path_glob}' must be inside the source directory "
            f"'{src_dir}' (got a path outside it)"
        )
    # Use the realpath-relative form for both file and dir scope labels so
    # the header agrees with the match rows when the model passed an
    # in-repo symlink as path_glob.
    rel_label = os.path.relpath(abs_path, real_root)
    if os.path.isfile(abs_path):
        return None, default_ext, rel_label, abs_path, ""
    if os.path.isdir(abs_path):
        return abs_path, default_ext, f"{rel_label}/**/*{default_ext}", None, ""
    return None, None, None, None, (
        f"ERROR: path_glob '{path_glob}' is not an extension (start with '.') "
        f"and does not resolve to a file or directory in the repo"
    )


def _is_inside(real_root, candidate_full_path):
    """True iff candidate's realpath is at or under real_root. Used to skip
    planted symlinks that would otherwise leak external file contents."""
    try:
        real_candidate = os.path.realpath(candidate_full_path)
    except OSError:
        return False
    return real_candidate == real_root or real_candidate.startswith(real_root + os.sep)


def _iter_matches(regex, search_root, repo_root, ext, limit):
    """Yield up to `limit` regex matches as 'path:line:text' strings.
    Symlinks pointing outside repo_root are skipped; in-repo symlinks
    are deduped by realpath so each physical file is grepped once."""
    real_root = os.path.realpath(repo_root)
    seen = set()
    count = 0
    for dirpath, _, files in os.walk(search_root):
        for fn in files:
            if not fn.endswith(ext):
                continue
            full = os.path.join(dirpath, fn)
            if not _is_inside(real_root, full):
                continue
            real_full = os.path.realpath(full)
            if real_full in seen:
                continue
            seen.add(real_full)
            try:
                with open(full, encoding="utf-8", errors="ignore") as f:
                    rel = os.path.relpath(full, real_root)
                    for lineno, line in enumerate(f, 1):
                        if regex.search(line):
                            yield f"{rel}:{lineno}:{line.rstrip()}"
                            count += 1
                            if count >= limit:
                                return
            except OSError:
                continue


def _iter_matches_in_file(regex, abs_path, repo_root, limit):
    """Yield matches from a single file. abs_path is expected to be
    realpath-validated by the caller; the _is_inside re-check below is
    defense-in-depth in case a future caller skips that step."""
    real_root = os.path.realpath(repo_root)
    if not _is_inside(real_root, abs_path):
        return
    rel = os.path.relpath(abs_path, real_root)
    count = 0
    try:
        with open(abs_path, encoding="utf-8", errors="ignore") as f:
            for lineno, line in enumerate(f, 1):
                if regex.search(line):
                    yield f"{rel}:{lineno}:{line.rstrip()}"
                    count += 1
                    if count >= limit:
                        return
    except OSError:
        return


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
    """Find files referencing a symbol, ranked by count, with line snippets."""
    if not symbol or not isinstance(symbol, str):
        return "ERROR: symbol must be a non-empty string"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", symbol):
        return f"ERROR: symbol '{symbol}' is not a valid identifier"
    pattern = re.compile(rf"\b{re.escape(symbol)}\b")
    real_root = os.path.realpath(repo_root)
    # Walk under the canonical search_root so emitted paths share real_root's
    # prefix; relpath then produces clean repo-relative output.
    search_root = os.path.realpath(os.path.join(repo_root, src_dir))
    if not os.path.isdir(search_root):
        return f"ERROR: search root '{src_dir}' does not exist in repo"
    if not (search_root == real_root or search_root.startswith(real_root + os.sep)):
        return f"ERROR: configured source dir '{src_dir}' resolves outside the repo root"
    file_hits = {}
    seen = set()
    for dirpath, _, files in os.walk(search_root):
        for fn in files:
            if not fn.endswith(default_ext):
                continue
            full = os.path.join(dirpath, fn)
            if not _is_inside(real_root, full):
                continue
            real_full = os.path.realpath(full)
            if real_full in seen:
                continue
            seen.add(real_full)
            try:
                with open(full, encoding="utf-8", errors="ignore") as f:
                    matches = []
                    occurrences = 0
                    for lineno, line in enumerate(f, 1):
                        line_count = len(pattern.findall(line))
                        if line_count:
                            matches.append((lineno, line.rstrip()))
                            occurrences += line_count
            except OSError:
                continue
            if matches:
                rel = os.path.relpath(full, real_root)
                file_hits[rel] = (occurrences, matches)
    if not file_hits:
        return f"No files reference '{symbol}' in {src_dir}"
    ranked = sorted(file_hits.items(), key=lambda kv: -kv[1][0])
    total = len(ranked)
    shown_total = min(_MAX_FILES_FOR_CALLERS, total)
    out = [f"Files referencing '{symbol}' ({shown_total} of {total} by count):"]
    for rel, (occ, matches) in ranked[:_MAX_FILES_LINE_SNIPPETS]:
        out.append(f"  {rel} ({occ} reference{'s' if occ != 1 else ''})")
        for lineno, text in matches[:_MAX_LINES_PER_FILE]:
            out.append(f"    line {lineno}: {text.strip()[:_MAX_SNIPPET_CHARS]}")
        if len(matches) > _MAX_LINES_PER_FILE:
            out.append(f"    ... [{len(matches) - _MAX_LINES_PER_FILE} more lines in this file]")
    names_only = ranked[_MAX_FILES_LINE_SNIPPETS:_MAX_FILES_FOR_CALLERS]
    if names_only:
        out.append(f"  (additional {len(names_only)} files with fewer references, names only:)")
        for rel, (occ, _) in names_only:
            out.append(f"    {rel} ({occ} reference{'s' if occ != 1 else ''})")
    if total > _MAX_FILES_FOR_CALLERS:
        out.append(f"  ... [{total - _MAX_FILES_FOR_CALLERS} more files truncated]")
    return _truncate("\n".join(out))


def find_tests_for(target, repo_root, src_dir, default_ext):
    """Find test files for a source file or symbol.

    First looks for naming-convention matches (X.scala → XTest.scala /
    XSpec.scala) under the test directory mirroring src_dir, then grep-matches
    the symbol name in remaining test files. Returns up to 20 candidates.
    """
    if not target or not isinstance(target, str):
        return "ERROR: target must be a non-empty string"
    real_root = os.path.realpath(repo_root)
    # Mirror src_dir into the test tree only when the substitution actually
    # changed the path; otherwise str.replace is a no-op and we'd walk the
    # source tree as if it were tests.
    mirrored = src_dir.replace("src/main", "src/test", 1)
    test_root = None
    if mirrored != src_dir:
        candidate = os.path.realpath(os.path.join(repo_root, mirrored))
        if os.path.isdir(candidate):
            test_root = candidate
    if test_root is None:
        candidate = os.path.realpath(os.path.join(repo_root, "src", "test"))
        if os.path.isdir(candidate):
            test_root = candidate
    if test_root is None:
        return "ERROR: no test directory found at src/test"
    if not (test_root == real_root or test_root.startswith(real_root + os.sep)):
        return "ERROR: test directory resolves outside the repo root"

    name = os.path.basename(target)
    if name.endswith(default_ext):
        name = name[:-len(default_ext)]
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        return f"ERROR: target '{target}' did not yield a valid identifier"

    convention_names = {f"{name}{suffix}{default_ext}" for suffix in ("Test", "Spec")}
    grep_pattern = re.compile(rf"\b{re.escape(name)}\b")
    # real_full → (is_convention, rel). A convention-named hit overwrites a
    # prior grep-match for the same physical file so symlink ordering in
    # os.walk doesn't cause the non-conventional path to win.
    by_real = {}
    limit = 20

    for dirpath, _, files in os.walk(test_root):
        for fn in files:
            if not fn.endswith(default_ext):
                continue
            full = os.path.join(dirpath, fn)
            if not _is_inside(real_root, full):
                continue
            real_full = os.path.realpath(full)
            existing = by_real.get(real_full)
            if existing and existing[0]:
                # Already accepted as convention; nothing better to find.
                continue
            is_convention = fn in convention_names
            # Stop accepting new entries past the limit, but still allow
            # upgrades (grep-match → convention-match) on entries already
            # tracked so symlink-ordering doesn't trap a worse path.
            if existing is None and len(by_real) >= limit:
                continue
            rel = os.path.relpath(full, real_root)
            if is_convention:
                by_real[real_full] = (True, rel)
            elif existing is None:
                try:
                    with open(full, encoding="utf-8", errors="ignore") as f:
                        if grep_pattern.search(f.read()):
                            by_real[real_full] = (False, rel)
                except OSError:
                    continue
        # Early exit when we have enough convention-named entries; no
        # future visit can upgrade those. Skip when only grep matches
        # exist since a later convention name could still upgrade.
        if (len(by_real) >= limit
                and all(is_conv for is_conv, _ in by_real.values())):
            break

    if not by_real:
        return f"No tests found for '{target}' under {os.path.relpath(test_root, real_root)}"
    # Convention names first, then grep matches; walk order preserved
    # within each group via dict insertion order.
    convention_hits = [rel for is_conv, rel in by_real.values() if is_conv]
    grep_hits = [rel for is_conv, rel in by_real.values() if not is_conv]
    out = [f"Tests for '{target}':"] + [f"  {rel}" for rel in convention_hits + grep_hits]
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
