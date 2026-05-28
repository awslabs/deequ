"""Tests for issue_bot.tools — the agentic pipeline's tool implementations."""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from issue_bot.tools import (
    grep_codebase,
    read_file,
    list_dir,
    find_callers,
    find_tests_for,
    ToolRunner,
    TOOL_SPECS,
)


@pytest.fixture
def repo(tmp_path):
    """Create a fake repo layout for tool tests."""
    src = tmp_path / "src" / "main" / "scala"
    src.mkdir(parents=True)
    test = tmp_path / "src" / "test" / "scala"
    test.mkdir(parents=True)
    (src / "Foo.scala").write_text(
        "case class Foo(x: Int)\n"
        "object Foo {\n"
        "  def bar(): Int = 42\n"
        "}\n"
    )
    (src / "Bar.scala").write_text(
        "import com.example.Foo\n"
        "class Bar { def use(): Foo = Foo(1) }\n"
    )
    (src / "Unused.scala").write_text("// nothing here\n")
    (test / "FooTest.scala").write_text(
        "class FooTest {\n"
        "  def testFoo(): Unit = { Foo(2) }\n"
        "}\n"
    )
    return tmp_path


def test_grep_codebase_finds_symbol(repo):
    out = grep_codebase("Foo", "", str(repo), "src/main/scala", ".scala")
    assert "Foo.scala" in out
    assert "Bar.scala" in out
    assert "Found" in out


def test_grep_codebase_no_matches(repo):
    out = grep_codebase("ThisDoesNotExist", "", str(repo), "src/main/scala", ".scala")
    assert "No matches" in out


def test_grep_codebase_invalid_regex(repo):
    out = grep_codebase("[invalid(", "", str(repo), "src/main/scala", ".scala")
    assert "ERROR" in out
    assert "regex" in out.lower()


def test_grep_codebase_missing_dir(tmp_path):
    out = grep_codebase("anything", "", str(tmp_path), "missing/dir", ".scala")
    assert "ERROR" in out


def test_grep_codebase_empty_pattern(repo):
    out = grep_codebase("", "", str(repo), "src/main/scala", ".scala")
    assert "ERROR" in out


def test_read_file_basic(repo):
    out = read_file("src/main/scala/Foo.scala", 1, 4, str(repo))
    assert "case class Foo" in out
    assert "1│" in out  # line numbers
    assert "lines 1-4" in out


def test_read_file_default_end(repo):
    out = read_file("src/main/scala/Foo.scala", 1, -2, str(repo))
    assert "case class Foo" in out


def test_read_file_path_traversal_rejected(repo):
    out = read_file("../../etc/passwd", 1, 10, str(repo))
    assert "ERROR" in out


def test_read_file_absolute_path_rejected(repo):
    out = read_file("/etc/passwd", 1, 10, str(repo))
    assert "ERROR" in out


def test_read_file_dotdot_rejected(repo):
    out = read_file("foo/../bar", 1, 10, str(repo))
    assert "ERROR" in out


def test_read_file_missing(repo):
    out = read_file("src/main/scala/DoesNotExist.scala", 1, 10, str(repo))
    assert "ERROR" in out
    assert "not found" in out


def test_read_file_caps_at_200_lines(repo):
    """A 1000-line file should return at most 200 lines per call."""
    big = repo / "src" / "main" / "scala" / "Big.scala"
    big.write_text("\n".join(f"line {i}" for i in range(1, 1001)) + "\n")
    out = read_file("src/main/scala/Big.scala", 1, -1, str(repo))
    # When end_line=-1, the function uses the file's full length but should still cap
    # at 200 lines per call. Check by counting line-prefix occurrences.
    line_count = sum(1 for ln in out.splitlines() if "│" in ln)
    assert line_count == 200, f"expected 200 lines, got {line_count}"


def test_list_dir(repo):
    out = list_dir("src/main/scala", str(repo))
    assert "Foo.scala" in out
    assert "Bar.scala" in out


def test_list_dir_path_traversal_rejected(repo):
    out = list_dir("../..", str(repo))
    assert "ERROR" in out


def test_list_dir_not_a_directory(repo):
    out = list_dir("src/main/scala/Foo.scala", str(repo))
    assert "ERROR" in out


def test_find_callers_ranks_by_count(repo):
    """Both Foo.scala and Bar.scala reference Foo; the file with more refs ranks higher."""
    # Add a third file that uses Foo many times to nail down the ordering
    extra = repo / "src" / "main" / "scala" / "Many.scala"
    extra.write_text("\n".join(["// Foo Foo Foo Foo Foo"] * 5))
    out = find_callers("Foo", str(repo), "src/main/scala", ".scala")
    assert "Foo.scala" in out
    assert "Bar.scala" in out
    assert "Many.scala" in out
    # Many.scala has the most references and should appear first
    many_idx = out.index("Many.scala")
    foo_idx = out.index("Foo.scala")
    bar_idx = out.index("Bar.scala")
    assert many_idx < foo_idx
    assert many_idx < bar_idx


def test_find_callers_invalid_symbol(repo):
    out = find_callers("not a valid identifier!", str(repo), "src/main/scala", ".scala")
    assert "ERROR" in out


def test_find_callers_rejects_symbol_with_trailing_newline(repo):
    """re.fullmatch (vs re.match with $) prevents 'Foo\\n' from sneaking
    past the identifier check — $ in non-MULTILINE mode matches before a
    final newline."""
    out = find_callers("Foo\n", str(repo), "src/main/scala", ".scala")
    assert "ERROR" in out
    assert "valid identifier" in out


def test_find_callers_no_matches(repo):
    out = find_callers("DoesNotExistAnywhere", str(repo), "src/main/scala", ".scala")
    assert "No files reference" in out


def test_find_tests_for_source_path(repo):
    out = find_tests_for("src/main/scala/Foo.scala", str(repo), "src/main/scala", ".scala")
    assert "FooTest.scala" in out


def test_find_tests_for_symbol(repo):
    out = find_tests_for("Foo", str(repo), "src/main/scala", ".scala")
    assert "FooTest.scala" in out


def test_find_tests_for_no_match(repo):
    out = find_tests_for("Nonexistent", str(repo), "src/main/scala", ".scala")
    assert "No tests found" in out


def test_find_tests_for_rejects_target_with_trailing_newline(repo):
    """re.fullmatch (vs re.match with $) rejects 'Foo\\n' so a regression
    to re.match would be caught here, matching find_callers' protection."""
    out = find_tests_for("Foo\n", str(repo), "src/main/scala", ".scala")
    assert "ERROR" in out
    assert "valid identifier" in out


class FakeCfg:
    def __init__(self, root):
        self.codebase_src_dir = "src/main/scala"
        self.codebase_file_ext = ".scala"


def test_tool_runner_dispatches_grep(repo):
    runner = ToolRunner(FakeCfg(repo), str(repo))
    out = runner.run("grep_codebase", {"pattern": "Foo"})
    assert "Foo.scala" in out


def test_tool_runner_dispatches_read_file(repo):
    runner = ToolRunner(FakeCfg(repo), str(repo))
    out = runner.run("read_file", {"path": "src/main/scala/Foo.scala", "start_line": 1, "end_line": 2})
    assert "case class Foo" in out


def test_tool_runner_unknown_tool(repo):
    runner = ToolRunner(FakeCfg(repo), str(repo))
    out = runner.run("not_a_real_tool", {})
    assert "ERROR" in out
    assert "unknown" in out.lower()


def test_tool_runner_non_dict_args(repo):
    runner = ToolRunner(FakeCfg(repo), str(repo))
    out = runner.run("read_file", "not a dict")
    assert "ERROR" in out


def test_tool_runner_missing_required_args(repo):
    runner = ToolRunner(FakeCfg(repo), str(repo))
    # Missing 'pattern' should not raise; tool should return ERROR string
    out = runner.run("grep_codebase", {})
    assert "ERROR" in out


def test_tool_runner_never_raises(repo):
    """Tools must never raise — they return error strings instead."""
    runner = ToolRunner(FakeCfg(repo), str(repo))
    # Throw garbage at every tool
    for name in ("grep_codebase", "read_file", "list_dir", "find_callers", "find_tests_for"):
        out = runner.run(name, {"path": None, "pattern": None, "symbol": None, "target": None})
        assert isinstance(out, str)


def test_tool_specs_well_formed():
    """TOOL_SPECS must conform to Bedrock's expected shape."""
    assert isinstance(TOOL_SPECS, list) and len(TOOL_SPECS) >= 5
    for spec in TOOL_SPECS:
        assert "toolSpec" in spec
        ts = spec["toolSpec"]
        assert "name" in ts and isinstance(ts["name"], str)
        assert "description" in ts and isinstance(ts["description"], str)
        assert "inputSchema" in ts
        assert "json" in ts["inputSchema"]
        schema = ts["inputSchema"]["json"]
        assert schema["type"] == "object"
        assert "properties" in schema
        assert "required" in schema


def test_grep_truncation_marker_on_huge_match(repo):
    """If grep returns >50K chars, output must be truncated with a marker."""
    huge = repo / "src" / "main" / "scala" / "Huge.scala"
    huge.write_text(("matchme " * 1000 + "\n") * 100)  # many matches per line
    out = grep_codebase("matchme", "", str(repo), "src/main/scala", ".scala")
    if len(out) >= 50_000:
        assert "truncated" in out.lower()


def test_read_file_end_line_negative_one_caps_at_200_lines_for_large_files(repo):
    """end_line=-1 on a >200-line file: the per-call cap (200) wins."""
    big = repo / "src" / "main" / "scala" / "Mid.scala"
    big.write_text("\n".join(f"line {i}" for i in range(1, 251)) + "\n")
    out = read_file("src/main/scala/Mid.scala", 1, -1, str(repo))
    line_count = sum(1 for ln in out.splitlines() if "│" in ln)
    assert line_count == 200
    assert "lines 1-200 of 250" in out


def test_read_file_end_line_negative_one_reaches_eof_when_remainder_fits(repo):
    """end_line=-1 with start past the cap: remainder is < 200 lines, so EOF wins."""
    big = repo / "src" / "main" / "scala" / "Mid.scala"
    big.write_text("\n".join(f"line {i}" for i in range(1, 251)) + "\n")
    out = read_file("src/main/scala/Mid.scala", 100, -1, str(repo))
    line_count = sum(1 for ln in out.splitlines() if "│" in ln)
    assert line_count == 151  # 250 - 100 + 1
    assert "lines 100-250 of 250" in out


def test_read_file_end_line_none_uses_default(repo):
    """end_line=None should behave the same as omitting the arg."""
    out = read_file("src/main/scala/Foo.scala", 1, None, str(repo))
    assert "case class Foo" in out
    assert "lines 1-" in out


# grep_codebase path_glob scope semantics: a real path scopes the search;
# extension filters and empty values keep the original whole-tree walk.


def test_grep_codebase_path_glob_scopes_to_single_file(repo):
    """A repo-relative file path narrows the search to that one file."""
    out = grep_codebase("Foo", "src/main/scala/Foo.scala", str(repo),
                        "src/main/scala", ".scala")
    # Must actually find matches (not just the no-match scope label) AND
    # exclude Bar.scala which would match a wider scope.
    assert "Found " in out
    assert "src/main/scala/Foo.scala:" in out
    assert "Bar.scala" not in out


def test_grep_codebase_path_glob_scopes_to_directory(repo):
    """A repo-relative directory path narrows the walk to that subtree."""
    # Tests live under src/test/scala — must NOT appear when scoped to src/main.
    out = grep_codebase("Foo", "src/main/scala", str(repo),
                        "src/main/scala", ".scala")
    assert "Foo.scala" in out
    assert "Bar.scala" in out
    assert "FooTest.scala" not in out


def test_grep_codebase_path_glob_extension_still_works(repo):
    """Back-compat: the '.ext' shape walks src_dir scope only.

    Negative control: FooTest.scala lives under src/test/scala (outside
    src_dir) and must NOT appear, so this test fails on an impl that
    walks the whole repo regardless of scope.
    """
    out = grep_codebase("Foo", ".scala", str(repo),
                        "src/main/scala", ".scala")
    assert "Foo.scala" in out
    assert "Bar.scala" in out
    assert "FooTest.scala" not in out


def test_grep_codebase_path_glob_empty_unchanged(repo):
    """Back-compat: empty path_glob walks src_dir with default_ext.

    Negative control: same as the extension test — verifies the empty
    case still scopes to src_dir, not the whole repo.
    """
    out = grep_codebase("Foo", "", str(repo),
                        "src/main/scala", ".scala")
    assert "Foo.scala" in out
    assert "Bar.scala" in out
    assert "FooTest.scala" not in out


def test_grep_codebase_path_glob_rejects_path_traversal(repo):
    """Path traversal must be rejected, like read_file."""
    out = grep_codebase("Foo", "../../etc/passwd", str(repo),
                        "src/main/scala", ".scala")
    assert out.startswith("ERROR:")
    assert "repo-relative" in out or "outside" in out


def test_grep_codebase_path_glob_nonexistent_path_returns_clear_error(repo):
    """A path that resolves outside src_dir or doesn't exist → clear error."""
    out = grep_codebase("Foo", "src/does/not/exist", str(repo),
                        "src/main/scala", ".scala")
    assert out.startswith("ERROR:")


def test_grep_codebase_scope_label_in_no_match_message(repo):
    """The 'no matches' message should reflect the actual scope so the
    model knows what was searched."""
    out = grep_codebase("ZZZNoSuchPattern", "src/main/scala/Foo.scala",
                        str(repo), "src/main/scala", ".scala")
    assert "No matches" in out
    assert "Foo.scala" in out


def test_grep_codebase_path_glob_dot_alone_rejected(repo):
    """'.' resolves to repo root (outside src_dir) — must be rejected,
    not silently broaden the search to docs/, tests/, build artifacts."""
    out = grep_codebase("Foo", ".", str(repo),
                        "src/main/scala", ".scala")
    assert out.startswith("ERROR:")
    assert "src/main/scala" in out  # error names the required scope


def test_grep_codebase_path_glob_parent_of_src_dir_rejected(repo):
    """'src' is a parent of src_dir 'src/main/scala' — must be rejected.
    Otherwise tests under src/test would leak into caller analysis."""
    out = grep_codebase("Foo", "src", str(repo),
                        "src/main/scala", ".scala")
    assert out.startswith("ERROR:")
    assert "src/main/scala" in out


def test_grep_codebase_path_glob_sibling_of_src_dir_rejected(repo):
    """A sibling directory (e.g., 'src/test/scala' when src_dir is
    'src/main/scala') must be rejected."""
    out = grep_codebase("Foo", "src/test/scala", str(repo),
                        "src/main/scala", ".scala")
    assert out.startswith("ERROR:")


def test_grep_codebase_skips_symlinks_pointing_outside_repo(tmp_path):
    """Security: a symlink inside src_dir pointing OUTSIDE the repo must
    not have its target read. A malicious PR could plant such a symlink
    and otherwise exfiltrate sensitive file contents into a public review."""
    src = tmp_path / "src" / "main" / "scala"
    src.mkdir(parents=True)
    (src / "normal.scala").write_text("Foo = inside\n")
    # tmp_path-scoped name so parallel pytest workers don't collide.
    secret = tmp_path.parent / f"secret_outside_grep_{tmp_path.name}.scala"
    try:
        secret.write_text("Foo = SHOULD_NEVER_LEAK\n")
        os.symlink(str(secret), str(src / "leak.scala"))
        out = grep_codebase("Foo", "", str(tmp_path), "src/main/scala", ".scala")
        assert "SHOULD_NEVER_LEAK" not in out
        assert "Foo = inside" in out
    finally:
        secret.unlink(missing_ok=True)


def test_find_callers_skips_symlinks_pointing_outside_repo(tmp_path):
    """Security: same protection on the find_callers walk path."""
    src = tmp_path / "src" / "main" / "scala"
    src.mkdir(parents=True)
    (src / "normal.scala").write_text("Foo = inside\n")
    # Use a tmp_path-scoped name so parallel pytest workers don't collide.
    secret = tmp_path.parent / f"secret_outside_callers_{tmp_path.name}.scala"
    try:
        secret.write_text("Foo = SHOULD_NEVER_LEAK\n")
        os.symlink(str(secret), str(src / "leak.scala"))
        out = find_callers("Foo", str(tmp_path), "src/main/scala", ".scala")
        assert "SHOULD_NEVER_LEAK" not in out
        assert "leak.scala" not in out
        # Positive control: the legitimate file IS still found, so a
        # regression that drops every result doesn't silently pass.
        assert "normal.scala" in out
    finally:
        secret.unlink(missing_ok=True)


def test_grep_codebase_relpath_is_clean_under_macos_tmpdir(tmp_path):
    """On macOS, tmp resolves through /private/. Paths in output must be
    repo-relative without '../../private/...' noise."""
    src = tmp_path / "src" / "main" / "scala"
    src.mkdir(parents=True)
    (src / "X.scala").write_text("Foo\n")
    out = grep_codebase("Foo", "", str(tmp_path), "src/main/scala", ".scala")
    # Must contain the repo-relative form and NOT the absolute escaped form.
    assert "src/main/scala/X.scala:" in out
    assert "../" not in out
    assert "/private/" not in out


def test_find_callers_relpath_is_clean_under_macos_tmpdir(tmp_path):
    """Same for find_callers."""
    src = tmp_path / "src" / "main" / "scala"
    src.mkdir(parents=True)
    (src / "X.scala").write_text("Foo\n")
    out = find_callers("Foo", str(tmp_path), "src/main/scala", ".scala")
    assert "src/main/scala/X.scala" in out
    assert "../" not in out
    assert "/private/" not in out


def test_grep_codebase_src_dir_escapes_repo_returns_error(tmp_path):
    """Operator misconfig: src_dir resolves outside repo (e.g., symlinked
    checkout). Must return a clear error rather than silent-empty."""
    elsewhere = tmp_path.parent / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / "X.scala").write_text("Foo\n")
    repo = tmp_path / "repo"
    repo.mkdir()
    # src_dir is a symlink to outside the repo
    os.symlink(str(elsewhere), str(repo / "src"))
    try:
        out = grep_codebase("Foo", "", str(repo), "src", ".scala")
        assert out.startswith("ERROR:")
        assert "outside" in out.lower()
    finally:
        # Clean up — Python pytest tmp_path will remove repo and its symlink,
        # but the elsewhere/ dir lives at tmp_path.parent so unlink it.
        import shutil
        shutil.rmtree(elsewhere, ignore_errors=True)


def test_find_callers_src_dir_escapes_repo_returns_error(tmp_path):
    """Same as above for find_callers."""
    elsewhere = tmp_path.parent / "elsewhere2"
    elsewhere.mkdir()
    (elsewhere / "X.scala").write_text("Foo\n")
    repo = tmp_path / "repo"
    repo.mkdir()
    os.symlink(str(elsewhere), str(repo / "src"))
    try:
        out = find_callers("Foo", str(repo), "src", ".scala")
        assert out.startswith("ERROR:")
        assert "outside" in out.lower()
    finally:
        import shutil
        shutil.rmtree(elsewhere, ignore_errors=True)


def test_find_callers_dedupes_in_repo_symlink(tmp_path):
    """A symlink A.scala -> B.scala (both inside repo) must not produce
    two file_hits entries with the same content. Otherwise ranked counts
    are inflated and the names-only cap fills with redundant rows."""
    src = tmp_path / "src" / "main" / "scala"
    src.mkdir(parents=True)
    (src / "B.scala").write_text("Foo target\n")
    os.symlink(str(src / "B.scala"), str(src / "A.scala"))
    out = find_callers("Foo", str(tmp_path), "src/main/scala", ".scala")
    # Either A.scala or B.scala appears, not both — they share content.
    appears_a = "A.scala" in out
    appears_b = "B.scala" in out
    assert appears_a != appears_b, (
        f"both A and B in output (no dedup): {out}"
    )


def test_grep_codebase_dedupes_in_repo_symlink(tmp_path):
    """Same dedup invariant for grep_codebase."""
    src = tmp_path / "src" / "main" / "scala"
    src.mkdir(parents=True)
    (src / "B.scala").write_text("Foo here\n")
    os.symlink(str(src / "B.scala"), str(src / "A.scala"))
    out = grep_codebase("Foo", "", str(tmp_path), "src/main/scala", ".scala")
    a_count = out.count("A.scala")
    b_count = out.count("B.scala")
    assert (a_count == 0) != (b_count == 0), (
        f"Foo line appears under both A and B (no dedup): {out}"
    )


def test_grep_codebase_broken_symlink_skipped_silently(tmp_path):
    """A broken symlink (target doesn't exist) must not crash or leak
    error noise — silently skip and continue with other files."""
    src = tmp_path / "src" / "main" / "scala"
    src.mkdir(parents=True)
    (src / "real.scala").write_text("Foo here\n")
    os.symlink(str(tmp_path / "does_not_exist.scala"), str(src / "broken.scala"))
    out = grep_codebase("Foo", "", str(tmp_path), "src/main/scala", ".scala")
    # Real file is found; broken symlink doesn't appear.
    assert "real.scala" in out
    assert "broken.scala" not in out
    # No error message about the broken symlink.
    assert "ERROR" not in out


def test_find_callers_broken_symlink_skipped_silently(tmp_path):
    """Same for find_callers."""
    src = tmp_path / "src" / "main" / "scala"
    src.mkdir(parents=True)
    (src / "real.scala").write_text("Foo\n")
    os.symlink(str(tmp_path / "does_not_exist.scala"), str(src / "broken.scala"))
    out = find_callers("Foo", str(tmp_path), "src/main/scala", ".scala")
    assert "real.scala" in out
    assert "broken.scala" not in out
    assert not out.startswith("ERROR")


def test_find_tests_for_skips_symlinks_pointing_outside_repo(tmp_path):
    """Same security guarantee on find_tests_for: a planted symlink
    inside the test tree must not have its out-of-repo target read."""
    test = tmp_path / "src" / "test" / "scala"
    test.mkdir(parents=True)
    src = tmp_path / "src" / "main" / "scala"
    src.mkdir(parents=True)
    (test / "RealTest.scala").write_text("class RealTest { def Foo() = ()  }\n")
    secret = tmp_path.parent / f"secret_outside_test_{tmp_path.name}.scala"
    try:
        secret.write_text("class SHOULD_NEVER_LEAK { Foo }\n")
        os.symlink(str(secret), str(test / "leak.scala"))
        out = find_tests_for("Foo", str(tmp_path), "src/main/scala", ".scala")
        assert "SHOULD_NEVER_LEAK" not in out
        assert "leak.scala" not in out
        # Positive control: legitimate test file IS surfaced.
        assert "RealTest.scala" in out
    finally:
        secret.unlink(missing_ok=True)


def test_find_tests_for_test_dir_escapes_repo_returns_error(tmp_path):
    """If the configured test dir resolves outside the repo (e.g., via a
    symlinked checkout), return a clear error rather than walk outside."""
    elsewhere = tmp_path.parent / "elsewhere_test"
    elsewhere.mkdir()
    (elsewhere / "ATest.scala").write_text("class ATest\n")
    repo = tmp_path / "repo"
    (repo / "src" / "main" / "scala").mkdir(parents=True)
    os.symlink(str(elsewhere), str(repo / "src" / "test"))
    try:
        out = find_tests_for("A", str(repo), "src/main/scala", ".scala")
        assert out.startswith("ERROR:")
        assert "outside" in out.lower()
    finally:
        import shutil
        shutil.rmtree(elsewhere, ignore_errors=True)


# find_callers returns matching-line snippets so the model can locate
# specific references without paging through the file.


def test_find_callers_includes_line_numbers(repo):
    """find_callers should emit 'line N:' for matching lines, not just file counts."""
    out = find_callers("Foo", str(repo), "src/main/scala", ".scala")
    assert "line " in out
    # Foo.scala line 1 is 'case class Foo(x: Int)'
    assert "case class Foo" in out


def test_find_callers_includes_line_context(repo):
    """The matching line text should be inlined so the model can decide
    whether to read more without another read_file call."""
    out = find_callers("Foo", str(repo), "src/main/scala", ".scala")
    # Bar.scala line 1: 'import com.example.Foo'
    assert "import com.example.Foo" in out
    # Bar.scala line 2: 'class Bar { def use(): Foo = Foo(1) }'
    assert "class Bar" in out


def test_find_callers_caps_snippets_per_file(repo):
    """A file with many references shows the first N lines plus a '...more' marker."""
    extra = repo / "src" / "main" / "scala" / "Heavy.scala"
    extra.write_text("\n".join(f"line{i}: Foo" for i in range(1, 11)) + "\n")
    out = find_callers("Foo", str(repo), "src/main/scala", ".scala")
    # Slice between the 'Heavy.scala' file row and the next file row
    # (any subsequent line that starts with '  ' followed by a non-space).
    heavy_start = out.index("Heavy.scala")
    next_file = -1
    cursor = heavy_start + 1
    while True:
        idx = out.find("\n  ", cursor)
        if idx == -1:
            break
        if len(out) > idx + 3 and out[idx + 3] != " ":
            next_file = idx
            break
        cursor = idx + 1
    heavy_section = out[heavy_start:next_file if next_file != -1 else len(out)]
    assert "more lines in this file" in heavy_section
    # Pin the cap: lines 1-5 are inlined, lines 6-10 are not.
    assert "line1: Foo" in heavy_section
    assert "line5: Foo" in heavy_section
    assert "line6: Foo" not in heavy_section
    assert "line10: Foo" not in heavy_section


def test_find_callers_counts_unchanged(repo):
    """The (N references) suffix is preserved for the model's ranking signal."""
    out = find_callers("Foo", str(repo), "src/main/scala", ".scala")
    assert "references" in out or "reference" in out


def test_find_callers_truncates_huge_lines(repo):
    """A pathologically long matching line shouldn't blow the result-size budget."""
    from issue_bot.tools import _MAX_SNIPPET_CHARS
    huge = repo / "src" / "main" / "scala" / "Huge.scala"
    huge.write_text("Foo " + ("x" * 5000) + "\n")
    out = find_callers("Foo", str(repo), "src/main/scala", ".scala")
    snippet_lines = [ln for ln in out.split("\n")
                     if "Foo" in ln and ln.lstrip().startswith("line ")]
    assert snippet_lines, "no inlined snippet found for Huge.scala"
    # Each inlined snippet line must be bounded: 'line N: ' prefix is < 16 chars,
    # body is capped at _MAX_SNIPPET_CHARS. Total must be well under 5000.
    longest = max(len(ln) for ln in snippet_lines)
    assert longest < _MAX_SNIPPET_CHARS + 32, (
        f"snippet line not truncated: {longest} chars (cap {_MAX_SNIPPET_CHARS} + prefix)"
    )


def test_find_callers_ranks_by_total_occurrences_not_lines(tmp_path):
    """A file with many same-line references must outrank a file with the
    same number of lines but fewer references each. Per-file occurrence
    count, not per-line, is the ranking signal."""
    src = tmp_path / "src" / "main" / "scala"
    src.mkdir(parents=True)
    # Dense: 1 line × 4 refs = 4 occurrences.
    (src / "Dense.scala").write_text("def f(a: Foo, b: Foo, c: Foo): Foo = ()\n")
    # Sparse: 3 lines × 1 ref = 3 occurrences.
    (src / "Sparse.scala").write_text("import Foo\nclass A\nval x: Foo = ???\nval y = 1\nval z: Foo = ???\n")
    out = find_callers("Foo", str(tmp_path), "src/main/scala", ".scala")
    dense_idx = out.index("Dense.scala")
    sparse_idx = out.index("Sparse.scala")
    assert dense_idx < sparse_idx, (
        f"Dense.scala (4 refs on 1 line) must outrank Sparse.scala "
        f"(3 refs on 3 lines):\n{out}"
    )
    assert "Dense.scala (4 references)" in out
    assert "Sparse.scala (3 references)" in out


def test_find_tests_for_handles_non_maven_src_dir(tmp_path):
    """If src_dir doesn't contain 'src/main' (e.g., 'code/scala'),
    find_tests_for must NOT walk the source tree as if it were tests."""
    code = tmp_path / "code" / "scala"
    code.mkdir(parents=True)
    test = tmp_path / "src" / "test" / "scala"
    test.mkdir(parents=True)
    (code / "Foo.scala").write_text("class Foo\n")
    (test / "FooTest.scala").write_text("class FooTest\n")
    out = find_tests_for("Foo", str(tmp_path), "code/scala", ".scala")
    assert "FooTest.scala" in out
    # Negative control: source tree must NOT be walked as tests.
    assert "code/scala/Foo.scala" not in out


def test_find_tests_for_no_test_dir_when_src_dir_lacks_src_main(tmp_path):
    """Non-Maven layout with no src/test/ either → clean error, not a
    silent walk of the source tree."""
    code = tmp_path / "code" / "scala"
    code.mkdir(parents=True)
    (code / "Foo.scala").write_text("class Foo\n")
    out = find_tests_for("Foo", str(tmp_path), "code/scala", ".scala")
    assert out.startswith("ERROR:")
    assert "no test directory" in out


def test_find_tests_for_prefers_convention_over_grep_match_on_symlink(tmp_path):
    """When a symlink and its convention-named target share a realpath,
    the convention-named hit must win regardless of os.walk order."""
    test = tmp_path / "src" / "test" / "scala"
    test.mkdir(parents=True)
    src = tmp_path / "src" / "main" / "scala"
    src.mkdir(parents=True)
    # The convention-named test mentions Foo.
    (test / "FooTest.scala").write_text("class FooTest { def Foo() = () }\n")
    # A non-conventional name in an alphabetically-earlier dir, symlinked
    # to the convention test (same realpath). os.walk visits 'aux/' first.
    aux = tmp_path / "src" / "test" / "aux"
    aux.mkdir(parents=True)
    os.symlink(str(test / "FooTest.scala"), str(aux / "Helper.scala"))
    out = find_tests_for("Foo", str(tmp_path), "src/main/scala", ".scala")
    # Convention path wins.
    assert "FooTest.scala" in out
    # Symlinked alias does not appear (would be redundant).
    assert "Helper.scala" not in out


def test_grep_codebase_path_glob_symlinked_file_uses_realpath_in_label(tmp_path):
    """When path_glob is a symlink to an in-repo file, the scope label
    should match the realpath-relative form so it agrees with match rows."""
    src = tmp_path / "src" / "main" / "scala"
    src.mkdir(parents=True)
    (src / "Real.scala").write_text("Foo here\n")
    os.symlink(str(src / "Real.scala"), str(src / "Alias.scala"))
    out = grep_codebase("Foo", "src/main/scala/Alias.scala", str(tmp_path),
                        "src/main/scala", ".scala")
    # Header and match rows agree: both refer to Real.scala (the realpath).
    assert "src/main/scala/Real.scala" in out
    # The symlink-as-typed name does NOT appear (would mislead the model).
    assert "Alias.scala" not in out


def test_grep_codebase_path_glob_absolute_path_rejected(repo):
    """Absolute path_glob must be rejected, paralleling read_file's guard."""
    out = grep_codebase("Foo", "/etc/passwd", str(repo),
                        "src/main/scala", ".scala")
    assert out.startswith("ERROR:")


def test_iter_matches_in_file_rejects_out_of_repo_path(tmp_path):
    """Defense-in-depth: if a future caller skips _resolve_grep_scope and
    passes an out-of-repo path directly, the helper must yield nothing."""
    import re
    from issue_bot.tools import _iter_matches_in_file
    repo = tmp_path / "repo"
    repo.mkdir()
    outside = tmp_path.parent / f"outside_{tmp_path.name}.scala"
    try:
        outside.write_text("Foo here\n")
        results = list(_iter_matches_in_file(
            re.compile("Foo"), str(outside), str(repo), 50,
        ))
        assert results == []
    finally:
        outside.unlink(missing_ok=True)


def test_find_tests_for_convention_upgrades_grep_match_past_limit(tmp_path):
    """Past the 20-entry limit, an in-flight convention-name visit must
    still upgrade an existing grep-match (same realpath) so symlink walk
    order can't trap a worse path."""
    test = tmp_path / "src" / "test" / "scala"
    test.mkdir(parents=True)
    src = tmp_path / "src" / "main" / "scala"
    src.mkdir(parents=True)
    # The real test file is convention-named, but a non-conventional
    # symlink in an alphabetically-earlier dir points to it.
    real_test = test / "TargetTest.scala"
    real_test.write_text("class TargetTest { def Target() = () }\n")
    aux = tmp_path / "src" / "test" / "aux"
    aux.mkdir(parents=True)
    os.symlink(str(real_test), str(aux / "Aux.scala"))
    # Fill the dict with 20 unrelated grep-matches that all reference Target.
    for i in range(20):
        (test / f"Other{i}.scala").write_text("// Target reference\n")
    out = find_tests_for("Target", str(tmp_path), "src/main/scala", ".scala")
    # Convention-named hit wins despite ordering and limit pressure.
    assert "TargetTest.scala" in out
    assert "Aux.scala" not in out
