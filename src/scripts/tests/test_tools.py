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
