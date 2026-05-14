#!/usr/bin/env python3
"""
Generates a comprehensive Deequ knowledge base from the repository source.

Two-tier strategy ensures all files are represented within budget:
  Small files (<=100 lines): Full source verbatim
  Larger files (>100 lines): Signatures + implementation-significant lines
    (method/class/trait declarations, SQL patterns like CAST/COALESCE,
     first line of each scaladoc block)

Usage (from repo root):
    python3 src/scripts/generate_kb.py > /tmp/deequ-kb.md
"""

import sys
import re
from pathlib import Path
from collections import defaultdict

REPO_ROOT = Path(".")
SRC_MAIN = REPO_ROOT / "src" / "main" / "scala"
SRC_TEST = REPO_ROOT / "src" / "test" / "scala"
README_PATH = REPO_ROOT / "README.md"
POM_PATH = REPO_ROOT / "pom.xml"

MAX_TOTAL_CHARS = 500000
SMALL_FILE_THRESHOLD = 100

_LICENSE_END_RE = re.compile(r'^\s*\*/\s*$')
_SIGNIFICANT_LINE_RE = re.compile(
    r'(def |val |var |class |trait |object |case class |override |extends |with |import )'
)
_IMPL_PATTERN_RE = re.compile(
    r'(CAST|COALESCE|expr\(|col\(|lit\(|sql\(|COUNT_COL|NullFieldReplacement|escapeColumn|removeEscapeColumn)'
)


def read_safe(path):
    try:
        return path.read_text(errors="replace")
    except Exception as e:
        return f"(could not read: {e})"


def strip_license(content):
    lines = content.split("\n")
    in_license = False
    result = []
    for i, line in enumerate(lines):
        if not in_license and ("Copyright" in line or "Licensed under" in line) and ("/*" in line or " *" in line):
            in_license = True
            if result and result[-1].strip() in ("/**", "/*"):
                result.pop()
            continue
        if in_license:
            if _LICENSE_END_RE.match(line):
                in_license = False
            continue
        result.append(line)
    return "\n".join(result).lstrip("\n")


def summarize_tier2(content):
    lines = content.split("\n")
    kept = []
    in_doc = False
    doc_first_line_seen = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("/**"):
            in_doc = True
            doc_first_line_seen = False
            continue
        if in_doc:
            if stripped.startswith("*/"):
                in_doc = False
                continue
            if not doc_first_line_seen and stripped.startswith("*") and len(stripped) > 2:
                kept.append("  // " + stripped.lstrip("* ").rstrip())
                doc_first_line_seen = True
            continue
        if _SIGNIFICANT_LINE_RE.search(line) or _IMPL_PATTERN_RE.search(line):
            kept.append(line)
        elif stripped == "}" or stripped == "{":
            if kept and not kept[-1].strip() == stripped:
                kept.append(line)
    return "\n".join(kept)




def collect_scala_files(root):
    if not root.exists():
        return {}
    grouped = defaultdict(list)
    for f in sorted(root.rglob("*.scala")):
        rel = f.relative_to(root)
        package_dir = str(rel.parent)
        grouped[package_dir].append(f)
    return dict(sorted(grouped.items()))


def main():
    if not SRC_MAIN.exists():
        print(f"Error: {SRC_MAIN} not found. Run this from the deequ repo root.", file=sys.stderr)
        sys.exit(1)

    out = []
    total_chars = [0]

    def emit(text):
        out.append(text)
        total_chars[0] += len(text)

    main_files = list(SRC_MAIN.rglob("*.scala"))
    test_files = list(SRC_TEST.rglob("*.scala")) if SRC_TEST.exists() else []

    emit("# Deequ Knowledge Base")
    emit("")
    emit(f"Source: {len(main_files)} main files, {len(test_files)} test files")
    emit("")

    if README_PATH.exists():
        emit("## README")
        emit("")
        emit(read_safe(README_PATH))
        emit("")

    if POM_PATH.exists():
        pom_text = read_safe(POM_PATH)
        emit("## Build Configuration (pom.xml excerpt)")
        emit("")
        emit("```xml")
        for line in pom_text.split("\n"):
            stripped = line.strip()
            if any(k in stripped for k in [
                "<groupId>com.amazon", "<artifactId>deequ", "<version>",
                "<spark.version>", "<scala.version>", "<scala.compat",
                "<java.version>", "<scalatest",
            ]):
                emit(line)
        emit("```")
        emit("")

    emit("## Source Code Reference")
    emit("")

    main_grouped = collect_scala_files(SRC_MAIN)

    for package_dir, files in main_grouped.items():
        package_label = package_dir.replace("/", ".")
        emit(f"## {package_label} ({len(files)} files)")
        emit("")

        for filepath in files:
            rel = filepath.relative_to(REPO_ROOT)
            content = read_safe(filepath)
            file_lines = content.count("\n") + 1

            if total_chars[0] >= MAX_TOTAL_CHARS:
                emit(f"#### `{rel}` ({file_lines} lines) — SKIPPED (KB size limit)")
                emit("")
                continue

            if file_lines <= SMALL_FILE_THRESHOLD:
                processed = strip_license(content)
                tier_label = "full"
            else:
                processed = summarize_tier2(strip_license(content))
                tier_label = "signatures"

            emit(f"#### `{rel}` ({file_lines} lines, {tier_label})")
            emit("")
            emit("```scala")
            emit(processed)
            emit("```")
            emit("")

    if test_files:
        emit("## Test Files")
        emit("")
        test_grouped = collect_scala_files(SRC_TEST)
        for package_dir, files in test_grouped.items():
            emit(f"### {package_dir.replace('/', '.')}")
            for f in files:
                rel = f.relative_to(REPO_ROOT)
                lc = f.read_text(errors="replace").count("\n") + 1
                emit(f"- `{rel}` ({lc} lines)")
            emit("")

    emit("## Common Usage Patterns")
    emit("")
    emit("### Basic Verification")
    emit("```scala")
    emit("val result = VerificationSuite().onData(df)")
    emit("  .addCheck(Check(CheckLevel.Error, \"checks\")")
    emit("    .isComplete(\"col\").isUnique(\"id\").hasSize(_ > 0))")
    emit("  .run()")
    emit("```")
    emit("")
    emit("### DQDL")
    emit("```scala")
    emit("val results = EvaluateDataQuality.process(df,")
    emit("  \"\"\"Rules=[IsComplete \"col\", RowCount > 0, Uniqueness \"id\" = 1.0]\"\"\")")
    emit("```")
    emit("")
    emit("### Row-Level Results")
    emit("```scala")
    emit("val results = EvaluateDataQuality.processRows(df, rules)")
    emit("results(\"rowLevelOutcomes\").select(\"DataQualityRulesPass\", \"DataQualityRulesFail\").show()")
    emit("```")
    emit("")
    emit("### Metrics Repository")
    emit("```scala")
    emit("val repo = FileSystemMetricsRepository(spark, \"s3://bucket/metrics.json\")")
    emit("VerificationSuite().onData(df).useRepository(repo)")
    emit("  .saveResultsWithKey(ResultKey(System.currentTimeMillis()))")
    emit("  .addCheck(...).run()")
    emit("```")
    emit("")
    emit("### Data Profiling")
    emit("```scala")
    emit("val profiles = ColumnProfilerRunner().onData(df).run()")
    emit("profiles.profiles.foreach { case (name, p) =>")
    emit("  println(s\"$name: completeness=${p.completeness}, type=${p.dataType}\")")
    emit("}")
    emit("```")
    emit("")
    emit("### Constraint Suggestions")
    emit("```scala")
    emit("val suggestions = ConstraintSuggestionRunner().onData(df)")
    emit("  .addConstraintRules(Rules.DEFAULT).run()")
    emit("```")
    emit("")
    emit("### Anomaly Detection")
    emit("```scala")
    emit("VerificationSuite().onData(df).useRepository(repo)")
    emit("  .saveResultsWithKey(key)")
    emit("  .addAnomalyCheck(RelativeRateOfChangeStrategy(maxRateIncrease = Some(2.0)), Size())")
    emit("  .run()")
    emit("```")
    emit("")

    emit("## Compatibility")
    emit("")
    emit("- Deequ 2.x requires Spark 3.1+, Scala 2.12, Java 8+")
    emit("- Deequ 1.x supports Spark 2.2.x through 3.0.x")
    emit("- Spark 2.x builds use Scala 2.11")
    emit("- Available on Maven Central: `com.amazon.deequ:deequ`")
    emit("- PyDeequ (Python wrapper): github.com/awslabs/python-deequ")

    result = "\n".join(out)
    print(result)
    print(f"\n<!-- KB stats: {len(result)} chars, {result.count(chr(10))} lines -->", file=sys.stderr)


if __name__ == "__main__":
    main()
