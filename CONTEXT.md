# Deequ — domain glossary

Living glossary of terms used in this repository. Capture only language that
needs precision; let well-named code speak for itself.

## Wire format (Spark Connect)

### Spec submessage

A small, reused protobuf submessage that carries a parameter bundle for one or
more `oneof` arms in the Deequ Connect schema. Examples: `ColumnSpec`,
`ColumnsSpec`, `PatternSpec`, `AllowedValuesSpec`, `QuantileSpec`,
`PairColumnsSpec`.

Naming convention: `…Spec` suffix. The suffix signals "this is a payload
bundle, not a top-level concept."

Spec submessages live at the top level of the schema so they can be referenced
from both `Constraint` and `Analyzer` `oneof` arms without duplication.

### Builder method (in this glossary's sense)

A public method on `Check` (Scala or Python) such as `isComplete`,
`hasCompleteness`, `hasPattern`. In the new schema, **one builder method
corresponds to exactly one `oneof` arm** — see ADR-0001.

### Canonical schema home

The `.proto` file lives in `deequ/src/main/protobuf/`. python-deequ consumes
generated stubs shipped in (or alongside) the Deequ JAR release. python-deequ
does not maintain its own copy of the schema.

### Constraint rule set

A named bundle of constraint-suggestion rules from
`com.amazon.deequ.suggestions.Rules`. The wire format encodes them as the
`ConstraintRuleSet` enum:

| Wire enum                          | Scala-side `Rules` value |
| ---------------------------------- | ------------------------ |
| `CONSTRAINT_RULE_SET_DEFAULT`      | `Rules.DEFAULT`          |
| `CONSTRAINT_RULE_SET_STRING`       | `Rules.STRING`           |
| `CONSTRAINT_RULE_SET_NUMERICAL`    | `Rules.NUMERICAL`        |
| `CONSTRAINT_RULE_SET_COMMON`       | `Rules.COMMON`           |
| `CONSTRAINT_RULE_SET_EXTENDED`     | `Rules.EXTENDED`         |

`predefined_types` deliberately stays as `map<string, string>` for now — the
set of supported type names is not closed. See ADR-0002 (when written) if/when
that changes.
