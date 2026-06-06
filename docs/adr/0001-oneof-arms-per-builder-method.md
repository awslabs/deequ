# ADR-0001 — `oneof` arms per builder method (not per family)

Date: 2026-05-29
Status: Accepted

## Context

The Spark Connect wire format for Deequ uses two messages — `ConstraintMessage`
and `AnalyzerMessage` — that act as kitchen-sinks keyed by a free-form
`string type = 1`. Whether a given field (`column`, `columns`, `pattern`,
`allowed_values`, `quantile`, …) is meaningful depends on the value of `type`.
That invariant is encoded only in doc-comments, the Python builder kwargs, and
the Scala decoder's `match` arms — three places, kept in sync by hand.

Stage 1 of the protobuf review proposes replacing this with a `oneof body`.
There are three candidate shapes for the arm granularity:

- **α** — one arm per builder method (~30 arms in `Constraint`, ~22 in
  `Analyzer`), with payload submessages reused across arms.
- **β** — one arm per parameter shape (~7 arms), with a discriminator enum
  inside each arm.
- **γ** — one arm per Deequ semantic family (~10 arms), discriminator enum
  inside.

## Decision

Use **α**: one `oneof` arm per builder method.

## Consequences

### Positive

- The arm name *is* the constraint identity. The Scala decoder switches on
  `getBodyCase` and the compiler enforces exhaustiveness — adding a new
  constraint means the schema, the Python emitter, and the Scala decoder all
  have to be touched, with the compiler telling each side what's missing.
- Each arm carries exactly the shape it needs. No "is this field meaningful?"
  branch on either side. The default `case _ =>` arm in the Scala builder
  becomes unreachable.
- Style-guide alignment: `proto3 §oneof` and dos-and-donts §encapsulate
  endorse one-shape-per-arm.

### Negative

- ~30 arms in `Constraint` and ~22 in `Analyzer` is visually long in the
  schema file. Mitigated by reusing a small set of payload submessages
  (`ColumnSpec`, `ColumnsSpec`, `PatternSpec`, `AllowedValuesSpec`,
  `QuantileSpec`, `PairColumnsSpec`) so each arm is one line.

### Rejected alternatives

- **β / γ** reintroduce the inner discriminator that Stage 1 was specifically
  about removing. The Scala side would still need
  `match (innerType, fields_set)` rather than just `match (oneof_arm)`.
  The promise of "schema = single source of truth" doesn't hold under β/γ.
