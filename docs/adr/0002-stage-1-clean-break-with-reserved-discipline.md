# ADR-0002 — Stage 1 schema is a clean wire break; future removals use `reserved`

Date: 2026-05-29
Status: Accepted

## Context

Stage 1 of the protobuf review (ADR-0001 + the supporting candidates) renames
messages, restructures `ConstraintMessage` and `AnalyzerMessage` into `oneof`
shapes, deletes orphan enums, and renumbers fields where natural. We had three
options for handling backwards compatibility:

- **W1** — clean break, free renumbering, no `reserved` discipline.
- **W2** — clean break, but `reserved` every removed field number and name.
- **W3** — keep old shapes alongside new shapes for a deprecation cycle.

At the time of decision, the v2 wire format ships only in
`pydeequ 2.0.0b1` + an internal Deequ build. There are no external clients
relying on b1.

## Decision

**W2: clean break for Stage 1, with `reserved` discipline going forward.**

Concretely:

1. The Stage 1 PR pair (deequ + python-deequ) replaces the old schema in one
   move. Old `pydeequ 2.0.0b1` against the new JAR (or vice versa) will fail
   to parse — by design.
2. Every field tag and field name we remove during Stage 1 gets a
   `reserved` declaration in its parent message, even though the audience
   today is internal. This sets the precedent for *future* schema breaks.

## Consequences

### Positive

- Stage 1 actually removes the kitchen-sink shapes. No "both shapes alive"
  decoder branches; no test-surface bloat.
- `reserved` discipline catches the next schema author's mistake at compile
  time. Aligns with the proto3 guide's standing advice.
- We can credit the beta tag honestly — clients should expect b2 to break b1.

### Negative

- Internal users running mismatched JAR + wheel during the rollout will see
  hard failures rather than a graceful deprecation warning. Mitigated by
  releasing both sides as a single coordinated pair.
- Future schema breaks beyond Stage 1 still cost a coordinated pair release;
  W2 doesn't change that. It only lets us *reuse* this discipline for them.

### When to revisit

When the v2 wire format gains an external client (a non-Amazon team, a
public Deequ release that depends on the Connect plugin, or a non-Python
client implementation). At that point, the next breaking change should
probably use W3 (deprecate-and-cycle) instead.
