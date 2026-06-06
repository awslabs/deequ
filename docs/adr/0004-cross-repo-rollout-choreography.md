# ADR-0004 — Cross-repo Stage 1 rollout: deequ-first, pinned JAR-by-tag

Date: 2026-05-29
Status: Accepted

## Context

Stage 1 of the protobuf review is one logical change spread across two
repositories: deequ (Scala plugin + canonical schema) and python-deequ
(client). The schema break is clean (ADR-0002), so the two repos must move
together — but they cannot merge atomically.

We considered three rollout shapes:

- **C1** — Deequ merges first, CI cuts a pre-release tag (e.g. `v2.0.0b2`),
  python-deequ pins to that tag, then merges.
- **C2** — Deequ produces an RC artifact in a non-public channel; both PRs
  merge in close succession; deequ promotes the RC to release.
- **C3** — Monorepo — move the schema and a Python harness into deequ
  entirely. Out of scope for Stage 1.

## Decision

Use **C1**, with a wire-format-version assertion as a runtime guard.

### Sequence

1. Deequ PR lands on master:
   - New `deequ_connect.proto` (Stage 1 schema, per ADR-0001 / ADR-0002).
   - Maven build packages it under `META-INF/protobuf/` (per ADR-0003).
   - `CheckBuilder.scala` and `AnalyzerBuilder.scala` rewritten as
     pattern matches on `getBodyCase`. Default `case _ => throw` arms
     become unreachable; the compiler enforces exhaustiveness.
   - Plugin integration tests assert the new shapes.
   - Release CI cuts `v2.0.0b2` JAR and publishes to GitHub Releases.
2. python-deequ PR opens, pinned to deequ `v2.0.0b2`:
   - Build hook downloads JAR, extracts `.proto`, runs `protoc`.
   - `pydeequ/v2/proto/` is `.gitignore`d; the checked-in copy is
     deleted.
   - Typed builders per `oneof` arm in `checks.py` and `analyzers.py`.
   - CI workflow downloads the same JAR for the integration-test phase.
   - Wheel `v2.0.0b2` is published.
3. Both repos' CHANGELOGs explicitly call out the wire break; users
   running `pydeequ 2.0.0b1` are told to upgrade pair-wise.

### Wire-format-version guard

The new `deequ_connect.proto` carries a `WIRE_FORMAT_VERSION` constant
(implemented as a `string wire_format_version = 1` on every Spark Connect
relation envelope, populated by the client emitter and validated by the
plugin). When the values disagree the plugin returns a structured error
message instead of a parse failure on the new shape. This keeps the
wire-break loud rather than silent during the rollout window.

## Consequences

### Positive

- **Internal-audience-appropriate.** No PyPI promotion ritual, no separate
  RC channel — just two PRs and a tag, which the team already does.
- **Wire mismatches surface as actionable errors.** A user with the wrong
  JAR/wheel pair gets a clear "expected version X, got Y" message rather
  than a low-level proto parse error.
- **Future stage rollouts inherit the same shape.** Stage 2 (1-1-1 file
  split, deferred candidates) follows the same C1 pattern.

### Negative

- **There is a window between deequ-merged and python-deequ-merged where
  python-deequ master still pins the old JAR.** Mitigated by the version
  guard above; harmless for any contributor who builds python-deequ
  master without explicitly bumping the JAR (they keep getting the old
  paired version).
- **The schema and the plugin can never merge atomically.** Deferred to
  C3 (monorepo) when/if the project ever needs that.

### When to revisit

- If a third client (Java, Go) ships, the rollout becomes
  N-repo-coordinated and C1 stops scaling. At that point look at C3 or a
  schema package on a registry (BSR or similar).
- If the `WIRE_FORMAT_VERSION` guard accumulates more responsibilities
  (capability negotiation, feature flags), promote it from a string
  constant to a typed `WireCapabilities` message.
