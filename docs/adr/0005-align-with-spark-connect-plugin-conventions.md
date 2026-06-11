# ADR-0005 — Align stub delivery and versioning with Spark Connect plugin conventions

Date: 2026-06-10
Status: Accepted
Supersedes: ADR-0003 (in full); ADR-0004 (the wire_format_version guard only)

## Context

ADR-0003 proposed shipping `deequ_connect.proto` inside the deequ JAR at
`META-INF/protobuf/deequ_connect.proto` and having python-deequ run `protoc`
at build time. ADR-0004 proposed adding a `wire_format_version` string field
to every Spark Connect Relation envelope, asserted server-side as a
JAR/wheel pairing handshake.

Both decisions were captured in the absence of community precedent. A
follow-up survey (workflow `wf_fb8960d9-146`, 2026-06-10) of five Spark
Connect plugin codebases — Apache Spark itself, Delta Connect, GraphFrames,
Apache Iceberg, Apache Sedona — produced primary-source evidence that
**none** of them does either thing. The relevant findings (citations below):

1. **No plugin ships `.proto` inside its JAR.** Apache Spark's
   `project/SparkBuild.scala` contains
   `case m if m.toLowerCase(Locale.ROOT).endsWith(".proto") => MergeStrategy.discard`
   with the comment *"Drop all proto files that are not needed as artifacts
   of the build."* Delta Connect's `connectCommon` module has no
   `resources/` directory and no jar mapping for the protobuf source.
   GraphFrames checks in the generated `_pb2.py` to its Python source tree
   (`python/graphframes/connect/proto/graphframes_pb2.py`).

2. **No plugin uses a `wire_format_version` field.** Spark's `UserContext`
   and `AnalyzePlanRequest` have no version field. The only version-adjacent
   field is `optional string client_type` whose own comment explicitly
   states *"will not be interpreted by the server."* Delta's `DeltaRelation`
   and `DeltaCommand` are bare `oneof` discriminators with no version field.
   GraphFrames evolves by appending `oneof method` cases.

3. **The Spark RelationPlugin contract is `byte[]` + `Any.parseFrom` +
   `is(class)` + `unpack(class)`.** The protobuf type URL of the wrapped
   message is the de-facto version discriminator. When a plugin doesn't
   recognize the type URL, it returns `Optional.empty()` and Spark surfaces
   `noHandlerFoundForExtension`. There is no handshake.

## Decision

**On stub delivery (replaces ADR-0003):**

- The deequ JAR ships only generated Java classes. Drop the
  `<resources>` block from `pom.xml` that copied `.proto` to
  `META-INF/protobuf/`.
- python-deequ checks in the generated `deequ_connect_pb2.py` and
  `deequ_connect_pb2.pyi` alongside its Python source, in
  `pydeequ/v2/proto/`. This matches GraphFrames' precedent.
- `scripts/regen_proto.py` becomes a developer convenience: run it manually
  whenever the schema changes in the deequ repo to refresh the checked-in
  stubs. It is no longer wired into the wheel build.
- python-deequ's CI does not download the deequ JAR for stub generation.
  It still downloads the JAR for the Spark Connect integration-test phase
  (no change there).

**On wire versioning (replaces the guard from ADR-0004):**

- Drop the `wire_format_version` field from every Spark Connect Relation
  envelope.
- Drop the `WIRE_FORMAT_VERSION` constant and `assertWireFormat()` helper
  from `DeequRelationPlugin`.
- Drop the `WIRE_FORMAT_VERSION` constant from `pydeequ/v2/proto/__init__.py`.
- Rely on the protobuf type URL as the wire-version discriminator. When a
  breaking schema change is needed, bump the message name or promote to a
  new package (e.g., `com.amazon.deequ.connect.v2`); older plugins
  naturally `Optional.empty()` on the new type URL.

**Unchanged:**

- ADR-0001 (`oneof` arms per builder method).
- ADR-0002 (clean break with `reserved` discipline).
- ADR-0004's deequ-first JAR-tag-pinned rollout sequence. Only the
  version-handshake part of ADR-0004 is superseded.

## Consequences

### Positive

- **Smaller surface area.** No build-time JAR-fetch dance; no env-var
  override; no protoc dependency at python-deequ install time. Contributors
  who clone python-deequ can run `pip install -e .` and the imports work.
- **Aligned with Spark.** Future maintainers reading the schema or the
  plugin will see the same shape Spark itself uses. No "why is there a
  version field here?" question.
- **Drift becomes auditable.** The diff in `pydeequ/v2/proto/*_pb2.py` is
  visible in PRs whenever the schema changes — protoc is deterministic, so
  a stale checked-in stub fails CI's regen-and-diff check.

### Negative

- **Manual regen step.** When the deequ schema changes, a contributor must
  run `python scripts/regen_proto.py DEEQU_JAR_PATH=...` and commit the
  diff. This is the GraphFrames pattern and is acceptable, but it is one
  more step than a fully-automated build hook would be.
- **`_pb2.py` diffs in PRs.** Generated files are noisy. Mitigated by
  treating them as build artifacts in PR review (look at the .proto diff,
  trust the regen).

### Drift safeguard

python-deequ's CI gains a job that runs `scripts/regen_proto.py` against
the pinned JAR and asserts `git diff --exit-code pydeequ/v2/proto/`. If
the checked-in stubs disagree with what protoc produces from the pinned
JAR, CI fails. This is the GraphFrames pattern adapted with explicit
verification.

## Citations

- Apache Spark `project/SparkBuild.scala` (master): `case m if m.toLowerCase(Locale.ROOT).endsWith(".proto") => MergeStrategy.discard`
- Apache Spark `sql/connect/server/src/main/java/org/apache/spark/sql/connect/plugin/RelationPlugin.java`: `Optional<LogicalPlan> transform(byte[] relation, SparkConnectPlanner planner)`
- Apache Spark `sql/connect/common/src/main/protobuf/spark/connect/base.proto`: `optional string client_type = 3;` with comment *"will not be interpreted by the server"*
- Delta Connect `spark-connect/common/src/main/protobuf/delta/connect/relations.proto`: bare `oneof relation_type`, no version field
- GraphFrames `python/graphframes/connect/proto/graphframes_pb2.py`: checked-in generated stub
- GraphFrames `connect/src/main/protobuf/graphframes.proto`: bare `oneof method` with 24 variants, no version field
