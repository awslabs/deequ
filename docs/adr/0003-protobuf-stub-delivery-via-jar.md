# ADR-0003 — Stage 1 stub delivery: ship `.proto` in the JAR; python-deequ runs `protoc` at build

Date: 2026-05-29
Status: Accepted

## Context

ADR-0001 settled the schema design; ADR-0002 settled the wire-break policy.
With deequ established as the canonical home of `deequ_connect.proto`, the
remaining question for Stage 1 is **how generated Python stubs reach
python-deequ**. Three concrete mechanisms were on the table:

- **D1** — Deequ Maven build runs `protoc --python_out`, packages the result
  as a sidecar wheel (`pydeequ-protobuf`), publishes to PyPI.
  python-deequ depends on it as a runtime package.
- **D2** — Deequ JAR includes the raw `.proto` source under
  `META-INF/protobuf/`. python-deequ's build downloads the JAR, extracts the
  `.proto`, runs `protoc --python_out`, and bundles the generated stubs into
  its own wheel. Generated stubs are not committed to python-deequ.
- **D3** — Same as D2 but generated stubs are committed; a sync script + a
  CI drift-check keeps them aligned with the JAR.

## Decision

Use **D2**.

- The deequ Maven build adds a `<resource>` include so
  `src/main/protobuf/deequ_connect.proto` is packaged at
  `META-INF/protobuf/deequ_connect.proto` inside the published JAR.
- python-deequ's build (Poetry-driven) gains a pre-build hook that
  downloads the pinned JAR, extracts the embedded `.proto`, runs `protoc`
  with `--python_out` + `--pyi_out`, and writes the generated files to
  `pydeequ/v2/proto/` *inside the build tree only*. The directory is
  `.gitignore`d.
- The published `pydeequ` wheel **bundles** the generated stubs so end
  users `pip install pydeequ` without needing `protoc`. `protoc` is a
  python-deequ build-time dependency, not a pydeequ runtime dependency.

## Consequences

### Positive

- **Drift impossible by construction.** Stubs are regenerated from the
  exact JAR python-deequ pins. There is no second source of truth to keep
  in sync.
- **Single PR pair per Stage 1 schema change.** Deequ ships the JAR,
  python-deequ bumps its pinned JAR version, build regenerates. No
  copy-paste of `.proto` files; no checked-in `_pb2.py` diff.
- **Clean schema-on-source statement.** A reader cloning python-deequ
  doesn't see schema files in the source tree — making it visible that
  the schema is owned elsewhere.
- **Smaller python-deequ git history.** Generated `_pb2.py` (and the
  larger `_pb2.pyi`) no longer churn in PRs.

### Negative

- python-deequ contributors need `protoc` available locally for first
  build. Mitigated: Poetry's pre-build hook can vendor a pinned `protoc`
  binary via `grpcio-tools` (a pure-Python wheel that includes the
  compiler), removing the system-`protoc` dependency entirely.
- CI gains a step that downloads the JAR. Mitigated: the JAR is already
  needed for the integration-test phase that starts the Spark Connect
  server, so the download is shared.
- IDE-level navigation from python-deequ source into the schema requires
  an extra step (look at the JAR or jump to the deequ repo). Acceptable
  trade-off; the schema is still text in a known location.

### Escape valve

If `protoc` in CI proves flaky across platforms, flip to **D3**: same
`.proto`-in-JAR contract, but commit the generated stubs and add a CI
drift-check. Migration is single-PR (commit the stubs; remove the
pre-build hook; add the drift-check job). Users see no change.

### When to revisit

When the Connect plugin gains a third client (Java client without
DataFrame access, Go client, etc.). At that point **D1** (sidecar wheels
per language, versioned BSR-style schema package) becomes the right shape.
Until then D2 is the cheapest correct answer.
