# 0004 - Live Stream Intake Seam

Monitored channel polling, stream access tier checks, and chat authorization tokens were previously coupled directly to the concrete `ChzzkClient` HTTP client. This required integration and lifecycle test suites to spin up local HTTP loopback servers on ephemeral OS ports, resulting in multi-second test runtimes (~25s) and potential port collision flakiness.

We introduced the `LiveStreamSource` interface seam (`src/chzzk/source.rs`) representing live broadcast stream intake. The seam provides an object-safe, thread-safe asynchronous contract using pinned boxed futures (`BoxFuture<'a, T>`). We provide two first-class adapters:
1. The production adapter implemented directly on `ChzzkClient` with zero intermediary wrapper overhead.
2. A deterministic in-memory fake adapter (`MockLiveStreamSource`, aliased as `FakeLiveStreamSource`) providing declarative sticky state configurations, sequential state transitions, API error injection, chat token simulation, and per-channel call counting.

## Considered Options

- **HTTP loopback testing (WireMock / local sockets)**: Rejected because binding OS network sockets incurs significant startup latency, risks port collisions in parallel test runners, and adds brittle timing dependencies for multi-tick lifecycle tests.
- **Static generics across engine structs (`<S: LiveStreamSource>`)**: Rejected because generic parameters would cascade throughout `EngineOrchestrator`, `RecordingSession`, CLI dispatch, and TUI runtime state. Dynamic dispatch via `Arc<dyn LiveStreamSource>` standardizes types cleanly and matches the existing `UploadBackend` pattern with negligible overhead for low-frequency polling.
- **Fragmented polling and chat token traits**: Rejected in favor of a single cohesive intake interface. Broadcast metadata, access tiers, and chat credentials form a unified stream intake boundary.

## Consequences

- Orchestrator and recording session tests can drive channel transitions, restriction gating, and upstream API failures entirely in-process in sub-milliseconds without network sockets.
- Production `ChzzkClient` satisfies `LiveStreamSource` directly, retaining inherent async methods for callers while enabling transparent trait object dispatch.
- Paved the way for migrating heavy integration suites to fast-feedback in-memory testing.
