# Consolidate Channel State into ChannelLifecycleRegistry

Channel runtime state was previously fragmented across eight independent collections with separate locks in `EngineState`, leading to lock-ordering fragility, invalid state combinations, and widespread test leakage past the interface. We replaced these with an atomic `ChannelLifecycleRegistry` where each channel exists in an explicit discrete state enum (`Idle`, `Recording`, `Cooldown`, `Restricted`) protected by a single in-memory lock.

## Considered Options

- **Sharded per-channel locks or multi-collection locks**: Rejected because operations take <1µs in-memory, monitored channels are few (1–50), and multiple collections risk lock drift, deadlocks, and illegal state combinations.
- **Temporary compatibility shims**: Rejected to enforce clean seam discipline ("the interface is the test surface") and permanently eliminate pass-through getters.

## Consequences

- Tests and callers query one typed interface; zero cross-collection lock acquisitions or deadlocks.
- State transitions automatically manage session cancellation tokens, streamer name caching, and cooldown validation.
- All 8 raw `Arc<Mutex<...>>` getters on `EngineOrchestrator` are deleted.
