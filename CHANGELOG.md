## 0.1.0

- `WincheRealtime.instance.list(path)`: `append` (all or none, retries never duplicate), `subscribe(after:)` (each
  item once, in order, through reconnects), `read`, `open`, `close`.
- Channels: `channel(path, name)` with `publish` / `publishJson` and `listen()` (live, never kept; a publish cut
  off by a drop fails as `unavailable` and is never sent twice).
- Presence: `presence(path)` with `join(data:, onDisconnect:)`, `leave()` and `watch()` (members now, then on each
  change); joined again by itself after a reconnect.
- One connection per app, signed in as the app's user, with the app's Integrity token; reconnects with backoff,
  sends again what a drop cut off, and reconnects with a fresh token when the sign-in expires (4401).
