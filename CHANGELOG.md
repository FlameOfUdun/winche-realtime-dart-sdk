## 0.1.0

- `WincheRealtime.instance.list(path)`: `append` (all or none, retries never duplicate), `subscribe(after:)` (each
  item once, in order, through reconnects), `read`, `open`, `close`.
- One connection per app, signed in as the app's user, with the app's Integrity token; reconnects with backoff,
  sends again what a drop cut off, and reconnects with a fresh token when the sign-in expires (4401).
