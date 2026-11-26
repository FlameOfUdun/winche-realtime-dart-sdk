## 0.1.2

- Needs winche_core 1.1.1: its error messages never carry a signed link's signature, a token or a password.

## 0.1.1

- `RealtimePage.more`: whether to read on after a page (pages stop at about 1 MB).
- A list deleted under a subscription is reported (`state: deleted`); a list made at the path later is followed from position 1.
- Sign-out never waits on a paused subscriber or on the socket's close; streams listened to after it end with `cancelled`.
- A connection that drops before the server's welcome is let go (no hidden second connection); quiet connections are pinged and replaced when half-open.
- A server that closes right after its welcome is redialled with backoff; a 401 on the upgrade refreshes the token.

## 0.1.0

- `WincheRealtime.instance.list(path)`: `append` (all or none, retries never duplicate), `subscribe(after:)` (each
  item once, in order, through reconnects), `read`, `open`, `close`.
- Channels: `channel(path, name)` with `publish` / `publishJson` and `listen()` (live, never kept; a publish cut
  off by a drop fails as `unavailable` and is never sent twice).
- Presence: `presence(path)` with `join(data:, onDisconnect:)`, `leave()` and `watch()` (members now, then on each
  change); joined again by itself after a reconnect.
- One connection per app, signed in as the app's user, with the app's Integrity token; reconnects with backoff,
  sends again what a drop cut off, and reconnects with a fresh token when the sign-in expires (4401).
