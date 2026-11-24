# winche_realtime

Dart client for **Winche Realtime**: ordered, append-only lists that devices write to and follow live, for data that
flows during a session (thousands of small items) rather than data that's kept.

```dart
import 'package:winche_realtime/winche_realtime.dart';

final list = WincheRealtime.instance.list('sessions/s1');

// Append: all or none; sending the same keys again (a retry) never duplicates.
await list.append([RealtimeItem.json('phone:1', {'event': 'start'})]);

// Follow: everything after a position, then new items as they come, each once and in order, through reconnects.
list.subscribe(after: 0).listen((batch) {
  for (final item in batch.items) apply(item.position, item.json);
  if (batch.state == 'closed') finish();
});

// Read a page, close the list.
final page = await list.read(after: 100, limit: 500);
await list.close();
```

- **Keys** are yours, best as `device:sequence`: a project's rules can require the device part to be the caller.
- **Payloads** are bytes the server never reads (`RealtimeItem.json` for JSON).
- **One connection per app**, made when first needed, as the signed-in user, with the app's Integrity token. It
  reconnects by itself (waiting longer each failed try), sends again what a drop cut off, and resumes every
  subscription from the last position it delivered. When the sign-in expires, it reconnects with a fresh token.
- **Nothing is kept on the device:** an app keeps what it needs (a list's last position, its own state).
- Errors are `WincheRealtimeException` (a `WincheException`): `code` and `reason` (`denied`, `list-closed`,
  `key-reused`, `list-full`).

The server address is `/realtime/v1/ws` under `WincheOptions.baseUrl`, or `WincheOptions.endpoints['realtime']`.

## Tests

`dart test` runs against an in-memory server that drops connections, repeats frames and expires sign-ins.
`WINCHE_REALTIME_URL=ws://localhost:5199/realtime/v1/ws dart test test/server_test.dart` runs end to end against
the server repository's sample host.

## License

MIT.
