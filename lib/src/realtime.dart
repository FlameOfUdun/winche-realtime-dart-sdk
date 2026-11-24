import 'package:winche_core/winche_core.dart';

import 'connection.dart';
import 'models.dart';
import 'socket.dart';
import 'web_socket_dial.dart';

/// Winche Realtime for an app: lists devices write to and follow live.
///
/// ```dart
/// final list = WincheRealtime.instance.list('sessions/s1');
/// await list.append([RealtimeItem.json('phone:1', {'event': 'start'})]);
/// list.subscribe().listen((batch) => batch.items.forEach(apply));
/// ```
///
/// One connection per app, made when first needed, as the signed-in user; it reconnects by itself and resumes every
/// subscription from where it was. Nothing is kept on the device: an app keeps what it needs itself.
final class WincheRealtime extends WincheService implements WincheSessionConsumer {
  WincheRealtime(super.app);

  /// Realtime for the default app.
  static WincheRealtime get instance => instanceFor(Winche.app);

  /// Realtime for [app].
  static WincheRealtime instanceFor(WincheApp app) => WincheService.instanceFor(app, () => WincheRealtime(app));

  /// The service's id in `WincheOptions.endpoints`.
  static const serviceId = 'realtime';

  /// How the connection behaves. Set it before the first use.
  WincheRealtimeSettings settings = const WincheRealtimeSettings();

  RealtimeConnection? _connection;

  /// The list at [path] (e.g. `sessions/s1`). Throws [WincheUnboundException] when nobody is signed in.
  RealtimeListRef list(String path) => (_connection ?? (throw WincheUnboundException())).list(path);

  /// The channel [name] at [path] (live messages, never kept). Throws [WincheUnboundException] when nobody is signed in.
  RealtimeChannelRef channel(String path, String name) => (_connection ?? (throw WincheUnboundException())).channel(path, name);

  /// Who's connected at [path]. Throws [WincheUnboundException] when nobody is signed in.
  RealtimePresenceRef presence(String path) => (_connection ?? (throw WincheUnboundException())).presence(path);

  @override
  Future<void> onSessionChanged(WincheSession? session) async {
    final previous = _connection;
    _connection = null;
    await previous?.dispose();
    if (session == null) return;
    final endpoint = (app.options ?? const WincheOptions()).endpointFor(serviceId, path: '/realtime/v1/ws', webSocket: true);
    final integrity = app.integrity;
    _connection = RealtimeConnection(
      settings: settings,
      token: ({bool forceRefresh = false}) => session.token(forceRefresh: forceRefresh),
      connector: (token) async {
        // The sign-in and the app's Integrity token, fresh on every (re)dial: headers, or in the address in a browser.
        if (webSocketHeadersSupported) {
          return openWebSocket(endpoint, {
            if (token != null) 'Authorization': 'Bearer $token',
            ...await integrity.webSocketHeaders(),
          });
        }
        final withToken = token == null ? endpoint : endpoint.replace(queryParameters: {...endpoint.queryParameters, 'access_token': token});
        return openWebSocket(await integrity.withToken(withToken), const {});
      },
    );
  }

  /// A rotated token changes nothing now: the open socket keeps its sign-in, and the next (re)connect reads the fresh
  /// one. The server closes it (4401) when its token expires, and it reconnects with a new one.
  @override
  Future<void> onTokenChanged() async {}

  @override
  Future<void> dispose() async {
    await _connection?.dispose();
    _connection = null;
    await super.dispose();
  }
}
