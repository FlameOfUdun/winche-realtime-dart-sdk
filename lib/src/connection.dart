import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:meta/meta.dart';
import 'package:winche_core/winche_core.dart';

import 'models.dart';
import 'socket.dart';

/// Opens a socket into the server, signed in with [token] (null: signed out).
typedef RealtimeConnector = Future<RealtimeSocket> Function(String? token);

/// The caller's sign-in token (fresh when [forceRefresh]), or null.
typedef RealtimeToken = Future<String?> Function({bool forceRefresh});

/// One connection to Winche Realtime, shared by every list of an app: it connects when first needed, reconnects when
/// dropped (waiting longer each failed try), sends again what a drop cut off, and resumes every subscription from the
/// last position it delivered, so a subscriber sees each item once, in order, with no gaps.
final class RealtimeConnection {
  RealtimeConnection({required RealtimeConnector connector, required RealtimeToken token, this.settings = const WincheRealtimeSettings()})
      : _connector = connector,
        _token = token;

  final RealtimeConnector _connector;
  final RealtimeToken _token;
  final WincheRealtimeSettings settings;

  RealtimeSocket? _socket;
  Completer<void>? _connected;
  Timer? _pings;
  DateTime _lastHeard = DateTime.now();
  bool _disposed = false;
  bool _refreshToken = false;
  int _next = 0;
  Duration? _backoff;
  final _pending = <String, _Request>{};
  final _subscriptions = <String, _Subscription>{};
  final _lives = <String, _Live>{};
  final _joins = <String, Map<String, Object?>>{};

  /// The list at [path].
  RealtimeListRef list(String path) => RealtimeListRef._(this, path);

  /// The channel [name] at [path]: live messages, never kept.
  RealtimeChannelRef channel(String path, String name) => RealtimeChannelRef._(this, path, name);

  /// Who's connected at [path].
  RealtimePresenceRef presence(String path) => RealtimePresenceRef._(this, path);

  /// Closes the connection; requests in progress fail, subscriptions end.
  Future<void> dispose() async {
    if (_disposed) return;
    _disposed = true;
    for (final r in _pending.values) {
      r.fail(const WincheRealtimeException(WincheErrorCode.cancelled, 'The connection was closed.'));
    }
    _pending.clear();
    for (final s in _subscriptions.values.toList()) {
      await s.controller.close();
    }
    _subscriptions.clear();
    for (final l in _lives.values.toList()) {
      await l.controller.close();
    }
    _lives.clear();
    _joins.clear();
    _pings?.cancel();
    await _socket?.close();
    _socket = null;
  }

  // ── requests ────────────────────────────────────────────────────────────────

  @visibleForTesting
  Future<Map<String, dynamic>> request(Map<String, Object?> frame) {
    if (_disposed) return Future.error(const WincheRealtimeException(WincheErrorCode.cancelled, 'The connection was closed.'));
    final id = 'r${++_next}';
    final request = _Request(id, {...frame, 'id': id});
    _pending[id] = request;
    request.timer = Timer(settings.requestTimeout, () {
      if (_pending.remove(id) != null) {
        request.fail(WincheRealtimeException(WincheErrorCode.unavailable, 'Winche Realtime didn\'t answer within ${settings.requestTimeout.inSeconds} s.'));
      }
    });
    _sendOrConnect(request.frame);
    return request.completer.future;
  }

  /// Sends a pending request now if connected; otherwise connects, and [_resume] sends every pending request then
  /// (once: never also here).
  void _sendOrConnect(Map<String, Object?> frame) {
    if (_socket != null && _connected?.isCompleted == true) {
      _sendIfOpen(frame);
    } else {
      _ensure().ignore();
    }
  }

  void _sendIfOpen(Map<String, Object?> frame) {
    final socket = _socket;
    if (socket != null && _connected?.isCompleted == true) socket.send(jsonEncode(frame));
  }

  // ── connecting ──────────────────────────────────────────────────────────────

  /// Connected, or connecting: completes once the server has said welcome.
  Future<void> _ensure() {
    if (_disposed) return Future.error(const WincheRealtimeException(WincheErrorCode.cancelled, 'The connection was closed.'));
    final current = _connected;
    if (current != null) return current.future;
    final connected = _connected = Completer<void>();
    connected.future.ignore();
    unawaited(_dial(connected));
    return connected.future;
  }

  Future<void> _dial(Completer<void> connected) async {
    while (!_disposed) {
      try {
        final token = await _token(forceRefresh: _refreshToken);
        _refreshToken = false;
        final socket = await _connector(token);
        if (_disposed) {
          await socket.close();
          return;
        }
        _socket = socket;
        final welcomed = Completer<void>();
        void gone() {
          if (!welcomed.isCompleted) {
            // Closed before the welcome: this dial failed (its loop tries again); nothing else is started.
            if (socket.closeCode == 4401) _refreshToken = true;
            welcomed.completeError(const WincheRealtimeException(WincheErrorCode.unavailable, 'Closed before the welcome.'));
            return;
          }
          _onClosed(socket);
        }
        socket.messages.listen((text) => _onFrame(socket, text, welcomed), onDone: gone, onError: (Object _) => gone(), cancelOnError: true);
        try {
          await welcomed.future.timeout(settings.requestTimeout);
        } catch (_) {
          if (identical(_socket, socket)) _socket = null;
          unawaited(socket.close().catchError((Object _) {}));
          rethrow;
        }
        _backoff = null;
        connected.complete();
        _startPings(socket);
        _resume();
        return;
      } catch (_) {
        if (_disposed) return;
        final wait = _backoff = _backoff == null ? settings.initialBackoff : _min(_backoff! * 2, settings.maxBackoff);
        await Future<void>.delayed(wait);
      }
    }
  }

  /// Connected again: what was cut off is sent again (keys make appends safe to repeat), and each subscription
  /// starts again after the last position it delivered.
  void _resume() {
    // Subscriptions, listens and watches are asked for again below (from where they got to): their old requests go.
    for (final id in _pending.entries.where((e) => const {'subscribe', 'listen', 'watch'}.contains(e.value.frame['type'])).map((e) => e.key).toList()) {
      _pending.remove(id)?.fail(const WincheRealtimeException(WincheErrorCode.unavailable, 'Asking again.'));
    }
    for (final r in _pending.values) {
      _sendIfOpen(r.frame);
    }
    for (final s in _subscriptions.values) {
      s.serverId = null;
      _subscribe(s);
    }
    for (final l in _lives.values) {
      l.serverId = null;
      _startLive(l);
    }
    // The server took us out of presence when the connection ended: back in, unless that join is being sent anyway.
    final joining = _pending.values.where((r) => r.frame['type'] == 'join').map((r) => r.frame['path']).toSet();
    for (final MapEntry(key: path, value: frame) in _joins.entries) {
      if (!joining.contains(path)) request(frame).ignore();
    }
  }

  /// Pings a quiet connection; one that stays quiet is dropped (and replaced) as though it had closed.
  void _startPings(RealtimeSocket socket) {
    _pings?.cancel();
    _lastHeard = DateTime.now();
    final tick = settings.pingTimeout ~/ 2;
    _pings = Timer.periodic(tick > Duration.zero ? tick : const Duration(milliseconds: 1), (timer) {
      if (!identical(socket, _socket)) return timer.cancel();
      final quiet = DateTime.now().difference(_lastHeard);
      if (quiet > settings.pingInterval + settings.pingTimeout) {
        timer.cancel();
        _onClosed(socket);
        unawaited(socket.close().catchError((Object _) {}));
      } else if (quiet >= settings.pingInterval) {
        socket.send(jsonEncode({'type': 'ping', 'id': 'ping'}));
      }
    });
  }

  void _onClosed(RealtimeSocket socket) {
    if (!identical(socket, _socket)) return;
    _pings?.cancel();
    _socket = null;
    _connected = null;
    if (socket.closeCode == 4401) _refreshToken = true;   // the sign-in expired: a fresh token next time
    // A message cut off may have gone out or not, and has no key to stop a second copy: it fails, never resent.
    for (final id in _pending.entries.where((e) => e.value.frame['type'] == 'publish').map((e) => e.key).toList()) {
      _pending.remove(id)?.fail(const WincheRealtimeException(WincheErrorCode.unavailable, 'The connection dropped while publishing: it may or may not have gone out.'));
    }
    if (_disposed) return;
    if (_pending.isNotEmpty || _subscriptions.isNotEmpty || _lives.isNotEmpty || _joins.isNotEmpty) unawaited(_ensure().catchError((Object _) {}));
  }

  void _onFrame(RealtimeSocket socket, String text, Completer<void> welcomed) {
    if (!identical(socket, _socket)) return;   // a socket let go: what it still says is no one's
    _lastHeard = DateTime.now();
    final Map<String, dynamic> frame;
    try {
      frame = jsonDecode(text) as Map<String, dynamic>;
    } catch (_) {
      return;
    }
    switch (frame['type']) {
      case 'welcome':
        if (!welcomed.isCompleted) welcomed.complete();
      case 'response':
        _pending.remove(frame['id'])?.succeed((frame['result'] as Map<String, dynamic>?) ?? const {});
      case 'error':
        final error = WincheRealtimeException.fromWire((frame['error'] as Map<String, dynamic>?) ?? const {});
        if (frame['id'] case final String id) _pending.remove(id)?.fail(error);
        if (frame['subscription'] case final String sub) {
          // The server stopped a subscription: start it again from where it got to.
          for (final s in _subscriptions.values.where((s) => s.serverId == sub)) {
            s.serverId = null;
            _subscribe(s);
          }
          for (final l in _lives.values.where((l) => l.serverId == sub)) {
            l.serverId = null;
            _startLive(l);
          }
        }
      case 'message' || 'presence':
        _lives.values.where((l) => l.serverId == frame['subscription']).firstOrNull?.onEvent(frame);
      case 'items':
        final s = _subscriptions.values.where((s) => s.serverId == frame['subscription']).firstOrNull;
        if (s == null) return;
        // Only what comes after what it has delivered, in order: a frame repeated or replayed adds nothing.
        final fresh = (frame['items'] as List).cast<Map<String, dynamic>>().map(RealtimeEntry.fromWire).where((e) => e.position > s.last).toList()
          ..sort((a, b) => a.position.compareTo(b.position));
        if (fresh.isEmpty) return;
        s.last = fresh.last.position;
        s.controller.add(RealtimeBatch(items: fresh));
      case 'state':
        final s = _subscriptions.values.where((s) => s.serverId == frame['subscription']).firstOrNull;
        final state = frame['state'] as String?;
        if (s == null || state == null || state == s.state) return;
        s.state = state;
        s.controller.add(RealtimeBatch(state: state));
    }
  }

  // ── subscriptions ───────────────────────────────────────────────────────────

  Stream<RealtimeBatch> _stream(String path, int after) {
    late final _Subscription s;
    final controller = StreamController<RealtimeBatch>(
      onListen: () {
        _subscriptions[s.local] = s;
        _subscribe(s);
      },
      onCancel: () {
        _subscriptions.remove(s.local);
        final serverId = s.serverId;
        if (serverId != null && _socket != null) {
          request({'type': 'unsubscribe', 'subscription': serverId}).ignore();
        }
      },
    );
    s = _Subscription('s${++_next}', path, after, controller);
    return controller.stream;
  }

  /// A live stream (listening on a channel, watching presence): asked for when listened to, again after each
  /// reconnect; cancelled when its listener cancels.
  Stream<T> _live<T>(Map<String, Object?> frame, void Function(_Live, Map<String, dynamic>) onResponse, void Function(_Live, Map<String, dynamic>) onEvent) {
    late final _Live live;
    final controller = StreamController<T>(
      onListen: () {
        _lives[live.local] = live;
        _startLive(live);
      },
      onCancel: () {
        _lives.remove(live.local);
        final serverId = live.serverId;
        if (serverId != null && _socket != null) request({'type': 'unsubscribe', 'subscription': serverId}).ignore();
      },
    );
    live = _Live('l${++_next}', frame, controller, onResponse, onEvent);
    return controller.stream;
  }

  void _startLive(_Live l) {
    final attempt = ++l.attempt;
    final id = 'r${++_next}';
    l.serverId = id;
    final r = _Request(id, {...l.frame, 'id': id});
    _pending[id] = r;
    r.completer.future.then((result) {
      if (attempt == l.attempt) l.respond(l, result);
    }, onError: (Object e) {
      if (attempt != l.attempt || !_lives.containsKey(l.local)) return;
      if (e is WincheRealtimeException && !e.isTransient) {
        _lives.remove(l.local);
        l.controller.addError(e);
        l.controller.close();
      }
    });
    _sendOrConnect(r.frame);
  }

  void _subscribe(_Subscription s) {
    final attempt = ++s.attempt;
    final id = 'r${++_next}';
    s.serverId = id;   // the server names the subscription after the request
    final r = _Request(id, {'type': 'subscribe', 'id': id, 'path': s.path, 'after': s.last});
    _pending[id] = r;
    r.completer.future.then((result) {
      if (attempt != s.attempt) return;
      if (result['list'] case final Map<String, dynamic> list) s.state ??= list['state'] as String?;
    }, onError: (Object e) {
      if (attempt != s.attempt || !_subscriptions.containsKey(s.local)) return;
      // Refused (rules, a bad path): the stream says so and ends. Anything else is retried on reconnect.
      if (e is WincheRealtimeException && !e.isTransient) {
        _subscriptions.remove(s.local);
        s.controller.addError(e);
        s.controller.close();
      }
    });
    _sendOrConnect(r.frame);
  }
}

/// A list: append to it, read it, follow it, open and close it.
final class RealtimeListRef {
  RealtimeListRef._(this._connection, this.path);

  final RealtimeConnection _connection;
  final String path;

  /// Appends [items] all or none: their positions, in order. Sending the same keys again (a retry) adds nothing.
  Future<List<int>> append(List<RealtimeItem> items) async {
    final result = await _connection.request({'type': 'append', 'path': path, 'items': [for (final i in items) i.toWire()]});
    return (result['positions'] as List).cast<int>();
  }

  /// Up to [limit] items after position [after].
  Future<RealtimePage> read({int after = 0, int limit = 500}) async {
    final result = await _connection.request({'type': 'read', 'path': path, 'after': after, 'limit': limit});
    return RealtimePage(
      (result['items'] as List).cast<Map<String, dynamic>>().map(RealtimeEntry.fromWire).toList(),
      result['list'] == null ? null : RealtimeListInfo.fromWire(result['list'] as Map<String, dynamic>),
    );
  }

  /// Every item after position [after] (all, by default), then new ones as they come, each once and in order, and
  /// the list's state when it changes. Follows through reconnects; ends with an error if refused.
  Stream<RealtimeBatch> subscribe({int after = 0}) => _connection._stream(path, after);

  /// Opens the list (one there already is answered as it is).
  Future<RealtimeListInfo> open() async =>
      RealtimeListInfo.fromWire((await _connection.request({'type': 'open', 'path': path}))['list'] as Map<String, dynamic>);

  /// Closes the list: it takes no more items, and still reads.
  Future<RealtimeListInfo> close() async =>
      RealtimeListInfo.fromWire((await _connection.request({'type': 'close', 'path': path}))['list'] as Map<String, dynamic>);
}

final class _Request {
  _Request(this.id, this.frame) {
    completer.future.ignore();
  }

  final String id;
  final Map<String, Object?> frame;
  final completer = Completer<Map<String, dynamic>>();
  Timer? timer;

  void succeed(Map<String, dynamic> result) {
    timer?.cancel();
    if (!completer.isCompleted) completer.complete(result);
  }

  void fail(Object error) {
    timer?.cancel();
    if (!completer.isCompleted) completer.completeError(error);
  }
}

final class _Subscription {
  _Subscription(this.local, this.path, this.last, this.controller);
  final String local;
  final String path;
  int last;
  String? state;
  String? serverId;
  int attempt = 0;
  final StreamController<RealtimeBatch> controller;
}

Duration _min(Duration a, Duration b) => a < b ? a : b;

/// A channel at a path: publish to it, listen on it. Messages are never kept.
final class RealtimeChannelRef {
  RealtimeChannelRef._(this._connection, this.path, this.name);

  final RealtimeConnection _connection;
  final String path;
  final String name;

  /// Publishes [payload] to everyone listening now. A publish cut off by a drop fails as `unavailable` (it may or may
  /// not have gone out) and is never sent twice.
  Future<void> publish(Uint8List payload) async {
    await _connection.request({'type': 'publish', 'path': path, 'channel': name, 'payload': base64Encode(payload)});
  }

  /// Publishes [value] as JSON.
  Future<void> publishJson(Object? value) => publish(Uint8List.fromList(utf8.encode(jsonEncode(value))));

  /// Messages from now on, through reconnects (those sent while disconnected are missed: channels keep nothing).
  Stream<RealtimeMessage> listen() => _connection._live<RealtimeMessage>(
        {'type': 'listen', 'path': path, 'channel': name},
        (_, _) {},
        (l, frame) => l.controller.add(RealtimeMessage.fromWire(frame)),
      );
}

/// Presence at a path: join it, leave it, watch who's there.
final class RealtimePresenceRef {
  RealtimePresenceRef._(this._connection, this.path);

  final RealtimeConnection _connection;
  final String path;

  /// Joins, saying [data] (JSON, up to 1 KB) of yourself. If the connection ends without [leave], [onDisconnect] is
  /// appended to the path's list, as you. After a reconnect you're joined again by yourself.
  Future<void> join({Map<String, Object?>? data, RealtimeItem? onDisconnect}) async {
    final frame = <String, Object?>{'type': 'join', 'path': path, 'data': ?data, if (onDisconnect != null) 'onDisconnect': onDisconnect.toWire()};
    _connection._joins[path] = frame;
    await _connection.request(frame);
  }

  /// Leaves on purpose: out, nothing appended, and not joined again after a reconnect.
  Future<void> leave() async {
    _connection._joins.remove(path);
    await _connection.request({'type': 'leave', 'path': path});
  }

  /// Who's here now, then again each time that changes, through reconnects.
  Stream<List<RealtimeMember>> watch() => _connection._live<List<RealtimeMember>>(
        {'type': 'watch', 'path': path},
        (l, result) => l.controller.add(RealtimeMember.listFromWire(result['members'])),
        (l, frame) => l.controller.add(RealtimeMember.listFromWire(frame['members'])),
      );
}

final class _Live {
  _Live(this.local, this.frame, this.controller, this.respond, this.event);
  final String local;
  final Map<String, Object?> frame;
  final StreamController<dynamic> controller;
  final void Function(_Live, Map<String, dynamic>) respond;
  final void Function(_Live, Map<String, dynamic>) event;
  String? serverId;
  int attempt = 0;
  void onEvent(Map<String, dynamic> frame) => event(this, frame);
}
