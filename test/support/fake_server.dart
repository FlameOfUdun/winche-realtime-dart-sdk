import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:winche_realtime/winche_realtime.dart';

/// An in-memory Winche Realtime server speaking the socket's protocol, with lists as the real one keeps them
/// (positions, keys that never duplicate, closing), and ways to misbehave: dropping connections, sending frames
/// twice, expiring a sign-in (close 4401), refusing paths.
final class FakeRealtimeServer {
  final _lists = <String, _List>{};
  final sockets = <FakeSocket>[];

  /// Paths whose requests are refused with permission-denied.
  final refused = <String>{};

  /// Each frame the server sends is sent twice (as a flaky relay might).
  bool duplicateFrames = false;

  /// The tokens connections were opened with, in order.
  final tokens = <String?>[];

  /// Dials so far.
  int dials = 0;

  /// Requests seen, by type.
  final requests = <String, int>{};

  /// A connector for [RealtimeConnection]: a new socket into this server.
  Future<RealtimeSocket> connect(String? token) async {
    dials++;
    tokens.add(token);
    final socket = FakeSocket(this);
    sockets.add(socket);
    scheduleMicrotask(() => socket._send({'type': 'welcome', 'connectionId': 'c$dials'}));
    return socket;
  }

  /// Every open connection drops (as a network loss would): no close frame, code 1006.
  void dropAll() {
    for (final s in List.of(sockets)) {
      s._drop(1006);
    }
  }

  /// The connection's sign-in expired: closed with 4401.
  void expire(FakeSocket socket) => socket._drop(4401);

  List<String> keysOf(String path) => (_lists[path]?.items ?? []).map((i) => i['key'] as String).toList();

  void _handle(FakeSocket socket, Map<String, dynamic> frame) {
    final id = frame['id'] as String?;
    final type = frame['type'] as String?;
    requests[type ?? ''] = (requests[type ?? ''] ?? 0) + 1;
    void reply(Object? result) => socket._send({'type': 'response', 'id': id, 'result': result});
    void fail(String code, String message, [String? reason]) =>
        socket._send({'type': 'error', 'id': id, 'error': {'code': code, 'message': message, 'reason': ?reason}});
    final path = frame['path'] as String?;
    if (path != null && refused.contains(path)) return fail('permission-denied', 'No.', 'denied');
    switch (type) {
      case 'ping':
        return reply(<String, Object?>{});
      case 'append':
        final list = _lists.putIfAbsent(path!, () => _List(path));
        final positions = <int>[];
        final fresh = <Map<String, dynamic>>[];
        for (final item in (frame['items'] as List).cast<Map<String, dynamic>>()) {
          final had = list.items.where((i) => i['key'] == item['key']).firstOrNull ?? fresh.where((i) => i['key'] == item['key']).firstOrNull;
          if (had != null) {
            if (had['payload'] != item['payload']) return fail('already-exists', 'Key reused.', 'key-reused');
            positions.add(had['position'] as int);
            continue;
          }
          if (list.state != 'open') return fail('failed-precondition', 'Closed.', 'list-closed');
          final entry = {'key': item['key'], 'position': list.items.length + fresh.length + 1, 'at': DateTime.now().toUtc().toIso8601String(), 'by': socket.uid, 'payload': item['payload']};
          fresh.add(entry);
          positions.add(entry['position'] as int);
        }
        list.items.addAll(fresh);
        reply({'positions': positions});
        if (fresh.isNotEmpty) _changed(path);
        return;
      case 'read':
        final list = _lists[path];
        final after = (frame['after'] as int?) ?? 0;
        final limit = (frame['limit'] as int?) ?? 500;
        return reply({'items': list == null ? <Map<String, dynamic>>[] : list.items.where((i) => (i['position'] as int) > after).take(limit).toList(), 'list': list?.info});
      case 'subscribe':
        final list = _lists[path];
        socket.subscriptions[id!] = _Sub(path!, (frame['after'] as int?) ?? 0, list?.state);
        reply({'subscription': id, 'list': list?.info});
        _deliver(socket, id);
        return;
      case 'unsubscribe':
        socket.subscriptions.remove(frame['subscription']);
        return reply(<String, Object?>{});
      case 'open':
        final list = _lists.putIfAbsent(path!, () => _List(path));
        reply({'list': list.info});
        return;
      case 'close':
        final list = _lists[path];
        if (list == null) return fail('not-found', 'No list.');
        list.state = 'closed';
        reply({'list': list.info});
        _changed(path!);
        return;
      default:
        return fail('invalid-argument', 'Unknown request $type.');
    }
  }

  void _changed(String path) {
    for (final s in List.of(sockets)) {
      for (final id in s.subscriptions.keys.toList()) {
        if (s.subscriptions[id]!.path == path) _deliver(s, id);
      }
    }
  }

  void _deliver(FakeSocket socket, String id) {
    final sub = socket.subscriptions[id];
    if (sub == null) return;
    final list = _lists[sub.path];
    if (list == null) return;
    final items = list.items.where((i) => (i['position'] as int) > sub.last).toList();
    if (items.isNotEmpty) {
      sub.last = items.last['position'] as int;
      socket._send({'type': 'items', 'subscription': id, 'path': sub.path, 'items': items});
    }
    if (list.state != sub.state) {
      sub.state = list.state;
      socket._send({'type': 'state', 'subscription': id, 'path': sub.path, 'state': list.state, 'count': list.items.length});
    }
  }
}

final class _List {
  _List(this.path);
  final String path;
  final items = <Map<String, dynamic>>[];
  String state = 'open';
  Map<String, Object?> get info => {'path': path, 'state': state, 'count': items.length, 'bytes': 0, 'openedAt': '2026-01-01T00:00:00Z', 'closedAt': null};
}

final class _Sub {
  _Sub(this.path, this.last, this.state);
  final String path;
  int last;
  String? state;
}

/// One connection into the fake server.
final class FakeSocket implements RealtimeSocket {
  FakeSocket(this._server);

  final FakeRealtimeServer _server;
  final _incoming = StreamController<String>();
  final subscriptions = <String, _Sub>{};
  int? _closeCode;
  String? uid;

  @override
  Stream<String> get messages => _incoming.stream;

  @override
  int? get closeCode => _closeCode;

  @override
  void send(String text) {
    if (_closeCode != null) return;
    final frame = jsonDecode(text) as Map<String, dynamic>;
    scheduleMicrotask(() { if (_closeCode == null) _server._handle(this, frame); });
  }

  @override
  Future<void> close([int? code]) async => _drop(code ?? 1000);

  void _send(Map<String, Object?> frame) {
    if (_closeCode != null) return;
    final text = jsonEncode(frame);
    _incoming.add(text);
    if (_server.duplicateFrames) _incoming.add(text);
  }

  void _drop(int code) {
    if (_closeCode != null) return;
    _closeCode = code;
    _server.sockets.remove(this);
    _incoming.close();
  }
}

/// Drops every connection now and then while [running], at random (seeded, so a failure repeats).
Future<void> chaos(FakeRealtimeServer server, bool Function() running, {int seed = 7}) async {
  final random = Random(seed);
  while (running()) {
    await Future<void>.delayed(Duration(milliseconds: 5 + random.nextInt(25)));
    if (random.nextInt(3) == 0) server.dropAll();
  }
}
