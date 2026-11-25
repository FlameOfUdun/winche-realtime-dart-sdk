import 'dart:async';
import 'dart:convert';
import 'dart:io' show WebSocketException;
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:winche_core/winche_core.dart';
import 'package:winche_realtime/winche_realtime.dart';

import 'support/fake_server.dart';

void main() {
  late FakeRealtimeServer server;
  final connections = <RealtimeConnection>[];

  RealtimeConnection connect({Future<String?> Function({bool forceRefresh})? token, WincheRealtimeSettings? settings}) {
    final c = RealtimeConnection(
      connector: (t) => server.connect(t),
      token: token ?? ({bool forceRefresh = false}) async => 'token',
      settings: settings ?? const WincheRealtimeSettings(initialBackoff: Duration(milliseconds: 5), maxBackoff: Duration(milliseconds: 40)),
    );
    connections.add(c);
    return c;
  }

  RealtimeItem item(String key) => RealtimeItem(key, Uint8List.fromList(utf8.encode('p-$key')));

  setUp(() => server = FakeRealtimeServer());
  tearDown(() async {
    for (final c in connections) {
      await c.dispose();
    }
    connections.clear();
  });

  test('appends get positions; a subscription gets what is there, then what comes', () async {
    final ann = connect().list('sessions/s1');
    final bob = connect().list('sessions/s1');
    expect(await ann.append([item('ann:1'), item('ann:2')]), [1, 2]);

    final seen = <RealtimeEntry>[];
    final sub = bob.subscribe().listen((b) => seen.addAll(b.items));
    await ann.append([item('ann:3')]);
    await _until(() => seen.length == 3);
    expect(seen.map((e) => e.position), [1, 2, 3]);
    expect(utf8.decode(seen.last.payload), 'p-ann:3');
    await sub.cancel();

    final page = await bob.read(after: 1, limit: 1);
    expect(page.items.map((e) => e.key), ['ann:2']);
    expect(page.list!.count, 3);
  });

  test('after a drop, subscriptions resume from their last position: nothing missed, nothing twice', () async {
    final ann = connect().list('sessions/s1');
    final bob = connect().list('sessions/s1');
    final seen = <int>[];
    final sub = bob.subscribe().listen((b) => seen.addAll(b.items.map((e) => e.position)));
    await ann.append([item('ann:1')]);
    await _until(() => seen.length == 1);

    server.dropAll();
    await ann.append([item('ann:2'), item('ann:3')]);   // sent once the connection is back
    await _until(() => seen.length == 3);
    expect(seen, [1, 2, 3]);
    await sub.cancel();
  });

  test('an append cut off by a drop is sent again, and the server keeps it once', () async {
    final ann = connect().list('sessions/s1');
    await ann.append([item('ann:1')]);
    server.duplicateFrames = true;   // even with every frame twice
    final appending = ann.append([item('ann:2')]);
    server.dropAll();
    expect(await appending, [2]);
    expect(server.keysOf('sessions/s1'), ['ann:1', 'ann:2']);
  });

  test('an expired sign-in (4401) reconnects with a fresh token', () async {
    final refreshes = <bool>[];
    final ann = connect(token: ({bool forceRefresh = false}) async {
      refreshes.add(forceRefresh);
      return forceRefresh ? 'fresh' : 'old';
    }).list('sessions/s1');
    await ann.append([item('ann:1')]);
    server.expire(server.sockets.single);
    await ann.append([item('ann:2')]);
    expect(server.tokens, ['old', 'fresh']);
    expect(refreshes.last, isTrue);
  });

  test('refusals reach the caller and are not retried', () async {
    final ann = connect().list('secret/s1');
    server.refused.add('secret/s1');
    await expectLater(ann.append([item('ann:1')]),
        throwsA(isA<WincheRealtimeException>().having((e) => e.code, 'code', WincheErrorCode.permissionDenied).having((e) => e.reason, 'reason', 'denied')));
    expect(server.requests['append'], 1);
    final subscribed = ann.subscribe();
    await expectLater(subscribed, emitsError(isA<WincheRealtimeException>()));
  });

  test('a closed list tells its subscribers, and refuses appends', () async {
    final host = connect().list('sessions/s1');
    final ann = connect().list('sessions/s1');
    await ann.append([item('ann:1')]);
    final states = <String>[];
    final sub = ann.subscribe().listen((b) { if (b.state != null) states.add(b.state!); });
    expect((await host.close()).state, 'closed');
    await _until(() => states.contains('closed'));
    await expectLater(ann.append([item('ann:2')]), throwsA(isA<WincheRealtimeException>().having((e) => e.code, 'code', WincheErrorCode.failedPrecondition)));
    await sub.cancel();
  });

  test('three devices with drops and duplicate frames end up with one order, every item once', () async {
    final devices = [connect(), connect(), connect()];
    final views = List.generate(3, (_) => <String>[]);
    final positions = List.generate(3, (_) => <int>[]);
    final subs = [
      for (var d = 0; d < 3; d++)
        devices[d].list('sessions/chaos').subscribe().listen((b) {
          views[d].addAll(b.items.map((e) => e.key));
          positions[d].addAll(b.items.map((e) => e.position));
        }),
    ];
    server.duplicateFrames = true;
    var running = true;
    final mess = chaos(server, () => running);
    await Future.wait([
      for (var d = 0; d < 3; d++)
        () async {
          for (var n = 0; n < 50; n++) {
            await devices[d].list('sessions/chaos').append([item('d$d:$n')]);
          }
        }(),
    ]);
    running = false;
    await mess;
    await _until(() => views.every((v) => v.length == 150), within: const Duration(seconds: 20));
    for (var d = 0; d < 3; d++) {
      expect(positions[d], List.generate(150, (i) => i + 1), reason: 'device $d: every position once, in order');
      expect(views[d], views[0], reason: 'device $d sees the same order');
    }
    expect(server.keysOf('sessions/chaos').length, 150);
    // Each device's own items in the order it appended them.
    for (var d = 0; d < 3; d++) {
      expect(views[0].where((k) => k.startsWith('d$d:')), List.generate(50, (n) => 'd$d:$n'));
    }
    for (final s in subs) {
      await s.cancel();
    }
  });

  for (final code in [1006, 4401]) {
    test('a socket that closes before its welcome ($code) is let go: one connection, joined once', () async {
      server.closeBeforeWelcome = 1;
      server.closeBeforeWelcomeCode = code;
      // A short welcome timeout: a dial left waiting on the closed socket would give up and dial a second one.
      final ann = connect(settings: const WincheRealtimeSettings(
          requestTimeout: Duration(milliseconds: 150), initialBackoff: Duration(milliseconds: 5), maxBackoff: Duration(milliseconds: 40)));
      await ann.presence('sessions/s1').join();
      await ann.list('sessions/s1').append([item('ann:1')]);
      await Future<void>.delayed(const Duration(milliseconds: 500));
      expect(server.dials, 2);
      expect(server.open, 1);   // no second, hidden connection
      expect(server.requests['join'], 1);
      if (code == 4401) expect(server.tokens, ['token', 'token']);
    });
  }

  test('a connection that goes quiet without closing is noticed by its pings and replaced', () async {
    final watcher = connect(settings: const WincheRealtimeSettings(
        initialBackoff: Duration(milliseconds: 5), maxBackoff: Duration(milliseconds: 40),
        pingInterval: Duration(milliseconds: 50), pingTimeout: Duration(milliseconds: 50)));
    final seen = <String>[];
    final sub = watcher.list('sessions/s1').subscribe().listen((b) => seen.addAll(b.items.map((e) => e.key)));
    await Future<void>.delayed(const Duration(milliseconds: 30));
    server.sockets.single.hang();
    await connect().list('sessions/s1').append([item('ann:1')]);
    await _until(() => seen.isNotEmpty, within: const Duration(seconds: 3));
    expect(seen, ['ann:1']);
    expect(server.dials, greaterThanOrEqualTo(3));   // the watcher dialled again
    await sub.cancel();
  });

  test('a read says whether more items follow its page', () async {
    final ann = connect().list('sessions/s1');
    await ann.append([item('ann:1'), item('ann:2'), item('ann:3')]);
    final first = await ann.read(limit: 2);
    expect((first.items.length, first.more), (2, true));
    final rest = await ann.read(after: first.items.last.position, limit: 2);
    expect((rest.items.single.key, rest.more), ('ann:3', false));
  });

  test('dispose finishes at once with a paused subscriber, and nothing is thrown after', () async {
    final errors = <Object>[];
    await runZonedGuarded(() async {
      final c = connect();
      final sub = c.list('sessions/s1').subscribe().listen((_) {});
      await c.list('sessions/s1').append([item('ann:1')]);
      sub.pause();   // an app busy in its listener
      await connect().list('sessions/s1').append([item('bob:1'), item('bob:2')]);
      await c.dispose().timeout(const Duration(seconds: 1));
      expect(server.open, 1);   // only the other connection: this one's socket is closed
      await Future<void>.delayed(const Duration(milliseconds: 100));
    }, (e, _) => errors.add(e));
    expect(errors, isEmpty);
  });

  test('a subscription made before dispose but listened to after it ends with cancelled', () async {
    final c = connect();
    final stream = c.list('sessions/s1').subscribe();
    await c.dispose();
    await expectLater(stream, emitsError(isA<WincheRealtimeException>().having((e) => e.code, 'code', WincheErrorCode.cancelled)));
  });

  test('a token asked for while disposing never dials', () async {
    final gate = Completer<String?>();
    final c = connect(token: ({bool forceRefresh = false}) => gate.future);
    c.list('sessions/s1').append([item('ann:1')]).ignore();
    await Future<void>.delayed(const Duration(milliseconds: 20));
    await c.dispose();
    gate.complete('token');
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(server.dials, 0);
  });

  test('a list deleted under a subscription: told, and a new list there is followed from 1', () async {
    final ann = connect();
    final batches = <RealtimeBatch>[];
    final sub = ann.list('sessions/s1').subscribe().listen(batches.add);
    await ann.list('sessions/s1').append([item('ann:1'), item('ann:2')]);
    await ann.list('sessions/s1').close();
    await _until(() => batches.any((b) => b.state == 'closed'));
    server.deleteList('sessions/s1');
    await _until(() => batches.any((b) => b.state == 'deleted'));
    await ann.list('sessions/s1').append([item('ann:new')]);
    await _until(() => batches.any((b) => b.items.any((e) => e.key == 'ann:new')));
    expect(batches.expand((b) => b.items).last.position, 1);
    await sub.cancel();
  });

  test('a server that welcomes then closes at once is redialled with growing waits', () async {
    server.closeAfterWelcome = 1000;
    final c = connect(settings: const WincheRealtimeSettings(initialBackoff: Duration(milliseconds: 40), maxBackoff: Duration(seconds: 5)));
    final sub = c.list('sessions/s1').subscribe().listen((_) {});
    await Future<void>.delayed(const Duration(milliseconds: 400));
    expect(server.dials, lessThanOrEqualTo(5));   // 40 + 80 + 160 ms...: not a dial every few milliseconds
    await sub.cancel();
  });

  test('a refused token (401 on the upgrade) is refreshed before the next try', () async {
    final asked = <bool>[];
    server.refuseNextWith = WebSocketException('refused', 401);
    final c = connect(token: ({bool forceRefresh = false}) async {
      asked.add(forceRefresh);
      return 'token';
    });
    await c.list('sessions/s1').append([item('ann:1')]);
    expect(asked, [false, true]);
  });
}

Future<void> _until(bool Function() done, {Duration within = const Duration(seconds: 5)}) async {
  final until = DateTime.now().add(within);
  while (!done()) {
    if (DateTime.now().isAfter(until)) fail('timed out');
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}
