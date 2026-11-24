import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:winche_core/winche_core.dart';
import 'package:winche_realtime/winche_realtime.dart';

import 'support/fake_server.dart';

void main() {
  late FakeRealtimeServer server;
  final connections = <RealtimeConnection>[];

  RealtimeConnection connect({Future<String?> Function({bool forceRefresh})? token}) {
    final c = RealtimeConnection(
      connector: (t) => server.connect(t),
      token: token ?? ({bool forceRefresh = false}) async => 'token',
      settings: const WincheRealtimeSettings(initialBackoff: Duration(milliseconds: 5), maxBackoff: Duration(milliseconds: 40)),
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
}

Future<void> _until(bool Function() done, {Duration within = const Duration(seconds: 5)}) async {
  final until = DateTime.now().add(within);
  while (!done()) {
    if (DateTime.now().isAfter(until)) fail('timed out');
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}
