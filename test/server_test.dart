// End to end against a real Winche Realtime server (the server repository's sample host): only when
// WINCHE_REALTIME_URL is set, e.g. ws://localhost:5199/realtime/v1/ws. The sample signs in whoever the token names.
@Tags(['server'])
library;

import 'dart:convert';
import 'dart:io';

import 'package:test/test.dart';
import 'package:winche_core/winche_core.dart';
import 'package:winche_realtime/src/socket.dart' show openWebSocket;
import 'package:winche_realtime/winche_realtime.dart';

void main() {
  final url = Platform.environment['WINCHE_REALTIME_URL'];
  final skip = url == null ? 'set WINCHE_REALTIME_URL to run against a server' : false;
  final run = DateTime.now().microsecondsSinceEpoch;
  final connections = <RealtimeConnection>[];

  RealtimeConnection as(String uid) {
    final c = RealtimeConnection(
      token: ({bool forceRefresh = false}) async => uid,
      connector: (token) => openWebSocket(Uri.parse(url!).replace(queryParameters: {'access_token': token!}), const {}),
    );
    connections.add(c);
    return c;
  }

  tearDown(() async {
    for (final c in connections) {
      await c.dispose();
    }
    connections.clear();
  });

  test('append, follow and read against the server', () async {
    final path = 'sessions/e2e-$run';
    final ann = as('ann').list(path);
    final bob = as('bob').list(path);
    expect(await ann.append([RealtimeItem.json('ann:1', {'n': 1}), RealtimeItem.json('ann:2', {'n': 2})]), [1, 2]);
    expect(await ann.append([RealtimeItem.json('ann:2', {'n': 2})]), [2]);   // a retry: nothing new

    final seen = <RealtimeEntry>[];
    final sub = bob.subscribe().listen((b) => seen.addAll(b.items));
    await bob.append([RealtimeItem.json('bob:1', {'n': 3})]);
    await _until(() => seen.length == 3);
    expect(seen.map((e) => e.position), [1, 2, 3]);
    expect(seen.map((e) => e.by), ['ann', 'ann', 'bob']);
    expect(seen.last.json, {'n': 3});
    await sub.cancel();

    final page = await bob.read(after: 1, limit: 1);
    expect(page.items.single.key, 'ann:2');
    expect(page.list!.count, 3);
  }, skip: skip);

  test('the server\'s rules and refusals reach the app', () async {
    final path = 'sessions/e2e-rules-$run';
    final bob = as('bob').list(path);
    await expectLater(bob.append([RealtimeItem.json('ann:1', 1)]),   // another device's key
        throwsA(isA<WincheRealtimeException>().having((e) => e.code, 'code', WincheErrorCode.permissionDenied)));
    await bob.append([RealtimeItem.json('bob:1', 1)]);
    expect((await bob.close()).isClosed, isTrue);
    await expectLater(bob.append([RealtimeItem.json('bob:2', 2)]),
        throwsA(isA<WincheRealtimeException>().having((e) => e.reason, 'reason', 'list-closed')));
    await expectLater(as('bob').list('other/x').append([RealtimeItem.json('bob:1', 1)]), throwsA(isA<WincheRealtimeException>()));
    await expectLater(bob.append([RealtimeItem('bob:3', utf8.encode('x'))]), throwsA(isA<WincheRealtimeException>()));
  }, skip: skip);

  test('channels and presence against the server', () async {
    final path = 'sessions/e2e-live-$run';
    final heard = <String>[];
    final listening = as('bob').channel(path, 'telemetry').listen().listen((m) => heard.add('${m.by}:${m.json}'));
    final members = <List<String?>>[];
    final watching = as('host').presence(path).watch().listen((m) => members.add(m.map((x) => x.uid).toList()));
    await _until(() => members.isNotEmpty);
    await Future<void>.delayed(const Duration(milliseconds: 200));   // the listen reached the server

    final annConnection = as('ann');
    await annConnection.channel(path, 'telemetry').publishJson({'hr': 72});
    await _until(() => heard.length == 1);
    expect(heard.single, 'ann:{hr: 72}');

    await annConnection.presence(path).join(data: {'role': 'nurse'}, onDisconnect: RealtimeItem.json('ann:dropped', {'event': 'dropped'}));
    await _until(() => members.last.contains('ann'));
    await annConnection.dispose();   // the connection ends without leaving
    await _until(() => !members.last.contains('ann'));
    expect((await as('host').list(path).read()).items.single.key, 'ann:dropped');
    await listening.cancel();
    await watching.cancel();
  }, skip: skip);
}

Future<void> _until(bool Function() done) async {
  final until = DateTime.now().add(const Duration(seconds: 10));
  while (!done()) {
    if (DateTime.now().isAfter(until)) fail('timed out');
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}
