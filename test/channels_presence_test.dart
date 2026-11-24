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

  RealtimeConnection as(String uid) {
    final c = RealtimeConnection(
      connector: (t) => server.connect(t),
      token: ({bool forceRefresh = false}) async => uid,
      settings: const WincheRealtimeSettings(initialBackoff: Duration(milliseconds: 5), maxBackoff: Duration(milliseconds: 40)),
    );
    connections.add(c);
    return c;
  }

  setUp(() => server = FakeRealtimeServer());
  tearDown(() async {
    for (final c in connections) {
      await c.dispose();
    }
    connections.clear();
  });

  test('messages reach listeners once, and listening resumes after a drop', () async {
    final ann = as('ann').channel('sessions/s1', 'telemetry');
    final heard = <String>[];
    final sub = as('bob').channel('sessions/s1', 'telemetry').listen().listen((m) => heard.add('${m.by}:${utf8.decode(m.payload)}'));
    await _until(() => server.requests['listen'] == 1);
    await ann.publish(Uint8List.fromList(utf8.encode('hr=72')));
    await _until(() => heard.length == 1);
    expect(heard, ['ann:hr=72']);

    server.dropAll();
    await _until(() => server.requests['listen'] == 2);   // listening again once back
    await ann.publishJson({'hr': 75});
    await _until(() => heard.length == 2);
    expect(heard.last, 'ann:{"hr":75}');
    await sub.cancel();
  });

  test('a publish cut off by a drop is not sent again (no duplicate messages): it fails as unavailable', () async {
    final ann = as('ann').channel('sessions/s1', 'telemetry');
    await ann.publishJson(1);
    final cut = ann.publishJson(2);
    server.dropAll();
    await expectLater(cut, throwsA(isA<WincheRealtimeException>().having((e) => e.code, 'code', WincheErrorCode.unavailable)));
    expect(server.requests['publish'], 1);
  });

  test('watchers see who joins and leaves; a drop runs the on-disconnect, and a reconnect rejoins', () async {
    final members = <List<String?>>[];
    final watch = as('host').presence('sessions/s1').watch().listen((m) => members.add(m.map((x) => x.uid).toList()));
    await _until(() => members.isNotEmpty);
    expect(members.last, isEmpty);

    final ann = as('ann').presence('sessions/s1');
    await ann.join(data: {'role': 'nurse'}, onDisconnect: RealtimeItem.json('ann:dropped', {'event': 'dropped'}));
    await _until(() => members.last.contains('ann'));

    // Ann's connection drops: she's gone (her item appended), then back on her own once reconnected.
    server.dropAll();
    await _until(() => server.keysOf('sessions/s1').contains('ann:dropped'));
    await _until(() => members.last.contains('ann'));

    await ann.leave();
    await _until(() => members.last.isEmpty);
    server.dropAll();   // left on purpose: not rejoined
    await Future<void>.delayed(const Duration(milliseconds: 200));
    expect(members.last, isEmpty);
    await watch.cancel();
  });

  test('presence data reaches watchers', () async {
    final seen = <RealtimeMember>[];
    final watch = as('host').presence('sessions/s1').watch().listen((m) => seen..clear()..addAll(m));
    await as('ann').presence('sessions/s1').join(data: {'role': 'nurse'});
    await _until(() => seen.isNotEmpty);
    expect(seen.single.data, {'role': 'nurse'});
    await watch.cancel();
  });
}

Future<void> _until(bool Function() done) async {
  final until = DateTime.now().add(const Duration(seconds: 5));
  while (!done()) {
    if (DateTime.now().isAfter(until)) fail('timed out');
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}
