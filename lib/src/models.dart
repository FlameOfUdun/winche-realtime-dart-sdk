import 'dart:convert';
import 'dart:typed_data';

import 'package:winche_core/winche_core.dart';

/// An item to append: your key (sending it again never duplicates it) and its payload (bytes the server never reads).
///
/// Keys are best as `device:sequence`, so a project's rules can check the device part is the caller's.
final class RealtimeItem {
  const RealtimeItem(this.key, this.payload);

  /// An item whose payload is [value] as JSON.
  RealtimeItem.json(this.key, Object? value) : payload = Uint8List.fromList(utf8.encode(jsonEncode(value)));

  final String key;
  final Uint8List payload;

  Map<String, Object?> toWire() => {'key': key, 'payload': base64Encode(payload)};
}

/// An item in a list: its key, its position (the same order for everyone), when, by whom, and its payload.
final class RealtimeEntry {
  const RealtimeEntry({required this.key, required this.position, required this.at, required this.by, required this.payload});

  factory RealtimeEntry.fromWire(Map<String, dynamic> w) => RealtimeEntry(
        key: w['key'] as String,
        position: w['position'] as int,
        at: DateTime.parse(w['at'] as String),
        by: w['by'] as String?,
        payload: base64Decode(w['payload'] as String),
      );

  final String key;
  final int position;
  final DateTime at;
  final String? by;
  final Uint8List payload;

  /// The payload read as JSON.
  Object? get json => jsonDecode(utf8.decode(payload));
}

/// A list: open (takes items) or closed (reads only), and how much it holds.
final class RealtimeListInfo {
  const RealtimeListInfo({required this.path, required this.state, required this.count, required this.bytes, required this.openedAt, this.closedAt});

  factory RealtimeListInfo.fromWire(Map<String, dynamic> w) => RealtimeListInfo(
        path: w['path'] as String,
        state: w['state'] as String,
        count: w['count'] as int,
        bytes: w['bytes'] as int,
        openedAt: DateTime.parse(w['openedAt'] as String),
        closedAt: w['closedAt'] == null ? null : DateTime.parse(w['closedAt'] as String),
      );

  final String path;
  final String state;
  final int count;
  final int bytes;
  final DateTime openedAt;
  final DateTime? closedAt;

  bool get isClosed => state == 'closed';
}

/// A page of a list: items after a position, and the list (null when there's none at the path).
final class RealtimePage {
  const RealtimePage(this.items, this.list);
  final List<RealtimeEntry> items;
  final RealtimeListInfo? list;
}

/// What a subscription delivers: new items in order (each position once), or the list's new state.
final class RealtimeBatch {
  const RealtimeBatch({this.items = const [], this.state});
  final List<RealtimeEntry> items;

  /// Set when the list opened or closed.
  final String? state;
}

/// A message on a channel: who sent it, when, and its payload. Never kept.
final class RealtimeMessage {
  const RealtimeMessage({required this.path, required this.channel, required this.by, required this.at, required this.payload});

  factory RealtimeMessage.fromWire(Map<String, dynamic> w) => RealtimeMessage(
        path: w['path'] as String,
        channel: w['channel'] as String,
        by: w['by'] as String?,
        at: DateTime.parse(w['at'] as String),
        payload: base64Decode(w['payload'] as String),
      );

  final String path;
  final String channel;
  final String? by;
  final DateTime at;
  final Uint8List payload;

  /// The payload read as JSON.
  Object? get json => jsonDecode(utf8.decode(payload));
}

/// Someone connected at a path: who, what they said of themselves when they joined, since when.
final class RealtimeMember {
  const RealtimeMember({required this.uid, required this.data, required this.joinedAt});

  factory RealtimeMember.fromWire(Map<String, dynamic> w) => RealtimeMember(
        uid: w['uid'] as String?,
        data: (w['data'] as Map<String, dynamic>?),
        joinedAt: DateTime.parse(w['joinedAt'] as String),
      );

  static List<RealtimeMember> listFromWire(Object? members) =>
      [for (final m in (members as List? ?? const [])) RealtimeMember.fromWire(m as Map<String, dynamic>)];

  final String? uid;
  final Map<String, dynamic>? data;
  final DateTime joinedAt;
}

/// Why Winche Realtime refused or failed.
final class WincheRealtimeException extends WincheException {
  const WincheRealtimeException(super.code, super.message, {this.reason});

  /// The server's more precise reason, e.g. `denied`, `list-closed`, `key-reused`, `list-full`.
  final String? reason;

  factory WincheRealtimeException.fromWire(Map<String, dynamic> e) => WincheRealtimeException(
        WincheErrorCode.tryParse(e['code'] as String?) ?? WincheErrorCode.unknown,
        (e['message'] as String?) ?? 'Winche Realtime refused.',
        reason: e['reason'] as String?,
      );
}

/// How the connection behaves.
final class WincheRealtimeSettings {
  const WincheRealtimeSettings({
    this.requestTimeout = const Duration(seconds: 30),
    this.initialBackoff = const Duration(milliseconds: 250),
    this.maxBackoff = const Duration(seconds: 30),
  });

  /// How long a request waits for its answer, reconnects included, before failing with `unavailable`.
  final Duration requestTimeout;

  /// The first wait before reconnecting; doubled each failed try, up to [maxBackoff].
  final Duration initialBackoff;
  final Duration maxBackoff;
}
