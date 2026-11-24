import 'dart:async';

import 'package:web_socket_channel/web_socket_channel.dart';

import 'web_socket_dial.dart';

/// A connection to the server, as the SDK uses it: text frames in, text frames out, and how it closed.
abstract interface class RealtimeSocket {
  /// Frames from the server; done when the connection closes.
  Stream<String> get messages;

  /// The close code, once closed (4401: the sign-in expired; null or 1006: dropped).
  int? get closeCode;

  void send(String text);

  Future<void> close([int? code]);
}

/// Opens a socket to [uri] with [headers] (a browser can't send headers: [uri] carries what it needs).
Future<RealtimeSocket> openWebSocket(Uri uri, Map<String, String> headers) async {
  final channel = connectWebSocket(uri, headers);
  await channel.ready;
  return _ChannelSocket(channel);
}

final class _ChannelSocket implements RealtimeSocket {
  _ChannelSocket(this._channel);

  final WebSocketChannel _channel;

  @override
  Stream<String> get messages => _channel.stream.where((m) => m is String).cast<String>();

  @override
  int? get closeCode => _channel.closeCode;

  @override
  void send(String text) => _channel.sink.add(text);

  @override
  Future<void> close([int? code]) => _channel.sink.close(code ?? 1000);
}
