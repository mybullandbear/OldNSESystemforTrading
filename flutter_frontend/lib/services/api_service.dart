import 'dart:html' as html;
import 'dart:convert';
import 'package:http/http.dart' as http;
import '../models/option_data.dart';

class ApiService {
  static String get _base {
    final host = html.window.location.hostname;
    if (host != null && host.isNotEmpty) {
      return 'http://$host:5000/api';
    }
    return 'http://127.0.0.1:5000/api';
  }
  static const _fast = Duration(seconds: 60);
  static const _slow = Duration(seconds: 120);

  // ── PHASE 1: one fast call for signals+spots+PCR ─────────
  Future<Map<String, QuickSummary>> getQuickSummary(String? date) async {
    final url = date != null ? '$_base/quick_summary?date=$date' : '$_base/quick_summary';
    final r = await http.get(Uri.parse(url)).timeout(_fast);
    if (r.statusCode == 200) {
      final Map<String, dynamic> data = json.decode(r.body);
      return data.map((k, v) => MapEntry(k, QuickSummary.fromJson(v)));
    }
    throw Exception('quick_summary failed: ${r.statusCode}');
  }

  // ── PHASE 2: OI stats (skip max pain for speed) ──────────
  Future<List<OiStat>> getOiStats(String symbol, String date,
      {bool skipMaxPain = false}) async {
    final url = '$_base/oi_stats?symbol=$symbol&date=$date'
        '${skipMaxPain ? "&skip_max_pain=true" : ""}';
    final r = await http.get(Uri.parse(url)).timeout(_slow);
    if (r.statusCode == 200) {
      final List<dynamic> data = json.decode(r.body);
      return data.map((e) => OiStat.fromJson(e)).toList();
    }
    throw Exception('oi_stats failed: ${r.statusCode}');
  }

  // ── PHASE 3: Option chain ─────────────────────────────────
  Future<List<String>> getDates() async {
    final r = await http.get(Uri.parse('$_base/dates')).timeout(_fast);
    if (r.statusCode == 200) return List<String>.from(json.decode(r.body));
    throw Exception('dates failed: ${r.statusCode}');
  }

  Future<List<String>> getTimestamps(String symbol, String date) async {
    final r = await http
        .get(Uri.parse('$_base/timestamps?symbol=$symbol&date=$date'))
        .timeout(_fast);
    if (r.statusCode == 200) return List<String>.from(json.decode(r.body));
    throw Exception('timestamps failed: ${r.statusCode}');
  }

  Future<List<OptionData>> getData(
      String symbol, String timestamp, String date) async {
    final url =
        '$_base/data?symbol=$symbol&timestamp=${Uri.encodeComponent(timestamp)}&date=$date';
    final r = await http.get(Uri.parse(url)).timeout(_fast);
    if (r.statusCode == 200) {
      final List<dynamic> data = json.decode(r.body);
      return data.map((e) => OptionData.fromJson(e)).toList();
    }
    throw Exception('data failed: ${r.statusCode}');
  }

  // ── PHASE 4: Execute Trade ─────────────────────────────────
  Future<bool> executeTrade(String symbol, String action, double atm) async {
    final url = '$_base/execute_trade';
    final r = await http.post(
      Uri.parse(url),
      headers: {'Content-Type': 'application/json'},
      body: json.encode({'symbol': symbol, 'action': action, 'atm': atm}),
    ).timeout(_fast);
    
    if (r.statusCode == 200) {
      final data = json.decode(r.body);
      return data['success'] == true;
    }
    return false;
  }

  // ── PHASE 5: Trading State & Auto Trade ────────────────────
  Future<Map<String, dynamic>> getTradingState() async {
    final r = await http.get(Uri.parse('$_base/trading_state')).timeout(_fast);
    if (r.statusCode == 200) {
      return json.decode(r.body) as Map<String, dynamic>;
    }
    throw Exception('trading_state failed');
  }

  Future<bool> toggleTradingConfig({bool? paperTrading, bool? tradingEnabled}) async {
    final body = <String, dynamic>{};
    if (paperTrading != null) body['paper_trading'] = paperTrading;
    if (tradingEnabled != null) body['trading_enabled'] = tradingEnabled;

    final r = await http.post(
      Uri.parse('$_base/trading_state/toggle'),
      headers: {'Content-Type': 'application/json'},
      body: json.encode(body),
    ).timeout(_fast);
    return r.statusCode == 200;
  }
  
  Future<bool> closePosition(String symbol) async {
    final r = await http.post(
      Uri.parse('$_base/close_position'),
      headers: {'Content-Type': 'application/json'},
      body: json.encode({'symbol': symbol}),
    ).timeout(_fast);
    return r.statusCode == 200;
  }
}
