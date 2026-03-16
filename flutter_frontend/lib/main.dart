import 'dart:async';
import 'dart:js' as js;
import 'package:flutter/material.dart';
import 'package:intl/intl.dart';
import 'package:fl_chart/fl_chart.dart';
import 'models/option_data.dart';
import 'services/api_service.dart';

void playAlertSound() {
  try {
    js.context.callMethod('eval', [
      """
      (function() {
        var ctx = new (window.AudioContext || window.webkitAudioContext)();
        var osc = ctx.createOscillator();
        var gain = ctx.createGain();
        osc.connect(gain);
        gain.connect(ctx.destination);
        osc.frequency.value = 880;
        osc.start();
        gain.gain.setValueAtTime(1, ctx.currentTime);
        gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.4);
        osc.stop(ctx.currentTime + 0.4);
      })()
      """
    ]);
  } catch(e) {}
}

const _mono = TextStyle(fontFamily: 'monospace');

void main() => runApp(const NseApp());

class NseApp extends StatelessWidget {
  const NseApp({super.key});
  @override
  Widget build(BuildContext context) => MaterialApp(
    title: 'NSE Option Chain Pro',
    debugShowCheckedModeBanner: false,
    theme: ThemeData.dark().copyWith(
      scaffoldBackgroundColor: kBg,
    ),
    home: const TradingDashboard(),
  );
}

// ─── Cyber-Onyx Design System ───────────────────────────────
const kBg          = Color(0xFF020408); // Ultimate depth dark
const kAccent      = Color(0xFF00E5FF); // Electric Cyan
const kAccent2     = Color(0xFFD500F9); // Cyber Magenta
const kSurface     = Color(0xFF0D1117); 
const kSurface2    = Color(0xFF161B22);
const kGlass       = Color(0x1AFFFFFF); 
const kGlassHighlight = Color(0x33FFFFFF);
const kBorder      = Color(0xFF1F2937);
const kNifty       = Color(0xFF2196F3);
const kBank        = Color(0xFFFFB300);
const kGreen       = Color(0xFF00F59B); // High-viz neon green
const kRed         = Color(0xFFFF3D71); // Sharp vivid red
const kOrange      = Color(0xFFFFAB00);
const kGrey        = Color(0xFF8B949E);

final kGlassDecoration = BoxDecoration(
  color: kSurface.withOpacity(0.7),
  borderRadius: BorderRadius.circular(20),
  border: Border.all(color: Colors.white.withOpacity(0.1), width: 1.5),
  boxShadow: [
    BoxShadow(color: Colors.black.withOpacity(0.5), blurRadius: 40, spreadRadius: -5),
  ],
);

// institutional typography defaults
const kHeaderStyle = TextStyle(fontSize: 22, fontWeight: FontWeight.w900, letterSpacing: -0.5, color: Colors.white);
const kSubStyle    = TextStyle(fontSize: 12, fontWeight: FontWeight.bold, color: kGrey, letterSpacing: 1.2);
const kMono        = TextStyle(fontFamily: 'monospace', fontWeight: FontWeight.bold);

// ─── Dashboard ─────────────────────────────────────────────────
class TradingDashboard extends StatefulWidget {
  const TradingDashboard({super.key});
  @override
  State<TradingDashboard> createState() => _TradingDashboardState();
}

class _TradingDashboardState extends State<TradingDashboard> {
  final _api = ApiService();
  Timer? _timer;

  String? _selectedDate;
  List<String> _dates = [];

  // Phase-1: quick summary (loads in <1s)
  Map<String, QuickSummary> _summary = {};

  // Phase-2: OI chart data (loads in background)
  Map<String, List<OiStat>> _oiStats = {};
  Map<String, bool> _oiLoading = {'NIFTY': true, 'BANKNIFTY': true};

  String? _error;
  bool _initialLoading = true;
  bool _refreshing = false;
  DateTime _lastUpdated = DateTime.now();
  Timer? _streamTimer;

  @override
  void initState() {
    super.initState();
    _boot();
    // Real-Time Livestream (Auto-updating in background)
    _streamTimer = Timer.periodic(const Duration(seconds: 4), (_) {
      if (!_initialLoading && _selectedDate != null) {
        _phase1(); // Silently updates signals, PCR, and Spot Price
      }
    });
  }

  @override
  void dispose() {
    _timer?.cancel();
    _streamTimer?.cancel();
    super.dispose();
  }

  // ── Boot: get date list then fire phase 1 ──────────────────
  Future<void> _boot() async {
    try {
      final dates = await _api.getDates();
      if (dates.isEmpty) {
        setState(() { _initialLoading = false; _error = 'No data in database.'; });
        return;
      }
      setState(() { _dates = dates; _selectedDate = dates.first; });
      await _phase1();
      _phase2and3(); // fire in background without await
      _timer = Timer.periodic(const Duration(seconds: 60), (_) {
        _phase1();
        _phase2and3();
      });
    } catch (e) {
      setState(() {
        _initialLoading = false;
        _error = 'Cannot connect to backend at http://127.0.0.1:5000\n\nError: $e';
      });
    }
  }

  // ── Phase 1: Quick summary (< 1 s) ───────────────────────
  Future<void> _phase1() async {
    try {
      final s = await _api.getQuickSummary(_selectedDate);
      
      // Check for signal shifts for audio Alert
      for (final sym in s.keys) {
         final newSig = s[sym]?.signal ?? '';
         final oldSig = _summary[sym]?.signal ?? '';
         final notifyNewExit = s[sym]?.exitAlert != null && _summary[sym]?.exitAlert == null;
         
         if ((newSig != oldSig && (newSig.contains('BUY') || newSig.contains('SELL'))) || notifyNewExit) {
             playAlertSound();
         }
      }

      setState(() {
        _summary = s;
        _initialLoading = false;
        _refreshing = false;
        _lastUpdated = DateTime.now();
      });
    } catch (e) {
      if (mounted) {
        setState(() { _initialLoading = false; _error = 'Error: $e'; });
      }
    }
  }

  // ── Phase 2: Charts (background) ────
  void _phase2and3() {
    final date = _selectedDate;
    if (date == null) return;

    for (final sym in ['NIFTY', 'BANKNIFTY']) {
      setState(() { _oiLoading[sym] = true; });

      // OI charts (we compute Max Pain now since you want visuals!)
      _api.getOiStats(sym, date, skipMaxPain: false).then((oi) {
        if (mounted) setState(() { _oiStats[sym] = oi; _oiLoading[sym] = false; });
      }).catchError((_) {
        if (mounted) setState(() => _oiLoading[sym] = false);
      });
    }
  }

  void _onDateChanged(String d) {
    setState(() {
      _selectedDate = d;
      _summary = {};
      _oiStats = {};
      _initialLoading = true;
    });
    _phase1().then((_) => _phase2and3());
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: kBg,
      body: Stack(
        children: [
          // Background visual anchor (Atmospheric glow)
          Positioned(
            top: -200, right: -200,
            child: Container(
              width: 500, height: 500,
              decoration: BoxDecoration(
                shape: BoxShape.circle,
                color: kAccent.withOpacity(0.05),
              ),
            ),
          ),
          
          Column(children: [
            _Header(
              dates: _dates, selectedDate: _selectedDate,
              lastUpdated: _lastUpdated, refreshing: _refreshing,
              onDateChanged: _onDateChanged, onRefresh: () { _refreshing = true; _phase1(); _phase2and3(); },
            ),
            if (!_initialLoading && _error == null)
              _GlobalSignalsBar(summary: _summary),
            Expanded(child: _buildBody()),
          ]),
        ],
      ),
      floatingActionButton: Container(
        decoration: BoxDecoration(
          gradient: const LinearGradient(colors: [kAccent, kAccent2]),
          borderRadius: BorderRadius.circular(30),
          boxShadow: [
            BoxShadow(color: kAccent.withOpacity(0.3), blurRadius: 12, spreadRadius: 1),
          ],
        ),
        child: FloatingActionButton.extended(
          onPressed: () => _showTradingHub(context),
          elevation: 0,
          backgroundColor: Colors.transparent,
          icon: const Icon(Icons.hub, color: Colors.white),
          label: const Text('COMMAND HUB', style: TextStyle(color: Colors.white, fontWeight: FontWeight.w900, letterSpacing: 0.5)),
        ),
      ),
    );
  }

  void _showTradingHub(BuildContext context) {
    showModalBottomSheet(
      context: context,
      isScrollControlled: true,
      backgroundColor: kBg,
      shape: const RoundedRectangleBorder(borderRadius: BorderRadius.vertical(top: Radius.circular(24))),
      builder: (context) {
        return StatefulBuilder(
          builder: (BuildContext context, StateSetter setModalState) {
            return FutureBuilder<Map<String, dynamic>>(
              future: _api.getTradingState(),
              builder: (context, snapshot) {
                if (snapshot.connectionState == ConnectionState.waiting) {
                   return const SizedBox(height: 300, child: Center(child: CircularProgressIndicator()));
                }
                if (snapshot.hasError) {
                   return SizedBox(height: 300, child: Center(child: Text('Error loading trading state:\n${snapshot.error}', style: const TextStyle(color: kRed))));
                }

                final state = snapshot.data!;
                final bool isPaper = state['paper_trading'] ?? true;
                final bool isAuto = state['trading_enabled'] ?? false;
                final Map<String, dynamic> positions = state['positions'] ?? {};

                return Container(
                  height: MediaQuery.of(context).size.height * 0.7,
                  padding: const EdgeInsets.all(24),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Row(
                        mainAxisAlignment: MainAxisAlignment.spaceBetween,
                        children: [
                          const Text('COMMAND CENTER', style: TextStyle(fontSize: 24, fontWeight: FontWeight.w900, color: Colors.white)),
                          IconButton(icon: const Icon(Icons.close, color: Colors.white), onPressed: () => Navigator.pop(context))
                        ]
                      ),
                      const SizedBox(height: 24),
                      
                      // Auto Trading Toggles
                      Container(
                        padding: const EdgeInsets.all(16),
                        decoration: BoxDecoration(color: kSurface, borderRadius: BorderRadius.circular(16)),
                        child: Column(
                          children: [
                            SwitchListTile(
                              title: const Text('PAPER TRADING MODE', style: TextStyle(fontWeight: FontWeight.bold, color: Colors.amber)),
                              subtitle: const Text('Simulate trades using local data instead of Zerodha', style: TextStyle(color: Colors.white54)),
                              value: isPaper,
                              activeColor: Colors.amber,
                              onChanged: (val) async {
                                 await _api.toggleTradingConfig(paperTrading: val);
                                 setModalState((){}); // Refresh modal
                              }
                            ),
                            const Divider(color: Colors.white10),
                            SwitchListTile(
                              title: const Text('AUTO TRADING ALGORITHM', style: TextStyle(fontWeight: FontWeight.bold, color: Colors.blueAccent)),
                              subtitle: const Text('Automatically execute entry signals when triggered', style: TextStyle(color: Colors.white54)),
                              value: isAuto,
                              activeColor: Colors.blueAccent,
                              onChanged: (val) async {
                                 await _api.toggleTradingConfig(tradingEnabled: val);
                                 setModalState((){});
                              }
                            ),
                          ]
                        )
                      ),
                      
                      const SizedBox(height: 24),
                      const Text('ACTIVE POSITIONS', style: TextStyle(fontSize: 16, fontWeight: FontWeight.bold, color: Colors.white54, letterSpacing: 1.5)),
                      const SizedBox(height: 12),
                      
                      Expanded(
                        child: ListView(
                          children: positions.entries.map((e) {
                            final String symbol = e.key;
                            final pos = e.value;
                            if (pos == null) {
                               return Card(
                                 color: kSurface,
                                 child: ListTile(
                                   title: Text(symbol, style: const TextStyle(fontWeight: FontWeight.bold, color: Colors.white)),
                                   trailing: const Text('NO ACTIVE POSITION', style: TextStyle(color: Colors.white30, fontWeight: FontWeight.bold)),
                                 )
                               );
                            }
                            
                            // Render Active Position
                            final isPaperPos = pos['trading_symbol']?.startsWith('PAPER') ?? false;
                            
                            return Card(
                                 color: kSurface,
                                 shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(12), side: BorderSide(color: isPaperPos ? Colors.amber.withOpacity(0.5) : Colors.blue.withOpacity(0.5))),
                                 child: Padding(
                                   padding: const EdgeInsets.all(16),
                                   child: Column(
                                     crossAxisAlignment: CrossAxisAlignment.start,
                                     children: [
                                       Row(
                                          mainAxisAlignment: MainAxisAlignment.spaceBetween,
                                          children: [
                                            Text(symbol, style: const TextStyle(fontSize: 18, fontWeight: FontWeight.w900, color: Colors.white)),
                                            Row(
                                              children: [
                                                Container(
                                                  padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 4),
                                                  decoration: BoxDecoration(color: isPaperPos ? Colors.amber.withOpacity(0.2) : Colors.blue.withOpacity(0.2), borderRadius: BorderRadius.circular(4)),
                                                  child: Text(isPaperPos ? 'PAPER TRADE' : 'LIVE TRADE', style: TextStyle(color: isPaperPos ? Colors.amber : Colors.blueAccent, fontWeight: FontWeight.bold, fontSize: 10))
                                                ),
                                                const SizedBox(width: 8),
                                                IconButton(
                                                  icon: const Icon(Icons.cancel, color: Colors.white54, size: 20),
                                                  padding: EdgeInsets.zero,
                                                  constraints: const BoxConstraints(),
                                                  onPressed: () async {
                                                     await ApiService().closePosition(symbol);
                                                     setModalState((){}); // Refresh GUI
                                                  }
                                                )
                                              ]
                                            )
                                          ]
                                       ),
                                       const SizedBox(height: 8),
                                       Text('SELL ${pos['strike']} ${pos['type']} • Qty: ${pos['quantity']}', style: TextStyle(color: pos['type'] == 'CE' ? kRed : kGreen, fontWeight: FontWeight.bold, fontSize: 16)),
                                       const SizedBox(height: 12),
                                       Row(
                                         mainAxisAlignment: MainAxisAlignment.spaceBetween,
                                         children: [
                                           Column(
                                             crossAxisAlignment: CrossAxisAlignment.start,
                                             children: [
                                                const Text('ENTRY PRICE', style: TextStyle(color: Colors.white54, fontSize: 10)),
                                                Text('₹${pos['entry_price']}', style: const TextStyle(color: Colors.white, fontWeight: FontWeight.bold, fontSize: 16))
                                             ]
                                           ),
                                           Column(
                                             crossAxisAlignment: CrossAxisAlignment.end,
                                             children: [
                                                const Text('ENTRY TIME', style: TextStyle(color: Colors.white54, fontSize: 10)),
                                                Text('${pos['timestamp']}', style: const TextStyle(color: Colors.white, fontWeight: FontWeight.bold, fontSize: 12))
                                             ]
                                           )
                                         ]
                                       ),
                                       if (pos['current_profit'] != null) ...[
                                         const SizedBox(height: 12),
                                         const Divider(color: Colors.white10),
                                         Row(
                                           mainAxisAlignment: MainAxisAlignment.spaceBetween,
                                           children: [
                                             const Text('CURRENT P&L', style: TextStyle(color: Colors.white, fontWeight: FontWeight.bold, fontSize: 12)),
                                             Text(
                                               (pos['current_profit'] as num) >= 0 ? '+₹${(pos['current_profit'] as num).toStringAsFixed(2)}' : '-₹${(pos['current_profit'] as num).abs().toStringAsFixed(2)}',
                                               style: TextStyle(
                                                 color: (pos['current_profit'] as num) >= 0 ? kGreen : kRed,
                                                 fontWeight: FontWeight.w900, fontSize: 24,
                                                 shadows: [Shadow(color: (pos['current_profit'] as num) >= 0 ? kGreen : kRed, blurRadius: 10)]
                                               )
                                             )
                                           ]
                                         )
                                       ]
                                     ]
                                   )
                                 )
                               );
                          }).toList()
                        )
                      )
                    ]
                  )
                );
              }
            );
          }
        );
      }
    );
  }

  Widget _buildBody() {
    if (_initialLoading) return const Center(child: _Spinner(label: 'Connecting to backend…'));
    if (_error != null) return _ErrorView(message: _error!, onRetry: _boot);
    
    return LayoutBuilder(builder: (context, constraints) {
      if (constraints.maxWidth < 900) {
        // Mobile / Tablet Stacked View
        return ListView(
          padding: EdgeInsets.zero,
          children: [
            _Panel(symbol: 'NIFTY', date: _selectedDate!, accent: kNifty, summary: _summary['NIFTY'],
              oiStats: _oiStats['NIFTY'] ?? [], oiLoading: _oiLoading['NIFTY'] ?? false,
              border: false, isMobile: true),
            const Divider(color: kBorder, height: 1, thickness: 1.5),
            _Panel(symbol: 'BANKNIFTY', date: _selectedDate!, accent: kBank,  summary: _summary['BANKNIFTY'],
              oiStats: _oiStats['BANKNIFTY'] ?? [], oiLoading: _oiLoading['BANKNIFTY'] ?? false,
              border: false, isMobile: true),
          ],
        );
      } else {
        // Desktop Side-by-Side View
        return Row(children: [
          _Panel(symbol: 'NIFTY', date: _selectedDate!, accent: kNifty, summary: _summary['NIFTY'],
            oiStats: _oiStats['NIFTY'] ?? [], oiLoading: _oiLoading['NIFTY'] ?? false,
            border: true, isMobile: false),
          _Panel(symbol: 'BANKNIFTY', date: _selectedDate!, accent: kBank,  summary: _summary['BANKNIFTY'],
            oiStats: _oiStats['BANKNIFTY'] ?? [], oiLoading: _oiLoading['BANKNIFTY'] ?? false,
            border: false, isMobile: false),
        ]);
      }
    });
  }
}

// ─── Global Pinned Signals Bar ─────────────────────────────────
class _GlobalSignalsBar extends StatelessWidget {
  final Map<String, QuickSummary> summary;
  const _GlobalSignalsBar({required this.summary});

  @override
  Widget build(BuildContext context) {
    return Container(
      height: 40,
      decoration: const BoxDecoration(
        color: Colors.transparent, 
        border: Border(bottom: BorderSide(color: Colors.white10, width: 0.5)),
      ),
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 12),
        child: Row(
          children: [
            Expanded(child: _SignalCard(symbol: 'NIFTY', accent: kNifty, summary: summary['NIFTY'])),
            const VerticalDivider(color: Colors.white10, width: 24, indent: 10, endIndent: 10),
            Expanded(child: _SignalCard(symbol: 'BANKNIFTY', accent: kBank, summary: summary['BANKNIFTY'])),
          ],
        ),
      ),
    );
  }
}

// ─── Panel ─────────────────────────────────────────────────────
class _Panel extends StatelessWidget {
  final String symbol;
  final String date;
  final Color accent;
  final QuickSummary? summary;
  final List<OiStat> oiStats;
  final bool oiLoading;
  final bool border;
  final bool isMobile;

  const _Panel({
    required this.symbol, required this.date, required this.accent, required this.summary,
    required this.oiStats, required this.oiLoading, required this.border, required this.isMobile,
  });

  @override
  Widget build(BuildContext context) {
    Widget content = Container(
      decoration: BoxDecoration(
        border: Border(right: BorderSide(color: border ? kBorder : Colors.transparent))),
      child: SingleChildScrollView(
        padding: const EdgeInsets.all(12),
        child: Column(crossAxisAlignment: CrossAxisAlignment.stretch, children: [
          // ② OI stat chips (instant after phase1)
          if (summary != null) _StatRow(summary: summary!, accent: accent),
          const SizedBox(height: 10),

          // ③ OI Line chart
          oiLoading
            ? _PlaceholderCard(label: 'Loading OI chart…', accent: accent, height: 160)
            : (oiStats.isEmpty
                ? _PlaceholderCard(label: 'No chart data', accent: accent, height: 160, error: true)
                : _OiLineChart(oiStats: oiStats, accent: accent)),
          const SizedBox(height: 10),

          // ④ Change in OI chart
          oiLoading
            ? _PlaceholderCard(label: 'Loading Change in OI…', accent: accent, height: 160)
            : (oiStats.isEmpty
                ? _PlaceholderCard(label: 'No chart data', accent: accent, height: 160, error: true)
                : _ChangeOiLineChart(oiStats: oiStats, accent: accent)),
          const SizedBox(height: 10),

          // ⑤ PCR chart
          oiLoading
            ? _PlaceholderCard(label: 'Loading PCR chart…', accent: accent, height: 140)
            : (oiStats.isEmpty
                ? _PlaceholderCard(label: 'No PCR data', accent: accent, height: 140, error: true)
                : _PcrChart(oiStats: oiStats, accent: accent)),
          const SizedBox(height: 10),

          // ⑥ Call / Put Difference chart
          oiLoading
            ? _PlaceholderCard(label: 'Loading Call-Put Diff…', accent: accent, height: 140)
            : (oiStats.isEmpty
                ? _PlaceholderCard(label: 'No chart data', accent: accent, height: 140, error: true)
                : _DiffChart(oiStats: oiStats, accent: accent)),
          const SizedBox(height: 10),

          // ⑦ Max Pain vs Spot chart
          oiLoading
            ? _PlaceholderCard(label: 'Loading Max Pain…', accent: accent, height: 140)
            : (oiStats.isEmpty
                ? _PlaceholderCard(label: 'No chart data', accent: accent, height: 140, error: true)
                : _MaxPainChart(oiStats: oiStats, accent: accent)),
          const SizedBox(height: 10),

          // ⑧ Option chain (On-demand Phase 3)
          _ExpandableOptionChain(
            symbol: symbol, date: date, accent: accent, spotPrice: summary?.spot),
        ]),
      ),
    );
    return isMobile ? content : Expanded(child: content);
  }
}

// ─── Pulsing Signal Card ───────────────────────────────────────
class _SignalCard extends StatefulWidget {
  final String symbol;
  final Color accent;
  final QuickSummary? summary;
  const _SignalCard({required this.symbol, required this.accent, required this.summary});

  @override
  State<_SignalCard> createState() => _SignalCardState();
}

class _SignalCardState extends State<_SignalCard> with SingleTickerProviderStateMixin {
  late AnimationController _anim;

  @override
  void initState() {
    super.initState();
    _anim = AnimationController(vsync: this, duration: const Duration(seconds: 1))..repeat(reverse: true);
  }

  @override
  void dispose() {
    _anim.dispose();
    super.dispose();
  }

  Color _sColor(String? c) {
    switch (c) { case 'green': return kGreen; case 'red': return kRed;
      case 'orange': return kOrange; default: return kGrey; }
  }

  @override
  Widget build(BuildContext context) {
    if (widget.summary == null) {
      return Container(
        height: 120,
        decoration: BoxDecoration(color: kSurface, borderRadius: BorderRadius.circular(16)),
        child: const Center(child: _Spinner(size: 24)),
      );
    }

    final sc = _sColor(widget.summary?.color);
    final String sig = widget.summary!.signal.toUpperCase();
    final bool isStrongSignal = sig.contains(RegExp(r'BUY|SELL|STRONG'));
    
    // Core signal words separate from the rest to make them huge
    String mainWord = sig;
    if (sig.contains('(')) {
       mainWord = sig.split('(')[0].trim();
    }

    return AnimatedBuilder(
      animation: _anim,
      builder: (context, child) {
        
        return Container(
          clipBehavior: Clip.antiAlias,
          decoration: BoxDecoration(
            borderRadius: BorderRadius.circular(8),
          ),
          child: Row(
            children: [
              // Tiny Indicator
              Container(
                width: 3, height: 14,
                decoration: BoxDecoration(
                  color: sc, borderRadius: BorderRadius.circular(2),
                  boxShadow: [if (isStrongSignal) BoxShadow(color: sc.withOpacity(0.6), blurRadius: 4)]
                ),
              ),
              const SizedBox(width: 8),
              
              // Symbol (Tiny weight)
              Text(widget.symbol == 'BANKNIFTY' ? 'BNF' : 'NFT', 
                style: TextStyle(color: widget.accent, fontSize: 10, fontWeight: FontWeight.w900, letterSpacing: 0.5)),
              const SizedBox(width: 8),
              
              // Signal
              Text(mainWord, style: TextStyle(color: sc, fontSize: 12, fontWeight: FontWeight.w900)),
              
              const Spacer(),
              
              // Spot
              Text(NumberFormat('#,##0').format(widget.summary!.spot), 
                style: kMono.copyWith(fontSize: 11, color: Colors.white, fontWeight: FontWeight.bold)),
              
              if (isStrongSignal) ...[
                const SizedBox(width: 8),
                Material(
                  color: sc,
                  borderRadius: BorderRadius.circular(4),
                  child: InkWell(
                    onTap: widget.summary!.atm == null ? null : () async {
                       playAlertSound();
                       final success = await ApiService().executeTrade(widget.symbol, sig, widget.summary!.atm!);
                       if (mounted) {
                         ScaffoldMessenger.of(context).showSnackBar(SnackBar(
                           backgroundColor: success ? kGreen : kRed,
                           content: Text('${widget.symbol} $mainWord EXECUTED', style: const TextStyle(fontWeight: FontWeight.w900, fontSize: 10))
                         ));
                       }
                    },
                    child: const Padding(
                      padding: EdgeInsets.symmetric(horizontal: 6, vertical: 3),
                      child: Text('EXE', style: TextStyle(color: Colors.black, fontWeight: FontWeight.w900, fontSize: 9)),
                    ),
                  ),
                ),
              ],
              
              if (!isStrongSignal) ...[
                const SizedBox(width: 8),
                Text('${widget.summary!.confluence}%', style: TextStyle(color: sc.withOpacity(0.8), fontSize: 10, fontWeight: FontWeight.w900)),
              ],
            ],
          ),
        );
      }
    );
  }
}

// ─── Stat row ──────────────────────────────────────────────────
class _StatRow extends StatelessWidget {
  final QuickSummary summary;
  final Color accent;
  const _StatRow({required this.summary, required this.accent});

  @override
  Widget build(BuildContext context) => Row(children: [
    _chip('CE OI', NumberFormat.compact().format(summary.ceOi), kRed),
    const SizedBox(width: 8),
    _chip('PE OI', NumberFormat.compact().format(summary.peOi), kGreen),
    const SizedBox(width: 8),
    _chip('PCR', summary.pcr.toStringAsFixed(2), summary.pcr >= 1 ? kGreen : kRed),
  ]);

  Widget _chip(String label, String val, Color color) => Expanded(
    child: Container(
      padding: const EdgeInsets.symmetric(vertical: 16, horizontal: 12),
      decoration: BoxDecoration(
        gradient: LinearGradient(
          colors: [kSurface, kSurface2.withOpacity(0.5)],
          begin: Alignment.topLeft, end: Alignment.bottomRight
        ),
        borderRadius: BorderRadius.circular(18), 
        border: Border.all(color: color.withOpacity(0.2), width: 1),
        boxShadow: [
          BoxShadow(color: color.withOpacity(0.04), blurRadius: 12, spreadRadius: 0, offset: const Offset(0, 4)),
        ]
      ),
      child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
        Text(label, style: kSubStyle.copyWith(fontSize: 8, color: Colors.white38)),
        const SizedBox(height: 6),
        Text(val, style: kMono.copyWith(color: color, fontWeight: FontWeight.w900, fontSize: 18)),
      ]),
    ),
  );
}

// ─── OI Line Chart ─────────────────────────────────────────────
class _OiLineChart extends StatelessWidget {
  final List<OiStat> oiStats;
  final Color accent;
  const _OiLineChart({required this.oiStats, required this.accent});

  @override
  Widget build(BuildContext context) {
    if (oiStats.length < 2) return const SizedBox();
    final step = (oiStats.length / 40).ceil().clamp(1, 9999);
    final samples = <OiStat>[];
    for (int i = 0; i < oiStats.length; i += step) samples.add(oiStats[i]);
    if (samples.isNotEmpty && samples.last != oiStats.last) samples.add(oiStats.last);

    double maxOi = samples.map((s) => s.ceOi > s.peOi ? s.ceOi : s.peOi).reduce((a, b) => a > b ? a : b).toDouble();
    double minOi = samples.map((s) => s.ceOi < s.peOi ? s.ceOi : s.peOi).reduce((a, b) => a < b ? a : b).toDouble();
    if (maxOi == minOi) maxOi += 1;

    double maxSpot = samples.map((s) => s.spot).reduce((a, b) => a > b ? a : b);
    double minSpot = samples.map((s) => s.spot).reduce((a, b) => a < b ? a : b);
    if (maxSpot == minSpot) maxSpot += 1;

    // Scale spot price to fit in OI chart range [minOi, maxOi] dynamically
    double scaleSpot(double spot) => minOi + ((spot - minSpot) / (maxSpot - minSpot)) * (maxOi - minOi);

    final ceSpots = <FlSpot>[];
    final peSpots = <FlSpot>[];
    final spotSpots = <FlSpot>[];

    for (int i = 0; i < samples.length; i++) {
        ceSpots.add(FlSpot(i.toDouble(), samples[i].ceOi.toDouble()));
        peSpots.add(FlSpot(i.toDouble(), samples[i].peOi.toDouble()));
        spotSpots.add(FlSpot(i.toDouble(), scaleSpot(samples[i].spot)));
    }

    return _ChartCard(
      title: 'TOTAL OPEN INTEREST & SPOT PRICE  🔴 CE  🟢 PE  ⚪ Spot',
      icon: Icons.show_chart_rounded, height: 160,
      child: LineChart(
        duration: Duration.zero,
        LineChartData(
          minY: minOi - (maxOi-minOi)*0.05,
          maxY: maxOi + (maxOi-minOi)*0.05,
          gridData: FlGridData(
            drawVerticalLine: false, 
            getDrawingHorizontalLine: (_) => const FlLine(color: kBorder, strokeWidth: 0.5)
          ),
          borderData: FlBorderData(show: false),
          titlesData: FlTitlesData(
            rightTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            topTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            bottomTitles: AxisTitles(sideTitles: SideTitles(
              showTitles: true, reservedSize: 14, 
              interval: (samples.length / 4).clamp(1, 9999).toDouble(),
              getTitlesWidget: (v, _) => Text(samples[v.toInt().clamp(0, samples.length - 1)].timestamp, 
                style: const TextStyle(color: Colors.white24, fontSize: 8)),
            )),
            leftTitles: AxisTitles(sideTitles: SideTitles(
              showTitles: true, reservedSize: 32,
              getTitlesWidget: (v, _) => Text(NumberFormat.compact().format(v), 
                style: const TextStyle(color: Colors.white24, fontSize: 8)),
            )),
          ),
          lineBarsData: [
            LineChartBarData(spots: ceSpots, isCurved: true, color: kRed, barWidth: 2.5, dotData: const FlDotData(show: false), belowBarData: BarAreaData(show: true, gradient: LinearGradient(colors: [kRed.withOpacity(0.12), Colors.transparent], begin: Alignment.topCenter, end: Alignment.bottomCenter))),
            LineChartBarData(spots: peSpots, isCurved: true, color: kGreen, barWidth: 2.5, dotData: const FlDotData(show: false), belowBarData: BarAreaData(show: true, gradient: LinearGradient(colors: [kGreen.withOpacity(0.12), Colors.transparent], begin: Alignment.topCenter, end: Alignment.bottomCenter))),
            LineChartBarData(spots: spotSpots, isCurved: true, color: Colors.white70, dashArray: [4, 4], barWidth: 1.5, dotData: const FlDotData(show: false)),
          ],
          lineTouchData: LineTouchData(touchTooltipData: LineTouchTooltipData(
            getTooltipItems: (ts) {
              if (ts.isEmpty) return [];
              final i = ts.first.x.toInt().clamp(0, samples.length - 1);
              final s = samples[i];
              return [
                LineTooltipItem('${s.timestamp}\nCE: ${NumberFormat.compact().format(s.ceOi)}\nPE: ${NumberFormat.compact().format(s.peOi)}\nSpot: ${s.spot.toStringAsFixed(1)}', const TextStyle(color: Colors.white, fontSize: 10)),
                ...List.generate(ts.length - 1, (_) => LineTooltipItem('', const TextStyle())), // hide duplicate tooltips
              ];
            }
          )),
        ),
      ),
    );
  }
}

// ─── PCR Line Chart ────────────────────────────────────────────
class _PcrChart extends StatelessWidget {
  final List<OiStat> oiStats;
  final Color accent;
  const _PcrChart({required this.oiStats, required this.accent});

  @override
  Widget build(BuildContext context) {
    final step = (oiStats.length / 30).ceil().clamp(1, 9999);
    final spots = <FlSpot>[];
    for (int i = 0; i < oiStats.length; i += step) {
      spots.add(FlSpot(i.toDouble(), oiStats[i].pcr));
    }
    if (spots.length < 2) return const SizedBox();

    final minY = (spots.map((s) => s.y).reduce((a, b) => a < b ? a : b) - 0.1).clamp(0.0, 99.0);
    final maxY =  spots.map((s) => s.y).reduce((a, b) => a > b ? a : b) + 0.1;

    return _ChartCard(
      title: 'PUT / CALL RATIO (PCR)  — — neutral at 1.0',
      icon: Icons.show_chart_rounded, height: 140,
      child: LineChart(
        duration: Duration.zero,
        LineChartData(
          minY: minY, maxY: maxY,
          gridData: FlGridData(drawVerticalLine: false,
            horizontalInterval: ((maxY - minY) / 3).clamp(0.01, 99),
            getDrawingHorizontalLine: (_) => const FlLine(color: kBorder, strokeWidth: 0.5)),
          borderData: FlBorderData(show: false),
          titlesData: FlTitlesData(
            rightTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            topTitles:   const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            bottomTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            leftTitles: AxisTitles(sideTitles: SideTitles(
              showTitles: true, reservedSize: 28,
              getTitlesWidget: (v, _) => Text(v.toStringAsFixed(2),
                style: const TextStyle(color: Colors.white24, fontSize: 8)),
            )),
          ),
          lineBarsData: [
            LineChartBarData(
              spots: spots, isCurved: true, color: accent, barWidth: 2,
              dotData: const FlDotData(show: false),
              belowBarData: BarAreaData(show: true, gradient: LinearGradient(
                colors: [accent.withOpacity(0.25), accent.withOpacity(0)],
                begin: Alignment.topCenter, end: Alignment.bottomCenter)),
            ),
            LineChartBarData(
              spots: [FlSpot(0, 1), FlSpot(spots.length.toDouble(), 1)],
              color: Colors.white24, barWidth: 1, dashArray: [4, 4],
              dotData: const FlDotData(show: false)),
          ],
          lineTouchData: LineTouchData(touchTooltipData: LineTouchTooltipData(
            getTooltipItems: (ts) => ts.map((t) => LineTooltipItem(
              'PCR: ${t.y.toStringAsFixed(2)}',
              const TextStyle(color: Colors.white, fontSize: 10))).toList())),
        ),
      ),
    );
  }
}

// ─── Change in OI Line Chart ───────────────────────────────────
class _ChangeOiLineChart extends StatelessWidget {
  final List<OiStat> oiStats;
  final Color accent;
  const _ChangeOiLineChart({required this.oiStats, required this.accent});

  @override
  Widget build(BuildContext context) {
    if (oiStats.length < 2) return const SizedBox();
    final step = (oiStats.length / 40).ceil().clamp(1, 9999);
    final samples = <OiStat>[];
    for (int i = 0; i < oiStats.length; i += step) samples.add(oiStats[i]);
    if (samples.isNotEmpty && samples.last != oiStats.last) samples.add(oiStats.last);

    double maxY = 0;
    double minY = 0;
    for (var s in samples) {
      if (s.ceChangeOi > maxY) maxY = s.ceChangeOi.toDouble();
      if (s.peChangeOi > maxY) maxY = s.peChangeOi.toDouble();
      if (s.ceChangeOi < minY) minY = s.ceChangeOi.toDouble();
      if (s.peChangeOi < minY) minY = s.peChangeOi.toDouble();
    }
    if (maxY == 0 && minY == 0) maxY = 1000;

    double maxSpot = samples.map((s) => s.spot).reduce((a, b) => a > b ? a : b);
    double minSpot = samples.map((s) => s.spot).reduce((a, b) => a < b ? a : b);
    if (maxSpot == minSpot) maxSpot += 1;

    // Scale Spot Price to fit in chart range
    double scaleSpot(double spot) => minY + ((spot - minSpot) / (maxSpot - minSpot)) * (maxY - minY);

    final ceSpots = <FlSpot>[];
    final peSpots = <FlSpot>[];
    final spotSpots = <FlSpot>[];

    for (int i = 0; i < samples.length; i++) {
        ceSpots.add(FlSpot(i.toDouble(), samples[i].ceChangeOi.toDouble()));
        peSpots.add(FlSpot(i.toDouble(), samples[i].peChangeOi.toDouble()));
        spotSpots.add(FlSpot(i.toDouble(), scaleSpot(samples[i].spot)));
    }

    return _ChartCard(
      title: 'CHANGE IN OI & SPOT PRICE  🔴 CE  🟢 PE  ⚪ Spot',
      icon: Icons.show_chart_rounded, height: 160,
      child: LineChart(
        duration: Duration.zero,
        LineChartData(
          maxY: maxY + (maxY - minY) * 0.05,
          minY: minY - (maxY - minY) * 0.05,
          gridData: FlGridData(
            drawVerticalLine: false,
            getDrawingHorizontalLine: (v) => FlLine(color: v == 0 ? Colors.white54 : kBorder, strokeWidth: v == 0 ? 1 : 0.5)
          ),
          borderData: FlBorderData(show: false),
          titlesData: FlTitlesData(
            rightTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            topTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            bottomTitles: AxisTitles(sideTitles: SideTitles(
              showTitles: true, reservedSize: 14, 
              interval: (samples.length / 4).clamp(1, 9999).toDouble(),
              getTitlesWidget: (v, _) => Text(samples[v.toInt().clamp(0, samples.length - 1)].timestamp, 
                style: const TextStyle(color: Colors.white24, fontSize: 8)),
            )),
            leftTitles: AxisTitles(sideTitles: SideTitles(
              showTitles: true, reservedSize: 32,
              getTitlesWidget: (v, _) => Text(NumberFormat.compact().format(v), 
                style: const TextStyle(color: Colors.white24, fontSize: 8)),
            )),
          ),
          lineBarsData: [
            LineChartBarData(spots: ceSpots, isCurved: true, color: kRed, barWidth: 2.5, dotData: const FlDotData(show: false), belowBarData: BarAreaData(show: true, gradient: LinearGradient(colors: [kRed.withOpacity(0.12), Colors.transparent], begin: Alignment.topCenter, end: Alignment.bottomCenter))),
            LineChartBarData(spots: peSpots, isCurved: true, color: kGreen, barWidth: 2.5, dotData: const FlDotData(show: false), belowBarData: BarAreaData(show: true, gradient: LinearGradient(colors: [kGreen.withOpacity(0.12), Colors.transparent], begin: Alignment.topCenter, end: Alignment.bottomCenter))),
            LineChartBarData(spots: spotSpots, isCurved: true, color: Colors.white70, dashArray: [4, 4], barWidth: 1.5, dotData: const FlDotData(show: false)),
          ],
          lineTouchData: LineTouchData(touchTooltipData: LineTouchTooltipData(
            getTooltipItems: (ts) {
              if (ts.isEmpty) return [];
              final i = ts.first.x.toInt().clamp(0, samples.length - 1);
              final s = samples[i];
              return [
                LineTooltipItem('${s.timestamp}\nCE Chg: ${NumberFormat.compact().format(s.ceChangeOi)}\nPE Chg: ${NumberFormat.compact().format(s.peChangeOi)}\nSpot: ${s.spot.toStringAsFixed(1)}', const TextStyle(color: Colors.white, fontSize: 10)),
                ...List.generate(ts.length - 1, (_) => LineTooltipItem('', const TextStyle())), // hide duplicate tooltips
              ];
            }
          )),
        ),
      ),
    );
  }
}

// ─── Call Put Difference Chart ─────────────────────────────────
class _DiffChart extends StatelessWidget {
  final List<OiStat> oiStats;
  final Color accent;
  const _DiffChart({required this.oiStats, required this.accent});

  @override
  Widget build(BuildContext context) {
    final step = (oiStats.length / 30).ceil().clamp(1, 9999);
    final spots = <FlSpot>[];
    for (int i = 0; i < oiStats.length; i += step) {
      double diff = (oiStats[i].ceOi - oiStats[i].peOi).toDouble();
      spots.add(FlSpot(i.toDouble(), diff));
    }
    if (spots.length < 2) return const SizedBox();

    double maxY = spots.map((s) => s.y).reduce((a, b) => a > b ? a : b);
    double minY = spots.map((s) => s.y).reduce((a, b) => a < b ? a : b);
    if (maxY == minY) maxY += 1;

    return _ChartCard(
      title: 'OI DIFFERENCE (CE - PE)  — > 0 is Bearish / < 0 is Bullish',
      icon: Icons.difference_outlined, height: 140,
      child: LineChart(
        duration: Duration.zero,
        LineChartData(
          minY: minY < 0 ? minY * 1.1 : 0, 
          maxY: maxY > 0 ? maxY * 1.1 : 0,
          gridData: FlGridData(drawVerticalLine: false,
            getDrawingHorizontalLine: (v) => FlLine(
              color: v == 0 ? Colors.white54 : kBorder, strokeWidth: v == 0 ? 1 : 0.5)),
          borderData: FlBorderData(show: false),
          titlesData: FlTitlesData(
            rightTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            topTitles:   const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            bottomTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            leftTitles: AxisTitles(sideTitles: SideTitles(
              showTitles: true, reservedSize: 32,
              getTitlesWidget: (v, _) => Text(NumberFormat.compact().format(v),
                style: const TextStyle(color: Colors.white24, fontSize: 8)),
            )),
          ),
          lineBarsData: [
            LineChartBarData(
              spots: spots, isCurved: true, color: Colors.blueAccent, barWidth: 2.5,
              dotData: const FlDotData(show: false),
              belowBarData: BarAreaData(show: true, gradient: LinearGradient(colors: [Colors.blueAccent.withOpacity(0.15), Colors.transparent], begin: Alignment.topCenter, end: Alignment.bottomCenter))
            ),
          ],
          lineTouchData: LineTouchData(touchTooltipData: LineTouchTooltipData(
            getTooltipItems: (ts) => ts.map((t) => LineTooltipItem(
              'Diff: ${NumberFormat.compact().format(t.y)}\n(>0 Bearish, <0 Bullish)',
              const TextStyle(color: Colors.white, fontSize: 10))).toList())),
        ),
      ),
    );
  }
}

// ─── Max Pain vs Spot Chart ────────────────────────────────────
class _MaxPainChart extends StatelessWidget {
  final List<OiStat> oiStats;
  final Color accent;
  const _MaxPainChart({required this.oiStats, required this.accent});

  @override
  Widget build(BuildContext context) {
    // Only use stats where maxPain is > 0 (available)
    final validStats = oiStats.where((s) => s.maxPain > 0).toList();
    if (validStats.length < 2) {
      return const Center(child: Text("Waiting for sufficient Max Pain data...", style: TextStyle(color: Colors.white24, fontSize: 10)));
    }

    final step = (validStats.length / 30).ceil().clamp(1, 9999);
    final spotSpots = <FlSpot>[];
    final mpSpots = <FlSpot>[];
    
    for (int i = 0; i < validStats.length; i += step) {
      spotSpots.add(FlSpot(i.toDouble(), validStats[i].spot));
      mpSpots.add(FlSpot(i.toDouble(), validStats[i].maxPain));
    }

    double maxSpot = spotSpots.map((s) => s.y).reduce((a, b) => a > b ? a : b);
    double minSpot = spotSpots.map((s) => s.y).reduce((a, b) => a < b ? a : b);
    double maxMp = mpSpots.map((s) => s.y).reduce((a, b) => a > b ? a : b);
    double minMp = mpSpots.map((s) => s.y).reduce((a, b) => a < b ? a : b);

    double maxY = maxSpot > maxMp ? maxSpot : maxMp;
    double minY = minSpot < minMp ? minSpot : minMp;

    double padding = (maxY - minY) * 0.1;
    if (padding == 0) padding = 10;

    return _ChartCard(
      title: 'MAX PAIN vs SPOT  ⚪ Spot  🟣 Max Pain',
      icon: Icons.timeline_rounded, height: 140,
      child: LineChart(
        duration: Duration.zero,
        LineChartData(
          minY: minY - padding,
          maxY: maxY + padding,
          gridData: FlGridData(drawVerticalLine: false,
            getDrawingHorizontalLine: (_) => const FlLine(color: kBorder, strokeWidth: 0.5)),
          borderData: FlBorderData(show: false),
          titlesData: FlTitlesData(
            rightTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            topTitles:   const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            bottomTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            leftTitles: AxisTitles(sideTitles: SideTitles(
              showTitles: true, reservedSize: 32,
              getTitlesWidget: (v, _) => Text(v.toStringAsFixed(0),
                style: const TextStyle(color: Colors.white24, fontSize: 8)),
            )),
          ),
          lineBarsData: [
            LineChartBarData(
              spots: spotSpots, isCurved: true, color: Colors.white60, barWidth: 1.5,
              dashArray: [4, 4], dotData: const FlDotData(show: false),
            ),
            LineChartBarData(
              spots: mpSpots, isCurved: true, color: Colors.deepPurpleAccent, barWidth: 2.5,
              dotData: const FlDotData(show: false),
              belowBarData: BarAreaData(show: true, gradient: LinearGradient(colors: [Colors.deepPurpleAccent.withOpacity(0.15), Colors.transparent], begin: Alignment.topCenter, end: Alignment.bottomCenter))
            ),
          ],
          lineTouchData: LineTouchData(touchTooltipData: LineTouchTooltipData(
            getTooltipItems: (ts) {
              if (ts.length < 2) return [];
              final sort = ts.toList()..sort((a,b) => a.barIndex.compareTo(b.barIndex));
              final spotVal = sort[0].y;
              final mpVal = sort[1].y;
              return [
                LineTooltipItem('Spot: ${spotVal.toStringAsFixed(1)}\nMP: ${mpVal.toStringAsFixed(1)}', 
                  const TextStyle(color: Colors.white, fontSize: 10)),
                LineTooltipItem('', const TextStyle()), // hide the second tooltip
              ];
            }
          )),
        ),
      ),
    );
  }
}

// ─── Institutional Chart Card Wrapper ──────────────────────────
class _ChartCard extends StatelessWidget {
  final String title;
  final IconData icon;
  final double height;
  final Widget child;
  const _ChartCard({required this.title, required this.icon,
    required this.height, required this.child});
  @override
  Widget build(BuildContext context) => Container(
    padding: const EdgeInsets.all(20),
    decoration: BoxDecoration(
      color: kSurface,
      borderRadius: BorderRadius.circular(20), 
      border: Border.all(color: Colors.white.withOpacity(0.05)),
      boxShadow: [
        BoxShadow(color: Colors.black.withOpacity(0.3), blurRadius: 15, offset: const Offset(0, 5)),
      ]
    ),
    child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
      Row(children: [
        Icon(icon, size: 12, color: kAccent.withOpacity(0.5)),
        const SizedBox(width: 8),
        Expanded(child: Text(title.toUpperCase(),
          style: kSubStyle.copyWith(fontSize: 9, color: Colors.white38))),
      ]),
      const SizedBox(height: 18),
      SizedBox(height: height, child: child),
    ]),
  );
}

// ─── Placeholder ───────────────────────────────────────────────
class _PlaceholderCard extends StatelessWidget {
  final String label;
  final Color accent;
  final double height;
  final bool error;
  const _PlaceholderCard({required this.label, required this.accent,
    required this.height, this.error = false});
  @override
  Widget build(BuildContext context) => Container(
    height: height,
    decoration: BoxDecoration(color: kSurface,
      borderRadius: BorderRadius.circular(14), border: Border.all(color: kBorder)),
    child: Center(child: Row(mainAxisAlignment: MainAxisAlignment.center, children: [
      if (!error) SizedBox(width: 14, height: 14,
        child: CircularProgressIndicator(strokeWidth: 1.5, color: accent))
      else const Icon(Icons.info_outline, size: 14, color: Colors.white24),
      const SizedBox(width: 10),
      Text(label, style: const TextStyle(color: Colors.white30, fontSize: 12)),
    ])),
  );
}

// ─── Institutional Option Chain Table ──────────────────────────
class _OptionChainTable extends StatelessWidget {
  final List<OptionData> optionData;
  final double? spotPrice;
  final Color accent;
  const _OptionChainTable({required this.optionData,
    required this.spotPrice, required this.accent});

  @override
  Widget build(BuildContext context) => Container(
    decoration: BoxDecoration(
      color: kSurface,
      borderRadius: BorderRadius.circular(20),
      border: Border.all(color: Colors.white.withOpacity(0.03)),
    ),
    clipBehavior: Clip.antiAlias,
    child: Column(crossAxisAlignment: CrossAxisAlignment.stretch, children: [
        _hdr(),
        const Divider(color: Colors.white12, height: 1),
        Expanded(
          child: ListView.builder(
            itemCount: optionData.length,
            itemBuilder: (context, i) {
              final r = optionData[i];
              return _OcRow(data: r,
                  isAtm: spotPrice != null && (r.strikePrice - spotPrice!).abs() < 51,
                  accent: accent);
            },
          ),
        ),
      ]),
  );

  Widget _hdr() {
    final s = kSubStyle.copyWith(fontSize: 9, color: Colors.white24);
    return Container(
      color: kSurface2,
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 12),
      child: Row(children: [
        Expanded(child: Text('OI', style: s, textAlign: TextAlign.right)),
        Expanded(child: Text('Δ OI', style: s, textAlign: TextAlign.right)),
        Expanded(child: Text('LTP', style: s, textAlign: TextAlign.right)),
        const Expanded(flex: 2,
          child: Text('STRIKE', style: TextStyle(color: Colors.white54, fontSize: 10, fontWeight: FontWeight.w900, letterSpacing: 1), textAlign: TextAlign.center)),
        Expanded(child: Text('LTP', style: s)),
        Expanded(child: Text('Δ OI', style: s)),
        Expanded(child: Text('OI', style: s)),
      ]));
  }
}

class _OcRow extends StatelessWidget {
  final OptionData data;
  final bool isAtm;
  final Color accent;
  const _OcRow({required this.data, required this.isAtm, required this.accent});

  Color _c(num? v) => (v == null || v == 0) ? Colors.white54 : v > 0 ? kGreen : kRed;

  @override
  Widget build(BuildContext context) {
    final c  = NumberFormat.compact();
    final lf = NumberFormat('#,##0.00');
    return Container(
      decoration: BoxDecoration(
        color: isAtm ? accent.withOpacity(0.07) : Colors.transparent,
        border: isAtm ? Border(
          left: BorderSide(color: accent, width: 2),
          right: BorderSide(color: accent, width: 2)) : null),
      padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 5),
      child: Row(children: [
        Expanded(child: Text(c.format(data.ce.oi ?? 0),
          style: const TextStyle(color: Colors.white60, fontSize: 10),
          textAlign: TextAlign.right)),
        Expanded(child: Text(c.format(data.ce.changeOi ?? 0),
          style: TextStyle(color: _c(data.ce.changeOi), fontSize: 10),
          textAlign: TextAlign.right)),
        Expanded(child: Text(lf.format(data.ce.lastPrice ?? 0),
          style: TextStyle(color: _c(data.ce.change), fontSize: 10),
          textAlign: TextAlign.right)),
        Expanded(flex: 2, child: Text(
          NumberFormat('#,###').format(data.strikePrice),
          textAlign: TextAlign.center,
          style: TextStyle(color: isAtm ? accent : Colors.white,
            fontWeight: isAtm ? FontWeight.bold : FontWeight.w500, fontSize: 11))),
        Expanded(child: Text(lf.format(data.pe.lastPrice ?? 0),
          style: TextStyle(color: _c(data.pe.change), fontSize: 10))),
        Expanded(child: Text(c.format(data.pe.changeOi ?? 0),
          style: TextStyle(color: _c(data.pe.changeOi), fontSize: 10))),
        Expanded(child: Text(c.format(data.pe.oi ?? 0),
          style: const TextStyle(color: Colors.white60, fontSize: 10))),
      ]),
    );
  }
}

// ─── Institutional Header ──────────────────────────────────────
class _Header extends StatefulWidget {
  final List<String> dates;
  final String? selectedDate;
  final DateTime lastUpdated;
  final bool refreshing;
  final ValueChanged<String> onDateChanged;
  final VoidCallback onRefresh;
  const _Header({required this.dates, required this.selectedDate,
    required this.lastUpdated, required this.refreshing,
    required this.onDateChanged, required this.onRefresh});
  @override
  State<_Header> createState() => _HeaderState();
}

class _HeaderState extends State<_Header> {
  late Timer _t;
  DateTime _now = DateTime.now();
  @override
  void initState() { super.initState(); _t = Timer.periodic(const Duration(seconds: 1), (_) => setState(() => _now = DateTime.now())); }
  @override
  void dispose() { _t.cancel(); super.dispose(); }
  @override
  Widget build(BuildContext context) => Container(
    height: 70,
    decoration: BoxDecoration(
      color: kSurface.withOpacity(0.85),
      border: const Border(bottom: BorderSide(color: Colors.white10, width: 0.5)),
    ),
    padding: const EdgeInsets.symmetric(horizontal: 24),
    child: Row(children: [
      Column(
        mainAxisAlignment: MainAxisAlignment.center,
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Container(width: 8, height: 8, decoration: const BoxDecoration(color: kAccent, shape: BoxShape.circle)),
              const SizedBox(width: 8),
              Text('QUANT TERMINAL V1', style: kSubStyle.copyWith(color: kAccent.withOpacity(0.8), letterSpacing: 2)),
            ],
          ),
          const Text('NSE OPTION CHAIN', style: kHeaderStyle),
        ],
      ),
      const Spacer(),
      if (widget.dates.isNotEmpty)
        Container(
          height: 40,
          padding: const EdgeInsets.symmetric(horizontal: 14),
          decoration: BoxDecoration(
            color: kSurface2,
            borderRadius: BorderRadius.circular(12),
            border: Border.all(color: Colors.white.withOpacity(0.05)),
          ),
          child: DropdownButtonHideUnderline(
            child: DropdownButton<String>(
              value: widget.selectedDate, dropdownColor: kSurface,
              style: const TextStyle(color: Colors.white, fontSize: 13, fontWeight: FontWeight.bold, letterSpacing: 1),
              icon: const Icon(Icons.keyboard_arrow_down_rounded, size: 18, color: kGrey),
              items: widget.dates.map((d) =>
                DropdownMenuItem(value: d, child: Text(d))).toList(),
              onChanged: (v) { if (v != null) widget.onDateChanged(v); })),
        ),
      const SizedBox(width: 25),
      // Clock & Sync Status
      Column(
        mainAxisAlignment: MainAxisAlignment.center,
        crossAxisAlignment: CrossAxisAlignment.end,
        children: [
          Text(DateFormat('HH:mm:ss').format(_now),
               style: kMono.copyWith(fontSize: 18, color: Colors.white, letterSpacing: 1)),
          Row(
            children: [
              const Text('SYSTEM LIVE', style: TextStyle(fontSize: 9, fontWeight: FontWeight.w900, color: kGreen, letterSpacing: 1)),
              const SizedBox(width: 6),
              Container(
                width: 6, height: 6, 
                decoration: BoxDecoration(
                  color: kGreen, shape: BoxShape.circle,
                  boxShadow: [BoxShadow(color: kGreen.withOpacity(0.5), blurRadius: 4, spreadRadius: 1)]
                )
              ),
            ],
          ),
        ],
      ),
      const SizedBox(width: 25),
      _RefreshButton(onPressed: widget.onRefresh, refreshing: widget.refreshing),
    ]),
  );
}

class _RefreshButton extends StatelessWidget {
  final VoidCallback onPressed;
  final bool refreshing;
  const _RefreshButton({required this.onPressed, required this.refreshing});
  @override
  Widget build(BuildContext context) => InkWell(
    onTap: onPressed,
    borderRadius: BorderRadius.circular(12),
    child: Container(
      width: 40, height: 40,
      decoration: BoxDecoration(
        color: Colors.white.withOpacity(0.03),
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: Colors.white.withOpacity(0.05)),
      ),
      child: Center(
        child: refreshing 
          ? const SizedBox(width: 18, height: 18, child: CircularProgressIndicator(strokeWidth: 2, color: kAccent))
          : const Icon(Icons.sync_rounded, color: kGrey, size: 22),
      ),
    ),
  );
}

// ─── Error view ────────────────────────────────────────────────
class _ErrorView extends StatelessWidget {
  final String message;
  final VoidCallback onRetry;
  const _ErrorView({required this.message, required this.onRetry});
  @override
  Widget build(BuildContext context) => Center(child: Column(
    mainAxisAlignment: MainAxisAlignment.center,
    children: [
      const Icon(Icons.error_outline_rounded, color: kRed, size: 48),
      const SizedBox(height: 16),
      Text(message, textAlign: TextAlign.center,
        style: const TextStyle(color: Colors.white54, fontSize: 13, height: 1.8)),
      const SizedBox(height: 20),
      ElevatedButton.icon(onPressed: onRetry,
        icon: const Icon(Icons.refresh_rounded, size: 16),
        label: const Text('Retry'),
        style: ElevatedButton.styleFrom(backgroundColor: kNifty)),
    ]));
}

// ─── Spinner ───────────────────────────────────────────────────
class _Spinner extends StatelessWidget {
  final double size;
  final String? label;
  const _Spinner({this.size = 32, this.label});
  @override
  Widget build(BuildContext context) => Column(mainAxisSize: MainAxisSize.min, children: [
    SizedBox(width: size, height: size,
      child: const CircularProgressIndicator(strokeWidth: 2,
        valueColor: AlwaysStoppedAnimation(kNifty))),
    if (label != null) ...[
      const SizedBox(height: 14),
      Text(label!, style: const TextStyle(color: Colors.white38, fontSize: 13)),
      const SizedBox(height: 6),
      Text('http://127.0.0.1:5000',
        style: _mono.copyWith(color: Colors.white24, fontSize: 10)),
    ],
  ]);
}

// ─── Expandable Option Chain ───────────────────────────────────
class _ExpandableOptionChain extends StatefulWidget {
  final String symbol;
  final String date;
  final double? spotPrice;
  final Color accent;
  const _ExpandableOptionChain({
    required this.symbol, required this.date,
    required this.spotPrice, required this.accent});

  @override
  State<_ExpandableOptionChain> createState() => _ExpandableOptionChainState();
}

class _ExpandableOptionChainState extends State<_ExpandableOptionChain> {
  final _api = ApiService();
  bool _expanded = false;
  bool _loading = false;
  List<OptionData> _data = [];
  String? _error;

  void _loadTask() async {
    setState(() { _loading = true; _error = null; });
    try {
      final ts = await _api.getTimestamps(widget.symbol, widget.date);
      if (ts.isEmpty) throw Exception('No timestamps');
      final od = await _api.getData(widget.symbol, ts.last, widget.date);
      if (mounted) setState(() { _data = od; _loading = false; });
    } catch (e) {
      if (mounted) setState(() { _error = 'Option chain error: $e'; _loading = false; });
    }
  }

  @override
  Widget build(BuildContext context) {
    return Container(
      decoration: BoxDecoration(color: kSurface,
        borderRadius: BorderRadius.circular(14), border: Border.all(color: kBorder)),
      child: Theme(
        data: Theme.of(context).copyWith(dividerColor: Colors.transparent),
        child: ExpansionTile(
          collapsedIconColor: Colors.white30,
          iconColor: widget.accent,
          title: Row(children: [
            Icon(Icons.table_rows_rounded, size: 14, color: _expanded ? widget.accent : Colors.white30),
            const SizedBox(width: 8),
            Text('VIEW OPTION CHAIN',
              style: TextStyle(color: _expanded ? widget.accent : Colors.white30, 
                fontSize: 10, letterSpacing: 1, fontWeight: FontWeight.bold)),
            if (_expanded && !_loading && _data.isNotEmpty) ...[
              const Spacer(),
              Text('${_data.length} strikes', style: const TextStyle(color: Colors.white24, fontSize: 9)),
            ]
          ]),
          onExpansionChanged: (exp) {
            setState(() => _expanded = exp);
            if (exp && _data.isEmpty && !_loading) _loadTask();
          },
          children: [
            const Divider(color: kBorder, height: 1),
            if (_loading)
               Padding(padding: const EdgeInsets.all(30), 
                 child: _Spinner(size: 20, label: 'Loading all strikes...')),
            if (!_loading && _error != null)
               Padding(padding: const EdgeInsets.all(20), 
                 child: Text(_error!, style: const TextStyle(color: kRed))),
            if (!_loading && _error == null && _data.isNotEmpty)
               ConstrainedBox(
                 constraints: const BoxConstraints(maxHeight: 400),
                 child: _OptionChainTable(optionData: _data, spotPrice: widget.spotPrice, accent: widget.accent),
               ),
          ],
        ),
      ),
    );
  }
}
