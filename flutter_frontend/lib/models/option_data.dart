class QuickSummary {
  final double spot;
  final String timestamp;
  final String signal;
  final String color;
  final double? atm;
  final int ceOi;
  final int peOi;
  final double pcr;
  final int confluence;
  final String? exitAlert;

  QuickSummary({required this.spot, required this.timestamp,
    required this.signal, required this.color, required this.atm,
    required this.ceOi, required this.peOi, required this.pcr,
    required this.confluence, this.exitAlert});

  factory QuickSummary.fromJson(Map<String, dynamic> j) => QuickSummary(
    spot:      (j['spot'] ?? 0).toDouble(),
    timestamp: j['timestamp'] ?? '',
    signal:    j['signal'] ?? 'WAIT',
    color:     j['color'] ?? 'gray',
    atm:       j['atm']?.toDouble(),
    ceOi:      (j['ce_oi'] ?? 0).toInt(),
    peOi:      (j['pe_oi'] ?? 0).toInt(),
    pcr:       (j['pcr'] ?? 0.0).toDouble(),
    confluence:(j['confluence'] ?? 50).toInt(),
    exitAlert: j['exit_alert'],
  );
}

class OiStat {
  final String timestamp;
  final int ceOi;
  final int peOi;
  final int ceChangeOi;
  final int peChangeOi;
  final double pcr;
  final double spot;
  final double maxPain;

  OiStat({required this.timestamp, required this.ceOi, required this.peOi,
    required this.ceChangeOi, required this.peChangeOi,
    required this.pcr, required this.spot, required this.maxPain});

  factory OiStat.fromJson(Map<String, dynamic> j) => OiStat(
    timestamp:  j['timestamp'] ?? '',
    ceOi:       (j['ce_oi'] ?? 0).toInt(),
    peOi:       (j['pe_oi'] ?? 0).toInt(),
    ceChangeOi: (j['ce_change_oi'] ?? 0).toInt(),
    peChangeOi: (j['pe_change_oi'] ?? 0).toInt(),
    pcr:        (j['pcr'] ?? 0.0).toDouble(),
    spot:       (j['spot'] ?? 0.0).toDouble(),
    maxPain:    (j['max_pain'] ?? 0.0).toDouble(),
  );
}

class OptionData {
  final double strikePrice;
  final String expiryDate;
  final double underlyingPrice;
  final OptionSide ce;
  final OptionSide pe;

  OptionData({required this.strikePrice, required this.expiryDate,
    required this.underlyingPrice, required this.ce, required this.pe});

  factory OptionData.fromJson(Map<String, dynamic> j) => OptionData(
    strikePrice:     (j['strike_price'] ?? 0).toDouble(),
    expiryDate:      j['expiry_date'] ?? '',
    underlyingPrice: (j['underlying_price'] ?? 0).toDouble(),
    ce: OptionSide.fromJson(j['ce'] ?? {}),
    pe: OptionSide.fromJson(j['pe'] ?? {}),
  );
}

class OptionSide {
  final double? lastPrice;
  final double? change;
  final int? oi;
  final int? changeOi;
  final int? volume;
  final double? iv;

  OptionSide({this.lastPrice, this.change, this.oi, this.changeOi, this.volume, this.iv});

  factory OptionSide.fromJson(Map<String, dynamic> j) => OptionSide(
    lastPrice: j['last_price']?.toDouble(),
    change:    j['change']?.toDouble(),
    oi:        j['oi']?.toInt(),
    changeOi:  j['change_oi']?.toInt(),
    volume:    j['volume']?.toInt(),
    iv:        j['iv']?.toDouble(),
  );
}

class SpotPrice {
  final double price;
  final String timestamp;
  SpotPrice({required this.price, required this.timestamp});
  factory SpotPrice.fromJson(Map<String, dynamic> j) => SpotPrice(
    price: (j['price'] ?? 0).toDouble(), timestamp: j['timestamp'] ?? '');
}

class MarketSignal {
  final String signal;
  final String color;
  final double spot;
  final double atm;
  MarketSignal({required this.signal, required this.color, required this.spot, required this.atm});
  factory MarketSignal.fromJson(Map<String, dynamic> j) => MarketSignal(
    signal: j['signal'] ?? 'WAIT', color: j['color'] ?? 'gray',
    spot: (j['spot'] ?? 0.0).toDouble(), atm: (j['atm'] ?? 0.0).toDouble());
}
