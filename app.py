from flask import Flask, jsonify, request, render_template
from flask_cors import CORS
from sqlalchemy.orm import sessionmaker
from fetch_nse_data import OptionChainData, get_db_engine
import os
import json
import time
from datetime import datetime

# Simple in-memory cache: {key: (timestamp, data)}
_cache = {}
CACHE_TTL = 3  # seconds (reduced for real-time streaming)

# Persistent cache for Max Pain since past minutes never change
# Key: "SYMBOL_YYYY-MM-DD HH:MM:SS" -> Value: max_pain_strike
_max_pain_cache = {}

def _cache_get(key):
    if key in _cache:
        ts, data = _cache[key]
        if time.time() - ts < CACHE_TTL:
            return data
    return None

def _cache_set(key, data):
    _cache[key] = (time.time(), data)

app = Flask(__name__)
CORS(app)


# We no longer have a global engine/session. 
# We get it dynamically based on the requested date.
@app.route('/')
def index():
    return render_template('index.html')

from notifications import get_telegram_status, set_telegram_status
import zerodha_trader

@app.route('/api/telegram_status', methods=['GET', 'POST'])
def telegram_status():
    if request.method == 'GET':
        return jsonify({'enabled': get_telegram_status()})
    
    if request.method == 'POST':
        data = request.json
        if 'enabled' in data:
            set_telegram_status(bool(data['enabled']))
            return jsonify({'success': True, 'enabled': get_telegram_status()})
        return jsonify({'error': 'Missing "enabled" field'}), 400

@app.route('/api/trading_status', methods=['GET', 'POST'])
def trading_status():
    if request.method == 'GET':
        return jsonify({'enabled': zerodha_trader.trader.get_trading_status()})
    
    if request.method == 'POST':
        data = request.json
        if 'enabled' in data:
            zerodha_trader.trader.set_trading_status(bool(data['enabled']))
            return jsonify({'success': True, 'enabled': zerodha_trader.trader.get_trading_status()})
        return jsonify({'error': 'Missing "enabled" field'}), 400

@app.route('/api/expiries')
def get_expiries():
    """Returns list of expiries from expiries.json"""
    try:
        with open("expiries.json", "r") as f:
            return jsonify(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return jsonify({})

@app.route('/api/trading_config', methods=['GET', 'POST'])
def trading_config_endpoint():
    """Endpoint for Trading Config (Expiry Selection)"""
    state = zerodha_trader.trader.load_trading_state()
    
    if request.method == 'GET':
        return jsonify(state.get("config", {
            "NIFTY_EXPIRY": None,
            "BANKNIFTY_EXPIRY": None
        }))
        
    if request.method == 'POST':
        new_config = request.json
        state = zerodha_trader.trader.load_trading_state()
        if "config" not in state: state["config"] = {}
        
        # Update config fields
        if "NIFTY_EXPIRY" in new_config:
            state["config"]["NIFTY_EXPIRY"] = new_config["NIFTY_EXPIRY"]
        if "BANKNIFTY_EXPIRY" in new_config:
            state["config"]["BANKNIFTY_EXPIRY"] = new_config["BANKNIFTY_EXPIRY"]
            
        if "BANKNIFTY_EXPIRY" in new_config:
            state["config"]["BANKNIFTY_EXPIRY"] = new_config["BANKNIFTY_EXPIRY"]
            
        zerodha_trader.trader.save_trading_state(state)
        return jsonify({"success": True, "config": state["config"]})

@app.route('/api/credentials', methods=['GET', 'POST'])
def credentials_endpoint():
    """Endpoint to update Zerodha Credentials"""
    
    if request.method == 'GET':
        # Return masked credentials
        # We read from zerodha_trader instance to get current loaded ones
        api_key = zerodha_trader.trader.api_key or ""
        token = zerodha_trader.trader.access_token or ""
        
        # Masking
        masked_key = f"{api_key[:4]}****{api_key[-4:]}" if len(api_key) > 8 else "****"
        masked_token = f"{token[:4]}****{token[-4:]}" if len(token) > 8 else "****"
        
        return jsonify({
            "api_key": masked_key,
            "access_token": masked_token,
            "is_set": bool(api_key and token)
        })

    if request.method == 'POST':
        data = request.json
        api_key = data.get("api_key")
        access_token = data.get("access_token") # Can be empty if just saving secret
        api_secret = data.get("api_secret")
        
        if not api_key:
            return jsonify({"error": "API Key is required"}), 400
            
        print(f"DEBUG: Receiving creds update. Key: {api_key}, Token present: {bool(access_token)}, Secret present: {bool(api_secret)}")
        success = zerodha_trader.trader.update_credentials(api_key, access_token, api_secret)
        if success:
            return jsonify({"success": True, "message": "Credentials updated."})
        else:
            return jsonify({"error": "Failed to update credentials"}), 500

@app.route('/api/login')
def login_redirect():
    """Redirects to Zerodha Login URL"""
    login_url = zerodha_trader.trader.get_login_url()
    if login_url:
        return jsonify({"login_url": login_url})
    else:
        return jsonify({"error": "Could not generate login URL. Check API Key."}), 500

@app.route('/api/callback')
def callback():
    """Handles callback from Zerodha"""
    request_token = request.args.get('request_token')
    status = request.args.get('status')
    
    if status != 'success' or not request_token:
        # Return error page that closes itself or shows msg
        return "<h3>Login Failed or Cancelled.</h3><script>setTimeout(window.close, 3000);</script>"

    # Use the secret from config (or memory if we cache it, but best from config as user entered it)
    # Problem: api_secret is needed. We assume user entered it in UI and it's saved in config BEFORE clicking login.
    # UI Workflow: User enters Key + Secret -> Clicks Save -> Clicks Login.
    
    # Reload config to get latest secret
    zerodha_trader.trader.load_config()
    api_secret = zerodha_trader.trader.api_secret
    
    if not api_secret:
        return "<h3>API Secret missing in config. Please enter Secret in Settings first.</h3>"
    
    success, msg = zerodha_trader.trader.generate_session(request_token, api_secret)
    
    if success:
        # Success Page that communicates with Parent
        return """
        <html>
        <body style="background: #1e293b; color: #fff; font-family: sans-serif; text-align: center; padding-top: 50px;">
            <h2>Login Successful!</h2>
            <p>You can close this window now.</p>
            <script>
                // Send message to parent
                if (window.opener) {
                    window.opener.postMessage('zerodha_login_success', '*');
                    window.close();
                } else {
                    document.write("Session generated. Return to the dashboard.");
                }
            </script>
        </body>
        </html>
        """
    else:
        return f"<h3>Login Failed: {msg}</h3>"

@app.route('/api/execute_trade', methods=['POST'])
def execute_trade_endpoint():
    """Manual 1-Click Execution Trigger from Frontend"""
    data = request.json
    symbol = data.get('symbol')
    action = data.get('action') # "BUY", "SELL" etc
    atm = data.get('atm')
    
    if not symbol or not atm:
         return jsonify({"error": "Symbol and ATM strike are required"}), 400
         
    # Mock signal formatting expected by zerodha module
    # The frontend just tells us it's a strongly bearish or bullish signal via action string.
    signal_str = action
    
    try:
        from zerodha_trader import trader
        from models import get_db_engine, OptionChainData
        from sqlalchemy.orm import sessionmaker
        
        # Get active expiry and option price
        dates_list = sorted([f.replace("option_chain_", "").replace(".db", "") for f in os.listdir("data") if f.startswith("option_chain_") and f.endswith(".db")], reverse=True)
        expiry_date = dates_list[0] if dates_list else None
        
        option_price = 0
        if expiry_date:
            engine = get_db_engine(expiry_date)
            Session = sessionmaker(bind=engine)
            session = Session()
            try:
                # Find the latest price for this strike
                record = session.query(OptionChainData).filter(
                    OptionChainData.symbol == symbol,
                    OptionChainData.strike_price == float(atm)
                ).order_by(OptionChainData.timestamp.desc()).first()
                if record:
                    # We are always selling, if bearish -> SELL CE, if bullish -> SELL PE
                    if "SELL CALL" in signal_str or "BUY PUT" in signal_str or "Bearish" in signal_str:
                         option_price = record.ce_last_price or 0
                    elif "SELL PUT" in signal_str or "BUY CALL" in signal_str or "Bullish" in signal_str:
                         option_price = record.pe_last_price or 0
            finally:
                session.close()
                
        signal_data = {
            'signal': signal_str,
            'atm': float(atm),
            'atm_option_price': option_price # Pass realistic price for Journal tracking
        }
        
        was_enabled = trader.get_trading_status()
        if not was_enabled: trader.set_trading_status(True)
        trader.execute_trade(signal_data, symbol, "AUTO")
        if not was_enabled: trader.set_trading_status(False) # Restore
            
        return jsonify({"success": True, "message": f"Execution request sent for {symbol} {action} @ {option_price}"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/close_position', methods=['POST'])
def close_position_endpoint():
    """Manually closes a paper position"""
    try:
        from zerodha_trader import trader
        data = request.json
        symbol = data.get('symbol')
        if not symbol: return jsonify({"error": "Symbol missing"}), 400
        trader.exit_position(symbol)
        return jsonify({"success": True, "message": f"Closed position for {symbol}"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/trading_state', methods=['GET'])
def get_trading_state():
    """Returns the current trading state and positions"""
    try:
        from zerodha_trader import trader
        state = trader.load_trading_state()
        return jsonify(state)
    except Exception as e:
         return jsonify({"error": str(e)}), 500

@app.route('/api/trading_state/toggle', methods=['POST'])
def toggle_trading_config():
    """Toggles Paper Trading or Live Auto Trading"""
    try:
        from zerodha_trader import trader
        data = request.json
        state = trader.load_trading_state()
        
        if 'paper_trading' in data:
             state['paper_trading'] = data['paper_trading']
             
        if 'trading_enabled' in data:
             state['trading_enabled'] = data['trading_enabled']
             
        trader.save_trading_state(state)
        return jsonify({"success": True, "state": state})
    except Exception as e:
         return jsonify({"error": str(e)}), 500

@app.route('/api/dates')
def get_available_dates():
    """Returns a list of available dates based on DB files."""
    data_dir = "data"
    if not os.path.exists(data_dir):
        return jsonify([])
    
    files = [f for f in os.listdir(data_dir) if f.startswith("option_chain_") and f.endswith(".db")]
    dates = []
    for f in files:
        try:
            # Extract date from filename: option_chain_YYYY-MM-DD.db
            date_str = f.replace("option_chain_", "").replace(".db", "")
            # Validate format
            datetime.strptime(date_str, '%Y-%m-%d')
            dates.append(date_str)
        except ValueError:
            continue
            
    # Sort dates descending (newest first)
    dates.sort(reverse=True)
    return jsonify(dates)

@app.route('/api/timestamps')
def get_timestamps():
    symbol = request.args.get('symbol', 'NIFTY')
    date_str = request.args.get('date') # Optional: YYYY-MM-DD
    
    engine = get_db_engine(date_str)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        timestamps = session.query(OptionChainData.timestamp)\
            .filter(OptionChainData.symbol == symbol)\
            .distinct()\
            .order_by(OptionChainData.timestamp)\
            .all()
        return jsonify([t[0].strftime('%Y-%m-%d %H:%M:%S') for t in timestamps])
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@app.route('/api/data')
def get_data():
    symbol = request.args.get('symbol', 'NIFTY')
    timestamp_str = request.args.get('timestamp')
    date_str = request.args.get('date') # Optional
    
    if not timestamp_str:
        return jsonify({'error': 'Timestamp is required'}), 400

    engine = get_db_engine(date_str)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        records = session.query(OptionChainData)\
            .filter(OptionChainData.symbol == symbol, OptionChainData.timestamp == timestamp)\
            .all()
        
        result = []
        for row in records:
            result.append({
                'strike_price': row.strike_price,
                'expiry_date': row.expiry_date,
                'underlying_price': row.underlying_price,
                'ce': {
                    'last_price': row.ce_last_price,
                    'change': row.ce_change,
                    'oi': row.ce_oi,
                    'change_oi': row.ce_change_oi,
                    'volume': row.ce_volume,
                    'iv': row.ce_iv
                },
                'pe': {
                    'last_price': row.pe_last_price,
                    'change': row.pe_change,
                    'oi': row.pe_oi,
                    'change_oi': row.pe_change_oi,
                    'volume': row.pe_volume,
                    'iv': row.pe_iv
                }
            })
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@app.route('/api/spots')
def get_latest_spots():
    """Returns the latest spot price for all indices. Defaults to latest DB file."""
    symbols = ["NIFTY", "BANKNIFTY"]
    response = {}
    
    date_str = request.args.get('date')
    
    if not date_str:
        # Find latest DB file
        data_dir = "data"
        if os.path.exists(data_dir):
            files = [f for f in os.listdir(data_dir) if f.startswith("option_chain_") and f.endswith(".db")]
            dates = []
            for f in files:
                try:
                    d_str = f.replace("option_chain_", "").replace(".db", "")
                    datetime.strptime(d_str, '%Y-%m-%d')
                    dates.append(d_str)
                except:
                    continue
            if dates:
                dates.sort(reverse=True)
                date_str = dates[0]
    
    engine = get_db_engine(date_str) # Handles None gracefully (today) but we prefer latest valid
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        for symbol in symbols:
            # Get latest record for symbol
            record = session.query(OptionChainData)\
                .filter(OptionChainData.symbol == symbol)\
                .order_by(OptionChainData.timestamp.desc())\
                .first()
            
            if record:
                response[symbol] = {
                    'price': record.underlying_price,
                    'timestamp': record.timestamp.strftime('%H:%M:%S')
                }
            else:
                response[symbol] = {
                    'price': "-",
                    'timestamp': "-"
                }
                
        return jsonify(response)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

def calculate_max_pain(session, symbol, timestamp):
    """Calculates Max Pain for a specific symbol and timestamp."""
    # This involves fetching all strikes for that timestamp
    rows = session.query(OptionChainData.strike_price, OptionChainData.ce_oi, OptionChainData.pe_oi)\
        .filter(OptionChainData.symbol == symbol, OptionChainData.timestamp == timestamp)\
        .all()
    
    if not rows:
        return 0

    strikes = [r.strike_price for r in rows]
    # Simple algorithm: For each strike, calculate total loss for option writers
    min_loss = float('inf')
    max_pain_strike = 0
    
    for strike_price in strikes:
        total_loss = 0
        for r in rows:
            # Call Writers Loss: if price expires at 'strike_price', calls < strike_price are in ITM?
            # Wait, if Market expires at S_expiry:
            # Call writer loss (ITM) = Max(0, S_expiry - K) * OI_call
            # Put writer loss (ITM) = Max(0, K - S_expiry) * OI_put
            
            # Here 'strike_price' is the simulated expiry price
            
            call_loss = max(0, strike_price - r.strike_price) * r.ce_oi
            put_loss = max(0, r.strike_price - strike_price) * r.pe_oi
            total_loss += call_loss + put_loss
            
        if total_loss < min_loss:
            min_loss = total_loss
            max_pain_strike = strike_price
            
    return max_pain_strike

@app.route('/api/oi_stats')
def get_oi_stats():
    symbol = request.args.get('symbol', 'NIFTY')
    date_str = request.args.get('date')  # Optional
    skip_max_pain = request.args.get('skip_max_pain', 'false').lower() == 'true'

    cache_key = f'oi_stats:{symbol}:{date_str}:{skip_max_pain}'
    cached = _cache_get(cache_key)
    if cached:
        return jsonify(cached)
    
    engine = get_db_engine(date_str)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        from sqlalchemy import func
        grouped_data = session.query(
            OptionChainData.timestamp,
            func.sum(OptionChainData.ce_oi).label('total_ce_oi'),
            func.sum(OptionChainData.pe_oi).label('total_pe_oi'),
            func.sum(OptionChainData.ce_change_oi).label('total_ce_change_oi'),
            func.sum(OptionChainData.pe_change_oi).label('total_pe_change_oi'),
            func.max(OptionChainData.underlying_price).label('spot')
        ).filter(OptionChainData.symbol == symbol)\
         .filter(func.time(OptionChainData.timestamp) >= '09:00:00')\
         .filter(func.time(OptionChainData.timestamp) <= '15:30:00')\
         .group_by(OptionChainData.timestamp)\
         .order_by(OptionChainData.timestamp)\
         .all()

        response_data = []
        for row in grouped_data:
            ts = row.timestamp
            ce_oi = row.total_ce_oi or 0
            pe_oi = row.total_pe_oi or 0
            pcr = round(pe_oi / ce_oi, 2) if ce_oi > 0 else 0
            
            ts_str = ts.strftime('%H:%M:%S') if hasattr(ts, 'strftime') else str(ts)[11:19]
            full_ts_str = str(ts)
            
            mp = 0
            if not skip_max_pain:
                mp_key = f"{symbol}_{full_ts_str}"
                if mp_key in _max_pain_cache:
                    mp = _max_pain_cache[mp_key]
                else:
                    mp = calculate_max_pain(session, symbol, ts)
                    _max_pain_cache[mp_key] = mp
            
            response_data.append({
                'timestamp': ts_str,
                'ce_oi': ce_oi,
                'pe_oi': pe_oi,
                'ce_change_oi': row.total_ce_change_oi or 0,
                'pe_change_oi': row.total_pe_change_oi or 0,
                'pcr': pcr,
                'spot': row.spot,
                'max_pain': mp
            })

        _cache_set(cache_key, response_data)
        return jsonify(response_data)
    except Exception as e:
        print(f"Error in get_oi_stats: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@app.route('/api/signals')
def get_signals():
    date_str = request.args.get('date')
    timestamp_str = request.args.get('timestamp')
    
    if not date_str or not timestamp_str:
        return jsonify({'error': 'Date and Timestamp required'}), 400

    engine = get_db_engine(date_str)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    response = {}
    symbols = ["NIFTY", "BANKNIFTY"]
    
    from datetime import timedelta
    
    try:
        target_timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        
        for symbol in symbols:
            # Find the closest record within a +/- 60 second window
            time_window = timedelta(seconds=60)
            
            # Subquery or logic to find nearest
            # We fetch all records in range and pick best one in Python for simplicity
            records = session.query(OptionChainData)\
                .filter(
                    OptionChainData.symbol == symbol, 
                    OptionChainData.timestamp >= target_timestamp - time_window,
                    OptionChainData.timestamp <= target_timestamp + time_window
                )\
                .all()
            
            if not records:
                response[symbol] = {'signal': 'No Data', 'color': 'gray'}
                continue
            
            # Select the batch (timestamp) closest to target
            # Group records by timestamp first? 
            # Actually, OptionChainData has one row per strike per timestamp/symbol.
            # We want to identify the "timestamp group" closest to target.
            
            unique_timestamps = list(set([r.timestamp for r in records]))
            closest_ts = min(unique_timestamps, key=lambda t: abs(t - target_timestamp))
            
            # Now filter records for ONLY that closest timestamp
            symbol_records = [r for r in records if r.timestamp == closest_ts]
            
            # Find ATM use closest_ts records
            if not symbol_records:
                 response[symbol] = {'signal': 'No Data', 'color': 'gray'}
                 continue

            spot_price = symbol_records[0].underlying_price
            atm_record = min(symbol_records, key=lambda x: abs(x.strike_price - spot_price))
            
            # Logic for Signals (Focused on "Selling" - Short Buildup)
            # Short Buildup: Price Down (< 0), OI Up (> 0) => RED color in table
            
            # Check CE (Call)
            ce_signal = ""
            if atm_record.ce_change < 0 and atm_record.ce_change_oi > 0:
                ce_signal = "SELL CALL" # Bearish
            
            # Check PE (Put)
            pe_signal = ""
            if atm_record.pe_change < 0 and atm_record.pe_change_oi > 0:
                pe_signal = "SELL PUT" # Bullish
                
            # Conflict Resolution / Priority
            # If both are Sell, it's a "Short Strangle/Straddle" idea, but simplistic view:
            final_signal = "WAIT"
            color = "gray"
            
            if ce_signal and not pe_signal:
                final_signal = "SELL CE (Bearish)"
                color = "red" # Market going down
            elif pe_signal and not ce_signal:
                final_signal = "SELL PE (Bullish)"
                color = "green" # Market going up
            elif ce_signal and pe_signal:
                final_signal = "SELL BOTH (Range)"
                color = "orange"
            else:
                # Secondary Logic: Long Buildup (Green in table)
                # CE Long Buildup: Price > 0, OI > 0 -> BUY CE (Bullish)
                # PE Long Buildup: Price > 0, OI > 0 -> BUY PE (Bearish)
                
                is_ce_long = atm_record.ce_change > 0 and atm_record.ce_change_oi > 0
                is_pe_long = atm_record.pe_change > 0 and atm_record.pe_change_oi > 0
                
                if is_ce_long and not is_pe_long:
                    final_signal = "BUY CE (Bullish)"
                    color = "green"
                elif is_pe_long and not is_ce_long:
                    final_signal = "BUY PE (Bearish)"
                    color = "red"
                else:
                    final_signal = "NEUTRAL"
                    color = "#94a3b8"

            response[symbol] = {
                'signal': final_signal,
                'color': color,
                'spot': spot_price,
                'atm': atm_record.strike_price
            }
            
        return jsonify(response)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@app.route('/api/quick_summary')
def quick_summary():
    """Single fast endpoint: returns spots + latest signals + PCR for both symbols.
    Designed to load in < 1 second for the dashboard header."""
    date_str = request.args.get('date')
    symbols = ['NIFTY', 'BANKNIFTY']
    result = {}

    # Find latest date if not provided
    if not date_str:
        data_dir = 'data'
        if os.path.exists(data_dir):
            files = [f for f in os.listdir(data_dir)
                     if f.startswith('option_chain_') and f.endswith('.db')]
            dates = []
            for f in files:
                try:
                    d = f.replace('option_chain_', '').replace('.db', '')
                    datetime.strptime(d, '%Y-%m-%d')
                    dates.append(d)
                except:
                    continue
            if dates:
                dates.sort(reverse=True)
                date_str = dates[0]

    cache_key = f'quick_summary:{date_str}'
    cached = _cache_get(cache_key)
    if cached:
        return jsonify(cached)

    engine = get_db_engine(date_str)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        from sqlalchemy import func

        for sym in symbols:
            # Latest record
            latest = (session.query(OptionChainData)
                      .filter(OptionChainData.symbol == sym)
                      .order_by(OptionChainData.timestamp.desc())
                      .first())

            if not latest:
                result[sym] = {'spot': None, 'signal': 'No Data', 'color': 'gray',
                               'atm': None, 'ce_oi': 0, 'pe_oi': 0, 'pcr': 0}
                continue

            spot_price = latest.underlying_price
            ts = latest.timestamp

            # All strikes at latest timestamp
            rows = (session.query(OptionChainData)
                    .filter(OptionChainData.symbol == sym,
                            OptionChainData.timestamp == ts)
                    .all())

            total_ce_oi = sum(r.ce_oi or 0 for r in rows)
            total_pe_oi = sum(r.pe_oi or 0 for r in rows)
            pcr = round(total_pe_oi / total_ce_oi, 2) if total_ce_oi > 0 else 0

            # ATM strike & signal
            atm = min(rows, key=lambda x: abs(x.strike_price - spot_price), default=None)
            signal, color = 'NEUTRAL', '#94a3b8'

            if atm:
                ce_short = atm.ce_change < 0 and atm.ce_change_oi > 0
                pe_short = atm.pe_change < 0 and atm.pe_change_oi > 0
                ce_long  = atm.ce_change > 0 and atm.ce_change_oi > 0
                pe_long  = atm.pe_change > 0 and atm.pe_change_oi > 0

                if ce_short and not pe_short:     signal, color = 'SELL CE (Bearish)', 'red'
                elif pe_short and not ce_short:   signal, color = 'SELL PE (Bullish)', 'green'
                elif ce_short and pe_short:       signal, color = 'SELL BOTH (Range)', 'orange'
                elif ce_long and not pe_long:     signal, color = 'BUY CE (Bullish)',  'green'
                elif pe_long and not ce_long:     signal, color = 'BUY PE (Bearish)',  'red'

            # Calculate Confluence & Dynamic Exit Alerts based on 5-minute momentum
            confluence = 50
            exit_alert = None
            
            if atm and signal != 'NEUTRAL' and signal != 'No Data':
                try:
                    from datetime import timedelta
                    past_time = ts - timedelta(minutes=5)
                    past_record = session.query(OptionChainData.underlying_price).filter(
                        OptionChainData.symbol == sym,
                        OptionChainData.timestamp <= past_time
                    ).order_by(OptionChainData.timestamp.desc()).first()
                    
                    trend_5m = 0
                    if past_record:
                        trend_5m = spot_price - past_record[0]
                        
                    # Base score based on PCR alignment
                    base = 65
                    if 'Bullish' in signal:
                        if pcr > 1.1: base += 15
                        elif pcr > 0.9: base += 5
                        if trend_5m > 0: base += 20 # Follows trend
                        confluence = min(99, base)
                        
                        # Exit Alert: Bullish signal but market is suddenly dumping (-10 points)
                        if trend_5m < -10:
                            exit_alert = "CLOSE CALLS (Trend Reversal)"
                            
                    elif 'Bearish' in signal:
                        if pcr < 0.8: base += 15
                        elif pcr < 1.0: base += 5
                        if trend_5m < 0: base += 20 # Follows trend
                        confluence = min(99, base)
                        
                        # Exit Alert: Bearish signal but market is suddenly rallying (+10 points)
                        if trend_5m > 10:
                            exit_alert = "CLOSE PUTS (Trend Reversal)"
                except:
                    pass

            result[sym] = {
                'spot':      spot_price,
                'timestamp': ts.strftime('%H:%M:%S'),
                'signal':    signal,
                'color':     color,
                'atm':       atm.strike_price if atm else None,
                'ce_oi':     total_ce_oi,
                'pe_oi':     total_pe_oi,
                'pcr':       pcr,
                'confluence': confluence,
                'exit_alert': exit_alert
            }
            
            # Auto Trading & PnL Updates
            try:
                from zerodha_trader import trader
                
                # Fetch realistic option price for the ATM string
                opt_price = 0
                if atm and ('BUY' in signal or 'SELL' in signal) and 'BOTH' not in signal:
                     if "CE" in signal or "CALL" in signal or "Bearish" in signal:
                         opt_price = atm.ce_last_price or 0
                     elif "PE" in signal or "PUT" in signal or "Bullish" in signal:
                         opt_price = atm.pe_last_price or 0
                         
                signal_data = {
                    'signal': signal,
                    'atm': atm.strike_price if atm else 0,
                    'atm_option_price': opt_price
                }
                
                # Update P&L if we have an active position
                state = trader.load_trading_state()
                pos = state.get("positions", {}).get(sym)
                
                if pos:
                    # Determine current option price of the held position
                    current_held_price = 0
                    if pos['type'] == 'CE':
                        held_record = next((r for r in rows if r.strike_price == pos['strike']), None)
                        current_held_price = held_record.ce_last_price if held_record else 0
                    elif pos['type'] == 'PE':
                        held_record = next((r for r in rows if r.strike_price == pos['strike']), None)
                        current_held_price = held_record.pe_last_price if held_record else 0
                        
                    if current_held_price > 0:
                        trader.update_pnl(sym, current_held_price)
                        # Re-load state in case update_pnl closed it
                        state = trader.load_trading_state()
                        pos = state.get("positions", {}).get(sym)
                        
                    # Auto Close if exit_alert
                    if exit_alert and pos:
                        trader.exit_position(sym)
                        
                # Auto Execution Logic
                is_auto = trader.get_trading_status()
                if is_auto and not pos and signal != 'NEUTRAL' and signal != 'No Data' and signal != 'WAIT':
                     # Execute automatically if confluence is high
                     if confluence >= 70:
                         trader.execute_trade(signal_data, sym, "AUTO")
                         
            except Exception as e:
                print("Error in AutoTrade/PnL calculation:", e)

        _cache_set(cache_key, result)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

