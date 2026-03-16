import schedule
import time
import os
import json
import pandas as pd
from datetime import datetime, timedelta, date
import calendar
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, text
from sqlalchemy.orm import declarative_base, sessionmaker
from curl_cffi import requests
import market_signals
import zerodha_trader

# --- Configuration ---
INDICES = ["NIFTY", "BANKNIFTY"]
DATA_DIR = "data"
EXPIRIES_FILE = "expiries.json"
LINKS_FILE = "nse_links.txt"

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# Database Setup
Base = declarative_base()

class OptionChainData(Base):
    __tablename__ = 'option_chain_data'
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    symbol = Column(String)
    expiry_date = Column(String)
    strike_price = Column(Float)
    underlying_price = Column(Float)
    
    # CE Data
    ce_last_price = Column(Float)
    ce_change = Column(Float)
    ce_oi = Column(Float)
    ce_change_oi = Column(Float)
    ce_volume = Column(Float)
    ce_iv = Column(Float)
    
    # PE Data
    pe_last_price = Column(Float)
    pe_change = Column(Float)
    pe_oi = Column(Float)
    pe_change_oi = Column(Float)
    pe_volume = Column(Float)
    pe_iv = Column(Float)

# Global cache for engine
_cached_engine = None
_cached_date = None

def get_db_engine(date_str=None):
    """Returns a DB engine for the specified date (YYYY-MM-DD). Defaults to today."""
    global _cached_engine, _cached_date
    
    if date_str is None:
        date_str = datetime.now().strftime('%Y-%m-%d')
    
    if _cached_engine is not None and _cached_date == date_str:
        return _cached_engine

    db_path = os.path.join(DATA_DIR, f"option_chain_{date_str}.db")
    engine = create_engine(f'sqlite:///{db_path}', echo=False, connect_args={'timeout': 10})
    Base.metadata.create_all(engine)
    
    # Migration check
    with engine.connect() as conn:
        try:
            result = conn.execute(text("PRAGMA table_info(option_chain_data)")).fetchall()
            columns = [row[1] for row in result]
            if 'underlying_price' not in columns:
                print(f"Migrating {db_path}: Adding underlying_price column...")
                conn.execute(text("ALTER TABLE option_chain_data ADD COLUMN underlying_price FLOAT"))
        except Exception as e:
            print(f"Migration check failed for {db_path}: {e}")
    
    _cached_engine = engine
    _cached_date = date_str
            
    return engine

class NSEFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
        self.initialized = False

    def initialize_session(self):
        try:
            print("Initializing NSE Session...", flush=True)
            self.session.get("https://www.nseindia.com", impersonate="chrome110", timeout=15)
            time.sleep(1)
            self.session.get("https://www.nseindia.com/option-chain", impersonate="chrome110", timeout=15)
            self.initialized = True
            return True
        except Exception as e:
            print(f"Session initialization failed: {e}", flush=True)
            return False

    def fetch_data(self, url):
        if not self.initialized:
            self.initialize_session()
            
        api_headers = self.headers.copy()
        api_headers["Referer"] = "https://www.nseindia.com/option-chain"
        api_headers["X-Requested-With"] = "XMLHttpRequest"
        
        try:
            response = self.session.get(url, impersonate="chrome110", headers=api_headers, timeout=10)
            if response.status_code == 401 or response.status_code == 403:
                self.initialize_session()
                response = self.session.get(url, impersonate="chrome110", headers=api_headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data and "records" in data:
                    return data
            
            print(f"Fetch failed for {url}. Status: {response.status_code}", flush=True)
            return None
        except Exception as e:
            print(f"Error fetching {url}: {e}", flush=True)
            return None

nse_fetcher = NSEFetcher()

def load_links():
    links = {}
    if os.path.exists(LINKS_FILE):
        try:
            with open(LINKS_FILE, "r") as f:
                for line in f:
                    if "=" in line and not line.startswith("#"):
                        k, v = line.split("=", 1)
                        links[k.strip()] = v.strip()
        except Exception as e:
            print(f"Error loading {LINKS_FILE}: {e}")
    return links

def get_target_expiry(symbol, available_expiries):
    """
    Determines the target expiry date based on user requirements:
    - NIFTY: Weekly (Tuesday)
    - BANKNIFTY: Monthly (Last Tuesday of the month)
    """
    today = datetime.now().date()
    expiry_dates = []
    for d in available_expiries:
        try:
            dt = datetime.strptime(d, "%d-%b-%Y").date()
            if dt >= today:
                expiry_dates.append((dt, d))
        except: continue
    
    expiry_dates.sort()
    if not expiry_dates: return None

    if symbol == "NIFTY":
        # Rule: Next Tuesday
        for dt, d_str in expiry_dates:
            if dt.weekday() == 1: # Tuesday
                return d_str
        return expiry_dates[0][1]

    elif symbol == "BANKNIFTY":
        # Rule: Last Tuesday of the month
        # Use first available expiry's month to find target
        first_dt = expiry_dates[0][0]
        curr_m, curr_y = first_dt.month, first_dt.year
        
        # Filter all expiries for this month
        month_expiries = [x for x in expiry_dates if x[0].month == curr_m and x[0].year == curr_y]
        
        # Find the last Tuesday available in this month
        last_tue = None
        for dt, d_str in reversed(month_expiries):
            if dt.weekday() == 1:
                last_tue = d_str
                break
        
        if last_tue: return last_tue
        
        # Fallback to the last expiry of the month (most common Monthly settlement)
        return month_expiries[-1][1]

    return expiry_dates[0][1]

def save_expiries(symbol, data):
    try:
        if "records" in data and "expiryDates" in data["records"]:
            new_expiries = data["records"]["expiryDates"]
            current_data = {}
            if os.path.exists(EXPIRIES_FILE):
                try:
                    with open(EXPIRIES_FILE, 'r') as f:
                        current_data = json.load(f)
                except: pass
            current_data[symbol] = new_expiries
            with open(EXPIRIES_FILE, 'w') as f:
                json.dump(current_data, f, indent=4)
    except Exception as e:
        print(f"Error saving expiries for {symbol}: {e}")

def process_data(data, expiry_date):
    records = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        # Check in records.data
        all_data = data["records"]["data"]
        if not all_data:
            return pd.DataFrame()
            
        for item in all_data:
            # The job function should ensure we only fetch data for the target expiry,
            # so this check might be redundant but kept for safety.
            if "expiryDate" in item and item["expiryDate"] != expiry_date:
                continue
                
            record = {
                "Timestamp": timestamp,
                "ExpiryDate": expiry_date,
                "StrikePrice": item["strikePrice"],
                "UnderlyingPrice": data["records"].get("underlyingValue", 0)
            }
                
            if "CE" in item:
                ce = item["CE"]
                record.update({
                    "CE_LastPrice": ce.get("lastPrice", 0),
                    "CE_Change": ce.get("change", 0),
                    "CE_OI": ce.get("openInterest", 0),
                    "CE_ChangeInOI": ce.get("changeinOpenInterest", 0),
                    "CE_Volume": ce.get("totalTradedVolume", 0),
                    "CE_IV": ce.get("impliedVolatility", 0),
                })
            else:
                record.update({
                    "CE_LastPrice": 0, "CE_Change": 0, "CE_OI": 0, 
                    "CE_ChangeInOI": 0, "CE_Volume": 0, "CE_IV": 0
                })

            if "PE" in item:
                pe = item["PE"]
                record.update({
                    "PE_LastPrice": pe.get("lastPrice", 0),
                    "PE_Change": pe.get("change", 0),
                    "PE_OI": pe.get("openInterest", 0),
                    "PE_ChangeInOI": pe.get("changeinOpenInterest", 0),
                    "PE_Volume": pe.get("totalTradedVolume", 0),
                    "PE_IV": pe.get("impliedVolatility", 0),
                })
            else:
                record.update({
                    "PE_LastPrice": 0, "PE_Change": 0, "PE_OI": 0, 
                    "PE_ChangeInOI": 0, "PE_Volume": 0, "PE_IV": 0
                })
            
            records.append(record)
        
        return pd.DataFrame(records)
    except Exception as e:
        print(f"Error processing data: {e}", flush=True)
        return pd.DataFrame()

def save_data(df, symbol):
    if df.empty: return
    session = None
    try:
        engine = get_db_engine()
        Session = sessionmaker(bind=engine)
        session = Session()
        records = []
        for _, row in df.iterrows():
            record = OptionChainData(
                timestamp=datetime.strptime(row['Timestamp'], "%Y-%m-%d %H:%M:%S"),
                symbol=symbol, expiry_date=row['ExpiryDate'],
                strike_price=row['StrikePrice'], underlying_price=row['UnderlyingPrice'],
                ce_last_price=row['CE_LastPrice'], ce_change=row['CE_Change'],
                ce_oi=row['CE_OI'], ce_change_oi=row['CE_ChangeInOI'],
                ce_volume=row['CE_Volume'], ce_iv=row['CE_IV'],
                pe_last_price=row['PE_LastPrice'], pe_change=row['PE_Change'],
                pe_oi=row['PE_OI'], pe_change_oi=row['PE_ChangeInOI'],
                pe_volume=row['PE_Volume'], pe_iv=row['PE_IV']
            )
            records.append(record)
        session.add_all(records)
        session.commit()
    except Exception as e:
        print(f"Error saving data for {symbol}: {e}")
        if session: session.rollback()
    finally:
        if session: session.close()

def is_market_open():
    now = datetime.now()
    if now.weekday() > 4: return False
    current_time = now.time()
    start_time = datetime.strptime("09:00", "%H:%M").time()
    end_time = datetime.strptime("15:30", "%H:%M").time()
    return start_time <= current_time <= end_time

def job(force=False):
    if not force and not is_market_open():
        print(f"Market closed. Skipping at {datetime.now().strftime('%H:%M:%S')}")
        return

    try:
        print(f"--- Starting Fetch Job at {datetime.now().strftime('%H:%M:%S')} ---", flush=True)
        links = load_links()

        for symbol in INDICES:
            print(f"[{symbol}] Initial fetch from links.txt...", flush=True)
            initial_url = links.get(symbol)
            if not initial_url:
                initial_url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
            
            data = nse_fetcher.fetch_data(initial_url)
            
            # --- Fallback: Scan future dates to pierce the WAF ---
            if not data or "records" not in data:
                print(f"[{symbol}] Root API returned empty data. Scanning for valid proxy expiry date...", flush=True)
                today = datetime.now()
                valid_data = None
                
                for i in range(30):
                    d_str = (today + timedelta(days=i)).strftime('%d-%b-%Y')
                    test_url = f"https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol={symbol}&expiry={d_str}"
                    try:
                        # Use nse_fetcher to ensure headers are copied/consistent
                        r_data = nse_fetcher.fetch_data(test_url)
                        if r_data and 'records' in r_data and 'expiryDates' in r_data['records']:
                            print(f"[{symbol}] Scanner found valid date: {d_str}")
                            valid_data = r_data
                            break
                    except: pass
                    
                if valid_data:
                    data = valid_data
            # ----------------------------------------------------
            if data:
                save_expiries(symbol, data)
                all_expiries = data["records"]["expiryDates"]
                target_expiry = get_target_expiry(symbol, all_expiries)
                
                print(f"[{symbol}] Target Tuesday Expiry: {target_expiry}", flush=True)
                
                # If target_expiry is different from current URL, we might need a direct fetch for that expiry
                # This check is simplified as the initial_url might not contain expiry info directly.
                # We always fetch with the target expiry for consistency.
                if target_expiry:
                    print(f"[{symbol}] Fetching specific data for {target_expiry}...", flush=True)
                    target_url = f"https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol={symbol}&expiry={target_expiry}"
                    data = nse_fetcher.fetch_data(target_url)
                else:
                    print(f"[{symbol}] Could not determine target expiry. Skipping data processing.", flush=True)
                    data = None # Ensure data is None if target_expiry is not found
                
                if data:
                    df = process_data(data, target_expiry)
                    if not df.empty:
                        save_data(df, symbol)
                        
                        # Automated Trading Logic
                        try:
                            engine = get_db_engine()
                            Session = sessionmaker(bind=engine)
                            session = Session()
                            latest_ts = datetime.strptime(df.iloc[0]['Timestamp'], "%Y-%m-%d %H:%M:%S")
                            sig_data = market_signals.calculate_signal(session, symbol, latest_ts, OptionChainData)
                            print(f"[{symbol}] Signal: {sig_data.get('signal')} ({sig_data.get('color')}) | Spot: {sig_data.get('spot')}")
                            
                            # current_time = datetime.now().time()
                            # start_time = datetime.strptime("09:25", "%H:%M").time()
                            # if current_time >= start_time:
                            #     zerodha_trader.trader.execute_trade(sig_data, symbol, target_expiry)
                            # 
                            # zerodha_trader.trader.manage_risk(symbol)
                            session.close()
                        except Exception as trade_e:
                            print(f"Trade Logic Error [{symbol}]: {trade_e}")
                    else:
                        print(f"[{symbol}] Processed data is empty.")
                else:
                    print(f"[{symbol}] Failed to fetch target data.")
            else:
                print(f"[{symbol}] Failed to initialize data from links.")
            time.sleep(2)
        print(f"--- Job Finished at {datetime.now().strftime('%H:%M:%S')} ---", flush=True)
    except Exception as e:
        print(f"CRITICAL ERROR in Job: {e}")

if __name__ == "__main__":
    print("NSE Option Chain Bot Started.")
    job(force=True)
    schedule.every(1).minutes.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)
