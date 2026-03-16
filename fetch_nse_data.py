import requests
import schedule
import time
import os
import json
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, text, Index
from sqlalchemy.orm import declarative_base, sessionmaker
import market_signals
import notifications
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# --- Configuration ---
INDICES = ["NIFTY", "BANKNIFTY"]
INDICES = ["NIFTY", "BANKNIFTY"]
DATA_DIR = "data"
EXPIRIES_FILE = "expiries.json"

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# Constants
OPTION_CHAIN_URL = "https://www.nseindia.com/api/option-chain-indices?symbol={}"

# Database Setup
Base = declarative_base()

class OptionChainData(Base):
    __tablename__ = 'option_chain_data'
    __table_args__ = (
        Index('idx_symbol_timestamp', 'symbol', 'timestamp'),
        Index('idx_expiry', 'expiry_date'),
    )
    
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

def load_links():
    links = {}
    try:
        file_path = "nse_links.txt"
        print(f"Reading links from {os.path.abspath(file_path)}", flush=True)
        with open(file_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    links[key.strip()] = value.strip()
        print(f"Loaded links: {links}", flush=True)
    except Exception as e:
        print(f"Error reading nse_links.txt: {e}")
    return links

def fetch_data(symbol):
    """Fetches option chain data for a given symbol using direct link provided by user."""
    links = load_links()
    url = links.get(symbol)
    
    if not url:
        print(f"No link found for {symbol} in nse_links.txt. Please paste the direct link.")
        return None

    print(f"Fetching {symbol} using provided link...", flush=True)
    
    # We use requests here since we assume the user provides a link that bypasses the main checks 
    # or is a direct resource that behaves differently. 
    # However, standard headers might still be needed.
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        # print(f"Response Status: {response.status_code}", flush=True)
        
        try:
            return response.json()
        except json.JSONDecodeError:
            print(f"JSON Decode Error for {symbol}!", flush=True)
            return None
    except Exception as e:
        print(f"Error fetching data: {e}", flush=True)
        return None

def get_current_expiry(data):
    try:
        expiry_dates = data["records"]["expiryDates"]
        # Make sure we select an expiry that actually has data in the 'data' array
        valid_expiries = set()
        for item in data["records"].get("data", []):
            if "expiryDate" in item:
                valid_expiries.add(item["expiryDate"])
                
        for exp in expiry_dates:
            if not valid_expiries or exp in valid_expiries:
                return exp
                
        return expiry_dates[0]
    except (KeyError, IndexError):
        return None

def save_expiries(symbol, data):
    try:
        if "records" in data and "expiryDates" in data["records"]:
            new_expiries = data["records"]["expiryDates"]
            
            # Load existing
            current_data = {}
            if os.path.exists(EXPIRIES_FILE):
                try:
                    with open(EXPIRIES_FILE, 'r') as f:
                        current_data = json.load(f)
                except:
                    pass
            
            # Update
            current_data[symbol] = new_expiries
            
            # Save
            with open(EXPIRIES_FILE, 'w') as f:
                json.dump(current_data, f, indent=4)
                
    except Exception as e:
        print(f"Error saving expiries for {symbol}: {e}")

def process_data(data, expiry_date):
    records = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Extract Index Change for Notifications
    # Usually in records.index.change or records.index.pChange
    index_change = 0
    try:
        if "index" in data["records"]:
             index_change = data["records"]["index"].get("change", 0)
        elif "underlyingValue" in data["records"]:
             # Fallback if change not explicitly explicitly provided, maybe unavailable
             pass
    except:
        pass

    try:
        if not data["records"]["data"]:
            return pd.DataFrame(), 0
            
        # Inspect first item for debugging
        # print("First item keys:", data["records"]["data"][0].keys(), flush=True)

        for item in data["records"]["data"]:
            # v3 API: items might not have 'expiryDate' if filtered by URL. 
            # If 'expiryDate' is present, check it. If not, assume it matches.
            if "expiryDate" in item and item["expiryDate"] != expiry_date:
                continue
                
            record = {
                "Timestamp": timestamp,
                "ExpiryDate": expiry_date, # Use the global expiry since item might not have it
                "StrikePrice": item["strikePrice"],
                "UnderlyingPrice": data["records"].get("underlyingValue", 0)
            }
                
            # CE Data
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

            # PE Data
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
        
        return pd.DataFrame(records), index_change
    except Exception as e:
        print(f"Error processing data: {e}", flush=True)
        return pd.DataFrame(), 0

def save_data(df, symbol):
    if df.empty:
        return

    session = None
    try:
        engine = get_db_engine()
        Session = sessionmaker(bind=engine)
        session = Session()
        
        records = []
        for _, row in df.iterrows():
            record = OptionChainData(
                timestamp=datetime.strptime(row['Timestamp'], "%Y-%m-%d %H:%M:%S"),
                symbol=symbol,
                expiry_date=row['ExpiryDate'],
                strike_price=row['StrikePrice'],
                underlying_price=row['UnderlyingPrice'],
                
                ce_last_price=row['CE_LastPrice'],
                ce_change=row['CE_Change'],
                ce_oi=row['CE_OI'],
                ce_change_oi=row['CE_ChangeInOI'],
                ce_volume=row['CE_Volume'],
                ce_iv=row['CE_IV'],
                
                pe_last_price=row['PE_LastPrice'],
                pe_change=row['PE_Change'],
                pe_oi=row['PE_OI'],
                pe_change_oi=row['PE_ChangeInOI'],
                pe_volume=row['PE_Volume'],
                pe_iv=row['PE_IV']
            )
            records.append(record)
        
        session.add_all(records)
        session.commit()
        print(f"Data saved for {symbol} to DB at {datetime.now().strftime('%H:%M:%S')}")
    except Exception as e:
        print(f"Error saving data for {symbol}: {e}")
        if session:
            session.rollback()
    finally:
        if session:
            session.close()

def is_market_open():
    """Checks if current time is within market hours (09:00 - 15:30) on weekdays."""
    now = datetime.now()
    if now.weekday() > 4:
        return False
    
    current_time = now.time()
    start_time = datetime.strptime("09:00", "%H:%M").time()
    end_time = datetime.strptime("15:30", "%H:%M").time()
    
    return start_time <= current_time <= end_time

def process_single_symbol(symbol):
    """Pipeline for a single symbol: Fetch -> Process -> Save"""
    try:
        data = fetch_data(symbol)
        if data:
            save_expiries(symbol, data) # Save full list
            expiry_date = get_current_expiry(data)
            if expiry_date:
                print(f"Fetching {symbol} data for expiry: {expiry_date}")
                df, index_change = process_data(data, expiry_date)
                save_data(df, symbol)
                
                # --- NEW: Calculate Signal and Notify ---
                try:
                    if not df.empty:
                        engine = get_db_engine()
                        Session = sessionmaker(bind=engine)
                        session = Session()
                        
                        # Convert last timestamp to datetime object
                        last_ts_str = df.iloc[-1]['Timestamp']
                        last_ts = datetime.strptime(last_ts_str, "%Y-%m-%d %H:%M:%S")
                        
                        signal_data = market_signals.calculate_signal(session, symbol, last_ts, OptionChainData)
                        notifications.check_and_send(symbol, signal_data)
                        
                        session.close()
                    else:
                        print(f"No valid data returned for {symbol} expiry {expiry_date}, skipping signal check.")
                except Exception as e:
                    print(f"Error in signal notification for {symbol}: {e}")
                # ----------------------------------------
            else:
                print(f"Could not determine expiry for {symbol}")
        time.sleep(1) # Gentle delay per thread
    except Exception as e:
        print(f"Error processing {symbol}: {e}")

from concurrent.futures import ThreadPoolExecutor

def cleanup_old_db_files():
    """Removes option chain database files older than 5 days to save space."""
    try:
        now = datetime.now()
        for f in os.listdir(DATA_DIR):
            if f.startswith("option_chain_") and f.endswith(".db"):
                date_str = f.replace("option_chain_", "").replace(".db", "")
                try:
                    file_date = datetime.strptime(date_str, "%Y-%m-%d")
                    # If file is older than 5 days, delete it
                    if (now - file_date).days > 5:
                        file_path = os.path.join(DATA_DIR, f)
                        os.remove(file_path)
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] Deleted old data file: {f}")
                except ValueError:
                    pass
    except Exception as e:
        print(f"Error cleaning up old DB files: {e}")

def job(force=False):
    """Main job to be scheduled."""
    if not force and not is_market_open():
        print(f"Market is closed. Skipping fetch at {datetime.now().strftime('%H:%M:%S')}")
        return

    try:
        print(f"Starting job at {datetime.now().strftime('%H:%M:%S')}")

        with ThreadPoolExecutor(max_workers=len(INDICES)) as executor:
            executor.map(process_single_symbol, INDICES)
    except Exception as e:
        print(f"CRITICAL ERROR in job: {e}")

if __name__ == "__main__":
    print("Starting NSE Option Chain Data Fetcher (SQL Storage)...")
    # Cleanup old DB files at startup
    cleanup_old_db_files()
    
    # Run once immediately (forced)
    job(force=True)
    
    # Schedule every 1 minute for data fetching
    schedule.every(1).minutes.do(job)
    # Schedule cleanup once a day at midnight
    schedule.every().day.at("00:00").do(cleanup_old_db_files)
    
    while True:
        schedule.run_pending()
        time.sleep(1)
