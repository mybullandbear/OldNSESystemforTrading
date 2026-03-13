import requests
import json
import os
from datetime import datetime

STATE_FILE = "signal_state.json"
CONFIG_FILE = "config.json"

def load_config():
    if not os.path.exists(CONFIG_FILE):
        return None
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading config: {e}")
        return None

def load_state():
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    except:
        return {}

def save_state(state):
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=4)
    except Exception as e:
        print(f"Error saving signal state: {e}")

def get_telegram_status():
    """Returns True if Telegram notifications are enabled, False otherwise."""
    state = load_state()
    # Default to True to maintain existing behavior
    return state.get("telegram_enabled", True)

def set_telegram_status(enabled: bool):
    """Sets the global Telegram notification status."""
    state = load_state()
    state["telegram_enabled"] = enabled
    save_state(state)

import screenshot_utils
import zerodha_trader

def send_telegram_message(message):
    if not get_telegram_status():
        print("Telegram notifications disabled by user.")
        return

    config = load_config()
    if not config:
        print("Config not found, skipping Telegram notification.")
        return

    bot_token = config.get("telegram_bot_token")
    chat_id = config.get("telegram_chat_id")

    if not bot_token or not chat_id or "YOUR_" in bot_token:
        print("Invalid Telegram credentials in config.json")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }
    
    try:
        response = requests.post(url, json=payload, timeout=5)
        if response.status_code != 200:
            print(f"Failed to send Telegram message: {response.text}")
        else:
            print("Telegram text notification sent.")
    except Exception as e:
        print(f"Error sending Telegram message: {e}")

def send_telegram_photo(image_path, caption=""):
    if not get_telegram_status():
        print("Telegram notifications disabled by user.")
        return

    config = load_config()
    if not config:
        return

    bot_token = config.get("telegram_bot_token")
    chat_id = config.get("telegram_chat_id")
    
    url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
    
    try:
        with open(image_path, "rb") as image_file:
            files = {"photo": image_file}
            data = {"chat_id": chat_id}
            response = requests.post(url, data=data, files=files, timeout=30)
            
        if response.status_code != 200:
             print(f"Failed to send Telegram photo: {response.text}")
        else:
             print("Telegram photo sent.")
    except Exception as e:
         print(f"Error sending photo: {e}")

def check_and_send(symbol, new_signal_data):
    """
    Checks if the signal has changed.
    Priority:
    1. Send Text Message IMMEDIATELY.
    2. Capture Screenshot.
    3. Send Screenshot.
    """
    state = load_state()
    last_signal = state.get(symbol, {}).get("signal", "None")
    current_signal = new_signal_data['signal']
    
    if current_signal != "Error" and current_signal != "No Data":
        # Check for Signal Change OR Max Pain Shift
        signal_changed = current_signal != last_signal
        
        last_max_pain = state.get(symbol, {}).get("max_pain", 0)
        current_max_pain = new_signal_data.get('max_pain', 0)
        max_pain_shifted = (last_max_pain != 0) and (current_max_pain != last_max_pain)
        
        if signal_changed or max_pain_shifted:
            # Timestamp & Emoji
            timestamp = datetime.now().strftime("%H:%M:%S")
            emoji = "🔴" if "SELL" in current_signal or "Bearish" in current_signal else "🟢"
            if "WAIT" in current_signal or "NEUTRAL" in current_signal:
                emoji = "⚪"
                
            # Construct Detailed Message
            option_details = ""
            if new_signal_data.get('signal_type') in ['CE', 'PE']:
                 opt_type = new_signal_data['signal_type']
                 price = new_signal_data.get('atm_option_price', 0)
                 option_details = f"\n*ATM {opt_type} Price:* {price}"
            
            # PCR & Max Pain Info
            pcr_info = f"\nPCR: {new_signal_data.get('pcr', '-')}"
            mp_info = f"\nMax Pain: {current_max_pain}"
            
            special_alert = ""
            if max_pain_shifted:
                special_alert = f"\n⚠️ *MAX PAIN SHIFT*: {last_max_pain} -> {current_max_pain}"
                emoji = "⚠️" # Override emoji for important structural shift
            
            message = (
                f"{emoji} *Alert: {symbol}*\n"
                f"Time: {timestamp}\n"
                f"Signal: *{current_signal}*\n"
                f"Spot: {new_signal_data['spot']}\n"
                f"ATM Strike: {new_signal_data['atm']}"
                f"{option_details}"
                f"{pcr_info}"
                f"{mp_info}"
                f"{special_alert}"
            )
            
            if signal_changed:
                 message += f"\nOld Signal: {last_signal}"
            
            print(f"Notification Triggered for {symbol}: Signal={signal_changed}, MP_Shift={max_pain_shifted}")
            
            # 1. Send Text Priority
            send_telegram_message(message)
            
            # Update State
            state[symbol] = {
                "signal": current_signal,
                "timestamp": timestamp,
                "spot": new_signal_data['spot'],
                "max_pain": current_max_pain
            }
            save_state(state)
            
            # 3. Execute Trade (Zerodha)
            # We need expiry date. It should be in new_signal_data.
            try:
                # Assuming new_signal_data has 'expiry_date'
                expiry_to_trade = new_signal_data.get('expiry_date') 
                if expiry_to_trade:
                    zerodha_trader.trader.execute_trade(new_signal_data, symbol, expiry_to_trade)
                else:
                    print("Expiry date missing in signal data, skipping trade execution.")
            except Exception as trade_err:
                print(f"Error executing trade: {trade_err}")

            # 2. Capture & Send Screenshot
            print("Capturing screenshot...")
            image_path = screenshot_utils.capture_charts(symbol)
            if image_path:
                 send_telegram_photo(image_path)
                 try:
                     os.remove(image_path)
                 except:
                     pass

def check_market_status(symbol, spot_price, day_change=0, force_test=False):
    """
    Checks for Market Opening (09:15-09:30) and Closing (15:30-15:45) events.
    Sends a summary message once per day per event.
    force_test: If True, sends both messages immediately for debugging.
    """
    state = load_state()
    today_str = datetime.now().strftime("%Y-%m-%d")
    now_time = datetime.now().time()
    
    # Time Ranges
    start_market = datetime.strptime("09:15", "%H:%M").time()
    end_open_window = datetime.strptime("09:45", "%H:%M").time() # Extended window just in case
    
    start_close = datetime.strptime("15:30", "%H:%M").time()
    end_close_window = datetime.strptime("16:00", "%H:%M").time()
    
    # 1. Market Opening Logic
    if force_test or (start_market <= now_time <= end_open_window):
        last_open = state.get(symbol, {}).get("last_open_date")
        if force_test or (last_open != today_str):
            # Calculate Previous Close if change is available
            prev_close = spot_price - day_change
            gap_dir = "FLAT"
            if day_change > 0: gap_dir = "GAP UP 🟢"
            elif day_change < 0: gap_dir = "GAP DOWN 🔴"
            
            msg = (
                f"🔔 *Market Opening: {symbol}*\n"
                f"Date: {today_str}\n"
                f"Spot: {spot_price}\n"
                f"Change: {day_change} ({gap_dir})\n"
                f"Prev Close: {round(prev_close, 2)}"
            )
            if force_test: msg = "[TEST] " + msg
            send_telegram_message(msg)
            
            if not force_test:
                # Update State
                if symbol not in state: state[symbol] = {}
                state[symbol]["last_open_date"] = today_str
                save_state(state)

    # 2. Market Closing Logic
    if force_test or (start_close <= now_time <= end_close_window):
        last_close = state.get(symbol, {}).get("last_close_date")
        if force_test or (last_close != today_str):
            percent = 0
            if spot_price != 0:
                # Approximate change %
                prev_val = spot_price - day_change
                if prev_val != 0:
                     percent = round((day_change / prev_val) * 100, 2)
            
            emoji = "🟢" if day_change >= 0 else "🔴"
            
            msg = (
                f"🏁 *Market Closing: {symbol}*\n"
                f"Date: {today_str}\n"
                f"Final Spot: {spot_price}\n"
                f"Day Change: {day_change} ({percent}%) {emoji}"
            )
            if force_test: msg = "[TEST] " + msg
            send_telegram_message(msg)
            
            if not force_test:
                # Update State
                if symbol not in state: state[symbol] = {}
                state[symbol]["last_close_date"] = today_str
                save_state(state)
