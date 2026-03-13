import json
import os
from datetime import datetime

TRADING_STATE_FILE = "trading_state.json"

class PaperTrader:
    def __init__(self):
        self.config_file = "config.json"
        
    def load_trading_state(self):
        default_state = {
            "trading_enabled": False,
            "paper_trading": True, # Force reality
            "positions": {
                "NIFTY": None,
                "BANKNIFTY": None
            }
        }
        if not os.path.exists(TRADING_STATE_FILE):
            return default_state
        try:
            with open(TRADING_STATE_FILE, 'r') as f:
                state = json.load(f)
                state['paper_trading'] = True
                return state
        except:
            return default_state

    def save_trading_state(self, state):
        try:
            with open(TRADING_STATE_FILE, 'w') as f:
                json.dump(state, f, indent=4)
        except Exception as e:
            print(f"Error saving trading state: {e}")

    def get_trading_status(self):
        state = self.load_trading_state()
        return state.get("trading_enabled", False)

    def set_trading_status(self, enabled):
        state = self.load_trading_state()
        state["trading_enabled"] = enabled
        self.save_trading_state(state)

    def execute_trade(self, signal_data, symbol, expiry_date):
        """Paper Execution Logic."""
        state = self.load_trading_state()
        
        # Check Active Position
        if state.get("positions", {}).get(symbol):
            # Already have a position, don't double up
            return

        signal = signal_data.get('signal', '')
        atm_strike = signal_data.get('atm', 0)
        option_price = signal_data.get('atm_option_price', 0)
        
        if not atm_strike or atm_strike == 0:
            return
            
        action = None
        if "SELL CALL" in signal or "BUY PUT" in signal or "Bearish" in signal:
            action = "SELL_CE"
        elif "SELL PUT" in signal or "BUY CALL" in signal or "Bullish" in signal:
            action = "SELL_PE"
        elif "SELL BOTH" in signal or "WAIT" in signal or "NEUTRAL" in signal:
            return

        if not action:
            return

        option_type = "CE" if action == "SELL_CE" else "PE"
        quantity = 65 if symbol == "NIFTY" else 30
        
        # Record paper trade
        if "positions" not in state: state["positions"] = {}
        state["positions"][symbol] = {
            "type": option_type,
            "strike": atm_strike,
            "trading_symbol": f"PAPER_{symbol}_{atm_strike}_{option_type}",
            "entry_price": option_price,
            "quantity": quantity,
            "highest_profit": 0,
            "current_profit": 0,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        self.save_trading_state(state)
        print(f"[{symbol}] Paper Entry: SELL {option_type} {atm_strike} @ {option_price}")
        
    def update_pnl(self, symbol, current_ltp):
        """Calculates P&L for Paper Trade."""
        state = self.load_trading_state()
        pos = state.get("positions", {}).get(symbol)
        if not pos: return
        
        entry_price = pos["entry_price"]
        quantity = pos["quantity"]
        
        # We are SELLING options. Profit = (Entry - Current) * Qty
        current_profit = (entry_price - current_ltp) * quantity
        
        highest_profit = pos.get("highest_profit", 0)
        if current_profit > highest_profit:
            highest_profit = current_profit
            
        pos["current_profit"] = current_profit
        pos["highest_profit"] = highest_profit
        self.save_trading_state(state)
        
        # Simple TS/SL 
        if highest_profit >= 3000 and current_profit <= 2000:
            print(f"[{symbol}] Trailing SL Hit in Paper Trade! Exiting...")
            self.exit_position(symbol)
            
    def exit_position(self, symbol):
        """Exits the paper position."""
        state = self.load_trading_state()
        if state.get("positions") and symbol in state["positions"]:
            state["positions"][symbol] = None
            self.save_trading_state(state)
            print(f"[{symbol}] Paper Position Closed.")

trader = PaperTrader()
