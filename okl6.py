import ccxt
import time
from datetime import datetime, UTC
import logging
import tkinter as tk
from tkinter import ttk, messagebox
import threading
import queue
import collections
import pandas as pd
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---

# Path to the Excel file containing crypto support data
EXCEL_FILE_PATH = r"C:\Users\Automatic\Desktop\arbitrageapp\crypto_exchange_support.xlsx"

# All possible exchanges CCXT supports (a subset for demonstration)
# This list is used to populate the initial exchange selection dropdowns
all_available_exchanges = [
    {'id': 'binance', 'name': 'Binance', 'type': 'cex'},
    {'id': 'coinbase', 'name': 'Coinbase', 'type': 'cex'},
    {'id': 'kraken', 'name': 'Kraken', 'type': 'cex'},
    {'id': 'bitget', 'name': 'Bitget', 'type': 'cex'},
    {'id': 'bybit', 'name': 'Bybit', 'type': 'cex'},
    {'id': 'kucoin', 'name': 'KuCoin', 'type': 'cex'},
    {'id': 'bitfinex', 'name': 'Bitfinex', 'type': 'cex'},
    {'id': 'cryptocom', 'name': 'Crypto.com', 'type': 'cex'},
    {'id': 'gateio', 'name': 'Gate.io', 'type': 'cex'},
    {'id': 'huobi', 'name': 'Huobi', 'type': 'cex'},
    {'id': 'mexc', 'name': 'MEXC', 'type': 'cex'},
    {'id': 'bitstamp', 'name': 'Bitstamp', 'type': 'cex'},
    {'id': 'lbank', 'name': 'LBank', 'type': 'cex'},
    {'id': 'coinex', 'name': 'CoinEx', 'type': 'cex'},
    {'id': 'ascendex', 'name': 'AscendEX', 'type': 'cex'},
    {'id': 'digifinex', 'name': 'DigiFineFinex', 'type': 'cex'},
    {'id': 'fmfwio', 'name': 'FMFW.io', 'type': 'cex'},
    {'id': 'hitbtc', 'name': 'HitBTC', 'type': 'cex'},
    {'id': 'phemex', 'name': 'Phemex', 'type': 'cex'},
    {'id': 'probit', 'name': 'ProBit Global', 'type': 'cex'},
    {'id': 'whitebit', 'name': 'WhiteBIT', 'type': 'cex'},
    {'id': 'woo', 'name': 'WOO X', 'type': 'cex'},
    {'id': 'poloniex', 'name': 'Poloniex', 'type': 'cex'},
    {'id': 'bitrue', 'name': 'Bitrue', 'type': 'cex'},
    {'id': 'tokocrypto', 'name': 'Tokocrypto', 'type': 'cex'},
    {'id': 'indodax', 'name': 'Indodax', 'type': 'cex'},
    {'id': 'upbit', 'name': 'Upbit', 'type': 'cex'},
]

# List of exchanges that do not support fetch_tickers with multiple symbols
# This is used by ExchangePriceFetcher to determine fetch strategy
SINGLE_TICKER_FETCH_EXCHANGES = ['cryptocom', 'bitfinex']


def load_and_filter_cryptos_from_excel(excel_path, selected_exchange_ids):
    """
    Loads the crypto support data from the Excel file and filters
    cryptocurrencies that are supported by ALL selected exchanges.

    Args:
        excel_path (str): The full path to the crypto_exchange_support.xlsx file.
        selected_exchange_ids (list): A list of exchange IDs (e.g., ['binance', 'mexc'])
                                      that the user has selected.

    Returns:
        list: A sorted list of cryptocurrency symbols (e.g., ['BTC', 'ETH'])
              that are supported by all specified exchanges.
    """
    if not os.path.exists(excel_path):
        messagebox.showerror("File Not Found", f"The Excel file was not found at: {excel_path}\n"
                                             "Please ensure the file exists at the specified location.")
        logger.error(f"Excel file not found at {excel_path}")
        return []

    try:
        # Read the Excel file. The 'Crypto' column is the index.
        df = pd.read_excel(excel_path, sheet_name='Crypto Support', index_col='Crypto')

        # Map display names from Excel columns back to CCXT IDs for filtering
        # This assumes the Excel columns are the 'name' from all_available_exchanges
        # and we need to convert them to 'id' for filtering.
        exchange_name_to_id_map = {ex['name']: ex['id'] for ex in all_available_exchanges}
        
        # Filter df columns to only include the selected exchanges' names
        # Need to convert selected_exchange_ids to their corresponding names in the DataFrame
        selected_exchange_names = [ex['name'] for ex in all_available_exchanges if ex['id'] in selected_exchange_ids]
        
        # Check if all selected exchange names exist as columns in the DataFrame
        missing_columns = [name for name in selected_exchange_names if name not in df.columns]
        if missing_columns:
            messagebox.showwarning("Missing Exchange Data", 
                                   f"The Excel file is missing data for the following selected exchanges: {', '.join(missing_columns)}\n"
                                   "These exchanges will be excluded from filtering.")
            # Filter out the missing columns from selected_exchange_names
            selected_exchange_names = [name for name in selected_exchange_names if name in df.columns]

        if not selected_exchange_names:
            logger.warning("No valid exchange columns found in Excel for the selected exchanges.")
            return []

        df_filtered = df[selected_exchange_names]

        # Convert '✅' to True and '❌' to False for boolean logic
        # Explicitly call .infer_objects(copy=False) to address FutureWarning
        df_filtered = df_filtered.replace({'✅': True, '❌': False}).infer_objects(copy=False)

        # Find cryptos where ALL selected exchanges have 'True' (i.e., '✅')
        # Use .all(axis=1) to check if all values in a row are True
        common_cryptos = df_filtered[df_filtered.all(axis=1)].index.tolist()

        logger.info(f"Filtered {len(common_cryptos)} common cryptocurrencies for selected exchanges: {selected_exchange_ids}")
        return sorted(common_cryptos)

    except FileNotFoundError:
        # This case is handled by the initial os.path.exists check
        return []
    except Exception as e:
        messagebox.showerror("Error Reading Excel", f"An error occurred while reading the Excel file: {e}")
        logger.error(f"Error reading Excel file {excel_path}: {type(e).__name__} - {str(e)}")
        return []


class ExchangePriceFetcher(threading.Thread):
    """
    A dedicated thread to continuously fetch prices for a single exchange.
    This version uses fetch_tickers for efficiency across multiple cryptocurrencies,
    but falls back to individual fetch_ticker calls for exchanges that don't support it.
    It now takes a dynamic list of `supported_cryptos_to_fetch`.
    """
    def __init__(self, exchange_id, exchange_type, data_queue, latest_prices_ref, 
                 supported_cryptos_to_fetch, interval=2):
        super().__init__()
        self.exchange_id = exchange_id
        self.exchange_type = exchange_type
        self.data_queue = data_queue
        self.latest_prices_ref = latest_prices_ref
        self.supported_cryptos_to_fetch = supported_cryptos_to_fetch # Dynamic list
        self.interval = interval
        self.running = True
        self.daemon = True
        self.last_fetch_time = 0
        self.exchange = None
        self.markets_loaded = False
        self.supported_symbols_on_exchange = {} # {base_crypto: actual_symbol_on_exchange}
        self.single_ticker_fetch_exchanges = SINGLE_TICKER_FETCH_EXCHANGES

    def _initialize_exchange(self):
        """Initializes the CCXT exchange instance and loads markets for CEXs."""
        try:
            exchange_class = getattr(ccxt, self.exchange_id)
            self.exchange = exchange_class({
                'enableRateLimit': True,
                'timeout': 30000, # 30 seconds timeout
            })
            self.exchange.load_markets()
            self.markets_loaded = True
            logger.info(f"Markets loaded for CEX {self.exchange_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize or load markets for CEX {self.exchange_id}: {type(e).__name__} - {str(e)}")
            self.data_queue.put({
                'type': 'price_update',
                'id': self.exchange_id,
                'base_crypto': None,
                'symbol': None,
                'bid_price': None,
                'ask_price': None,
                'duration': None,
                'error': f"Initialization failed: {str(e)}"
            })
            return False

    def _determine_actual_symbol(self, base_crypto):
        """
        Determines the actual trading symbol for a given base_crypto on the CEX exchange.
        Caches results for efficiency. Tries multiple common stablecoin suffixes.
        Prioritizes spot markets.
        """
        if base_crypto in self.supported_symbols_on_exchange:
            return self.supported_symbols_on_exchange[base_crypto]

        # Common stablecoin suffixes to try
        suffixes_to_try = ['/USDT', '/USD', '/USDC', '-USDT', '-USD', '-USDC', '_USDT', '_USD', '_USDC']
        
        unique_suffixes = []
        for s in suffixes_to_try:
            if s not in unique_suffixes:
                unique_suffixes.append(s)

        for suffix in unique_suffixes:
            symbol_candidate = f"{base_crypto}{suffix}"
            if symbol_candidate in self.exchange.markets:
                market = self.exchange.markets[symbol_candidate]
                if market['spot']: # Ensure it's a spot market
                    self.supported_symbols_on_exchange[base_crypto] = symbol_candidate
                    return symbol_candidate
        
        logger.debug(f"No suitable SPOT market symbol found for {base_crypto} on {self.exchange_id}. Tried suffixes: {unique_suffixes}")
        return None

    def _fetch_all_supported_crypto_prices(self):
        """
        Fetches prices for all `supported_cryptos_to_fetch` using fetch_tickers for efficiency (CEX),
        or individual fetch_ticker calls (CEX).
        Now fetching highest bid and lowest ask.
        """
        if not self.markets_loaded:
            if not self._initialize_exchange():
                return

        start_time_ns = time.time_ns()
        fetched_base_cryptos_in_batch = set()

        if self.exchange_id in self.single_ticker_fetch_exchanges:
            for base_crypto in self.supported_cryptos_to_fetch:
                actual_symbol = self._determine_actual_symbol(base_crypto)
                if actual_symbol:
                    try:
                        ticker = self.exchange.fetch_ticker(actual_symbol)
                        bid_price = ticker.get('bid')
                        ask_price = ticker.get('ask')
                        duration_ms = (time.time_ns() - start_time_ns) // 1_000_000

                        self.data_queue.put({
                            'type': 'price_update',
                            'id': self.exchange_id,
                            'base_crypto': base_crypto,
                            'symbol': actual_symbol,
                            'bid_price': bid_price,
                            'ask_price': ask_price,
                            'duration': duration_ms,
                            'error': None
                        })
                        fetched_base_cryptos_in_batch.add(base_crypto)
                        logger.debug(f"Fetched {base_crypto} from CEX {self.exchange_id} individually.")

                    except Exception as e:
                        logger.error(f"Error fetching {base_crypto} from CEX {self.exchange_id} individually: {type(e).__name__} - {str(e)}")
                        self.data_queue.put({
                            'type': 'price_update',
                            'id': self.exchange_id,
                            'base_crypto': base_crypto,
                            'symbol': actual_symbol,
                            'bid_price': None,
                            'ask_price': None,
                            'duration': None,
                            'error': f"Individual fetch failed: {str(e)}"
                        })
                else:
                    self.data_queue.put({
                        'type': 'price_update',
                        'id': self.exchange_id,
                        'base_crypto': base_crypto,
                        'symbol': None,
                        'bid_price': None,
                        'ask_price': None,
                        'duration': None,
                        'error': 'No suitable market found'
                    })
        else:
            symbols_to_fetch_unique = set()
            for base_crypto in self.supported_cryptos_to_fetch:
                actual_symbol = self._determine_actual_symbol(base_crypto)
                if actual_symbol:
                    symbols_to_fetch_unique.add(actual_symbol)
                else:
                    self.data_queue.put({
                        'type': 'price_update',
                        'id': self.exchange_id,
                        'base_crypto': base_crypto,
                        'symbol': None,
                        'bid_price': None,
                        'ask_price': None,
                        'duration': None,
                        'error': 'No suitable market found'
                    })
            
            symbols_to_fetch = list(symbols_to_fetch_unique)

            if not symbols_to_fetch:
                logger.warning(f"No symbols to fetch for CEX {self.exchange_id} in this cycle.")
                return

            try:
                tickers = self.exchange.fetch_tickers(symbols_to_fetch)
                end_time_ns = time.time_ns()
                duration_ms = (end_time_ns - start_time_ns) // 1_000_000

                for symbol, ticker in tickers.items():
                    base_crypto = symbol.split('/')[0].split('-')[0].split('_')[0]
                    bid_price = ticker.get('bid')
                    ask_price = ticker.get('ask')

                    self.data_queue.put({
                        'type': 'price_update',
                        'id': self.exchange_id,
                        'base_crypto': base_crypto,
                        'symbol': symbol,
                        'bid_price': bid_price,
                        'ask_price': ask_price,
                        'duration': duration_ms,
                        'error': None
                    })
                    fetched_base_cryptos_in_batch.add(base_crypto)

                logger.info(f"Successfully fetched {len(tickers)} tickers from CEX {self.exchange_id} in {duration_ms} ms")

            except ccxt.ExchangeNotAvailable as e:
                logger.error(f"CEX {self.exchange_id} is not available: {str(e)}")
            except ccxt.NetworkError as e:
                logger.error(f"Network error with CEX {self.exchange_id}: {str(e)}")
            except ccxt.DDoSProtection as e:
                logger.error(f"DDoS Protection for CEX {self.exchange_id}: {str(e)}")
            except ccxt.RequestTimeout as e:
                logger.error(f"Request Timeout for CEX {self.exchange_id}: {str(e)}")
            except Exception as e:
                logger.error(f"An unexpected error occurred fetching tickers from CEX {self.exchange_id}: {type(e).__name__} - {str(e)}")
            
            # Ensure all `supported_cryptos_to_fetch` send an update, even if not found in fetch_tickers
            for base_crypto in self.supported_cryptos_to_fetch:
                if base_crypto not in fetched_base_cryptos_in_batch:
                    self.data_queue.put({
                        'type': 'price_update',
                        'id': self.exchange_id,
                        'base_crypto': base_crypto,
                        'symbol': None,
                        'bid_price': None,
                        'ask_price': None,
                        'duration': None,
                        'error': 'Not found or failed in batch fetch'
                    })

    def run(self):
        if not self._initialize_exchange():
            return

        while self.running:
            current_time = time.time()
            if current_time - self.last_fetch_time >= self.interval:
                self._fetch_all_supported_crypto_prices()
                self.last_fetch_time = current_time
            time.sleep(0.1)

    def stop(self):
        self.running = False
        logger.info(f"Stopped fetching for {self.exchange_id}")

    def force_fetch(self):
        """Forces an immediate fetch for this specific exchange."""
        self.last_fetch_time = 0
        logger.info(f"Forcing immediate fetch for {self.exchange_id}")


class ExchangeManager:
    """
    Manages active exchange threads and their configurations.
    Now dynamically receives `supported_cryptos_list`.
    """
    def __init__(self, data_queue, latest_prices_ref, supported_cryptos_list, fetch_interval=2, exchange_intervals=None):
        self.data_queue = data_queue
        self.latest_prices_ref = latest_prices_ref
        self.supported_cryptos_list = supported_cryptos_list # The dynamically filtered list
        self.fetch_interval = fetch_interval
        self.exchange_intervals = exchange_intervals if exchange_intervals is not None else {}
        self.active_exchanges = {} 

    def add_exchange(self, exchange_id, exchange_type):
        if exchange_id not in self.active_exchanges:
            logger.info(f"Adding {exchange_type} exchange: {exchange_id}")
            interval = self.exchange_intervals.get(exchange_id, self.fetch_interval)
            fetcher_thread = ExchangePriceFetcher(
                exchange_id, exchange_type, self.data_queue, self.latest_prices_ref,
                self.supported_cryptos_list, interval # Pass the filtered crypto list
            )
            fetcher_thread.start()
            self.active_exchanges[exchange_id] = {
                'thread': fetcher_thread,
                'type': exchange_type
            }
            # Fix: Changed 'ex_type' to 'exchange_type' to resolve NameError
            self.data_queue.put({'type': 'add_exchange_row', 'id': exchange_id, 'ex_type': exchange_type})
        else:
            logger.warning(f"Exchange {exchange_id} is already active.")

    def remove_exchange(self, exchange_id):
        if exchange_id in self.active_exchanges:
            logger.info(f"Removing exchange: {exchange_id}")
            self.active_exchanges[exchange_id]['thread'].stop()
            self.active_exchanges[exchange_id]['thread'].join(timeout=1)
            del self.active_exchanges[exchange_id]
            self.data_queue.put({'type': 'remove_exchange_row', 'id': exchange_id})
        else:
            logger.warning(f"Exchange {exchange_id} is not active.")

    def force_refresh_all(self):
        for exchange_data in self.active_exchanges.values():
            exchange_data['thread'].force_fetch()

    def stop_all(self):
        for exchange_data in self.active_exchanges.values():
            exchange_data['thread'].stop()
        
        for exchange_data in self.active_exchanges.values():
            exchange_data['thread'].join(timeout=1)
        logger.info("All exchange fetcher threads stopped.")


class CryptoPriceApp:
    """
    Tkinter GUI application for displaying cryptocurrency prices and spreads.
    Now includes dynamic exchange selection and crypto filtering based on an Excel file.
    """
    def __init__(self, master):
        self.master = master
        self.master.title("Advanced Live Crypto Arbitrage Watcher")
        self.master.geometry("1200x700")

        self.style = ttk.Style()
        self.style.theme_use('clam')
        self.style.configure("Treeview.Heading", font=('Inter', 12, 'bold'))
        self.style.configure("Treeview", font=('Inter', 11))
        self.style.configure("TButton", font=('Inter', 11))
        self.style.configure("TLabel", font=('Inter', 11))
        self.style.configure("TMenubutton", font=('Inter', 11))
        self.style.configure("TCheckbutton", font=('Inter', 11)) # For exchange selection

        self.data_queue = queue.Queue()
        self.latest_prices = collections.defaultdict(lambda: collections.defaultdict(dict))
        self.previous_prices = collections.defaultdict(dict)

        self.specific_exchange_intervals = {
            'binance': 2,
            'mexc': 3,
            'bitfinex': 5,
            'kraken': 3,
        }

        self.selected_exchange_ids = [] # Stores IDs of exchanges selected by the user
        self.filtered_supported_cryptos = [] # Dynamically updated list of cryptos to scrape
        # Set initial value to 'BTC'
        self.current_crypto_base = tk.StringVar(value='BTC') 

        # Initialize ExchangeManager with empty lists initially
        self.exchange_manager = ExchangeManager(self.data_queue, self.latest_prices, 
                                                self.filtered_supported_cryptos, # Pass reference to dynamic list
                                                fetch_interval=2, 
                                                exchange_intervals=self.specific_exchange_intervals)
        
        self.exchange_scrape_stats = {} # Populated after exchanges are loaded

        self.sort_orders = {
            "main_table": {
                "Bid Price": "desc",
                "Scrape Duration (ms)": "asc",
                "Avg Scrape (ms)": "asc"
            },
            "spreads_table_buy_sell": { # New key for the first spreads table
                "Crypto (Buy)": "asc", 
                "Exchange1 Bid (Buy)": "desc", 
                "Exchange2 Ask (Sell)": "desc",
                "Spread (Ex2 Ask - Ex1 Bid) (%)": "desc",
            },
            "spreads_table_sell_buy": { # New key for the second spreads table
                "Crypto (Sell)": "asc", 
                "Exchange2 Bid (Buy)": "desc",
                "Exchange1 Ask (Sell)": "desc",
                "Spread (Ex1 Ask - Ex2 Bid) (%)": "desc",
            }
        }
        self.current_main_sort_col = "Bid Price"
        # Default sort columns for the new spreads tables
        self.current_spreads_sort_col_buy_sell = "Crypto (Buy)" 
        self.current_spreads_sort_col_sell_buy = "Crypto (Sell)"
        
        self.create_widgets()
        self.tree.tag_configure("rising", background="#e0ffe0")
        self.tree.tag_configure("falling", background="#ffe0e0")
        self.tree.tag_configure("no_change", background="")
        
        self.update_prices_gui()
        self.master.protocol("WM_DELETE_WINDOW", self.on_closing)

    def create_widgets(self):
        # Main container frame for overall layout
        main_container = ttk.Frame(self.master, padding=10)
        main_container.pack(fill="both", expand=True)

        # Configure grid weights for responsive layout
        main_container.columnconfigure(0, weight=1)
        main_container.rowconfigure(1, weight=1) # Row for the notebook (tabs)

        # --- Top Control Frame (Grid layout) ---
        top_control_frame = ttk.LabelFrame(main_container, text="Controls", padding=10)
        top_control_frame.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        top_control_frame.columnconfigure(1, weight=1) # Column for the main controls to expand

        # Exchange Selection Frame (Left side of top_control_frame)
        exchange_selection_frame = ttk.LabelFrame(top_control_frame, text="Select Exchanges", padding=10)
        exchange_selection_frame.grid(row=0, column=0, rowspan=2, padx=(0, 10), sticky="nswe")
        exchange_selection_frame.rowconfigure(0, weight=1) # Make canvas expandable
        exchange_selection_frame.columnconfigure(0, weight=1) # Make canvas expandable

        self.exchange_checkbox_vars = {}
        self.exchange_checkboxes = {}
        self.exchange_names_to_ids = {ex['name']: ex['id'] for ex in all_available_exchanges if ex['type'] == 'cex'}
        sorted_exchange_names = sorted(self.exchange_names_to_ids.keys())

        canvas_exchanges = tk.Canvas(exchange_selection_frame, borderwidth=0, background="#f0f0f0") # Light grey background for canvas
        canvas_exchanges.grid(row=0, column=0, sticky="nsew")
        
        scrollbar_exchanges = ttk.Scrollbar(exchange_selection_frame, orient="vertical", command=canvas_exchanges.yview)
        scrollbar_exchanges.grid(row=0, column=1, sticky="ns")
        canvas_exchanges.configure(yscrollcommand=scrollbar_exchanges.set)

        self.checkbox_frame = ttk.Frame(canvas_exchanges, padding="5")
        canvas_exchanges.create_window((0, 0), window=self.checkbox_frame, anchor="nw")
        
        # Bind the frame's size to update the scrollregion
        self.checkbox_frame.bind("<Configure>", lambda e: canvas_exchanges.configure(scrollregion = canvas_exchanges.bbox("all")))

        for ex_name in sorted_exchange_names:
            var = tk.BooleanVar(value=False)
            cb = ttk.Checkbutton(self.checkbox_frame, text=ex_name, variable=var)
            cb.pack(anchor="w", padx=2, pady=1)
            self.exchange_checkbox_vars[ex_name] = var
            self.exchange_checkboxes[ex_name] = cb # Store checkbox widget

        # Main Controls Frame (Right side of top_control_frame)
        main_controls_frame = ttk.Frame(top_control_frame, padding=10)
        main_controls_frame.grid(row=0, column=1, sticky="nsew")
        main_controls_frame.columnconfigure(1, weight=1) # Make the second column expandable for dropdown/buttons

        # Layout for buttons and dropdowns within main_controls_frame
        load_button = ttk.Button(main_controls_frame, text="Load Selected Exchanges & Cryptos", command=self.load_selected_exchanges_and_cryptos)
        load_button.grid(row=0, column=0, columnspan=2, sticky="ew", pady=5)

        crypto_label = ttk.Label(main_controls_frame, text="Select Crypto:")
        crypto_label.grid(row=1, column=0, sticky="w", padx=(0,5))
        self.crypto_dropdown = ttk.OptionMenu(
            main_controls_frame, self.current_crypto_base, self.current_crypto_base.get(),
            # The menu options will be set dynamically after loading cryptos
            command=self.change_crypto_base
        )
        self.crypto_dropdown.grid(row=1, column=1, sticky="ew", pady=5)
        self.crypto_dropdown.config(state="disabled") # Disable until cryptos are loaded

        refresh_button = ttk.Button(main_controls_frame, text="Refresh All", command=self.exchange_manager.force_refresh_all)
        refresh_button.grid(row=2, column=0, columnspan=2, sticky="ew", pady=5)
        
        self.view_spreads_button = ttk.Button(main_controls_frame, text="View Spreads", command=self.toggle_spreads_view)
        self.view_spreads_button.grid(row=3, column=0, columnspan=2, sticky="ew", pady=5)

        stop_all_button = ttk.Button(main_controls_frame, text="Stop All", command=self.on_closing)
        stop_all_button.grid(row=4, column=0, columnspan=2, sticky="ew", pady=5)

        # --- Main Content Area with ttk.Notebook (tabs) ---
        self.notebook = ttk.Notebook(main_container)
        self.notebook.grid(row=1, column=0, sticky="nsew", pady=(10, 0))

        # Tab 1: Live Prices
        self.main_prices_tab = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.main_prices_tab, text="Live Prices")
        self.main_prices_tab.columnconfigure(0, weight=1) # Make treeview expandable
        self.main_prices_tab.rowconfigure(0, weight=1) # Make treeview expandable

        columns = ("Exchange", "Symbol", "Bid Price", "Ask Price", "Scrape Duration (ms)", "Avg Scrape (ms)") 
        self.tree = ttk.Treeview(self.main_prices_tab, columns=columns, show="headings")
        self.tree.grid(row=0, column=0, sticky="nsew") # Use grid for treeview
        
        for col in columns:
            self.tree.heading(col, text=col, anchor=tk.W)
            self.tree.column(col, width=150, anchor=tk.W) # Default width
            if col in ["Bid Price", "Ask Price", "Scrape Duration (ms)", "Avg Scrape (ms)"]:
                self.tree.heading(col, command=lambda c=col: self.sort_column(c, self.tree, "main_table"))

        self.tree.column("Exchange", width=120)
        self.tree.column("Symbol", width=80)
        self.tree.column("Bid Price", width=120, anchor=tk.E)
        self.tree.column("Ask Price", width=120, anchor=tk.E)
        self.tree.column("Scrape Duration (ms)", width=120, anchor=tk.E)
        self.tree.column("Avg Scrape (ms)", width=120, anchor=tk.E)

        main_scrollbar = ttk.Scrollbar(self.main_prices_tab, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=main_scrollbar.set)
        main_scrollbar.grid(row=0, column=1, sticky="ns") # Place scrollbar next to treeview

        # Tab 2: Arbitrage Spreads
        self.spreads_tab = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.spreads_tab, text="Arbitrage Spreads")
        self.spreads_tab.columnconfigure(0, weight=1) # Make first column (table 1) expandable
        self.spreads_tab.columnconfigure(1, weight=1) # Make second column (table 2) expandable
        self.spreads_tab.rowconfigure(0, weight=1) # Make row expandable for both tables

        # Frame for Buy on Ex1, Sell on Ex2 table
        spreads_frame_buy_sell = ttk.LabelFrame(self.spreads_tab, text="Buy on Ex1, Sell on Ex2", padding=5)
        spreads_frame_buy_sell.grid(row=0, column=0, sticky="nsew", padx=(0, 5))
        spreads_frame_buy_sell.columnconfigure(0, weight=1)
        spreads_frame_buy_sell.rowconfigure(0, weight=1)

        # Columns for the first arbitrage table
        columns_buy_sell = (
            "Crypto (Buy)", 
            "Ex1 Bid (Buy)", 
            "Ex2 Ask (Sell)", 
            "Spread (Ex2 Ask - Ex1 Bid) (%)"
        )
        self.spreads_tree_buy_sell = ttk.Treeview(spreads_frame_buy_sell, columns=columns_buy_sell, show="headings")
        self.spreads_tree_buy_sell.grid(row=0, column=0, sticky="nsew")
        
        for col in columns_buy_sell:
            self.spreads_tree_buy_sell.heading(col, text=col, anchor=tk.W)
            self.spreads_tree_buy_sell.column(col, width=120, anchor=tk.W)
            if "Spread" in col:
                self.spreads_tree_buy_sell.column(col, width=200, anchor=tk.E)
            elif "Bid" in col or "Ask" in col:
                self.spreads_tree_buy_sell.column(col, width=150, anchor=tk.E)
            self.spreads_tree_buy_sell.heading(col, command=lambda c=col: self.sort_column(c, self.spreads_tree_buy_sell, "spreads_table_buy_sell"))

        scrollbar_buy_sell = ttk.Scrollbar(spreads_frame_buy_sell, orient="vertical", command=self.spreads_tree_buy_sell.yview)
        self.spreads_tree_buy_sell.configure(yscrollcommand=scrollbar_buy_sell.set)
        scrollbar_buy_sell.grid(row=0, column=1, sticky="ns")

        # Frame for Buy on Ex2, Sell on Ex1 table
        spreads_frame_sell_buy = ttk.LabelFrame(self.spreads_tab, text="Buy on Ex2, Sell on Ex1", padding=5)
        spreads_frame_sell_buy.grid(row=0, column=1, sticky="nsew", padx=(5, 0))
        spreads_frame_sell_buy.columnconfigure(0, weight=1)
        spreads_frame_sell_buy.rowconfigure(0, weight=1)

        # Columns for the second arbitrage table
        columns_sell_buy = (
            "Crypto (Sell)", 
            "Ex2 Bid (Buy)", 
            "Ex1 Ask (Sell)", 
            "Spread (Ex1 Ask - Ex2 Bid) (%)"
        )
        self.spreads_tree_sell_buy = ttk.Treeview(spreads_frame_sell_buy, columns=columns_sell_buy, show="headings")
        self.spreads_tree_sell_buy.grid(row=0, column=0, sticky="nsew")

        for col in columns_sell_buy:
            self.spreads_tree_sell_buy.heading(col, text=col, anchor=tk.W)
            self.spreads_tree_sell_buy.column(col, width=120, anchor=tk.W)
            if "Spread" in col:
                self.spreads_tree_sell_buy.column(col, width=200, anchor=tk.E)
            elif "Bid" in col or "Ask" in col:
                self.spreads_tree_sell_buy.column(col, width=150, anchor=tk.E)
            self.spreads_tree_sell_buy.heading(col, command=lambda c=col: self.sort_column(c, self.spreads_tree_sell_buy, "spreads_table_sell_buy"))

        scrollbar_sell_buy = ttk.Scrollbar(spreads_frame_sell_buy, orient="vertical", command=self.spreads_tree_sell_buy.yview)
        self.spreads_tree_sell_buy.configure(yscrollcommand=scrollbar_sell_buy.set)
        scrollbar_sell_buy.grid(row=0, column=1, sticky="ns")
        
        # --- Status Bar (Bottom of main_container) ---
        status_info_frame = ttk.LabelFrame(main_container, text="Status", padding=10)
        status_info_frame.grid(row=2, column=0, sticky="ew", pady=(10,0))
        status_info_frame.columnconfigure(0, weight=1) # Make status label expandable
        status_info_frame.columnconfigure(1, weight=1) # Make avg scrape label expandable

        self.status_label = ttk.Label(status_info_frame, text="Please select exchanges and click 'Load'.", font=('Inter', 11))
        self.status_label.grid(row=0, column=0, sticky="w")
        
        self.avg_total_scrape_time_label = ttk.Label(status_info_frame, text="Avg scrape time (all exchanges, last cycle): N/A", font=('Inter', 11))
        self.avg_total_scrape_time_label.grid(row=0, column=1, sticky="e")
        
        self.exchange_rows = {} # Stores {'exchange_id': 'treeview_item_id'}
        self.current_view = "main" # Keep track of the current view (though notebook handles visibility)


    def load_selected_exchanges_and_cryptos(self):
        """
        Loads selected exchanges and filters cryptocurrencies based on the Excel file.
        This method is called when the user clicks the "Load Selected Exchanges & Cryptos" button.
        """
        selected_exchanges = []
        for ex_name, var in self.exchange_checkbox_vars.items():
            if var.get():
                selected_exchanges.append(self.exchange_names_to_ids[ex_name])

        if len(selected_exchanges) < 2:
            messagebox.showwarning("Selection Error", "Please select at least two exchanges to enable spread calculation.")
            return

        self.selected_exchange_ids = selected_exchanges
        logger.info(f"User selected exchanges: {self.selected_exchange_ids}")

        # Stop any currently running exchange threads
        self.exchange_manager.stop_all()

        # Clear existing data in GUI and internal states
        for item in self.tree.get_children():
            self.tree.delete(item)
        for item in self.spreads_tree_buy_sell.get_children(): # Clear first spreads table
            self.spreads_tree_buy_sell.delete(item)
        for item in self.spreads_tree_sell_buy.get_children(): # Clear second spreads table
            self.spreads_tree_sell_buy.delete(item)

        self.exchange_rows.clear()
        self.exchange_scrape_stats.clear()
        self.latest_prices.clear()
        self.previous_prices.clear()
        self.exchange_manager.active_exchanges.clear() # Ensure manager's active exchanges are clear

        # Load and filter cryptos based on selected exchanges
        self.filtered_supported_cryptos = load_and_filter_cryptos_from_excel(EXCEL_FILE_PATH, self.selected_exchange_ids)

        if not self.filtered_supported_cryptos:
            messagebox.showwarning("No Common Cryptos", "No common cryptocurrencies found across the selected exchanges in the Excel file. Please choose different exchanges or update the Excel file.")
            self.status_label.config(text="No common cryptos found. Please re-select exchanges.")
            self.crypto_dropdown.config(state="disabled")
            self.current_crypto_base.set('N/A')
            return

        # --- Set BTC as default crypto if available ---
        default_crypto = 'N/A'
        if 'BTC' in self.filtered_supported_cryptos:
            default_crypto = 'BTC'
        elif self.filtered_supported_cryptos:
            default_crypto = self.filtered_supported_cryptos[0]
        
        self.current_crypto_base.set(default_crypto)
        # Update the crypto dropdown with the filtered list and the selected default
        self.crypto_dropdown.set_menu(self.current_crypto_base.get(), *self.filtered_supported_cryptos)
        self.crypto_dropdown.config(state="normal")
        self.master.title(f"Advanced Live {self.current_crypto_base.get()} Price Watcher")

        # Re-initialize ExchangeManager with the selected exchanges and filtered cryptos
        # The ExchangeManager will now use this specific list for all its fetchers
        self.exchange_manager = ExchangeManager(self.data_queue, self.latest_prices, 
                                                self.filtered_supported_cryptos, # Pass the filtered list
                                                fetch_interval=2, 
                                                exchange_intervals=self.specific_exchange_intervals)
        
        # Add selected exchanges to the manager, which will start their threads
        for ex_id in self.selected_exchange_ids:
            ex_type = next((ex['type'] for ex in all_available_exchanges if ex['id'] == ex_id), 'cex') # Default to cex
            self.exchange_manager.add_exchange(ex_id, ex_type)
            self.exchange_scrape_stats[ex_id] = {'total_duration': 0, 'count': 0, 'average': 0}


        self.status_label.config(text=f"Loaded {len(self.selected_exchange_ids)} exchanges and {len(self.filtered_supported_cryptos)} common cryptos.")
        logger.info("Exchanges and cryptos loaded successfully.")


    def change_crypto_base(self, new_crypto_base):
        """
        Handles the change of the base cryptocurrency.
        This only changes the *displayed* crypto in the main table.
        The fetchers continue to fetch all filtered cryptos.
        """
        if self.current_crypto_base.get() == new_crypto_base:
            return

        logger.info(f"Changing displayed crypto base from {self.current_crypto_base.get()} to {new_crypto_base}")
        self.current_crypto_base.set(new_crypto_base)
        self.master.title(f"Advanced Live {new_crypto_base} Price Watcher")
        
        # Clear main table and re-populate with current data for the new crypto
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.exchange_rows.clear() # Clear existing rows
        
        # Re-add rows for active exchanges, showing "N/A" initially for the new crypto
        for ex_id in self.exchange_manager.active_exchanges:
            ex_type = self.exchange_manager.active_exchanges[ex_id]['type']
            # We don't have the exact symbol here, so we use a placeholder for display
            self._add_exchange_row_to_tree(ex_id, ex_type, f"{new_crypto_base}/USDT") 
        
        logger.info(f"Successfully changed displayed crypto base to {new_crypto_base}")


    def get_sort_value(self, value, col):
        """
        Helper function to convert string values from Treeview cells into sortable types.
        Handles "N/A" and "Failed to fetch" for numerical columns, placing them at the end.
        """
        if col in ["Bid Price", "Ask Price", "Scrape Duration (ms)", "Avg Scrape (ms)"] or \
           "Bid" in col or "Ask" in col or "Spread" in col: # Generic check for price/spread columns
            if isinstance(value, str):
                value = value.replace('$', '').replace(',', '').replace(' ms', '').replace(' %', '').strip()
            try:
                return float(value)
            except ValueError:
                return float('inf') # Places non-numeric values at the end when sorting numerically
        return value

    def _apply_sort(self, col, tree_widget, table_type, sort_order):
        """
        Applies sorting to the Treeview by reordering existing items.
        """
        l = []
        for k in tree_widget.get_children(''):
            item_values = tree_widget.item(k, 'values')
            try:
                col_index = tree_widget["columns"].index(col)
                value_to_sort = item_values[col_index]
            except (ValueError, IndexError):
                value_to_sort = "N/A" # Default if column not found or index out of bounds

            sort_value = self.get_sort_value(value_to_sort, col)
            l.append((sort_value, k))

        l.sort(key=lambda x: x[0], reverse=(sort_order == "desc"))

        for index, (val, k) in enumerate(l):
            tree_widget.move(k, '', index)

        # Update heading text to show sort order
        display_text = col
        if col in ["Bid Price", "Ask Price"] or "Bid" in col or "Ask" in col:
            display_text += " (High)" if sort_order == "desc" else " (Low)"
        elif col in ["Scrape Duration (ms)", "Avg Scrape (ms)"]:
            display_text += " (Fastest)" if sort_order == "asc" else " (Slowest)"
        elif "Spread" in col:
            display_text += " (High)" if sort_order == "desc" else " (Low)"
        elif col.startswith("Crypto"): # Now 'Crypto (Buy)' or 'Crypto (Sell)'
            display_text += " (Z-A)" if sort_order == "desc" else " (A-Z)"
        
        # Reset all headings first
        for c in tree_widget["columns"]:
            tree_widget.heading(c, text=c)
        # Then set the active column's heading
        tree_widget.heading(col, text=display_text)


    def sort_column(self, col, tree_widget, table_type):
        """
        Toggles the sort order for the clicked column and applies the sort.
        """
        current_sort_order = self.sort_orders[table_type].get(col, "asc")
        new_sort_order = "desc" if current_sort_order == "asc" else "asc"
        self.sort_orders[table_type][col] = new_sort_order
        
        # Update the correct current sort column based on which tree_widget was clicked
        if table_type == "main_table":
            self.current_main_sort_col = col
        elif table_type == "spreads_table_buy_sell":
            self.current_spreads_sort_col_buy_sell = col
        elif table_type == "spreads_table_sell_buy":
            self.current_spreads_sort_col_sell_buy = col

        self._apply_sort(col, tree_widget, table_type, new_sort_order)


    def _add_exchange_row_to_tree(self, exchange_id, ex_type, symbol):
        """Helper to add a new row to the Treeview."""
        if exchange_id not in self.exchange_rows:
            display_name = f"{exchange_id.capitalize()} ({ex_type.upper()})"
            item_id = self.tree.insert("", "end", iid=exchange_id, values=(display_name, symbol, "N/A", "N/A", "N/A", "N/A"))
            self.exchange_rows[exchange_id] = item_id
            self.exchange_scrape_stats[exchange_id] = {'total_duration': 0, 'count': 0, 'average': 0}


    def _remove_exchange_row_from_tree(self, exchange_id):
        """Helper to remove a row from the Treeview."""
        if exchange_id in self.exchange_rows:
            self.tree.delete(self.exchange_rows[exchange_id])
            del self.exchange_rows[exchange_id]
            if exchange_id in self.exchange_scrape_stats:
                del self.exchange_scrape_stats[exchange_id]
            # Also clean up from latest_prices and previous_prices for all cryptos
            for crypto_prices in self.latest_prices.values():
                if exchange_id in crypto_prices:
                    del crypto_prices[exchange_id]
            for crypto_prices in self.previous_prices.values():
                if exchange_id in crypto_prices:
                    del crypto_prices[exchange_id]


    def toggle_spreads_view(self):
        """Toggles between the main exchange table and the spreads table."""
        # Get the current selected tab's text
        current_tab_text = self.notebook.tab(self.notebook.select(), "text")

        if current_tab_text == "Live Prices":
            self.notebook.select(self.spreads_tab)
            self.view_spreads_button.config(text="View Exchange Prices")
            self.update_spreads_table() # Ensure spreads table is updated when switching to it
        else:
            self.notebook.select(self.main_prices_tab)
            self.view_spreads_button.config(text="View Spreads")
            # Re-apply sort to ensure main table is sorted correctly after switching back
            self._apply_sort(
                self.current_main_sort_col,
                self.tree,
                "main_table",
                self.sort_orders["main_table"][self.current_main_sort_col]
            )


    def update_spreads_table(self):
        """
        Updates the spreads table with calculated spreads between selected exchanges.
        This now dynamically handles any two selected exchanges, showing two types of spreads.
        """
        # Clear existing entries for both tables
        for item in self.spreads_tree_buy_sell.get_children():
            self.spreads_tree_buy_sell.delete(item)
        for item in self.spreads_tree_sell_buy.get_children():
            self.spreads_tree_sell_buy.delete(item)

        if len(self.selected_exchange_ids) < 2:
            # If less than two exchanges are selected, show a message and switch back to main table
            self.status_label.config(text="Select at least two exchanges to view spreads.")
            # Switch back to Live Prices tab if currently on Spreads tab
            if self.notebook.tab(self.notebook.select(), "text") == "Arbitrage Spreads":
                self.notebook.select(self.main_prices_tab)
                self.view_spreads_button.config(text="View Spreads")
            return

        # Use the first two selected exchanges for spread calculation
        ex_id1 = self.selected_exchange_ids[0]
        ex_id2 = self.selected_exchange_ids[1]

        # Get display names for the selected exchanges
        ex_name1 = next((ex['name'] for ex in all_available_exchanges if ex['id'] == ex_id1), ex_id1.capitalize())
        ex_name2 = next((ex['name'] for ex in all_available_exchanges if ex['id'] == ex_id2), ex_id2.capitalize())

        # Define columns for the first spreads table (Buy on Ex1, Sell on Ex2)
        columns_buy_sell = (
            "Crypto (Buy)", 
            f"{ex_name1} Bid (Buy)", 
            f"{ex_name2} Ask (Sell)", 
            f"Spread ({ex_name2} Ask - {ex_name1} Bid) (%)"
        )
        self.spreads_tree_buy_sell["columns"] = columns_buy_sell
        for col in columns_buy_sell:
            self.spreads_tree_buy_sell.heading(col, text=col, anchor=tk.W)
            if "Spread" in col:
                self.spreads_tree_buy_sell.column(col, width=200, anchor=tk.E)
            elif "Bid" in col or "Ask" in col:
                self.spreads_tree_buy_sell.column(col, width=150, anchor=tk.E)
            elif col.startswith("Crypto"):
                self.spreads_tree_buy_sell.column(col, width=100, anchor=tk.W)
            else:
                self.spreads_tree_buy_sell.column(col, width=100, anchor=tk.W)
            self.spreads_tree_buy_sell.heading(col, command=lambda c=col: self.sort_column(c, self.spreads_tree_buy_sell, "spreads_table_buy_sell"))

        # Define columns for the second spreads table (Buy on Ex2, Sell on Ex1)
        columns_sell_buy = (
            "Crypto (Sell)", 
            f"{ex_name2} Bid (Buy)", 
            f"{ex_name1} Ask (Sell)", 
            f"Spread ({ex_name1} Ask - {ex_name2} Bid) (%)"
        )
        self.spreads_tree_sell_buy["columns"] = columns_sell_buy
        for col in columns_sell_buy:
            self.spreads_tree_sell_buy.heading(col, text=col, anchor=tk.W)
            if "Spread" in col:
                self.spreads_tree_sell_buy.column(col, width=200, anchor=tk.E)
            elif "Bid" in col or "Ask" in col:
                self.spreads_tree_sell_buy.column(col, width=150, anchor=tk.E)
            elif col.startswith("Crypto"):
                self.spreads_tree_sell_buy.column(col, width=100, anchor=tk.W)
            else:
                self.spreads_tree_sell_buy.column(col, width=100, anchor=tk.W)
            self.spreads_tree_sell_buy.heading(col, command=lambda c=col: self.sort_column(c, self.spreads_tree_sell_buy, "spreads_table_sell_buy"))
        
        spread_data_to_display = []

        for crypto_base in self.filtered_supported_cryptos: # 'crypto_base' refers to BTC, ETH etc.
            ex1_prices = self.latest_prices.get(crypto_base, {}).get(ex_id1)
            ex2_prices = self.latest_prices.get(crypto_base, {}).get(ex_id2)

            # Only proceed if both exchanges have data for the crypto
            if ex1_prices and ex2_prices:
                ex1_bid = ex1_prices.get('bid')
                ex1_ask = ex1_prices.get('ask')
                ex2_bid = ex2_prices.get('bid')
                ex2_ask = ex2_prices.get('ask')
                
                # Get the symbol for the crypto from one of the exchanges (assuming consistent symbols)
                symbol_display = ex1_prices.get('symbol', f"{crypto_base}/?") 

                # Only proceed if all required prices are available (not None)
                if all(p is not None for p in [ex1_bid, ex1_ask, ex2_bid, ex2_ask]):
                    spread1_percentage = "N/A"
                    spread2_percentage = "N/A"
                    
                    # Calculate Spread 1: (Ex2 Ask - Ex1 Bid) / Ex1 Bid
                    # This represents buying on Ex1 (at its bid) and selling on Ex2 (at its ask)
                    if ex1_bid > 0: # Ensure no division by zero
                        spread1_percentage = ((ex2_ask - ex1_bid) / ex1_bid) * 100
                    
                    # Calculate Spread 2: (Ex1 Ask - Ex2 Bid) / Ex2 Bid
                    # This represents buying on Ex2 (at its bid) and selling on Ex1 (at its ask)
                    if ex2_bid > 0: # Ensure no division by zero
                        spread2_percentage = ((ex1_ask - ex2_bid) / ex2_bid) * 100
                        
                    spread_data_to_display.append({
                        'crypto_base': crypto_base, # Keep original crypto base for internal use
                        'symbol_display': symbol_display, # This is the new 'Crypto' column content
                        'ex1_bid': ex1_bid,
                        'ex1_ask': ex1_ask,
                        'spread1': spread1_percentage,
                        'ex2_bid': ex2_bid,
                        'ex2_ask': ex2_ask,
                        'spread2': spread2_percentage
                    })

        # Apply sorting to the spread data before inserting into the treeview
        # Sort for Buy on Ex1, Sell on Ex2 table
        sort_col_buy_sell = self.current_spreads_sort_col_buy_sell
        sort_order_buy_sell = self.sort_orders["spreads_table_buy_sell"].get(sort_col_buy_sell, "asc") 

        def get_spread_sort_key_buy_sell(item):
            if sort_col_buy_sell == "Crypto (Buy)":
                return item['symbol_display']
            elif sort_col_buy_sell == f"{ex_name1} Bid (Buy)":
                return item['ex1_bid'] if item['ex1_bid'] is not None else float('-inf')
            elif sort_col_buy_sell == f"{ex_name2} Ask (Sell)":
                return item['ex2_ask'] if item['ex2_ask'] is not None else float('-inf')
            elif sort_col_buy_sell == f"Spread ({ex_name2} Ask - {ex_name1} Bid) (%)":
                return item['spread1'] if item['spread1'] != "N/A" else float('-inf')
            return 0

        spread_data_to_display_copy1 = list(spread_data_to_display) # Create a copy for independent sorting
        spread_data_to_display_copy1.sort(key=get_spread_sort_key_buy_sell, reverse=(sort_order_buy_sell == "desc"))

        for item in spread_data_to_display_copy1:
            self.spreads_tree_buy_sell.insert("", "end", values=(
                item['symbol_display'], # First 'Crypto (Buy)' column content
                f"${item['ex1_bid']:.6f}" if item['ex1_bid'] is not None else "N/A", # Ex1 Bid
                f"${item['ex2_ask']:.6f}" if item['ex2_ask'] is not None else "N/A", # Ex2 Ask
                f"{item['spread1']:.2f} %" if item['spread1'] != "N/A" else "N/A"
            ))

        # Sort for Buy on Ex2, Sell on Ex1 table
        sort_col_sell_buy = self.current_spreads_sort_col_sell_buy
        sort_order_sell_buy = self.sort_orders["spreads_table_sell_buy"].get(sort_col_sell_buy, "asc") 

        def get_spread_sort_key_sell_buy(item):
            if sort_col_sell_buy == "Crypto (Sell)":
                return item['symbol_display']
            elif sort_col_sell_buy == f"{ex_name2} Bid (Buy)":
                return item['ex2_bid'] if item['ex2_bid'] is not None else float('-inf')
            elif sort_col_sell_buy == f"{ex_name1} Ask (Sell)":
                return item['ex1_ask'] if item['ex1_ask'] is not None else float('-inf')
            elif sort_col_sell_buy == f"Spread ({ex_name1} Ask - {ex_name2} Bid) (%)":
                return item['spread2'] if item['spread2'] != "N/A" else float('-inf')
            return 0

        spread_data_to_display_copy2 = list(spread_data_to_display) # Create another copy for independent sorting
        spread_data_to_display_copy2.sort(key=get_spread_sort_key_sell_buy, reverse=(sort_order_sell_buy == "desc"))

        for item in spread_data_to_display_copy2:
            self.spreads_tree_sell_buy.insert("", "end", values=(
                item['symbol_display'], # Second 'Crypto (Sell)' column content
                f"${item['ex2_bid']:.6f}" if item['ex2_bid'] is not None else "N/A", # Ex2 Bid
                f"${item['ex1_ask']:.6f}" if item['ex1_ask'] is not None else "N/A", # Ex1 Ask
                f"{item['spread2']:.2f} %" if item['spread2'] != "N/A" else "N/A"
            ))

        # Ensure the initial sort is applied after populating.
        self._apply_sort(
            self.current_spreads_sort_col_buy_sell,
            self.spreads_tree_buy_sell,
            "spreads_table_buy_sell",
            self.sort_orders["spreads_table_buy_sell"][self.current_spreads_sort_col_buy_sell]
        )
        self._apply_sort(
            self.current_spreads_sort_col_sell_buy,
            self.spreads_tree_sell_buy,
            "spreads_table_sell_buy",
            self.sort_orders["spreads_table_sell_buy"][self.current_spreads_sort_col_sell_buy]
        )


    def update_prices_gui(self):
        """
        Checks the queue for new data and updates the GUI.
        This method is called periodically via master.after().
        """
        total_durations_this_cycle = []
        try:
            while True:
                item = self.data_queue.get_nowait()
                
                if item['type'] == 'price_update':
                    exchange_id = item['id']
                    bid_price = item['bid_price']
                    ask_price = item['ask_price']
                    symbol = item['symbol']
                    duration = item['duration']
                    base_crypto = item['base_crypto']
                    error_message = item['error'] # Get the error message from the item

                    # Update latest prices for spread calculation (for all cryptos)
                    # Store the symbol along with bid/ask prices
                    self.latest_prices[base_crypto][exchange_id] = {'bid': bid_price, 'ask': ask_price, 'symbol': symbol}

                    # Get previous price for highlighting (using bid price for comparison)
                    previous_bid_price = self.previous_prices[base_crypto].get(exchange_id)
                    
                    if duration is not None:
                        total_durations_this_cycle.append(duration)
                        if exchange_id in self.exchange_scrape_stats:
                            self.exchange_scrape_stats[exchange_id]['total_duration'] += duration
                            self.exchange_scrape_stats[exchange_id]['count'] += 1
                            if self.exchange_scrape_stats[exchange_id]['count'] > 0:
                                self.exchange_scrape_stats[exchange_id]['average'] = \
                                    self.exchange_scrape_stats[exchange_id]['total_duration'] / \
                                    self.exchange_scrape_stats[exchange_id]['count']
                    
                    # Only update the main table if the price update is for the currently selected crypto
                    if base_crypto == self.current_crypto_base.get() and exchange_id in self.exchange_rows:
                        item_id = self.exchange_rows[exchange_id]
                        current_avg_scrape = self.exchange_scrape_stats[exchange_id]['average'] if exchange_id in self.exchange_scrape_stats else 0
                        
                        ex_type = self.exchange_manager.active_exchanges.get(exchange_id, {}).get('type', '')
                        display_name = f"{exchange_id.capitalize()} ({ex_type.upper()})"

                        tags = ()
                        if bid_price is not None and previous_bid_price is not None:
                            if bid_price > previous_bid_price:
                                tags = ("rising",)
                            elif bid_price < previous_bid_price:
                                tags = ("falling",)
                            else:
                                tags = ("no_change",)
                        elif previous_bid_price is not None and bid_price is None:
                            tags = ("falling",)
                        elif previous_bid_price is None and bid_price is not None:
                            tags = ("rising",)

                        formatted_bid_price = "N/A"
                        formatted_ask_price = "N/A"

                        if bid_price is not None:
                            if bid_price < 1:
                                formatted_bid_price = f"${bid_price:,.5f}" 
                            elif bid_price < 10:
                                formatted_bid_price = f"${bid_price:,.4f}"
                            elif bid_price < 100:
                                formatted_bid_price = f"${bid_price:,.3f}"
                            else:
                                formatted_bid_price = f"${bid_price:,.2f}"
                        
                        if ask_price is not None:
                            if ask_price < 1:
                                formatted_ask_price = f"${ask_price:,.5f}" 
                            elif ask_price < 10:
                                formatted_ask_price = f"${ask_price:,.4f}"
                            elif ask_price < 100:
                                formatted_ask_price = f"${ask_price:,.3f}"
                            else:
                                formatted_ask_price = f"${ask_price:,.2f}"

                        if bid_price is not None or ask_price is not None:
                            self.tree.item(item_id, values=(
                                display_name,
                                symbol,
                                formatted_bid_price,
                                formatted_ask_price,
                                f"{duration:.2f}" if duration is not None else "N/A",
                                f"{current_avg_scrape:.2f}" if current_avg_scrape > 0 else "N/A"
                            ), tags=tags)
                        else:
                            # Display "N/A" if the error specifically indicates no suitable market,
                            # otherwise display "Failed to fetch" for other errors.
                            display_status = "N/A" if error_message == 'No suitable market found' else "Failed to fetch"
                            self.tree.item(item_id, values=(
                                display_name,
                                symbol if symbol else f"{base_crypto}/?",
                                display_status,
                                display_status,
                                "N/A",
                                f"{current_avg_scrape:.2f}" if current_avg_scrape > 0 else "N/A"
                            ), tags=("falling",)) # Use falling tag for any non-successful fetch

                    self.previous_prices[base_crypto][exchange_id] = bid_price
                
                elif item['type'] == 'add_exchange_row':
                    # This is called when an exchange thread starts.
                    # Only add a row if the currently selected crypto is being displayed.
                    if item['id'] in self.selected_exchange_ids and self.current_crypto_base.get() != 'N/A':
                        symbol_for_display = f"{self.current_crypto_base.get()}/USDT"
                        self._add_exchange_row_to_tree(item['id'], item['ex_type'], symbol_for_display)
                
                elif item['type'] == 'remove_exchange_row':
                    self._remove_exchange_row_from_tree(item['id'])
                
                current_time = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')
                self.status_label.config(text=f"Last GUI update: {current_time}")

        except queue.Empty:
            pass

        if total_durations_this_cycle:
            avg_scrape_time_overall = sum(total_durations_this_cycle) / len(total_durations_this_cycle)
            self.avg_total_scrape_time_label.config(text=f"Avg scrape time (all exchanges, last cycle): {avg_scrape_time_overall:.2f} ms")
        else:
            self.avg_total_scrape_time_label.config(text="Avg scrape time (all exchanges, last cycle): N/A")

        # Apply sorting based on the currently active tab
        current_tab_text = self.notebook.tab(self.notebook.select(), "text")
        if current_tab_text == "Live Prices":
            self._apply_sort(
                self.current_main_sort_col,
                self.tree,
                "main_table",
                self.sort_orders["main_table"][self.current_main_sort_col]
            )
        elif current_tab_text == "Arbitrage Spreads":
            # Apply sorting to both spreads tables independently
            self.update_spreads_table() 
            self._apply_sort(
                self.current_spreads_sort_col_buy_sell,
                self.spreads_tree_buy_sell,
                "spreads_table_buy_sell",
                self.sort_orders["spreads_table_buy_sell"][self.current_spreads_sort_col_buy_sell]
            )
            self._apply_sort(
                self.current_spreads_sort_col_sell_buy,
                self.spreads_tree_sell_buy,
                "spreads_table_sell_buy",
                self.sort_orders["spreads_table_sell_buy"][self.current_spreads_sort_col_sell_buy]
            )

        self.master.after(200, self.update_prices_gui)

    def on_closing(self):
        """
        Handles the window closing event to stop all background threads.
        """
        self.exchange_manager.stop_all()
        self.master.destroy()

# --- Main Application Entry Point ---
if __name__ == "__main__":
    root = tk.Tk()
    app = CryptoPriceApp(root)
    root.mainloop()
