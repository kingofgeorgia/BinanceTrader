
import csv
import hashlib
import hmac
import json
import socket
import threading
import time
import tkinter as tk
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from pathlib import Path
from tkinter import messagebox, ttk
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    import websocket
except ImportError:
    websocket = None

BASE_URL = "https://demo-api.binance.com/api"
DEMO_STREAM_BASE_URL = "wss://demo-stream.binance.com/ws"
DEMO_WS_API_BASE_URL = "wss://demo-ws-api.binance.com/ws-api/v3"
TIMEOUT_SECONDS = 15
OPEN_ORDERS_RETRY_COUNT = 2
OPEN_ORDERS_RETRY_DELAY_SECONDS = 0.7
WEBSOCKET_RECONNECT_DELAY_SECONDS = 5
ZERO = Decimal("0")
MIN_RESTORED_POSITION_NOTIONAL = Decimal("1")
KEYSTORE_PATH = Path(__file__).resolve().parent / "binance_demo_keys.json"

PNL_COLOR_PROFIT = "#0a7f2e"
PNL_COLOR_LOSS = "#b42318"
PNL_COLOR_NEUTRAL = "#475467"
PNL_COLOR_WARNING = "#b54708"
LOG_COLOR_INFO = "#344054"
LOG_COLOR_DATA = "#1d4ed8"
LOG_COLOR_RULE = "#98A2B3"
TRADE_JOURNAL_CSV_PATH = Path(__file__).resolve().parent / "trades.csv"
ORDER_EVENTS_JSONL_PATH = Path(__file__).resolve().parent / "orders.jsonl"
POSITIONS_JSON_PATH = Path(__file__).resolve().parent / "positions.json"
TRADE_HISTORY_INTERVAL_SECONDS = {
    "15m": 15 * 60,
    "30m": 30 * 60,
    "1h": 60 * 60,
    "4h": 4 * 60 * 60,
    "12h": 12 * 60 * 60,
    "24h": 24 * 60 * 60,
}
KLINE_INTERVALS = ("1m", "3m", "5m", "15m", "30m", "1h", "4h")
EMA_FAST_PERIOD = 20
EMA_SLOW_PERIOD = 50
ATR_PERIOD = 14
RSI_PERIOD = 14
VOLUME_LOOKBACK = 20


class BinanceDemoError(RuntimeError):
    pass


class BinanceDemoClient:
    def __init__(self, api_key: str, api_secret: str) -> None:
        self.api_key = api_key.strip()
        self.api_secret = api_secret.strip()
        if not self.api_key or not self.api_secret:
            raise BinanceDemoError("Укажи API Key и Secret Key.")

    def _request(self, method: str, path: str, params: dict | None = None, signed: bool = False) -> dict | list:
        params = dict(params or {})
        headers: dict[str, str] = {}

        if signed:
            params.setdefault("recvWindow", 5000)
            params["timestamp"] = int(time.time() * 1000)
            query = urlencode(params, doseq=True, safe="")
            signature = hmac.new(
                self.api_secret.encode("utf-8"),
                query.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            payload = f"{query}&signature={signature}"
            headers["X-MBX-APIKEY"] = self.api_key
        else:
            payload = urlencode(params, doseq=True, safe="") if params else ""
            if self.api_key:
                headers["X-MBX-APIKEY"] = self.api_key

        url = f"{BASE_URL}{path}"
        data = None

        if method.upper() == "GET":
            if payload:
                url = f"{url}?{payload}"
        else:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            data = payload.encode("utf-8")

        request = Request(url=url, data=data, headers=headers, method=method.upper())

        try:
            with urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw) if raw.strip() else {}
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise BinanceDemoError(f"HTTP {exc.code}: {body}") from exc
        except (TimeoutError, socket.timeout) as exc:
            raise BinanceDemoError(f"Network timeout after {TIMEOUT_SECONDS}s: {method.upper()} {path}") from exc
        except URLError as exc:
            reason = getattr(exc, "reason", exc)
            if isinstance(reason, (TimeoutError, socket.timeout)) or "timed out" in str(reason).lower():
                raise BinanceDemoError(f"Network timeout after {TIMEOUT_SECONDS}s: {method.upper()} {path}") from exc
            raise BinanceDemoError(f"Ошибка сети: {exc}") from exc

    def get_server_time(self) -> dict:
        return self._request("GET", "/v3/time")

    def get_account(self) -> dict:
        return self._request("GET", "/v3/account", {"omitZeroBalances": "true"}, signed=True)

    def get_book_ticker(self, symbol: str) -> dict:
        return self._request("GET", "/v3/ticker/bookTicker", {"symbol": symbol.upper()})

    def get_all_book_tickers(self) -> list:
        return self._request("GET", "/v3/ticker/bookTicker")

    def get_klines(self, symbol: str, interval: str = "1m", limit: int = 120) -> list:
        return self._request(
            "GET",
            "/v3/klines",
            {"symbol": symbol.upper(), "interval": interval, "limit": limit},
        )

    def get_open_orders(self, symbol: str | None = None) -> list:
        params = {"symbol": symbol.upper()} if symbol else {}
        return self._request("GET", "/v3/openOrders", params, signed=True)

    def get_open_orders_retry(self, symbol: str | None = None) -> list:
        last_error: Exception | None = None
        for attempt in range(OPEN_ORDERS_RETRY_COUNT + 1):
            try:
                return self.get_open_orders(symbol=symbol)
            except BinanceDemoError as exc:
                last_error = exc
                if "timeout" not in str(exc).lower() or attempt >= OPEN_ORDERS_RETRY_COUNT:
                    break
                time.sleep(OPEN_ORDERS_RETRY_DELAY_SECONDS * (attempt + 1))
        raise last_error or BinanceDemoError("Open orders request failed.")

    def get_order(self, symbol: str, order_id: str) -> dict:
        return self._request("GET", "/v3/order", {"symbol": symbol.upper(), "orderId": order_id}, signed=True)

    def cancel_order(self, symbol: str, order_id: str) -> dict:
        return self._request("DELETE", "/v3/order", {"symbol": symbol.upper(), "orderId": order_id}, signed=True)

    def get_my_trades(
        self,
        symbol: str,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int | None = None,
    ) -> list:
        params: dict[str, str | int] = {"symbol": symbol.upper()}
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        if limit is not None:
            params["limit"] = limit
        return self._request("GET", "/v3/myTrades", params, signed=True)

    def get_exchange_info(self, symbol: str) -> dict:
        result = self._request("GET", "/v3/exchangeInfo", {"symbol": symbol.upper()})
        symbols = result.get("symbols", [])
        if not symbols:
            raise BinanceDemoError(f"Не удалось получить exchange info для {symbol}.")
        return symbols[0]

    def test_limit_buy(self, symbol: str, quantity: str, price: str) -> dict:
        return self._request(
            "POST",
            "/v3/order/test",
            {
                "symbol": symbol.upper(),
                "side": "BUY",
                "type": "LIMIT",
                "timeInForce": "GTC",
                "quantity": quantity,
                "price": price,
            },
            signed=True,
        )

    def test_market_buy(self, symbol: str, quantity: str) -> dict:
        return self._request(
            "POST",
            "/v3/order/test",
            {
                "symbol": symbol.upper(),
                "side": "BUY",
                "type": "MARKET",
                "quantity": quantity,
            },
            signed=True,
        )

    def test_market_sell(self, symbol: str, quantity: str) -> dict:
        return self._request(
            "POST",
            "/v3/order/test",
            {
                "symbol": symbol.upper(),
                "side": "SELL",
                "type": "MARKET",
                "quantity": quantity,
            },
            signed=True,
        )

    def live_limit_buy(self, symbol: str, quantity: str, price: str) -> dict:
        return self._request(
            "POST",
            "/v3/order",
            {
                "symbol": symbol.upper(),
                "side": "BUY",
                "type": "LIMIT",
                "timeInForce": "GTC",
                "quantity": quantity,
                "price": price,
                "newOrderRespType": "FULL",
            },
            signed=True,
        )

    def market_buy(self, symbol: str, quantity: str) -> dict:
        return self._request(
            "POST",
            "/v3/order",
            {
                "symbol": symbol.upper(),
                "side": "BUY",
                "type": "MARKET",
                "quantity": quantity,
                "newOrderRespType": "FULL",
            },
            signed=True,
        )

    def market_sell(self, symbol: str, quantity: str) -> dict:
        return self._request(
            "POST",
            "/v3/order",
            {
                "symbol": symbol.upper(),
                "side": "SELL",
                "type": "MARKET",
                "quantity": quantity,
                "newOrderRespType": "FULL",
            },
            signed=True,
        )


def decimal_or_zero(value: str | int | float | None) -> Decimal:
    if value is None:
        return ZERO
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return ZERO


def format_decimal(value: Decimal, places: int = 8) -> str:
    quant = Decimal("1").scaleb(-places)
    try:
        rounded = value.quantize(quant, rounding=ROUND_DOWN)
        if rounded == ZERO:
            rounded = ZERO
        text = format(rounded, "f")
    except InvalidOperation:
        text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def format_decimal_fixed(value: Decimal, places: int = 4) -> str:
    quant = Decimal("1").scaleb(-places)
    try:
        rounded = value.quantize(quant, rounding=ROUND_DOWN)
    except InvalidOperation:
        rounded = value
    if rounded == ZERO:
        rounded = ZERO
    return f"{rounded:.{places}f}"


def round_step_down(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value // step) * step


def sign_ws_api_params(api_secret: str, params: dict[str, str | int | float]) -> str:
    payload = "&".join(f"{key}={params[key]}" for key in sorted(params))
    return hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()


class DemoMarketStreamService:
    def __init__(self, symbol: str, on_status, on_book_ticker, on_error) -> None:
        self.symbol = symbol.upper()
        self.on_status = on_status
        self.on_book_ticker = on_book_ticker
        self.on_error = on_error
        self._active = False
        self._closing = False
        self._thread: threading.Thread | None = None
        self._ws_app = None

    def start(self) -> None:
        if websocket is None:
            raise BinanceDemoError("Package websocket-client is not installed. Run: pip install websocket-client")
        if self._active:
            return
        self._active = True
        self._closing = False
        self._thread = threading.Thread(target=self._run_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._active = False
        self._closing = True
        ws_app = self._ws_app
        if ws_app is not None:
            try:
                ws_app.close()
            except Exception:
                pass

    def is_running(self) -> bool:
        return self._active

    def _stream_url(self) -> str:
        return f"{DEMO_STREAM_BASE_URL}/{self.symbol.lower()}@bookTicker"

    def _run_forever(self) -> None:
        while self._active:
            self.on_status("connecting", self.symbol)
            try:
                self._ws_app = websocket.WebSocketApp(
                    self._stream_url(),
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                    on_ping=self._on_ping,
                )
                self._ws_app.run_forever(ping_interval=0)
            except Exception as exc:
                self.on_error(f"Market stream error for {self.symbol}: {exc}")
            finally:
                self._ws_app = None
            if self._active:
                self.on_status("reconnecting", self.symbol)
                time.sleep(WEBSOCKET_RECONNECT_DELAY_SECONDS)

    def _on_open(self, _ws) -> None:
        self.on_status("connected", self.symbol)

    def _on_message(self, _ws, message: str) -> None:
        try:
            payload = json.loads(message)
        except Exception as exc:
            self.on_error(f"Market stream parse error for {self.symbol}: {exc}")
            return
        self.on_book_ticker(self.symbol, payload)

    def _on_error(self, _ws, error) -> None:
        if self._active:
            self.on_error(f"Market stream error for {self.symbol}: {error}")

    def _on_close(self, _ws, _status_code, _message) -> None:
        if self._closing or not self._active:
            self.on_status("stopped", self.symbol)
        else:
            self.on_status("disconnected", self.symbol)

    def _on_ping(self, ws_app, payload) -> None:
        try:
            if getattr(ws_app, "sock", None) is not None:
                ws_app.sock.pong(payload)
        except Exception:
            pass


class DemoUserDataStreamService:
    def __init__(self, api_key: str, api_secret: str, on_status, on_event, on_error) -> None:
        self.api_key = api_key.strip()
        self.api_secret = api_secret.strip()
        self.on_status = on_status
        self.on_event = on_event
        self.on_error = on_error
        self._active = False
        self._closing = False
        self._thread: threading.Thread | None = None
        self._ws_app = None
        self._subscription_request_id = ""

    def start(self) -> None:
        if websocket is None:
            raise BinanceDemoError("Package websocket-client is not installed. Run: pip install websocket-client")
        if not self.api_key or not self.api_secret:
            raise BinanceDemoError("API Key and Secret Key are required for user data stream.")
        if self._active:
            return
        self._active = True
        self._closing = False
        self._thread = threading.Thread(target=self._run_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._active = False
        self._closing = True
        ws_app = self._ws_app
        if ws_app is not None:
            try:
                ws_app.close()
            except Exception:
                pass

    def is_running(self) -> bool:
        return self._active

    def _run_forever(self) -> None:
        while self._active:
            self.on_status("connecting")
            try:
                self._ws_app = websocket.WebSocketApp(
                    DEMO_WS_API_BASE_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                    on_ping=self._on_ping,
                )
                self._ws_app.run_forever(ping_interval=0)
            except Exception as exc:
                self.on_error(f"User stream error: {exc}")
            finally:
                self._ws_app = None
            if self._active:
                self.on_status("reconnecting")
                time.sleep(WEBSOCKET_RECONNECT_DELAY_SECONDS)

    def _signature_request(self) -> dict:
        params: dict[str, str | int] = {
            "apiKey": self.api_key,
            "timestamp": int(time.time() * 1000),
            "recvWindow": 5000,
        }
        params["signature"] = sign_ws_api_params(self.api_secret, params)
        self._subscription_request_id = str(uuid.uuid4())
        return {
            "id": self._subscription_request_id,
            "method": "userDataStream.subscribe.signature",
            "params": params,
        }

    def _on_open(self, ws_app) -> None:
        self.on_status("authorizing")
        ws_app.send(json.dumps(self._signature_request(), separators=(",", ":")))

    def _on_message(self, _ws, message: str) -> None:
        try:
            payload = json.loads(message)
        except Exception as exc:
            self.on_error(f"User stream parse error: {exc}")
            return
        if "event" in payload:
            self.on_event(payload)
            return
        status = payload.get("status")
        if payload.get("id") == self._subscription_request_id and status == 200:
            subscription_id = payload.get("result", {}).get("subscriptionId")
            self.on_status("subscribed", subscription_id)
            return
        if status and status != 200:
            error = payload.get("error", {})
            self.on_error(f"User stream response error: {error.get('code', '?')} {error.get('msg', payload)}")

    def _on_error(self, _ws, error) -> None:
        if self._active:
            self.on_error(f"User stream error: {error}")

    def _on_close(self, _ws, _status_code, _message) -> None:
        if self._closing or not self._active:
            self.on_status("stopped")
        else:
            self.on_status("disconnected")

    def _on_ping(self, ws_app, payload) -> None:
        try:
            if getattr(ws_app, "sock", None) is not None:
                ws_app.sock.pong(payload)
        except Exception:
            pass


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Binance Spot Demo GUI")
        self.geometry("1540x960")
        self.minsize(1320, 860)

        self.last_order_id_var = tk.StringVar(value="")
        self.auto_refresh_enabled = tk.BooleanVar(value=False)
        self.auto_refresh_interval_var = tk.StringVar(value="5")
        self.auto_trade_enabled = tk.BooleanVar(value=False)
        self.auto_trade_interval_var = tk.StringVar(value="5")
        self.auto_trade_window_var = tk.StringVar(value="12")
        self.auto_trade_buy_threshold_var = tk.StringVar(value="0.25")
        self.auto_trade_take_profit_var = tk.StringVar(value="0.50")
        self.auto_trade_stop_loss_var = tk.StringVar(value="0.35")
        self.auto_trade_cooldown_var = tk.StringVar(value="30")
        self.auto_trade_auto_select_var = tk.BooleanVar(value=True)
        self.auto_trade_scan_symbols_var = tk.StringVar(value="BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,ADAUSDT,DOGEUSDT")
        self.auto_trade_fee_pct_var = tk.StringVar(value="0.10")
        self.auto_trade_min_net_profit_var = tk.StringVar(value="0.15")
        self.auto_trade_max_spread_var = tk.StringVar(value="0.20")
        self.auto_trade_max_positions_var = tk.StringVar(value="3")
        self.auto_trade_capital_per_position_var = tk.StringVar(value="100")
        self.auto_trade_parallel_var = tk.BooleanVar(value=True)
        self.auto_trade_workers_var = tk.StringVar(value="4")
        self.auto_trade_kline_interval_var = tk.StringVar(value="1m")
        self.auto_trade_kline_limit_var = tk.StringVar(value="120")
        self.auto_trade_min_quote_volume_var = tk.StringVar(value="25000")
        self.auto_trade_min_atr_pct_var = tk.StringVar(value="0.03")
        self.auto_trade_max_atr_pct_var = tk.StringVar(value="2.50")
        self.auto_trade_trend_filter_var = tk.BooleanVar(value=True)
        self.auto_trade_trailing_stop_var = tk.StringVar(value="0.30")
        self.auto_trade_break_even_profit_var = tk.StringVar(value="0.25")
        self.auto_trade_status_var = tk.StringVar(value="Autotrade: OFF")
        self.auto_trade_last_check_var = tk.StringVar(value="Last check: never")
        self.strategy_mode_var = tk.StringVar(value="Mean reversion")
        self.execution_mode_var = tk.StringVar(value="Test first")
        self.price_trigger_condition_var = tk.StringVar(value="ask <=")
        self.price_trigger_value_var = tk.StringVar(value="")
        self.use_risk_sizing_var = tk.BooleanVar(value=False)
        self.risk_deposit_var = tk.StringVar(value="1000")
        self.risk_per_trade_pct_var = tk.StringVar(value="1.0")
        self.risk_stop_mode_var = tk.StringVar(value="Stop %")
        self.risk_stop_value_var = tk.StringVar(value="0.35")
        self.risk_max_daily_loss_pct_var = tk.StringVar(value="3.0")
        self.risk_max_open_orders_var = tk.StringVar(value="1")
        self.risk_max_losing_streak_var = tk.StringVar(value="3")
        self.risk_recommended_qty_var = tk.StringVar(value="Risk qty: manual")
        self.risk_allowed_loss_var = tk.StringVar(value="Max loss/trade: —")
        self.risk_preview_var = tk.StringVar(value="Risk preview: set deposit, risk %, and stop.")
        self.trade_history_interval_var = tk.StringVar(value="1h")
        self.trade_history_limit_var = tk.StringVar(value="50")
        self.health_api_var = tk.StringVar(value="API: unknown")
        self.health_strategy_var = tk.StringVar(value="Strategy: Mean reversion")
        self.health_execution_var = tk.StringVar(value="Execution: Test first")
        self.health_market_ws_var = tk.StringVar(value="Market WS: off")
        self.health_user_ws_var = tk.StringVar(value="User WS: off")
        self.health_position_var = tk.StringVar(value="Position: flat")
        self.health_orders_var = tk.StringVar(value="Open orders: ?")
        self.health_last_fill_var = tk.StringVar(value="Last fill: —")
        self.health_today_var = tk.StringVar(value="Today: 0 fills | P/L 0")
        self.health_risk_var = tk.StringVar(value="Risk: filters enabled")
        self.orders_summary_open_var = tk.StringVar(value="Открытые ордера: ?")
        self.orders_summary_filled_var = tk.StringVar(value="Выполнено ордеров: 0")
        self.orders_summary_profit_var = tk.StringVar(value="Заработок: 0.0000 USDT")
        self.current_open_orders_count = 0
        self.open_orders_by_symbol: dict[str, int] = {}
        self.show_secret_var = tk.BooleanVar(value=False)
        self.pnl_var = tk.StringVar(value="P/L: —")
        self.position_var = tk.StringVar(value="Позиция: —")
        self.auto_refresh_job = None
        self.auto_refresh_in_flight = False
        self.auto_trade_job = None
        self.auto_trade_in_flight = False
        self.auto_trade_price_history: list[Decimal] = []
        self.auto_trade_price_histories: dict[str, list[Decimal]] = {}
        self.auto_trade_entry_price: Decimal | None = None
        self.auto_trade_entry_qty: Decimal | None = None
        self.auto_trade_entry_fee_estimate_quote = ZERO
        self.auto_trade_positions: dict[str, dict] = {}
        self.auto_trade_cooldown_until = 0.0
        self.auto_trade_last_status_text = "Autotrade: OFF"
        self.auto_trade_cycle_count = 0
        self.position_entry_timestamp: str = ""
        self.position_entry_reason: str = ""
        self.position_entry_order_id: str = ""
        self.position_entry_commission: Decimal = ZERO
        self.today_fill_count = 0
        self.today_realized_pnl = ZERO
        self.consecutive_loss_count = 0
        self.today_metrics_date = datetime.now().date().isoformat()
        self.journal_lock = threading.Lock()
        self.market_stream_service: DemoMarketStreamService | None = None
        self.market_stream_symbol = ""
        self.user_stream_service: DemoUserDataStreamService | None = None
        self.user_stream_key = ""
        self.user_stream_subscription_id: int | None = None
        self.latest_book_ticker: dict[str, str | float | Decimal] = {}
        self.user_stream_open_orders: dict[str, set[str]] = {}
        self.market_stream_resubscribe_job = None

        self._build_ui()
        self._center_window()
        self._bind_paste_shortcuts()
        self._bind_log_shortcuts()
        self.strategy_mode_var.trace_add("write", self._on_mode_var_changed)
        self.execution_mode_var.trace_add("write", self._on_mode_var_changed)
        self.symbol_var.trace_add("write", self._on_symbol_var_changed)
        for variable in (
            self.use_risk_sizing_var,
            self.risk_deposit_var,
            self.risk_per_trade_pct_var,
            self.risk_stop_mode_var,
            self.risk_stop_value_var,
            self.risk_max_daily_loss_pct_var,
            self.risk_max_open_orders_var,
            self.risk_max_losing_streak_var,
        ):
            variable.trace_add("write", self._on_risk_var_changed)
        self._load_saved_keys()
        self._load_position_snapshot()
        self._sync_mode_health()
        self._refresh_today_health()
        self._refresh_risk_summary()
        self._write_position_snapshot()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

        self.log_info(
            "Используй только новые demo-ключи. Старые ключи, которые уже попадали в чат, лучше удалить и перевыпустить."
        )

    def _build_ui(self) -> None:
        outer = ttk.Frame(self, padding=12)
        outer.pack(fill="both", expand=True)

        main = ttk.PanedWindow(outer, orient="horizontal")
        main.pack(fill="both", expand=True)

        left = ttk.Frame(main)
        right = ttk.Frame(main)
        main.add(left, weight=3)
        main.add(right, weight=2)

        creds = ttk.LabelFrame(left, text="Подключение и параметры", padding=12)
        creds.pack(fill="x")
        creds.columnconfigure(1, weight=1)
        creds.columnconfigure(2, weight=1)

        ttk.Label(creds, text="API Key").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        self.api_key_var = tk.StringVar()
        self.api_key_entry = tk.Entry(creds, textvariable=self.api_key_var, width=100, font=("Consolas", 10))
        self.api_key_entry.grid(row=0, column=1, columnspan=2, sticky="ew", pady=4)
        ttk.Button(creds, text="Вставить", command=lambda: self.paste_to_var(self.api_key_var, "api")).grid(
            row=0, column=3, sticky="w", pady=4
        )

        ttk.Label(creds, text="Secret Key").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
        self.api_secret_var = tk.StringVar()
        self.api_secret_entry = tk.Entry(creds, textvariable=self.api_secret_var, width=100, font=("Consolas", 10), show="*")
        self.api_secret_entry.grid(row=1, column=1, columnspan=2, sticky="ew", pady=4)

        secret_controls = ttk.Frame(creds)
        secret_controls.grid(row=1, column=3, sticky="w", pady=4)
        ttk.Button(secret_controls, text="Вставить", command=lambda: self.paste_to_var(self.api_secret_var, "secret")).pack(side="left")
        ttk.Checkbutton(
            secret_controls,
            text="Показать",
            variable=self.show_secret_var,
            command=self.toggle_secret_visibility,
        ).pack(side="left", padx=(8, 0))

        helper = ttk.Frame(creds)
        helper.grid(row=2, column=1, columnspan=3, sticky="w", pady=(0, 8))
        ttk.Button(helper, text="Вставить оба ключа из буфера", command=self.paste_both_from_clipboard).pack(side="left")
        ttk.Button(helper, text="Очистить ключи", command=self.clear_keys).pack(side="left", padx=(8, 0))
        ttk.Label(
            helper,
            text="Поддерживается Ctrl+V, Shift+Insert и кнопки «Вставить». Ключи сохраняются в корне рядом со скриптом.",
        ).pack(side="left", padx=(12, 0))

        ttk.Label(creds, text="Symbol").grid(row=3, column=0, sticky="w", padx=(0, 8), pady=4)
        self.symbol_var = tk.StringVar(value="BTCUSDT")
        tk.Entry(creds, textvariable=self.symbol_var, width=20, font=("Consolas", 10)).grid(row=3, column=1, sticky="w", pady=4)

        ttk.Label(creds, text="Quantity").grid(row=3, column=2, sticky="e", padx=(16, 8), pady=4)
        self.quantity_var = tk.StringVar(value="0.001")
        tk.Entry(creds, textvariable=self.quantity_var, width=20, font=("Consolas", 10)).grid(row=3, column=3, sticky="w", pady=4)

        ttk.Label(creds, text="Price").grid(row=4, column=0, sticky="w", padx=(0, 8), pady=4)
        self.price_var = tk.StringVar(value="")
        tk.Entry(creds, textvariable=self.price_var, width=20, font=("Consolas", 10)).grid(row=4, column=1, sticky="w", pady=4)

        ttk.Label(creds, text="Order ID").grid(row=4, column=2, sticky="e", padx=(16, 8), pady=4)
        self.order_id_var = tk.StringVar(value="")
        tk.Entry(creds, textvariable=self.order_id_var, width=28, font=("Consolas", 10)).grid(row=4, column=3, sticky="w", pady=4)

        ttk.Label(
            creds,
            text="Если Price пустой, программа возьмёт лучший ask и поставит BUY-лимит на 0.1% ниже.",
        ).grid(row=5, column=1, columnspan=3, sticky="w", pady=(2, 0))

        actions = ttk.LabelFrame(left, text="Действия", padding=12)
        actions.pack(fill="x", pady=(12, 0))

        self.check_button = ttk.Button(actions, text="Проверить API", command=self.check_api)
        self.check_button.pack(side="left")
        self.test_order_button = ttk.Button(actions, text="TEST order", command=self.send_test_order)
        self.test_order_button.pack(side="left", padx=6)
        self.live_order_button = ttk.Button(actions, text="Реальный DEMO BUY", command=self.send_live_order)
        self.live_order_button.pack(side="left", padx=6)
        self.status_button = ttk.Button(actions, text="Проверить статус", command=self.check_order_status)
        self.status_button.pack(side="left", padx=6)
        self.cancel_button = ttk.Button(actions, text="Отменить ордер", command=self.cancel_order)
        self.cancel_button.pack(side="left", padx=6)
        self.close_sell_button = ttk.Button(actions, text="Закрыть позицию SELL", command=self.close_position_sell)
        self.close_sell_button.pack(side="left", padx=6)
        self.open_orders_button = ttk.Button(actions, text="Открытые ордера", command=self.check_open_orders)
        self.open_orders_button.pack(side="left", padx=6)
        self.copy_log_button = ttk.Button(actions, text="Copy log", command=self.copy_all_log_text)
        self.copy_log_button.pack(side="left", padx=6)
        self.clear_button = ttk.Button(actions, text="Очистить лог", command=self.clear_log)
        self.clear_button.pack(side="left", padx=6)

        auto = ttk.LabelFrame(left, text="Автообновление статуса", padding=12)
        auto.pack(fill="x", pady=(12, 0))

        ttk.Label(auto, text="Интервал").pack(side="left")
        interval_combo = ttk.Combobox(
            auto,
            textvariable=self.auto_refresh_interval_var,
            values=("1", "2", "3", "5", "10", "15", "30", "60"),
            width=8,
            state="readonly",
        )
        interval_combo.pack(side="left", padx=(8, 6))
        ttk.Label(auto, text="сек.").pack(side="left")

        self.auto_status_checkbox = ttk.Checkbutton(
            auto,
            text="Включить автообновление статуса выбранного/последнего ордера",
            variable=self.auto_refresh_enabled,
            command=self.toggle_auto_refresh,
        )
        self.auto_status_checkbox.pack(side="left", padx=(16, 0))

        self.last_order_label = ttk.Label(auto, text="Последний ордер: —")
        self.last_order_label.pack(side="right")

        summary = ttk.LabelFrame(left, text="Мониторинг сделки", padding=12)
        summary.pack(fill="x", pady=(12, 0))

        orders_summary = ttk.LabelFrame(right, text="Сводка ордеров", padding=12)
        orders_summary.pack(fill="x", padx=(12, 0), pady=(0, 12))
        orders_summary.columnconfigure(0, weight=1)
        orders_summary.columnconfigure(1, weight=1)
        orders_summary.columnconfigure(2, weight=1)

        ttk.Label(
            orders_summary,
            textvariable=self.orders_summary_open_var,
            font=("Segoe UI", 10, "bold"),
        ).grid(row=0, column=0, sticky="w", padx=(0, 20), pady=2)
        ttk.Label(
            orders_summary,
            textvariable=self.orders_summary_filled_var,
            font=("Segoe UI", 10, "bold"),
        ).grid(row=0, column=1, sticky="w", padx=(0, 20), pady=2)
        self.orders_summary_profit_label = tk.Label(
            orders_summary,
            textvariable=self.orders_summary_profit_var,
            font=("Segoe UI", 10, "bold"),
            fg=PNL_COLOR_NEUTRAL,
            anchor="w",
        )
        self.orders_summary_profit_label.grid(row=0, column=2, sticky="w", pady=2)

        autotrade = ttk.LabelFrame(left, text="Autotrade (DEMO)", padding=12)
        autotrade.pack(fill="x", pady=(12, 0))
        autotrade.columnconfigure(1, weight=1)
        autotrade.columnconfigure(3, weight=1)
        autotrade.columnconfigure(5, weight=1)
        autotrade.columnconfigure(7, weight=1)

        ttk.Label(autotrade, text="Interval").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Combobox(
            autotrade,
            textvariable=self.auto_trade_interval_var,
            values=("1", "2", "3", "5", "10", "15", "30", "60"),
            width=8,
            state="readonly",
        ).grid(row=0, column=1, sticky="w", pady=4)

        ttk.Label(autotrade, text="SMA window").grid(row=0, column=2, sticky="e", padx=(16, 8), pady=4)
        tk.Entry(autotrade, textvariable=self.auto_trade_window_var, width=10, font=("Consolas", 10)).grid(
            row=0, column=3, sticky="w", pady=4
        )

        ttk.Label(autotrade, text="Buy dip %").grid(row=0, column=4, sticky="e", padx=(16, 8), pady=4)
        tk.Entry(autotrade, textvariable=self.auto_trade_buy_threshold_var, width=10, font=("Consolas", 10)).grid(
            row=0, column=5, sticky="w", pady=4
        )

        ttk.Label(autotrade, text="Cooldown").grid(row=0, column=6, sticky="e", padx=(16, 8), pady=4)
        tk.Entry(autotrade, textvariable=self.auto_trade_cooldown_var, width=10, font=("Consolas", 10)).grid(
            row=0, column=7, sticky="w", pady=4
        )

        ttk.Label(autotrade, text="Take profit %").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
        tk.Entry(autotrade, textvariable=self.auto_trade_take_profit_var, width=10, font=("Consolas", 10)).grid(
            row=1, column=1, sticky="w", pady=4
        )

        ttk.Label(autotrade, text="Stop loss %").grid(row=1, column=2, sticky="e", padx=(16, 8), pady=4)
        tk.Entry(autotrade, textvariable=self.auto_trade_stop_loss_var, width=10, font=("Consolas", 10)).grid(
            row=1, column=3, sticky="w", pady=4
        )

        ttk.Label(
            autotrade,
            text="Strategy: auto-select scans symbols, buys dips below SMA, and sells only when target covers fees/spread buffer. Demo only.",
        ).grid(row=1, column=4, columnspan=4, sticky="w", pady=4)

        ttk.Checkbutton(
            autotrade,
            text="Auto-select",
            variable=self.auto_trade_auto_select_var,
        ).grid(row=2, column=0, sticky="w", pady=4)

        ttk.Label(autotrade, text="Scan symbols").grid(row=2, column=1, sticky="e", padx=(8, 8), pady=4)
        tk.Entry(autotrade, textvariable=self.auto_trade_scan_symbols_var, width=46, font=("Consolas", 10)).grid(
            row=2, column=2, columnspan=4, sticky="ew", pady=4
        )

        ttk.Label(autotrade, text="Fee %").grid(row=2, column=6, sticky="e", padx=(16, 8), pady=4)
        tk.Entry(autotrade, textvariable=self.auto_trade_fee_pct_var, width=10, font=("Consolas", 10)).grid(
            row=2, column=7, sticky="w", pady=4
        )

        ttk.Label(autotrade, text="Min net %").grid(row=3, column=0, sticky="w", padx=(0, 8), pady=4)
        tk.Entry(autotrade, textvariable=self.auto_trade_min_net_profit_var, width=10, font=("Consolas", 10)).grid(
            row=3, column=1, sticky="w", pady=4
        )

        ttk.Label(autotrade, text="Max spread %").grid(row=3, column=2, sticky="e", padx=(16, 8), pady=4)
        tk.Entry(autotrade, textvariable=self.auto_trade_max_spread_var, width=10, font=("Consolas", 10)).grid(
            row=3, column=3, sticky="w", pady=4
        )

        ttk.Label(autotrade, text="Max positions").grid(row=3, column=4, sticky="e", padx=(16, 8), pady=4)
        tk.Entry(autotrade, textvariable=self.auto_trade_max_positions_var, width=10, font=("Consolas", 10)).grid(
            row=3, column=5, sticky="w", pady=4
        )

        ttk.Label(autotrade, text="USDT/position").grid(row=3, column=6, sticky="e", padx=(16, 8), pady=4)
        tk.Entry(autotrade, textvariable=self.auto_trade_capital_per_position_var, width=10, font=("Consolas", 10)).grid(
            row=3, column=7, sticky="w", pady=4
        )

        ttk.Checkbutton(
            autotrade,
            text="Parallel scan",
            variable=self.auto_trade_parallel_var,
        ).grid(row=4, column=0, sticky="w", pady=(10, 0))

        ttk.Label(autotrade, text="Workers").grid(row=4, column=1, sticky="e", padx=(8, 8), pady=(10, 0))
        tk.Entry(autotrade, textvariable=self.auto_trade_workers_var, width=10, font=("Consolas", 10)).grid(
            row=4, column=2, sticky="w", pady=(10, 0)
        )

        ttk.Label(autotrade, text="Klines").grid(row=5, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Combobox(
            autotrade,
            textvariable=self.auto_trade_kline_interval_var,
            values=KLINE_INTERVALS,
            width=8,
            state="readonly",
        ).grid(row=5, column=1, sticky="w", pady=4)

        ttk.Label(autotrade, text="Bars").grid(row=5, column=2, sticky="e", padx=(16, 8), pady=4)
        ttk.Combobox(
            autotrade,
            textvariable=self.auto_trade_kline_limit_var,
            values=("60", "120", "200", "500", "1000"),
            width=8,
            state="readonly",
        ).grid(row=5, column=3, sticky="w", pady=4)

        ttk.Label(autotrade, text="Min vol USDT").grid(row=5, column=4, sticky="e", padx=(16, 8), pady=4)
        tk.Entry(autotrade, textvariable=self.auto_trade_min_quote_volume_var, width=12, font=("Consolas", 10)).grid(
            row=5, column=5, sticky="w", pady=4
        )

        ttk.Checkbutton(
            autotrade,
            text="Trend EMA",
            variable=self.auto_trade_trend_filter_var,
        ).grid(row=5, column=6, columnspan=2, sticky="w", padx=(16, 0), pady=4)

        ttk.Label(autotrade, text="ATR % min/max").grid(row=6, column=0, sticky="w", padx=(0, 8), pady=4)
        atr_controls = ttk.Frame(autotrade)
        atr_controls.grid(row=6, column=1, columnspan=2, sticky="w", pady=4)
        tk.Entry(atr_controls, textvariable=self.auto_trade_min_atr_pct_var, width=8, font=("Consolas", 10)).pack(side="left")
        tk.Entry(atr_controls, textvariable=self.auto_trade_max_atr_pct_var, width=8, font=("Consolas", 10)).pack(
            side="left", padx=(6, 0)
        )

        ttk.Label(autotrade, text="Trail %").grid(row=6, column=3, sticky="e", padx=(16, 8), pady=4)
        tk.Entry(autotrade, textvariable=self.auto_trade_trailing_stop_var, width=10, font=("Consolas", 10)).grid(
            row=6, column=4, sticky="w", pady=4
        )

        ttk.Label(autotrade, text="BE arm %").grid(row=6, column=5, sticky="e", padx=(16, 8), pady=4)
        tk.Entry(autotrade, textvariable=self.auto_trade_break_even_profit_var, width=10, font=("Consolas", 10)).grid(
            row=6, column=6, sticky="w", pady=4
        )

        auto_trade_buttons = ttk.Frame(autotrade)
        auto_trade_buttons.grid(row=7, column=0, columnspan=2, sticky="w", pady=(10, 0))

        self.start_auto_trade_button = ttk.Button(
            auto_trade_buttons,
            text="Start autotrade",
            command=self.start_auto_trading,
        )
        self.start_auto_trade_button.pack(side="left")

        self.stop_auto_trade_button = ttk.Button(
            auto_trade_buttons,
            text="Stop autotrade",
            command=self.stop_auto_trading,
        )
        self.stop_auto_trade_button.pack(side="left", padx=(8, 0))

        self.auto_trade_status_label = tk.Label(
            autotrade,
            textvariable=self.auto_trade_status_var,
            font=("Segoe UI", 10, "bold"),
            anchor="w",
            fg=PNL_COLOR_NEUTRAL,
        )
        self.auto_trade_status_label.grid(row=7, column=2, columnspan=6, sticky="ew", padx=(16, 0), pady=(10, 0))
        self.auto_trade_last_check_label = tk.Label(
            autotrade,
            textvariable=self.auto_trade_last_check_var,
            font=("Consolas", 9),
            anchor="w",
            fg=PNL_COLOR_NEUTRAL,
        )
        self.auto_trade_last_check_label.grid(row=8, column=0, columnspan=8, sticky="ew", pady=(8, 0))
        self._sync_auto_trade_buttons()

        strategy = ttk.LabelFrame(left, text="Strategy & Execution", padding=12)
        strategy.pack(fill="x", pady=(12, 0))
        strategy.columnconfigure(1, weight=1)
        strategy.columnconfigure(3, weight=1)
        strategy.columnconfigure(5, weight=1)

        ttk.Label(strategy, text="Strategy").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Combobox(
            strategy,
            textvariable=self.strategy_mode_var,
            values=("Price trigger", "Mean reversion"),
            width=18,
            state="readonly",
        ).grid(row=0, column=1, sticky="w", pady=4)

        ttk.Label(strategy, text="Execution").grid(row=0, column=2, sticky="e", padx=(16, 8), pady=4)
        ttk.Combobox(
            strategy,
            textvariable=self.execution_mode_var,
            values=("Manual", "Test first", "Live demo"),
            width=18,
            state="readonly",
        ).grid(row=0, column=3, sticky="w", pady=4)

        ttk.Label(strategy, text="Price trigger").grid(row=0, column=4, sticky="e", padx=(16, 8), pady=4)
        trigger_controls = ttk.Frame(strategy)
        trigger_controls.grid(row=0, column=5, sticky="w", pady=4)
        ttk.Combobox(
            trigger_controls,
            textvariable=self.price_trigger_condition_var,
            values=("ask <=", "bid <=", "ask >=", "bid >="),
            width=10,
            state="readonly",
        ).pack(side="left")
        tk.Entry(trigger_controls, textvariable=self.price_trigger_value_var, width=12, font=("Consolas", 10)).pack(
            side="left", padx=(8, 0)
        )

        ttk.Label(
            strategy,
            text="Price trigger buys on the selected market condition. Mean reversion uses the SMA/dip fields above. Manual = signal only.",
        ).grid(row=1, column=0, columnspan=6, sticky="w", pady=(4, 0))

        risk = ttk.LabelFrame(left, text="Risk Manager", padding=12)
        risk.pack(fill="x", pady=(12, 0))
        risk.columnconfigure(1, weight=1)
        risk.columnconfigure(3, weight=1)
        risk.columnconfigure(5, weight=1)

        ttk.Checkbutton(
            risk,
            text="Use risk sizing for BUY",
            variable=self.use_risk_sizing_var,
            command=self._refresh_risk_summary,
        ).grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)

        ttk.Label(risk, text="Deposit").grid(row=0, column=1, sticky="e", padx=(16, 8), pady=4)
        tk.Entry(risk, textvariable=self.risk_deposit_var, width=12, font=("Consolas", 10)).grid(
            row=0, column=2, sticky="w", pady=4
        )

        ttk.Label(risk, text="Risk %").grid(row=0, column=3, sticky="e", padx=(16, 8), pady=4)
        tk.Entry(risk, textvariable=self.risk_per_trade_pct_var, width=12, font=("Consolas", 10)).grid(
            row=0, column=4, sticky="w", pady=4
        )

        ttk.Label(risk, text="Stop").grid(row=0, column=5, sticky="e", padx=(16, 8), pady=4)
        stop_controls = ttk.Frame(risk)
        stop_controls.grid(row=0, column=6, sticky="w", pady=4)
        ttk.Combobox(
            stop_controls,
            textvariable=self.risk_stop_mode_var,
            values=("Stop %", "Stop price"),
            width=12,
            state="readonly",
        ).pack(side="left")
        tk.Entry(stop_controls, textvariable=self.risk_stop_value_var, width=12, font=("Consolas", 10)).pack(
            side="left", padx=(8, 0)
        )

        ttk.Label(risk, text="Max daily loss %").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
        tk.Entry(risk, textvariable=self.risk_max_daily_loss_pct_var, width=12, font=("Consolas", 10)).grid(
            row=1, column=1, sticky="w", pady=4
        )

        ttk.Label(risk, text="Max open orders").grid(row=1, column=2, sticky="e", padx=(16, 8), pady=4)
        tk.Entry(risk, textvariable=self.risk_max_open_orders_var, width=12, font=("Consolas", 10)).grid(
            row=1, column=3, sticky="w", pady=4
        )

        ttk.Label(risk, text="Max losing streak").grid(row=1, column=4, sticky="e", padx=(16, 8), pady=4)
        tk.Entry(risk, textvariable=self.risk_max_losing_streak_var, width=12, font=("Consolas", 10)).grid(
            row=1, column=5, sticky="w", pady=4
        )

        ttk.Button(risk, text="Calculate risk qty", command=self.calculate_risk_quantity).grid(
            row=1, column=6, sticky="w", padx=(16, 0), pady=4
        )

        ttk.Label(risk, textvariable=self.risk_recommended_qty_var).grid(row=2, column=0, columnspan=2, sticky="w", pady=(8, 0))
        ttk.Label(risk, textvariable=self.risk_allowed_loss_var).grid(row=2, column=2, columnspan=2, sticky="w", pady=(8, 0))
        ttk.Label(risk, textvariable=self.risk_preview_var).grid(row=2, column=4, columnspan=3, sticky="w", pady=(8, 0))

        trade_history = ttk.LabelFrame(left, text="Trade History", padding=12)
        trade_history.pack(fill="x", pady=(12, 0))

        ttk.Label(trade_history, text="Interval").pack(side="left")
        ttk.Combobox(
            trade_history,
            textvariable=self.trade_history_interval_var,
            values=tuple(TRADE_HISTORY_INTERVAL_SECONDS.keys()),
            width=8,
            state="readonly",
        ).pack(side="left", padx=(8, 12))

        ttk.Label(trade_history, text="Max rows").pack(side="left")
        ttk.Combobox(
            trade_history,
            textvariable=self.trade_history_limit_var,
            values=("20", "50", "100", "200", "500", "1000"),
            width=8,
            state="readonly",
        ).pack(side="left", padx=(8, 12))

        self.trade_history_button = ttk.Button(
            trade_history,
            text="Load trades",
            command=self.load_trade_history,
        )
        self.trade_history_button.pack(side="left")
        self.backtest_button = ttk.Button(
            trade_history,
            text="Run backtest",
            command=self.run_backtest,
        )
        self.backtest_button.pack(side="left", padx=(8, 0))

        ttk.Label(
            trade_history,
            text="Loads account trades or backtests the current autotrade signal on klines.",
        ).pack(side="left", padx=(12, 0))

        health = ttk.LabelFrame(left, text="Strategy Health", padding=12)
        health.pack(fill="x", pady=(12, 0))
        health.columnconfigure(1, weight=1)
        health.columnconfigure(3, weight=1)

        ttk.Label(health, textvariable=self.health_api_var).grid(row=0, column=0, sticky="w", padx=(0, 24), pady=2)
        ttk.Label(health, textvariable=self.health_strategy_var).grid(row=0, column=1, sticky="w", padx=(0, 24), pady=2)
        ttk.Label(health, textvariable=self.health_execution_var).grid(row=0, column=2, sticky="w", padx=(0, 24), pady=2)
        ttk.Label(health, textvariable=self.health_risk_var).grid(row=0, column=3, sticky="w", pady=2)
        ttk.Label(health, textvariable=self.health_position_var).grid(row=1, column=0, sticky="w", padx=(0, 24), pady=2)
        ttk.Label(health, textvariable=self.health_orders_var).grid(row=1, column=1, sticky="w", padx=(0, 24), pady=2)
        ttk.Label(health, textvariable=self.health_last_fill_var).grid(row=1, column=2, sticky="w", padx=(0, 24), pady=2)
        ttk.Label(health, textvariable=self.health_today_var).grid(row=1, column=3, sticky="w", pady=2)
        ttk.Label(health, textvariable=self.health_market_ws_var).grid(row=2, column=0, sticky="w", padx=(0, 24), pady=2)
        ttk.Label(health, textvariable=self.health_user_ws_var).grid(row=2, column=1, sticky="w", padx=(0, 24), pady=2)

        self.position_label = tk.Label(
            summary,
            textvariable=self.position_var,
            font=("Segoe UI", 10, "bold"),
            anchor="w",
            fg=PNL_COLOR_NEUTRAL,
        )
        self.position_label.pack(side="left")

        self.pnl_label = tk.Label(
            summary,
            textvariable=self.pnl_var,
            font=("Segoe UI", 10, "bold"),
            anchor="w",
            fg=PNL_COLOR_NEUTRAL,
        )
        self.pnl_label.pack(side="left", padx=(24, 0))

        log_box = ttk.LabelFrame(right, text="Лог", padding=12)
        log_box.pack(fill="both", expand=True, padx=(12, 0))

        self.log = tk.Text(
            log_box,
            wrap="word",
            font=("Consolas", 10),
            padx=8,
            pady=8,
            spacing1=1,
            spacing3=4,
            undo=True,
        )
        self.log.pack(side="left", fill="both", expand=True)
        self.log.tag_config("info", foreground=LOG_COLOR_INFO)
        self.log.tag_config("ok", foreground=PNL_COLOR_PROFIT, font=("Consolas", 10, "bold"))
        self.log.tag_config("profit", foreground=PNL_COLOR_PROFIT)
        self.log.tag_config("loss", foreground=PNL_COLOR_LOSS)
        self.log.tag_config("neutral", foreground=PNL_COLOR_NEUTRAL)
        self.log.tag_config("warning", foreground=PNL_COLOR_WARNING, font=("Consolas", 10, "bold"))
        self.log.tag_config("error", foreground=PNL_COLOR_LOSS, font=("Consolas", 10, "bold"))
        self.log.tag_config("data_header", foreground=LOG_COLOR_DATA, font=("Consolas", 10, "bold"))
        self.log.tag_config("data_body", foreground=LOG_COLOR_INFO, lmargin1=18, lmargin2=18)
        self.log.tag_config("data_rule", foreground=LOG_COLOR_RULE)

        scroll = ttk.Scrollbar(log_box, orient="vertical", command=self.log.yview)
        scroll.pack(side="right", fill="y")
        self.log.configure(yscrollcommand=scroll.set)

    def _center_window(self) -> None:
        self.update_idletasks()
        width = max(self.winfo_width(), self.winfo_reqwidth(), 1540)
        height = max(self.winfo_height(), self.winfo_reqheight(), 960)
        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()
        width = min(width, screen_width)
        height = min(height, screen_height)
        x = max((screen_width - width) // 2, 0)
        y = max((screen_height - height) // 2, 0)
        self.geometry(f"{width}x{height}+{x}+{y}")

    def _bind_paste_shortcuts(self) -> None:
        for widget in (self.api_key_entry, self.api_secret_entry):
            widget.bind("<Control-v>", self._paste_event)
            widget.bind("<Control-V>", self._paste_event)
            widget.bind("<Shift-Insert>", self._paste_event)

    def _paste_event(self, event) -> str:
        try:
            text = self.clipboard_get()
            event.widget.delete(0, "end")
            event.widget.insert(0, text)
        except tk.TclError:
            pass
        return "break"

    def _bind_log_shortcuts(self) -> None:
        self.log.bind("<Control-c>", self.copy_selected_log_text)
        self.log.bind("<Control-C>", self.copy_selected_log_text)
        self.log.bind("<Control-a>", self.select_all_log_text)
        self.log.bind("<Control-A>", self.select_all_log_text)
        self.log.bind("<Button-3>", self._show_log_context_menu)

    def _copy_to_clipboard(self, text: str) -> None:
        self.clipboard_clear()
        self.clipboard_append(text)
        self.update_idletasks()

    def copy_selected_log_text(self, event=None) -> str:
        try:
            text = self.log.get("sel.first", "sel.last")
        except tk.TclError:
            self.log_warn("Select text in the log first, or use Copy log to copy everything.")
            return "break"

        self._copy_to_clipboard(text)
        self.log_info("Selected log text copied to clipboard.")
        return "break"

    def copy_all_log_text(self) -> None:
        text = self.log.get("1.0", "end-1c")
        if not text.strip():
            self.log_warn("Log is empty.")
            return
        self._copy_to_clipboard(text)
        self.log_info("Full log copied to clipboard.")

    def select_all_log_text(self, event=None) -> str:
        self.log.tag_add("sel", "1.0", "end-1c")
        self.log.mark_set("insert", "1.0")
        self.log.see("insert")
        return "break"

    def _show_log_context_menu(self, event) -> str:
        menu = tk.Menu(self, tearoff=0)
        menu.add_command(label="Copy selected", command=self.copy_selected_log_text)
        menu.add_command(label="Copy all", command=self.copy_all_log_text)
        menu.add_command(label="Select all", command=self.select_all_log_text)
        menu.tk_popup(event.x_root, event.y_root)
        menu.grab_release()
        return "break"

    def _on_mode_var_changed(self, *_args) -> None:
        self._sync_mode_health()
        self._write_position_snapshot()

    def _on_risk_var_changed(self, *_args) -> None:
        self._refresh_risk_summary()
        self._write_position_snapshot()

    def _sync_mode_health(self) -> None:
        strategy = self.strategy_mode_var.get().strip() or "unknown"
        execution = self.execution_mode_var.get().strip() or "unknown"
        self.health_strategy_var.set(f"Strategy: {strategy}")
        self.health_execution_var.set(f"Execution: {execution}")

    def _validate_risk_config(self) -> dict:
        stop_mode = self.risk_stop_mode_var.get().strip()
        if stop_mode not in ("Stop %", "Stop price"):
            raise BinanceDemoError("Unsupported risk stop mode.")
        return {
            "use_risk_sizing": bool(self.use_risk_sizing_var.get()),
            "deposit": self._validate_decimal_field(self.risk_deposit_var.get(), "Deposit"),
            "risk_pct": self._validate_decimal_field(self.risk_per_trade_pct_var.get(), "Risk %"),
            "stop_mode": stop_mode,
            "stop_value": self._validate_decimal_field(self.risk_stop_value_var.get(), "Risk stop value"),
            "max_daily_loss_pct": self._validate_decimal_field(
                self.risk_max_daily_loss_pct_var.get(),
                "Max daily loss %",
                allow_zero=True,
            ),
            "max_open_orders": self._validate_int_field(
                self.risk_max_open_orders_var.get(),
                "Max open orders",
                minimum=0,
            ),
            "max_losing_streak": self._validate_int_field(
                self.risk_max_losing_streak_var.get(),
                "Max losing streak",
                minimum=0,
            ),
        }

    def _refresh_risk_summary(self) -> None:
        try:
            config = self._validate_risk_config()
        except Exception as exc:
            self.risk_recommended_qty_var.set("Risk qty: —")
            self.risk_allowed_loss_var.set("Max loss/trade: —")
            self.risk_preview_var.set(f"Risk preview: {str(exc)[:90]}")
            return

        mode = "auto" if config["use_risk_sizing"] else "manual"
        max_loss = config["deposit"] * config["risk_pct"] / Decimal("100")
        self.risk_recommended_qty_var.set(f"Risk qty: {mode}")
        self.risk_allowed_loss_var.set(f"Max loss/trade: {format_decimal(max_loss, 2)}")
        self.risk_preview_var.set(
            f"Risk preview: stop={config['stop_mode']} {format_decimal(config['stop_value'], 4)} | "
            f"daily={format_decimal(config['max_daily_loss_pct'], 2)}% | openOrders<={config['max_open_orders']} | "
            f"streak<={config['max_losing_streak']}"
        )

    def _set_health_api(self, message: str) -> None:
        self.health_api_var.set(f"API: {message}")

    def _update_health_orders(self, count: int, symbol: str = "") -> None:
        if symbol:
            self.open_orders_by_symbol[symbol] = count
            self.current_open_orders_count = sum(self.open_orders_by_symbol.values())
        else:
            self.current_open_orders_count = count
        suffix = f" on {symbol}" if symbol else ""
        self.health_orders_var.set(f"Open orders: {count}{suffix}")
        self._refresh_order_summary()

    def _refresh_order_summary(self) -> None:
        self.orders_summary_open_var.set(f"Открытые ордера: {self.current_open_orders_count}")
        self.orders_summary_filled_var.set(f"Выполнено ордеров: {self.today_fill_count}")
        sign = "+" if self.today_realized_pnl > ZERO else ""
        self.orders_summary_profit_var.set(f"Заработок: {sign}{format_decimal_fixed(self.today_realized_pnl, 4)} USDT")
        if hasattr(self, "orders_summary_profit_label"):
            color = PNL_COLOR_NEUTRAL
            if self.today_realized_pnl > ZERO:
                color = PNL_COLOR_PROFIT
            elif self.today_realized_pnl < ZERO:
                color = PNL_COLOR_LOSS
            self.orders_summary_profit_label.configure(fg=color)

    def _update_health_position(
        self,
        symbol: str = "",
        quantity: Decimal | None = None,
        entry_price: Decimal | None = None,
        label: str | None = None,
    ) -> None:
        if label is not None:
            self.health_position_var.set(f"Position: {label}")
        elif self.auto_trade_positions:
            total_cost = sum(
                decimal_or_zero(item.get("entry_price")) * decimal_or_zero(item.get("qty"))
                for item in self.auto_trade_positions.values()
            )
            self.health_position_var.set(
                f"Position: {len(self.auto_trade_positions)} open | cost≈{format_decimal(total_cost, 2)} USDT"
            )
        elif symbol and quantity is not None and quantity > ZERO:
            entry_text = f" | avg={format_decimal(entry_price, 2)}" if entry_price and entry_price > ZERO else ""
            self.health_position_var.set(f"Position: {symbol} | qty={format_decimal(quantity)}{entry_text}")
        else:
            self.health_position_var.set("Position: flat")
        self._write_position_snapshot()

    def _set_health_last_fill(self, message: str) -> None:
        self.health_last_fill_var.set(f"Last fill: {message}")
        self._write_position_snapshot()

    def _roll_today_metrics_if_needed(self) -> None:
        today = datetime.now().date().isoformat()
        if self.today_metrics_date == today:
            return
        self.today_metrics_date = today
        self.today_fill_count = 0
        self.today_realized_pnl = ZERO

    def _refresh_today_health(self) -> None:
        self._roll_today_metrics_if_needed()
        sign = "+" if self.today_realized_pnl >= ZERO else ""
        self.health_today_var.set(
            f"Today: {self.today_fill_count} fills | P/L {sign}{format_decimal(self.today_realized_pnl, 2)} | loss streak {self.consecutive_loss_count}"
        )
        self._refresh_order_summary()

    def _register_fill(self, pnl_quote: Decimal | None = None) -> None:
        self._roll_today_metrics_if_needed()
        self.today_fill_count += 1
        if pnl_quote is not None:
            self.today_realized_pnl += pnl_quote
            if pnl_quote < ZERO:
                self.consecutive_loss_count += 1
            else:
                self.consecutive_loss_count = 0
        self.after(0, self._refresh_today_health)
        self.after(0, self._write_position_snapshot)

    def _quote_asset_for_symbol(self, client: BinanceDemoClient, symbol: str) -> str:
        try:
            return client.get_exchange_info(symbol).get("quoteAsset", "QUOTE")
        except Exception:
            return "QUOTE"

    def _risk_stop_for_entry(self, entry_price: Decimal, config: dict) -> tuple[Decimal, Decimal, str]:
        if entry_price <= ZERO:
            raise BinanceDemoError("Entry price must be greater than zero for risk sizing.")
        if config["stop_mode"] == "Stop %":
            stop_distance = entry_price * config["stop_value"] / Decimal("100")
            stop_price = entry_price - stop_distance
            label = f"{format_decimal(config['stop_value'], 2)}%"
        else:
            stop_price = config["stop_value"]
            if stop_price >= entry_price:
                raise BinanceDemoError("Stop price must be below entry price for BUY sizing.")
            stop_distance = entry_price - stop_price
            label = format_decimal(stop_price, 2)
        if stop_distance <= ZERO or stop_price <= ZERO:
            raise BinanceDemoError("Risk stop produces a non-positive stop distance.")
        return stop_distance, stop_price, label

    def _quantity_filter_for_order(self, symbol_info: dict, order_type: str) -> dict | None:
        if order_type.upper() == "MARKET":
            return self._find_filter(symbol_info, "MARKET_LOT_SIZE") or self._find_filter(symbol_info, "LOT_SIZE")
        return self._find_filter(symbol_info, "LOT_SIZE")

    def _quantity_filters_for_order(self, symbol_info: dict, order_type: str) -> list[dict]:
        filters: list[dict] = []
        lot_filter = self._find_filter(symbol_info, "LOT_SIZE")
        market_filter = self._find_filter(symbol_info, "MARKET_LOT_SIZE")
        if order_type.upper() == "MARKET":
            for item in (lot_filter, market_filter):
                if item and item not in filters:
                    filters.append(item)
        elif lot_filter:
            filters.append(lot_filter)
        return filters

    def _quantity_filter_bounds(self, filters: list[dict]) -> tuple[Decimal, Decimal, Decimal]:
        min_qty = ZERO
        max_qty = ZERO
        step_size = ZERO
        for item in filters:
            item_min = decimal_or_zero(item.get("minQty"))
            item_max = decimal_or_zero(item.get("maxQty"))
            item_step = decimal_or_zero(item.get("stepSize"))
            if item_min > min_qty:
                min_qty = item_min
            if item_max > ZERO:
                max_qty = item_max if max_qty <= ZERO else min(max_qty, item_max)
            if item_step > step_size:
                step_size = item_step
        return min_qty, max_qty, step_size

    def _round_quantity_for_filters(self, quantity: Decimal, filters: list[dict]) -> Decimal:
        _min_qty, max_qty, _step_size = self._quantity_filter_bounds(filters)
        if max_qty > ZERO:
            quantity = min(quantity, max_qty)
        steps = sorted(
            (decimal_or_zero(item.get("stepSize")) for item in filters),
            reverse=True,
        )
        steps = [step for step in steps if step > ZERO]
        for _ in range(max(1, len(steps))):
            previous = quantity
            for step in steps:
                quantity = round_step_down(quantity, step)
            if quantity == previous:
                break
        return quantity

    def _validate_quantity_filters(self, quantity: Decimal, filters: list[dict], field_name: str = "Quantity") -> None:
        for item in filters:
            filter_type = str(item.get("filterType", "LOT_SIZE"))
            min_qty = decimal_or_zero(item.get("minQty"))
            max_qty = decimal_or_zero(item.get("maxQty"))
            step_size = decimal_or_zero(item.get("stepSize"))
            if min_qty > ZERO and quantity < min_qty:
                raise BinanceDemoError(
                    f"{field_name} {format_decimal(quantity)} is below {filter_type}.minQty {format_decimal(min_qty)}."
                )
            if max_qty > ZERO and quantity > max_qty:
                raise BinanceDemoError(
                    f"{field_name} {format_decimal(quantity)} exceeds {filter_type}.maxQty {format_decimal(max_qty)}."
                )
            self._ensure_step_alignment(quantity, step_size, f"{field_name} ({filter_type})")

    def _calculate_risk_position_size(
        self,
        client: BinanceDemoClient,
        symbol: str,
        order_type: str,
        entry_price: Decimal,
    ) -> dict:
        config = self._validate_risk_config()
        symbol_info = client.get_exchange_info(symbol)
        quote_asset = symbol_info.get("quoteAsset", "QUOTE")
        stop_distance, stop_price, stop_label = self._risk_stop_for_entry(entry_price, config)
        allowed_loss = config["deposit"] * config["risk_pct"] / Decimal("100")
        raw_qty = allowed_loss / stop_distance
        max_affordable_qty = config["deposit"] / entry_price
        bounded_qty = min(raw_qty, max_affordable_qty)
        qty_filters = self._quantity_filters_for_order(symbol_info, order_type)
        rounded_qty = self._round_quantity_for_filters(bounded_qty, qty_filters)
        if rounded_qty <= ZERO:
            raise BinanceDemoError("Risk sizing rounded quantity down to zero.")
        self._validate_quantity_filters(rounded_qty, qty_filters, "Risk quantity")
        notional = rounded_qty * entry_price
        self._validate_notional_filters(symbol_info, notional, is_market=order_type.upper() == "MARKET")

        rr_ratio = ZERO
        try:
            take_profit_pct = self._validate_decimal_field(self.auto_trade_take_profit_var.get(), "Take profit %")
            reward_distance = entry_price * take_profit_pct / Decimal("100")
            if stop_distance > ZERO:
                rr_ratio = reward_distance / stop_distance
        except Exception:
            rr_ratio = ZERO

        return {
            "config": config,
            "symbol_info": symbol_info,
            "quote_asset": quote_asset,
            "entry_price": entry_price,
            "stop_distance": stop_distance,
            "stop_price": stop_price,
            "stop_label": stop_label,
            "allowed_loss": allowed_loss,
            "raw_qty": raw_qty,
            "max_affordable_qty": max_affordable_qty,
            "quantity": rounded_qty,
            "notional": notional,
            "rr_ratio": rr_ratio,
        }

    def _apply_risk_model_preview(self, model: dict, set_quantity: bool = False) -> None:
        qty_text = format_decimal(model["quantity"])
        allowed_loss = model["allowed_loss"]
        quote_asset = model["quote_asset"]
        rr_ratio = model["rr_ratio"]
        rr_text = f" | R/R {format_decimal(rr_ratio, 2)}" if rr_ratio > ZERO else ""
        self.risk_recommended_qty_var.set(f"Risk qty: {qty_text}")
        self.risk_allowed_loss_var.set(
            f"Max loss/trade: {format_decimal(allowed_loss, 2)} {quote_asset}"
        )
        self.risk_preview_var.set(
            f"Risk preview: entry={format_decimal(model['entry_price'], 2)} | stop={format_decimal(model['stop_price'], 2)} | "
            f"notional={format_decimal(model['notional'], 2)} {quote_asset}{rr_text}"
        )
        self.health_risk_var.set(
            f"Risk: OK | qty={qty_text} | maxLoss={format_decimal(allowed_loss, 2)} {quote_asset} | stop={format_decimal(model['stop_price'], 2)}"
        )
        if set_quantity:
            self.quantity_var.set(qty_text)

    def _all_open_orders(self, client: BinanceDemoClient) -> list:
        symbols = {self.symbol_var.get().strip().upper()}
        symbols.update(self.auto_trade_positions.keys())
        raw_symbols = self.auto_trade_scan_symbols_var.get().replace("\n", ",")
        for item in raw_symbols.split(","):
            symbol = item.strip().upper()
            if symbol:
                symbols.add(symbol)

        orders: list = []
        errors: list[str] = []
        for symbol in sorted(item for item in symbols if item):
            try:
                orders.extend(client.get_open_orders_retry(symbol=symbol))
            except BinanceDemoError as exc:
                errors.append(f"{symbol}: {exc}")
        if errors:
            self.after(
                0,
                lambda text="; ".join(errors[:3]): self.log_warn(
                    f"Open-orders check skipped some symbols after retries: {text}"
                ),
            )
        return orders

    def _enforce_risk_limits(self, client: BinanceDemoClient, symbol: str) -> dict:
        config = self._validate_risk_config()
        quote_asset = self._quote_asset_for_symbol(client, symbol)
        if config["max_daily_loss_pct"] > ZERO:
            daily_limit = config["deposit"] * config["max_daily_loss_pct"] / Decimal("100")
            if self.today_realized_pnl <= -daily_limit:
                message = (
                    f"Risk lock: daily loss limit hit ({format_decimal(self.today_realized_pnl, 2)} {quote_asset} <= -{format_decimal(daily_limit, 2)} {quote_asset})."
                )
                self.after(0, lambda text=message: self.health_risk_var.set(text))
                raise BinanceDemoError(
                    message
                )
        if config["max_losing_streak"] > 0 and self.consecutive_loss_count >= config["max_losing_streak"]:
            message = (
                f"Risk lock: losing streak {self.consecutive_loss_count} reached the limit {config['max_losing_streak']}."
            )
            self.after(0, lambda text=message: self.health_risk_var.set(text))
            raise BinanceDemoError(
                message
            )
        if config["max_open_orders"] > 0:
            open_orders = self._all_open_orders(client)
            if len(open_orders) >= config["max_open_orders"]:
                message = (
                    f"Risk lock: open orders {len(open_orders)} reached the limit {config['max_open_orders']}."
                )
                self.after(0, lambda text=message: self.health_risk_var.set(text))
                raise BinanceDemoError(
                    message
                )
        self.after(
            0,
            lambda qa=quote_asset: self.health_risk_var.set(
                f"Risk: limits OK | daily P/L={format_decimal(self.today_realized_pnl, 2)} {qa} | streak={self.consecutive_loss_count}"
            ),
        )
        return config

    def _position_snapshot_payload(self) -> dict:
        return {
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "symbol": self.symbol_var.get().strip().upper(),
            "strategy_mode": self.strategy_mode_var.get().strip(),
            "execution_mode": self.execution_mode_var.get().strip(),
            "auto_trade_enabled": bool(self.auto_trade_enabled.get()),
            "risk": {
                "use_risk_sizing": bool(self.use_risk_sizing_var.get()),
                "deposit": self.risk_deposit_var.get().strip(),
                "risk_pct": self.risk_per_trade_pct_var.get().strip(),
                "stop_mode": self.risk_stop_mode_var.get().strip(),
                "stop_value": self.risk_stop_value_var.get().strip(),
                "max_daily_loss_pct": self.risk_max_daily_loss_pct_var.get().strip(),
                "max_open_orders": self.risk_max_open_orders_var.get().strip(),
                "max_losing_streak": self.risk_max_losing_streak_var.get().strip(),
                "recommended_qty": self.risk_recommended_qty_var.get(),
                "preview": self.risk_preview_var.get(),
            },
            "metrics": {
                "today_fill_count": self.today_fill_count,
                "today_realized_pnl": format_decimal(self.today_realized_pnl, 2),
                "today_metrics_date": self.today_metrics_date,
                "consecutive_loss_count": self.consecutive_loss_count,
            },
            "position": {
                "entry_price": format_decimal(self.auto_trade_entry_price) if self.auto_trade_entry_price else "",
                "entry_qty": format_decimal(self.auto_trade_entry_qty) if self.auto_trade_entry_qty else "",
                "entry_timestamp": self.position_entry_timestamp,
                "entry_reason": self.position_entry_reason,
                "entry_order_id": self.position_entry_order_id,
                "entry_commission": format_decimal(self.position_entry_commission),
            },
            "positions": {
                symbol: {
                    "entry_price": format_decimal(decimal_or_zero(position.get("entry_price"))),
                    "qty": format_decimal(decimal_or_zero(position.get("qty"))),
                    "entry_timestamp": str(position.get("entry_timestamp", "")),
                    "entry_reason": str(position.get("entry_reason", "")),
                    "entry_order_id": str(position.get("entry_order_id", "")),
                    "entry_fee_estimate_quote": format_decimal(
                        decimal_or_zero(position.get("entry_fee_estimate_quote"))
                    ),
                    "peak_price": format_decimal(decimal_or_zero(position.get("peak_price"))),
                    "trailing_stop_price": format_decimal(decimal_or_zero(position.get("trailing_stop_price"))),
                    "break_even_armed": bool(position.get("break_even_armed", False)),
                    "break_even_price": format_decimal(decimal_or_zero(position.get("break_even_price"))),
                }
                for symbol, position in sorted(self.auto_trade_positions.items())
            },
            "health": {
                "api": self.health_api_var.get(),
                "orders": self.health_orders_var.get(),
                "position": self.health_position_var.get(),
                "last_fill": self.health_last_fill_var.get(),
                "today": self.health_today_var.get(),
                "risk": self.health_risk_var.get(),
            },
        }

    def _write_position_snapshot(self) -> None:
        payload = self._position_snapshot_payload()
        with self.journal_lock:
            POSITIONS_JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _load_position_snapshot(self) -> None:
        if not POSITIONS_JSON_PATH.exists():
            return
        try:
            payload = json.loads(POSITIONS_JSON_PATH.read_text(encoding="utf-8"))
        except Exception as exc:
            self.after(0, lambda e=exc: self.log_warn(f"Could not load positions.json: {e}"))
            return

        metrics = payload.get("metrics", {}) if isinstance(payload, dict) else {}
        try:
            saved_date = str(metrics.get("today_metrics_date", ""))
            if saved_date == self.today_metrics_date:
                self.today_fill_count = int(metrics.get("today_fill_count", 0) or 0)
                self.today_realized_pnl = decimal_or_zero(metrics.get("today_realized_pnl"))
                self.consecutive_loss_count = int(metrics.get("consecutive_loss_count", 0) or 0)
        except Exception:
            pass

        positions = payload.get("positions", {}) if isinstance(payload, dict) else {}
        if isinstance(positions, dict):
            for symbol, position in positions.items():
                if not isinstance(position, dict):
                    continue
                normalized_symbol = str(symbol).strip().upper()
                entry_price = decimal_or_zero(position.get("entry_price"))
                qty = decimal_or_zero(position.get("qty"))
                if not normalized_symbol or entry_price <= ZERO or qty <= ZERO:
                    continue
                notional = entry_price * qty
                if notional < MIN_RESTORED_POSITION_NOTIONAL:
                    self.after(
                        0,
                        lambda s=normalized_symbol, n=notional: self.log_warn(
                            f"Skipped restored dust position {s}: notional≈{format_decimal(n, 4)} USDT."
                        ),
                    )
                    continue
                self.auto_trade_positions[normalized_symbol] = {
                    "entry_price": entry_price,
                    "qty": qty,
                    "entry_timestamp": str(position.get("entry_timestamp", "")),
                    "entry_reason": str(position.get("entry_reason", "Restored from positions.json")),
                    "entry_order_id": str(position.get("entry_order_id", "")),
                    "entry_fee_estimate_quote": decimal_or_zero(position.get("entry_fee_estimate_quote")),
                    "peak_price": max(decimal_or_zero(position.get("peak_price")), entry_price),
                    "trailing_stop_price": decimal_or_zero(position.get("trailing_stop_price")),
                    "break_even_armed": self._position_flag_enabled(position.get("break_even_armed")),
                    "break_even_price": decimal_or_zero(position.get("break_even_price")),
                }

        if self.auto_trade_positions:
            first_symbol, first_position = next(iter(self.auto_trade_positions.items()))
            self.auto_trade_entry_price = decimal_or_zero(first_position.get("entry_price"))
            self.auto_trade_entry_qty = decimal_or_zero(first_position.get("qty"))
            self.position_entry_timestamp = str(first_position.get("entry_timestamp", ""))
            self.position_entry_reason = str(first_position.get("entry_reason", "Restored from positions.json"))
            self.position_entry_order_id = str(first_position.get("entry_order_id", ""))
            self.auto_trade_entry_fee_estimate_quote = decimal_or_zero(first_position.get("entry_fee_estimate_quote"))
            self.after(
                0,
                lambda count=len(self.auto_trade_positions): self.log_warn(
                    f"Restored {count} autotrade position(s) from positions.json."
                ),
            )
            self.after(0, lambda: self._update_health_position(label=None))

    def _append_jsonl(self, path: Path, payload: dict) -> None:
        with self.journal_lock:
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def _append_trade_journal_row(self, row: dict) -> None:
        fieldnames = [
            "timestamp",
            "symbol",
            "side",
            "event",
            "reason",
            "order_id",
            "status",
            "price",
            "executed_qty",
            "quote_qty",
            "pnl_quote",
            "commission",
            "commission_asset",
            "strategy_mode",
            "execution_mode",
            "entry_order_id",
            "entry_reason",
        ]
        with self.journal_lock:
            file_exists = TRADE_JOURNAL_CSV_PATH.exists()
            with TRADE_JOURNAL_CSV_PATH.open("a", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()
                writer.writerow({name: row.get(name, "") for name in fieldnames})

    def _extract_commission_details(self, order: dict) -> tuple[Decimal, str]:
        commissions: dict[str, Decimal] = {}
        for fill in order.get("fills", []):
            asset = str(fill.get("commissionAsset", "")).strip()
            if not asset:
                continue
            commissions[asset] = commissions.get(asset, ZERO) + decimal_or_zero(fill.get("commission"))
        if not commissions:
            return ZERO, ""
        if len(commissions) == 1:
            asset, amount = next(iter(commissions.items()))
            return amount, asset
        asset_text = ",".join(sorted(commissions.keys()))
        total = sum(commissions.values(), ZERO)
        return total, asset_text

    def _journal_order_event(self, event_type: str, symbol: str, reason: str, order: dict, extra: dict | None = None) -> None:
        payload = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "event_type": event_type,
            "reason": reason,
            "symbol": symbol,
            "strategy_mode": self.strategy_mode_var.get().strip(),
            "execution_mode": self.execution_mode_var.get().strip(),
            "order": order,
        }
        if extra:
            payload["extra"] = extra
        self._append_jsonl(ORDER_EVENTS_JSONL_PATH, payload)

    def _journal_trade_fill(
        self,
        symbol: str,
        side: str,
        event: str,
        reason: str,
        order: dict,
        pnl_quote: Decimal | None = None,
    ) -> None:
        executed_qty = decimal_or_zero(order.get("executedQty"))
        if executed_qty <= ZERO:
            return
        commission_amount, commission_asset = self._extract_commission_details(order)
        average_price = self._extract_average_fill_price(order, ZERO)
        self._append_trade_journal_row(
            {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "symbol": symbol,
                "side": side,
                "event": event,
                "reason": reason,
                "order_id": str(order.get("orderId", "")),
                "status": str(order.get("status", "")),
                "price": format_decimal(average_price),
                "executed_qty": format_decimal(executed_qty),
                "quote_qty": format_decimal(decimal_or_zero(order.get("cummulativeQuoteQty"))),
                "pnl_quote": format_decimal(pnl_quote, 2) if pnl_quote is not None else "",
                "commission": format_decimal(commission_amount),
                "commission_asset": commission_asset,
                "strategy_mode": self.strategy_mode_var.get().strip(),
                "execution_mode": self.execution_mode_var.get().strip(),
                "entry_order_id": self.position_entry_order_id,
                "entry_reason": self.position_entry_reason,
            }
        )

    def _risk_entry_price(self, client: BinanceDemoClient, symbol: str, order_type: str, price_text: str | None = None) -> Decimal:
        if order_type.upper() == "LIMIT":
            if price_text is None:
                price_text = self._resolved_order_price(client, symbol)
            return decimal_or_zero(price_text)
        _bid, ask, _mid = self._current_market_prices(client, symbol)
        return ask

    def _resolve_buy_quantity(
        self,
        client: BinanceDemoClient,
        symbol: str,
        order_type: str,
        price_text: str | None = None,
        apply_risk_to_field: bool = True,
    ) -> str:
        if not self.use_risk_sizing_var.get():
            quantity = self._validate_quantity()
            self.after(0, self._refresh_risk_summary)
            return quantity
        entry_price = self._risk_entry_price(client, symbol, order_type, price_text=price_text)
        model = self._calculate_risk_position_size(client, symbol, order_type, entry_price)
        self.after(0, lambda m=model, apply=apply_risk_to_field: self._apply_risk_model_preview(m, set_quantity=apply))
        return format_decimal(model["quantity"])

    def calculate_risk_quantity(self) -> None:
        self._run_async("Risk sizing calculation", self._calculate_risk_quantity_impl)

    def _calculate_risk_quantity_impl(self) -> None:
        client = self._client()
        symbol = self.symbol_var.get().strip().upper()
        if not symbol:
            raise BinanceDemoError("Set a symbol before calculating risk quantity.")
        order_type = "LIMIT" if self.price_var.get().strip() else "MARKET"
        entry_price = self._risk_entry_price(client, symbol, order_type, price_text=self.price_var.get().strip() or None)
        model = self._calculate_risk_position_size(client, symbol, order_type, entry_price)
        quantity = format_decimal(model["quantity"])
        self.after(0, lambda m=model: self._apply_risk_model_preview(m, set_quantity=True))
        self.after(
            0,
            lambda s=symbol, q=quantity, kind=order_type: self.log_ok(
                f"Risk quantity calculated: {s} | type={kind} | qty={q}"
            ),
        )

    def _on_symbol_var_changed(self, *_args) -> None:
        if self.market_stream_service is None or not self.market_stream_service.is_running():
            return
        if self.market_stream_resubscribe_job is not None:
            try:
                self.after_cancel(self.market_stream_resubscribe_job)
            except Exception:
                pass
        self.market_stream_resubscribe_job = self.after(500, self._restart_market_stream_for_current_symbol)

    def _ensure_websocket_support(self) -> None:
        if websocket is None:
            raise BinanceDemoError("Package websocket-client is not installed. Run: pip install websocket-client")

    def _market_prices_cached(self, symbol: str) -> tuple[Decimal, Decimal, Decimal] | None:
        cached_symbol = str(self.latest_book_ticker.get("symbol", "")).upper()
        if cached_symbol != symbol.upper():
            return None
        updated_at = float(self.latest_book_ticker.get("updated_at", 0.0) or 0.0)
        if not updated_at or time.time() - updated_at > 15:
            return None
        bid = decimal_or_zero(self.latest_book_ticker.get("bid"))
        ask = decimal_or_zero(self.latest_book_ticker.get("ask"))
        if bid <= ZERO or ask <= ZERO:
            return None
        return bid, ask, (bid + ask) / Decimal("2")

    def _current_market_prices(self, client: BinanceDemoClient, symbol: str) -> tuple[Decimal, Decimal, Decimal]:
        cached = self._market_prices_cached(symbol)
        if cached is not None:
            return cached
        book = client.get_book_ticker(symbol)
        bid, ask, mid = self._market_prices(book)
        self.latest_book_ticker = {
            "symbol": symbol,
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "updated_at": time.time(),
            "source": "rest",
        }
        return bid, ask, mid

    def _restart_market_stream_for_current_symbol(self) -> None:
        self.market_stream_resubscribe_job = None
        symbol = self.symbol_var.get().strip().upper()
        if not symbol:
            return
        self._start_market_stream(force=True)

    def _start_market_stream(self, force: bool = False) -> None:
        self._ensure_websocket_support()
        symbol = self.symbol_var.get().strip().upper()
        if not symbol:
            raise BinanceDemoError("Set a symbol before starting market stream.")
        if (
            not force
            and self.market_stream_service is not None
            and self.market_stream_service.is_running()
            and self.market_stream_symbol == symbol
        ):
            return
        self._stop_market_stream(log_message=False)
        service = DemoMarketStreamService(
            symbol=symbol,
            on_status=lambda status, stream_symbol: self.after(
                0, lambda s=status, sym=stream_symbol: self._handle_market_stream_status(s, sym)
            ),
            on_book_ticker=lambda stream_symbol, payload: self.after(
                0, lambda sym=stream_symbol, data=payload: self._handle_market_book_ticker(sym, data)
            ),
            on_error=lambda message: self.after(0, lambda text=message: self._handle_market_stream_error(text)),
        )
        self.market_stream_service = service
        self.market_stream_symbol = symbol
        service.start()

    def _stop_market_stream(self, log_message: bool = True) -> None:
        if self.market_stream_resubscribe_job is not None:
            try:
                self.after_cancel(self.market_stream_resubscribe_job)
            except Exception:
                pass
            self.market_stream_resubscribe_job = None
        service = self.market_stream_service
        self.market_stream_service = None
        self.market_stream_symbol = ""
        if service is not None:
            service.stop()
        self.health_market_ws_var.set("Market WS: off")
        if log_message:
            self.log_info("Market WebSocket stopped.")

    def _start_user_data_stream(self, force: bool = False) -> None:
        self._ensure_websocket_support()
        api_key = self.api_key_var.get().strip()
        api_secret = self.api_secret_var.get().strip()
        if not api_key or not api_secret:
            raise BinanceDemoError("API Key and Secret Key are required for user data stream.")
        if (
            not force
            and self.user_stream_service is not None
            and self.user_stream_service.is_running()
            and self.user_stream_key == api_key
        ):
            return
        self._stop_user_data_stream(log_message=False)
        service = DemoUserDataStreamService(
            api_key=api_key,
            api_secret=api_secret,
            on_status=lambda status, subscription_id=None: self.after(
                0, lambda s=status, sid=subscription_id: self._handle_user_stream_status(s, sid)
            ),
            on_event=lambda payload: self.after(0, lambda data=payload: self._handle_user_stream_payload(data)),
            on_error=lambda message: self.after(0, lambda text=message: self._handle_user_stream_error(text)),
        )
        self.user_stream_service = service
        self.user_stream_key = api_key
        service.start()

    def _stop_user_data_stream(self, log_message: bool = True) -> None:
        service = self.user_stream_service
        self.user_stream_service = None
        self.user_stream_key = ""
        self.user_stream_subscription_id = None
        if service is not None:
            service.stop()
        self.health_user_ws_var.set("User WS: off")
        if log_message:
            self.log_info("User data WebSocket stopped.")

    def _ensure_demo_streams(self) -> None:
        self._start_market_stream(force=False)
        self._start_user_data_stream(force=False)

    def _handle_market_stream_status(self, status: str, symbol: str) -> None:
        label = f"Market WS: {status} | {symbol}"
        self.health_market_ws_var.set(label)
        if status == "connected":
            self.log_ok(f"Market WebSocket connected for {symbol}.")
        elif status == "reconnecting":
            self.log_warn(f"Market WebSocket reconnecting for {symbol}...")
        elif status == "disconnected":
            self.log_warn(f"Market WebSocket disconnected for {symbol}.")
        elif status == "stopped":
            self.log_info(f"Market WebSocket stopped for {symbol}.")

    def _handle_market_stream_error(self, message: str) -> None:
        self.health_market_ws_var.set("Market WS: error")
        self.log_warn(message)

    def _handle_market_book_ticker(self, symbol: str, payload: dict) -> None:
        bid = decimal_or_zero(payload.get("b") or payload.get("bidPrice"))
        ask = decimal_or_zero(payload.get("a") or payload.get("askPrice"))
        if bid <= ZERO or ask <= ZERO:
            return
        mid = (bid + ask) / Decimal("2")
        self.latest_book_ticker = {
            "symbol": symbol,
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "updated_at": time.time(),
            "source": "ws",
        }
        self.health_market_ws_var.set(
            f"Market WS: live | {symbol} | bid={format_decimal(bid, 2)} | ask={format_decimal(ask, 2)}"
        )

    def _handle_user_stream_status(self, status: str, subscription_id: int | None = None) -> None:
        if status == "subscribed":
            self.user_stream_subscription_id = subscription_id
            self.health_user_ws_var.set(f"User WS: live | subscription={subscription_id}")
            self.log_ok(f"User data WebSocket subscribed. subscriptionId={subscription_id}")
            return
        self.health_user_ws_var.set(f"User WS: {status}")
        if status == "connecting":
            self.log_info("User data WebSocket connecting...")
        elif status == "authorizing":
            self.log_info("User data WebSocket authorizing...")
        elif status == "reconnecting":
            self.log_warn("User data WebSocket reconnecting...")
        elif status == "disconnected":
            self.log_warn("User data WebSocket disconnected.")
        elif status == "stopped":
            self.log_info("User data WebSocket stopped.")

    def _handle_user_stream_error(self, message: str) -> None:
        self.health_user_ws_var.set("User WS: error")
        self.log_warn(message)

    def _current_symbol_open_order_count(self) -> int:
        symbol = self.symbol_var.get().strip().upper()
        return len(self.user_stream_open_orders.get(symbol, set()))

    def _handle_user_stream_payload(self, payload: dict) -> None:
        event = payload.get("event", {})
        event_type = str(event.get("e", ""))
        if not event_type:
            return
        if event_type == "executionReport":
            self._handle_execution_report_event(event, payload.get("subscriptionId"))
        elif event_type == "outboundAccountPosition":
            self.log_info("[USER-STREAM] Account balances updated.")
        elif event_type == "balanceUpdate":
            asset = event.get("a", "")
            delta = event.get("d", "")
            self.log_info(f"[USER-STREAM] Balance update: {asset} delta={delta}")

    def _handle_execution_report_event(self, event: dict, subscription_id: int | None) -> None:
        symbol = str(event.get("s", "")).upper()
        if not symbol:
            return
        order_id = str(event.get("i", ""))
        side = str(event.get("S", "")).upper()
        status = str(event.get("X", "")).upper()
        execution_type = str(event.get("x", "")).upper()
        last_qty = decimal_or_zero(event.get("l"))
        cumulative_qty = decimal_or_zero(event.get("z"))
        cumulative_quote = decimal_or_zero(event.get("Z"))
        last_price = decimal_or_zero(event.get("L"))
        commission = decimal_or_zero(event.get("n"))
        commission_asset = str(event.get("N") or "")

        if status in {"NEW", "PARTIALLY_FILLED"}:
            self.user_stream_open_orders.setdefault(symbol, set()).add(order_id)
        elif order_id:
            self.user_stream_open_orders.setdefault(symbol, set()).discard(order_id)

        self._update_health_orders(len(self.user_stream_open_orders.get(symbol, set())), symbol)
        if symbol == self.symbol_var.get().strip().upper():
            self._set_last_order_id(order_id)

        average_price = ZERO
        if cumulative_qty > ZERO and cumulative_quote > ZERO:
            average_price = cumulative_quote / cumulative_qty
        elif last_price > ZERO:
            average_price = last_price

        if side == "BUY" and cumulative_qty > ZERO:
            self.auto_trade_entry_price = average_price if average_price > ZERO else self.auto_trade_entry_price
            self.auto_trade_entry_qty = cumulative_qty
            self.position_entry_order_id = order_id
            self.position_entry_timestamp = self.position_entry_timestamp or datetime.now().isoformat(timespec="seconds")
            if not self.position_entry_reason:
                self.position_entry_reason = "User stream sync"
            if average_price > ZERO:
                self.auto_trade_positions[symbol] = {
                    "entry_price": average_price,
                    "qty": cumulative_qty,
                    "entry_timestamp": datetime.now().isoformat(timespec="seconds"),
                    "entry_reason": self.position_entry_reason,
                    "entry_order_id": order_id,
                    "entry_fee_estimate_quote": ZERO,
                    "peak_price": average_price,
                    "trailing_stop_price": ZERO,
                    "break_even_armed": False,
                    "break_even_price": ZERO,
                }
            if symbol == self.symbol_var.get().strip().upper():
                self._update_health_position(symbol, cumulative_qty, self.auto_trade_entry_price)

        if execution_type == "TRADE" and last_qty > ZERO:
            fill_text = (
                f"{side} {symbol} | lastQty={format_decimal(last_qty)} | lastPrice={format_decimal(last_price, 2)} | "
                f"status={status}"
            )
            if commission_asset:
                fill_text += f" | fee={format_decimal(commission)} {commission_asset}"
            self._set_health_last_fill(fill_text)
            self.log_info(f"[USER-STREAM] {fill_text}")

        if side == "SELL" and status == "FILLED":
            self.auto_trade_entry_qty = None
            self.auto_trade_entry_price = None
            self.position_entry_timestamp = ""
            self.position_entry_reason = ""
            self.position_entry_order_id = ""
            self.position_entry_commission = ZERO
            self.auto_trade_positions.pop(symbol, None)
            if symbol == self.symbol_var.get().strip().upper():
                self._update_health_position(label=None)

        if subscription_id is not None:
            self.health_user_ws_var.set(f"User WS: live | subscription={subscription_id} | last={execution_type}/{status}")

    def _load_saved_keys(self) -> None:
        if not KEYSTORE_PATH.exists():
            return
        try:
            data = json.loads(KEYSTORE_PATH.read_text(encoding="utf-8"))
        except Exception as exc:
            self.log_warn(f"Не удалось прочитать сохранённые ключи из {KEYSTORE_PATH.name}: {exc}")
            return

        self.api_key_var.set(str(data.get("api_key", "")))
        self.api_secret_var.set(str(data.get("secret_key", "")))
        self.log_info(f"Ключи автоматически загружены из {KEYSTORE_PATH.name}")

    def _save_keys_to_file(self) -> None:
        payload = {
            "api_key": self.api_key_var.get().strip(),
            "secret_key": self.api_secret_var.get().strip(),
            "saved_at": datetime.now().isoformat(timespec="seconds"),
        }
        KEYSTORE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        self.log_ok(f"Ключи сохранены в {KEYSTORE_PATH}")

    def _delete_keys_file(self) -> None:
        if KEYSTORE_PATH.exists():
            KEYSTORE_PATH.unlink()
            self.log_info(f"Файл с ключами удалён: {KEYSTORE_PATH}")

    def paste_to_var(self, variable: tk.StringVar, kind: str) -> None:
        try:
            variable.set(self.clipboard_get())
        except tk.TclError:
            self.log_warn("Буфер обмена пуст или недоступен.")
            return

        self._save_keys_to_file()
        label = "API Key" if kind == "api" else "Secret Key"
        self.log_ok(f"{label} вставлен и сохранён.")

    def paste_both_from_clipboard(self) -> None:
        try:
            raw = self.clipboard_get()
        except tk.TclError:
            self.log_warn("Буфер обмена пуст или недоступен.")
            return

        lines = [line.strip() for line in raw.splitlines() if line.strip()]
        if len(lines) < 2:
            self.log_warn("В буфере нужно 2 строки: API Key и Secret Key.")
            return

        self.api_key_var.set(lines[0])
        self.api_secret_var.set(lines[1])
        self._save_keys_to_file()
        self.log_ok("API Key и Secret Key вставлены из буфера и сохранены.")

    def toggle_secret_visibility(self) -> None:
        self.api_secret_entry.configure(show="" if self.show_secret_var.get() else "*")

    def clear_keys(self) -> None:
        self.api_key_var.set("")
        self.api_secret_var.set("")
        self._stop_user_data_stream(log_message=False)
        self._delete_keys_file()
        self.log_info("Поля API Key и Secret Key очищены.")

    def _client(self) -> BinanceDemoClient:
        return BinanceDemoClient(api_key=self.api_key_var.get(), api_secret=self.api_secret_var.get())

    def _timestamp(self) -> str:
        return datetime.now().strftime("%H:%M:%S")

    def _log_tag_for_level(self, level: str) -> str:
        return {
            "INFO": "info",
            "OK": "ok",
            "WARN": "warning",
            "ERROR": "error",
            "P/L": "neutral",
        }.get(level, "info")

    def _log(self, level: str, message: str, tag: str | None = None) -> None:
        prefix = f"[{self._timestamp()}] {level:<5} | "
        resolved_tag = tag or self._log_tag_for_level(level)
        self.log.insert("end", prefix + message + "\n", resolved_tag)
        self.log.see("end")

    def log_info(self, message: str) -> None:
        self._log("INFO", message, "info")

    def log_ok(self, message: str) -> None:
        self._log("OK", message, "ok")

    def log_warn(self, message: str) -> None:
        self._log("WARN", message, "warning")

    def log_error(self, message: str) -> None:
        self._log("ERROR", message, "error")

    def log_block(self, title: str, payload: dict | list | str) -> None:
        if not isinstance(payload, str):
            payload = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        payload = payload.rstrip()
        rule = "-" * 96
        self.log.insert("end", f"[{self._timestamp()}] DATA  | {title}\n", "data_header")
        self.log.insert("end", rule + "\n", "data_rule")
        self.log.insert("end", payload + "\n", "data_body")
        self.log.insert("end", rule + "\n\n", "data_rule")
        self.log.see("end")

    def log_pnl(self, message: str, pnl_value: Decimal | None) -> None:
        if pnl_value is None:
            tag = "neutral"
        elif pnl_value > ZERO:
            tag = "profit"
        elif pnl_value < ZERO:
            tag = "loss"
        else:
            tag = "neutral"
        self._log("P/L", message, tag)

    def clear_log(self) -> None:
        self.log.delete("1.0", "end")

    def _set_busy(self, state: str) -> None:
        for button in (
            self.check_button,
            self.test_order_button,
            self.live_order_button,
            self.status_button,
            self.cancel_button,
            self.close_sell_button,
            self.open_orders_button,
            self.trade_history_button,
            self.backtest_button,
        ):
            button.configure(state=state)

    def _run_async(self, action_name: str, func, silence_errors: bool = False) -> None:
        self._set_busy("disabled")
        self.log_info(f"Запуск: {action_name}")

        def runner() -> None:
            try:
                func()
            except Exception as exc:
                self.after(0, lambda: self._handle_error(exc, silence_errors))
            finally:
                self.after(0, lambda: self._set_busy("normal"))

        threading.Thread(target=runner, daemon=True).start()

    def _handle_error(self, exc: Exception, silence_errors: bool) -> None:
        self._set_health_api("last call failed")
        self.health_risk_var.set("Risk: last validation/order call failed")
        self.log_error(str(exc))
        if not silence_errors:
            messagebox.showerror("Ошибка", str(exc))

    def _validate_quantity(self) -> str:
        quantity = self.quantity_var.get().strip()
        try:
            value = Decimal(quantity)
            if value <= 0:
                raise BinanceDemoError("Quantity должна быть больше нуля.")
        except InvalidOperation as exc:
            raise BinanceDemoError("Поле Quantity заполнено некорректно.") from exc
        return quantity

    def _validate_interval_seconds(self) -> int:
        raw_value = self.auto_refresh_interval_var.get().strip()
        try:
            seconds = int(raw_value)
        except ValueError as exc:
            raise BinanceDemoError("Интервал автообновления должен быть целым числом.") from exc
        if seconds < 1:
            raise BinanceDemoError("Интервал автообновления должен быть не меньше 1 секунды.")
        return seconds

    def _validate_int_field(self, raw_value: str, field_name: str, minimum: int = 1) -> int:
        try:
            value = int(raw_value.strip())
        except ValueError as exc:
            raise BinanceDemoError(f"{field_name} must be an integer.") from exc
        if value < minimum:
            raise BinanceDemoError(f"{field_name} must be >= {minimum}.")
        return value

    def _validate_decimal_field(self, raw_value: str, field_name: str, allow_zero: bool = False) -> Decimal:
        try:
            value = Decimal(raw_value.strip())
        except InvalidOperation as exc:
            raise BinanceDemoError(f"{field_name} is invalid.") from exc
        if allow_zero:
            if value < ZERO:
                raise BinanceDemoError(f"{field_name} cannot be negative.")
        elif value <= ZERO:
            raise BinanceDemoError(f"{field_name} must be greater than zero.")
        return value

    def _validate_strategy_mode(self) -> str:
        mode = self.strategy_mode_var.get().strip()
        if mode not in ("Price trigger", "Mean reversion"):
            raise BinanceDemoError("Unsupported strategy mode.")
        return mode

    def _validate_execution_mode(self) -> str:
        mode = self.execution_mode_var.get().strip()
        if mode not in ("Manual", "Test first", "Live demo"):
            raise BinanceDemoError("Unsupported execution mode.")
        return mode

    def _validate_price_trigger_config(self) -> dict:
        condition = self.price_trigger_condition_var.get().strip()
        if condition not in ("ask <=", "bid <=", "ask >=", "bid >="):
            raise BinanceDemoError("Unsupported price trigger condition.")
        raw_target = self.price_trigger_value_var.get().strip()
        if not raw_target:
            raise BinanceDemoError("Set a price trigger value or switch Strategy to Mean reversion.")
        target = self._validate_decimal_field(raw_target, "Price trigger value")
        market_field, operator = condition.split()
        return {
            "condition": condition,
            "market_field": market_field,
            "operator": operator,
            "target": target,
        }

    def _parse_auto_trade_scan_symbols(self) -> list[str]:
        raw = self.auto_trade_scan_symbols_var.get().replace("\n", ",")
        symbols: list[str] = []
        seen: set[str] = set()
        for item in raw.split(","):
            symbol = item.strip().upper()
            if not symbol or symbol in seen:
                continue
            if not symbol.endswith("USDT"):
                raise BinanceDemoError(f"Auto-select supports USDT pairs only: {symbol}.")
            symbols.append(symbol)
            seen.add(symbol)
        if not symbols:
            raise BinanceDemoError("Add at least one scan symbol for auto-select.")
        return symbols

    def _validate_auto_trade_config(self) -> dict:
        fee_pct = self._validate_decimal_field(self.auto_trade_fee_pct_var.get(), "Fee %", allow_zero=True)
        min_net_profit_pct = self._validate_decimal_field(
            self.auto_trade_min_net_profit_var.get(), "Min net %", allow_zero=True
        )
        max_spread_pct = self._validate_decimal_field(self.auto_trade_max_spread_var.get(), "Max spread %")
        kline_interval = self.auto_trade_kline_interval_var.get().strip()
        if kline_interval not in KLINE_INTERVALS:
            raise BinanceDemoError("Unsupported kline interval.")
        min_atr_pct = self._validate_decimal_field(self.auto_trade_min_atr_pct_var.get(), "ATR min %", allow_zero=True)
        max_atr_pct = self._validate_decimal_field(self.auto_trade_max_atr_pct_var.get(), "ATR max %", allow_zero=True)
        if max_atr_pct > ZERO and min_atr_pct > max_atr_pct:
            raise BinanceDemoError("ATR min % cannot be greater than ATR max %.")
        config = {
            "interval_seconds": self._validate_int_field(self.auto_trade_interval_var.get(), "Auto interval", minimum=1),
            "window": self._validate_int_field(self.auto_trade_window_var.get(), "SMA window", minimum=3),
            "buy_threshold_pct": self._validate_decimal_field(self.auto_trade_buy_threshold_var.get(), "Buy dip %"),
            "take_profit_pct": self._validate_decimal_field(self.auto_trade_take_profit_var.get(), "Take profit %"),
            "stop_loss_pct": self._validate_decimal_field(self.auto_trade_stop_loss_var.get(), "Stop loss %"),
            "cooldown_seconds": self._validate_int_field(self.auto_trade_cooldown_var.get(), "Cooldown", minimum=0),
            "auto_select": bool(self.auto_trade_auto_select_var.get()),
            "scan_symbols": self._parse_auto_trade_scan_symbols(),
            "fee_pct": fee_pct,
            "round_trip_fee_pct": fee_pct * Decimal("2"),
            "min_net_profit_pct": min_net_profit_pct,
            "max_spread_pct": max_spread_pct,
            "max_positions": self._validate_int_field(self.auto_trade_max_positions_var.get(), "Max positions", minimum=1),
            "capital_per_position": self._validate_decimal_field(
                self.auto_trade_capital_per_position_var.get(), "USDT/position"
            ),
            "parallel_enabled": bool(self.auto_trade_parallel_var.get()),
            "parallel_workers": min(
                self._validate_int_field(self.auto_trade_workers_var.get(), "Workers", minimum=1),
                8,
            ),
            "kline_interval": kline_interval,
            "kline_limit": min(
                self._validate_int_field(self.auto_trade_kline_limit_var.get(), "Kline bars", minimum=EMA_SLOW_PERIOD + 10),
                1000,
            ),
            "min_quote_volume": self._validate_decimal_field(
                self.auto_trade_min_quote_volume_var.get(), "Min vol USDT", allow_zero=True
            ),
            "min_atr_pct": min_atr_pct,
            "max_atr_pct": max_atr_pct,
            "trend_filter_enabled": bool(self.auto_trade_trend_filter_var.get()),
            "trailing_stop_pct": self._validate_decimal_field(
                self.auto_trade_trailing_stop_var.get(), "Trail %", allow_zero=True
            ),
            "break_even_profit_pct": self._validate_decimal_field(
                self.auto_trade_break_even_profit_var.get(), "BE arm %", allow_zero=True
            ),
            "strategy_mode": self._validate_strategy_mode(),
            "execution_mode": self._validate_execution_mode(),
        }
        if config["strategy_mode"] == "Price trigger":
            config["price_trigger"] = self._validate_price_trigger_config()
        return config

    def _validate_trade_history_limit(self) -> int:
        value = self._validate_int_field(self.trade_history_limit_var.get(), "Trade history limit", minimum=1)
        return min(value, 1000)

    def _trade_history_time_range(self) -> tuple[str, int, int]:
        interval_label = self.trade_history_interval_var.get().strip()
        seconds = TRADE_HISTORY_INTERVAL_SECONDS.get(interval_label)
        if seconds is None:
            raise BinanceDemoError("Unsupported trade history interval.")
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - seconds * 1000
        return interval_label, start_ms, end_ms

    def _find_filter(self, symbol_info: dict, filter_type: str) -> dict | None:
        for item in symbol_info.get("filters", []):
            if item.get("filterType") == filter_type:
                return item
        return None

    def _ensure_step_alignment(self, value: Decimal, step: Decimal, field_name: str) -> None:
        if step <= ZERO:
            return
        rounded = round_step_down(value, step)
        if rounded != value:
            raise BinanceDemoError(
                f"{field_name} must match step {format_decimal(step)}. Try {format_decimal(rounded)}."
            )

    def _validate_notional_filters(self, symbol_info: dict, notional: Decimal, is_market: bool) -> None:
        minimum = ZERO
        maximum = ZERO

        min_notional_filter = self._find_filter(symbol_info, "MIN_NOTIONAL")
        if min_notional_filter:
            apply_to_market = str(min_notional_filter.get("applyToMarket", "true")).lower() == "true"
            if not is_market or apply_to_market:
                minimum = max(minimum, decimal_or_zero(min_notional_filter.get("minNotional")))

        notional_filter = self._find_filter(symbol_info, "NOTIONAL")
        if notional_filter:
            apply_min = str(notional_filter.get("applyMinToMarket", "true")).lower() == "true"
            apply_max = str(notional_filter.get("applyMaxToMarket", "true")).lower() == "true"
            if not is_market or apply_min:
                minimum = max(minimum, decimal_or_zero(notional_filter.get("minNotional")))
            if not is_market or apply_max:
                maximum = decimal_or_zero(notional_filter.get("maxNotional"))

        if minimum > ZERO and notional < minimum:
            raise BinanceDemoError(
                f"Order notional {format_decimal(notional, 2)} is below minNotional {format_decimal(minimum, 2)}."
            )
        if maximum > ZERO and notional > maximum:
            raise BinanceDemoError(
                f"Order notional {format_decimal(notional, 2)} exceeds maxNotional {format_decimal(maximum, 2)}."
            )

    def _validate_order_filters(
        self,
        client: BinanceDemoClient,
        symbol: str,
        quantity_text: str,
        side: str,
        order_type: str,
        price_text: str | None = None,
    ) -> dict:
        symbol_info = client.get_exchange_info(symbol)
        quantity = decimal_or_zero(quantity_text)
        if quantity <= ZERO:
            raise BinanceDemoError("Quantity must be greater than zero.")

        price = decimal_or_zero(price_text) if price_text else None
        reference_price = price
        is_market = order_type.upper() == "MARKET"

        if is_market:
            quantity_filters = self._quantity_filters_for_order(symbol_info, "MARKET")
            self._validate_quantity_filters(quantity, quantity_filters, "Quantity")

            bid, ask, _ = self._current_market_prices(client, symbol)
            reference_price = ask if side.upper() == "BUY" else bid
            notional = reference_price * quantity
        else:
            if price is None or price <= ZERO:
                raise BinanceDemoError("Limit order price must be greater than zero.")
            price_filter = self._find_filter(symbol_info, "PRICE_FILTER")
            if price_filter:
                min_price = decimal_or_zero(price_filter.get("minPrice"))
                max_price = decimal_or_zero(price_filter.get("maxPrice"))
                tick_size = decimal_or_zero(price_filter.get("tickSize"))
                if min_price > ZERO and price < min_price:
                    raise BinanceDemoError(f"Price {format_decimal(price, 2)} is below minPrice {format_decimal(min_price, 2)}.")
                if max_price > ZERO and price > max_price:
                    raise BinanceDemoError(f"Price {format_decimal(price, 2)} exceeds maxPrice {format_decimal(max_price, 2)}.")
                self._ensure_step_alignment(price, tick_size, "Price")

            quantity_filters = self._quantity_filters_for_order(symbol_info, "LIMIT")
            self._validate_quantity_filters(quantity, quantity_filters, "Quantity")

            notional = price * quantity

        self._validate_notional_filters(symbol_info, notional, is_market=is_market)
        self.after(
            0,
            lambda s=symbol, side_text=side.upper(), kind=order_type.upper(), q=quantity, p=reference_price, n=notional: (
                self.health_risk_var.set(
                    f"Risk: filters OK | {s} | {side_text} {kind} | qty={format_decimal(q)} | notional={format_decimal(n, 2)}"
                ),
                self.log_info(
                    f"Filters OK: {s} | {side_text} {kind} | qty={format_decimal(q)} | refPrice={format_decimal(p, 2)} | notional={format_decimal(n, 2)}"
                ),
            ),
        )
        return {
            "symbol_info": symbol_info,
            "quantity": quantity,
            "price": price,
            "reference_price": reference_price,
            "notional": notional,
        }

    def _resolved_order_price(self, client: BinanceDemoClient, symbol: str) -> str:
        raw_price = self.price_var.get().strip()
        if raw_price:
            try:
                value = Decimal(raw_price)
                if value <= 0:
                    raise BinanceDemoError("Price должна быть больше нуля.")
            except InvalidOperation as exc:
                raise BinanceDemoError("Поле Price заполнено некорректно.") from exc
            return raw_price

        _, ask_price, _ = self._current_market_prices(client, symbol)
        symbol_info = client.get_exchange_info(symbol)
        tick_size = self._extract_filter_value(symbol_info, "PRICE_FILTER", "tickSize", "0.01")
        raw_limit_price = ask_price * Decimal("0.999")
        if tick_size > ZERO:
            limit_price = round_step_down(raw_limit_price, tick_size)
        else:
            limit_price = raw_limit_price.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        self.after(0, lambda: self.price_var.set(format(limit_price, "f")))
        self.after(
            0,
            lambda: self.log_info(
                f"Авто-цена рассчитана от ask {format(ask_price, 'f')}: limit BUY = {format(limit_price, 'f')}"
            ),
        )
        return format(limit_price, "f")

    def _selected_order_id(self) -> str:
        manual = self.order_id_var.get().strip()
        if manual:
            return manual
        fallback = self.last_order_id_var.get().strip()
        if fallback:
            self.after(0, lambda: self.order_id_var.set(fallback))
            return fallback
        raise BinanceDemoError("Укажи Order ID или сначала отправь/получи ордер.")

    def _set_last_order_id(self, order_id: str) -> None:
        order_id = str(order_id)
        self.last_order_id_var.set(order_id)
        self.order_id_var.set(order_id)
        self.last_order_label.configure(text=f"Последний ордер: {order_id}")

    def _format_order_summary(self, order: dict) -> str:
        return (
            f"orderId={order.get('orderId', '—')} | symbol={order.get('symbol', '—')} | "
            f"side={order.get('side', '—')} | type={order.get('type', '—')} | "
            f"status={order.get('status', '—')} | price={order.get('price', '—')} | "
            f"origQty={order.get('origQty', '—')} | executedQty={order.get('executedQty', '—')}"
        )

    def _update_position_labels(self, position_text: str, pnl_text: str, pnl_color: str = PNL_COLOR_NEUTRAL) -> None:
        self.position_var.set(position_text)
        self.pnl_var.set(pnl_text)
        self.pnl_label.configure(fg=pnl_color)

    def _base_asset_balance(self, account: dict, asset: str) -> dict | None:
        for balance in account.get("balances", []):
            if balance.get("asset") == asset:
                return balance
        return None

    def _set_auto_trade_status(self, message: str, color: str = PNL_COLOR_NEUTRAL, log_level: str | None = None) -> None:
        changed = message != self.auto_trade_last_status_text
        self.auto_trade_last_status_text = message
        self.auto_trade_status_var.set(message)
        self.auto_trade_status_label.configure(fg=color)
        if changed and log_level == "info":
            self.log_info(message)
        elif changed and log_level == "warn":
            self.log_warn(message)
        elif changed and log_level == "ok":
            self.log_ok(message)

    def _set_auto_trade_last_check(self, message: str, color: str = PNL_COLOR_NEUTRAL) -> None:
        self.auto_trade_last_check_var.set(message)
        self.auto_trade_last_check_label.configure(fg=color)

    def _publish_auto_trade_heartbeat(
        self,
        stage: str,
        symbol: str,
        mid: Decimal,
        average_mid: Decimal,
        dip_pct: Decimal,
        color: str = PNL_COLOR_NEUTRAL,
    ) -> None:
        self.auto_trade_cycle_count += 1
        cycle_number = self.auto_trade_cycle_count
        heartbeat = (
            f"Last check: {self._timestamp()} | cycle #{cycle_number} | {stage} | {symbol} | "
            f"mid={format_decimal(mid, 2)} | SMA={format_decimal(average_mid, 2)} | dip={format_decimal(dip_pct, 2)}%"
        )

        self.after(0, lambda h=heartbeat, c=color: self._set_auto_trade_last_check(h, c))
        self.after(
            0,
            lambda n=cycle_number, s=stage, sym=symbol, m=mid, avg=average_mid, dip=dip_pct: self.log_info(
                f"[AUTO-TRADE] tick #{n} | {s} | {sym} | mid={format_decimal(m, 2)} | SMA={format_decimal(avg, 2)} | dip={format_decimal(dip, 2)}%"
            ),
        )

    def _sync_auto_trade_buttons(self) -> None:
        if self.auto_trade_enabled.get():
            self.start_auto_trade_button.configure(state="disabled")
            self.stop_auto_trade_button.configure(state="normal")
        else:
            self.start_auto_trade_button.configure(state="normal")
            self.stop_auto_trade_button.configure(state="disabled")

    def _clear_auto_trade_runtime(self, clear_history: bool = True, clear_position_basis: bool = True) -> None:
        if clear_position_basis:
            self.auto_trade_entry_price = None
            self.auto_trade_entry_qty = None
            self.position_entry_timestamp = ""
            self.position_entry_reason = ""
            self.position_entry_order_id = ""
            self.position_entry_commission = ZERO
            self.auto_trade_entry_fee_estimate_quote = ZERO
            self.auto_trade_positions.clear()
        self.auto_trade_cooldown_until = 0.0
        if clear_history:
            self.auto_trade_price_history.clear()
            self.auto_trade_price_histories.clear()

    def _refresh_order_snapshot(self, client: BinanceDemoClient, symbol: str, order: dict) -> dict:
        order_id = str(order.get("orderId", "")).strip()
        if not order_id:
            return order
        try:
            fresh = client.get_order(symbol=symbol, order_id=order_id)
            if fresh:
                return fresh
        except Exception:
            pass
        return order

    def _extract_average_fill_price(self, order: dict, fallback_price: Decimal) -> Decimal:
        executed_qty = decimal_or_zero(order.get("executedQty"))
        cumulative_quote = decimal_or_zero(order.get("cummulativeQuoteQty"))
        if executed_qty > ZERO and cumulative_quote > ZERO:
            return cumulative_quote / executed_qty
        price = decimal_or_zero(order.get("price"))
        if price > ZERO:
            return price
        return fallback_price

    def _market_prices(self, book: dict) -> tuple[Decimal, Decimal, Decimal]:
        bid = decimal_or_zero(book.get("bidPrice"))
        ask = decimal_or_zero(book.get("askPrice"))
        if bid <= ZERO or ask <= ZERO:
            raise BinanceDemoError("Binance returned an invalid bid/ask for autotrade.")
        return bid, ask, (bid + ask) / Decimal("2")

    def _spread_pct(self, bid: Decimal, ask: Decimal, mid: Decimal) -> Decimal:
        if mid <= ZERO:
            return ZERO
        return (ask - bid) / mid * Decimal("100")

    def _auto_trade_required_profit_pct(self, config: dict, spread_pct: Decimal) -> Decimal:
        fee_and_buffer = config["round_trip_fee_pct"] + spread_pct + config["min_net_profit_pct"]
        return max(config["take_profit_pct"], fee_and_buffer)

    def _auto_trade_estimated_net_pnl(
        self,
        entry_price: Decimal,
        exit_price: Decimal,
        quantity: Decimal,
        config: dict,
    ) -> tuple[Decimal, Decimal, Decimal]:
        gross = (exit_price - entry_price) * quantity
        entry_fee = entry_price * quantity * config["fee_pct"] / Decimal("100")
        exit_fee = exit_price * quantity * config["fee_pct"] / Decimal("100")
        return gross - entry_fee - exit_fee, entry_fee, exit_fee

    def _position_flag_enabled(self, value) -> bool:
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def _auto_trade_exit_snapshot(
        self,
        position_state: dict,
        entry_price: Decimal,
        bid: Decimal,
        quantity: Decimal,
        spread_pct: Decimal,
        config: dict,
    ) -> dict:
        pnl_pct = ((bid - entry_price) / entry_price * Decimal("100")) if entry_price > ZERO else ZERO
        estimated_net_pnl, entry_fee_estimate, exit_fee_estimate = self._auto_trade_estimated_net_pnl(
            entry_price,
            bid,
            quantity,
            config,
        )
        required_profit_pct = self._auto_trade_required_profit_pct(config, spread_pct)

        peak_price = max(decimal_or_zero(position_state.get("peak_price")), entry_price, bid)
        position_state["peak_price"] = peak_price

        trailing_stop_price = ZERO
        if config["trailing_stop_pct"] > ZERO and peak_price > entry_price:
            trailing_stop_price = peak_price * (Decimal("1") - config["trailing_stop_pct"] / Decimal("100"))
            position_state["trailing_stop_price"] = trailing_stop_price
        else:
            position_state["trailing_stop_price"] = ZERO

        break_even_armed = self._position_flag_enabled(position_state.get("break_even_armed"))
        break_even_price = ZERO
        if config["break_even_profit_pct"] > ZERO:
            break_even_price = entry_price * (
                Decimal("1") + config["round_trip_fee_pct"] / Decimal("100")
            )
            if pnl_pct >= config["break_even_profit_pct"] and estimated_net_pnl >= ZERO:
                break_even_armed = True
            position_state["break_even_armed"] = break_even_armed
            position_state["break_even_price"] = break_even_price

        exit_reason = ""
        if pnl_pct >= required_profit_pct and estimated_net_pnl > ZERO:
            exit_reason = "fee-aware take-profit"
        elif trailing_stop_price > ZERO and bid <= trailing_stop_price and estimated_net_pnl > ZERO:
            exit_reason = "trailing-stop"
        elif break_even_armed and bid <= break_even_price and estimated_net_pnl >= ZERO:
            exit_reason = "break-even-stop"
        elif pnl_pct <= -config["stop_loss_pct"]:
            exit_reason = "stop-loss"

        return {
            "exit_reason": exit_reason,
            "pnl_pct": pnl_pct,
            "estimated_net_pnl": estimated_net_pnl,
            "entry_fee_estimate": entry_fee_estimate,
            "exit_fee_estimate": exit_fee_estimate,
            "required_profit_pct": required_profit_pct,
            "peak_price": peak_price,
            "trailing_stop_price": trailing_stop_price,
            "break_even_armed": break_even_armed,
            "break_even_price": break_even_price,
        }

    def _quantity_for_capital(
        self,
        client: BinanceDemoClient,
        symbol: str,
        entry_price: Decimal,
        capital: Decimal,
    ) -> str:
        if entry_price <= ZERO:
            raise BinanceDemoError("Entry price must be greater than zero.")
        symbol_info = client.get_exchange_info(symbol)
        qty_filters = self._quantity_filters_for_order(symbol_info, "MARKET")
        qty = self._round_quantity_for_filters(capital / entry_price, qty_filters)
        if qty <= ZERO:
            raise BinanceDemoError(f"Capital per position is too small for {symbol}.")
        self._validate_quantity_filters(qty, qty_filters, f"{symbol} quantity")
        self._validate_notional_filters(symbol_info, qty * entry_price, is_market=True)
        return format_decimal(qty)

    def _auto_trade_history_for(self, symbol: str) -> list[Decimal]:
        symbol = symbol.upper()
        history = self.auto_trade_price_histories.setdefault(symbol, [])
        if symbol == self.symbol_var.get().strip().upper():
            self.auto_trade_price_history = history
        return history

    def _append_auto_trade_price(self, symbol: str, mid: Decimal, window: int) -> tuple[list[Decimal], Decimal, Decimal]:
        history = self._auto_trade_history_for(symbol)
        history.append(mid)
        if len(history) > window:
            del history[:-window]
        average_mid = sum(history, ZERO) / Decimal(len(history))
        dip_pct = ((average_mid - mid) / average_mid * Decimal("100")) if average_mid > ZERO else ZERO
        return history, average_mid, dip_pct

    def _parse_klines(self, rows: list) -> list[dict]:
        parsed: list[dict] = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 8:
                continue
            open_price = decimal_or_zero(row[1])
            high = decimal_or_zero(row[2])
            low = decimal_or_zero(row[3])
            close = decimal_or_zero(row[4])
            volume = decimal_or_zero(row[5])
            quote_volume = decimal_or_zero(row[7])
            if open_price <= ZERO or high <= ZERO or low <= ZERO or close <= ZERO:
                continue
            parsed.append(
                {
                    "open_time": int(row[0]) if str(row[0]).isdigit() else 0,
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": volume,
                    "quote_volume": quote_volume,
                }
            )
        return parsed

    def _ema(self, values: list[Decimal], period: int) -> Decimal | None:
        if len(values) < period:
            return None
        ema = sum(values[:period], ZERO) / Decimal(period)
        multiplier = Decimal("2") / Decimal(period + 1)
        for value in values[period:]:
            ema = (value - ema) * multiplier + ema
        return ema

    def _atr(self, candles: list[dict], period: int = ATR_PERIOD) -> Decimal | None:
        if len(candles) < period + 1:
            return None
        ranges: list[Decimal] = []
        previous_close = candles[0]["close"]
        for candle in candles[1:]:
            high = candle["high"]
            low = candle["low"]
            true_range = max(high - low, abs(high - previous_close), abs(low - previous_close))
            ranges.append(true_range)
            previous_close = candle["close"]
        if len(ranges) < period:
            return None
        return sum(ranges[-period:], ZERO) / Decimal(period)

    def _rsi(self, values: list[Decimal], period: int = RSI_PERIOD) -> Decimal | None:
        if len(values) <= period:
            return None
        gains = ZERO
        losses = ZERO
        window = values[-(period + 1):]
        for previous, current in zip(window, window[1:]):
            change = current - previous
            if change > ZERO:
                gains += change
            else:
                losses += abs(change)
        average_gain = gains / Decimal(period)
        average_loss = losses / Decimal(period)
        if average_loss == ZERO:
            return Decimal("100") if average_gain > ZERO else Decimal("50")
        relative_strength = average_gain / average_loss
        return Decimal("100") - (Decimal("100") / (Decimal("1") + relative_strength))

    def _indicator_snapshot_from_candles(self, candles: list[dict]) -> dict:
        if len(candles) < EMA_SLOW_PERIOD:
            return {
                "ok": False,
                "error": f"not enough kline rows ({len(candles)}/{EMA_SLOW_PERIOD})",
            }
        closes = [item["close"] for item in candles]
        last_close = closes[-1]
        ema_fast = self._ema(closes, EMA_FAST_PERIOD)
        ema_slow = self._ema(closes, EMA_SLOW_PERIOD)
        atr = self._atr(candles)
        atr_pct = (atr / last_close * Decimal("100")) if atr is not None and last_close > ZERO else None
        rsi = self._rsi(closes)
        volume_window = candles[-min(VOLUME_LOOKBACK, len(candles)):]
        quote_volume = sum((item["quote_volume"] for item in volume_window), ZERO)
        base_volume = sum((item["volume"] for item in volume_window), ZERO)
        trend_ok = bool(
            ema_fast is not None
            and ema_slow is not None
            and ema_fast >= ema_slow
            and last_close >= ema_slow
        )
        return {
            "ok": True,
            "error": "",
            "candles": candles,
            "close": last_close,
            "ema_fast": ema_fast,
            "ema_slow": ema_slow,
            "atr_pct": atr_pct,
            "rsi": rsi,
            "quote_volume": quote_volume,
            "base_volume": base_volume,
            "trend_ok": trend_ok,
            "rows": len(candles),
        }

    def _indicator_snapshot_from_rows(self, rows: list) -> dict:
        return self._indicator_snapshot_from_candles(self._parse_klines(rows))

    def _indicator_snapshot_for_symbol(self, client: BinanceDemoClient, symbol: str, config: dict) -> dict:
        try:
            rows = client.get_klines(symbol, config["kline_interval"], config["kline_limit"])
            snapshot = self._indicator_snapshot_from_rows(rows)
            snapshot["symbol"] = symbol
            return snapshot
        except Exception as exc:
            return {"symbol": symbol, "ok": False, "error": str(exc)}

    def _attach_candidate_filters(self, candidate: dict, indicator: dict | None, config: dict) -> dict:
        blocked_reasons = [candidate["blocked_reason"]] if candidate.get("blocked_reason") else []
        if not indicator or not indicator.get("ok"):
            blocked_reasons.append(f"klines unavailable: {(indicator or {}).get('error', 'missing')}")
            candidate["blocked_reason"] = "; ".join(blocked_reasons)
            candidate["score"] = candidate["score"] - Decimal("100")
            return candidate

        candidate.update(
            {
                "ema_fast": indicator.get("ema_fast"),
                "ema_slow": indicator.get("ema_slow"),
                "atr_pct": indicator.get("atr_pct"),
                "rsi": indicator.get("rsi"),
                "quote_volume": indicator.get("quote_volume", ZERO),
                "base_volume": indicator.get("base_volume", ZERO),
                "trend_ok": bool(indicator.get("trend_ok")),
                "kline_rows": indicator.get("rows", 0),
            }
        )

        quote_volume = decimal_or_zero(candidate.get("quote_volume"))
        atr_pct = candidate.get("atr_pct")
        if config["min_quote_volume"] > ZERO and quote_volume < config["min_quote_volume"]:
            blocked_reasons.append(
                f"volume {format_decimal(quote_volume, 2)} < min {format_decimal(config['min_quote_volume'], 2)}"
            )
        if atr_pct is None:
            blocked_reasons.append("ATR unavailable")
        else:
            if config["min_atr_pct"] > ZERO and atr_pct < config["min_atr_pct"]:
                blocked_reasons.append(
                    f"ATR {format_decimal(atr_pct, 3)}% < min {format_decimal(config['min_atr_pct'], 3)}%"
                )
            if config["max_atr_pct"] > ZERO and atr_pct > config["max_atr_pct"]:
                blocked_reasons.append(
                    f"ATR {format_decimal(atr_pct, 3)}% > max {format_decimal(config['max_atr_pct'], 3)}%"
                )
        if config["trend_filter_enabled"] and not candidate["trend_ok"]:
            ema_fast = candidate.get("ema_fast")
            ema_slow = candidate.get("ema_slow")
            fast_text = format_decimal(ema_fast, 2) if isinstance(ema_fast, Decimal) else "n/a"
            slow_text = format_decimal(ema_slow, 2) if isinstance(ema_slow, Decimal) else "n/a"
            blocked_reasons.append(f"trend filter failed EMA{EMA_FAST_PERIOD}={fast_text} EMA{EMA_SLOW_PERIOD}={slow_text}")

        score = candidate["score"]
        if candidate["trend_ok"]:
            score += Decimal("0.25")
        if isinstance(atr_pct, Decimal):
            score += min(atr_pct, Decimal("1.0")) / Decimal("4")
        rsi = candidate.get("rsi")
        if isinstance(rsi, Decimal) and Decimal("25") <= rsi <= Decimal("55"):
            score += (Decimal("55") - rsi) / Decimal("20")
        candidate["score"] = score
        candidate["blocked_reason"] = "; ".join(blocked_reasons)
        return candidate

    def _mean_reversion_signal(self, candidate: dict, config: dict) -> tuple[bool, str]:
        if not candidate.get("ready"):
            return False, f"warmup {candidate.get('history_size', 0)}/{config['window']}"
        if candidate.get("blocked_reason"):
            return False, candidate["blocked_reason"]
        dip_pct = candidate["dip_pct"]
        if dip_pct < config["buy_threshold_pct"]:
            return False, f"dip {format_decimal(dip_pct, 2)}% < buy {format_decimal(config['buy_threshold_pct'], 2)}%"
        spread_pct = candidate["spread_pct"]
        required_profit_pct = self._auto_trade_required_profit_pct(config, spread_pct)
        atr_pct = candidate.get("atr_pct")
        rsi = candidate.get("rsi")
        quote_volume = decimal_or_zero(candidate.get("quote_volume"))
        details = [
            f"dip {format_decimal(dip_pct, 2)}% below SMA",
            f"spread {format_decimal(spread_pct, 3)}%",
            f"target net-aware {format_decimal(required_profit_pct, 2)}%",
        ]
        if isinstance(atr_pct, Decimal):
            details.append(f"ATR {format_decimal(atr_pct, 3)}%")
        if isinstance(rsi, Decimal):
            details.append(f"RSI {format_decimal(rsi, 1)}")
        if quote_volume > ZERO:
            details.append(f"vol {format_decimal(quote_volume, 0)} USDT")
        details.append("trend OK" if candidate.get("trend_ok") else "trend unchecked")
        return True, "Mean reversion buy: " + " | ".join(details)

    def _backtest_candidate_from_candles(self, symbol: str, candles: list[dict], config: dict) -> dict:
        closes = [item["close"] for item in candles]
        close = closes[-1]
        spread_pct = min(config["max_spread_pct"], Decimal("0.10"))
        bid = close
        ask = close * (Decimal("1") + spread_pct / Decimal("100"))
        mid = (bid + ask) / Decimal("2")
        window_size = min(config["window"], len(closes))
        average_mid = sum(closes[-window_size:], ZERO) / Decimal(window_size)
        dip_pct = ((average_mid - close) / average_mid * Decimal("100")) if average_mid > ZERO else ZERO
        ready = len(closes) >= config["window"]
        blocked_reason = ""
        if spread_pct > config["max_spread_pct"]:
            blocked_reason = f"spread {format_decimal(spread_pct, 3)}% > max {format_decimal(config['max_spread_pct'], 3)}%"
        candidate = {
            "symbol": symbol,
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "spread_pct": spread_pct,
            "history_size": len(closes),
            "average_mid": average_mid,
            "dip_pct": dip_pct,
            "ready": ready,
            "blocked_reason": blocked_reason,
            "score": dip_pct - spread_pct - config["round_trip_fee_pct"],
        }
        indicator = self._indicator_snapshot_from_candles(candles)
        return self._attach_candidate_filters(candidate, indicator, config)

    def _parallel_map_ordered(self, func, items: list, config: dict) -> list:
        if not config.get("parallel_enabled") or len(items) <= 1:
            return [func(item) for item in items]
        workers = max(1, min(int(config.get("parallel_workers", 4)), 8, len(items)))
        results: list = [None] * len(items)
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="autotrade") as executor:
            future_to_index = {executor.submit(func, item): index for index, item in enumerate(items)}
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                results[index] = future.result()
        return results

    def _fetch_book_ticker_for_symbol(self, client: BinanceDemoClient, symbol: str) -> tuple[str, dict | None, str]:
        try:
            return symbol, client.get_book_ticker(symbol), ""
        except Exception as exc:
            return symbol, None, str(exc)

    def _position_check_snapshot(self, client: BinanceDemoClient, symbol: str) -> dict:
        try:
            bid, ask, mid = self._current_market_prices(client, symbol)
            balance = self._position_balance_for_symbol(client, symbol)
            is_sellable, sell_qty, base_asset, sell_details, dust_reason = self._sellable_balance_status(client, symbol)
            open_orders: list = []
            open_orders_error = ""
            try:
                open_orders = client.get_open_orders_retry(symbol=symbol)
            except BinanceDemoError as exc:
                open_orders_error = str(exc)
            return {
                "symbol": symbol,
                "ok": True,
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "balance": balance,
                "is_sellable": is_sellable,
                "sell_qty": sell_qty,
                "base_asset": base_asset,
                "sell_details": sell_details,
                "dust_reason": dust_reason,
                "open_orders": open_orders,
                "open_orders_error": open_orders_error,
                "error": "",
            }
        except Exception as exc:
            return {"symbol": symbol, "ok": False, "error": str(exc)}

    def _open_orders_check_snapshot(self, client: BinanceDemoClient, symbol: str) -> dict:
        try:
            return {
                "symbol": symbol,
                "ok": True,
                "open_orders": client.get_open_orders_retry(symbol=symbol),
                "error": "",
            }
        except Exception as exc:
            return {"symbol": symbol, "ok": False, "open_orders": [], "error": str(exc)}

    def _select_auto_trade_candidate(self, client: BinanceDemoClient, config: dict) -> dict:
        symbols = config["scan_symbols"] if config["auto_select"] else [self.symbol_var.get().strip().upper()]
        candidates = self._scan_auto_trade_candidates(client, config, symbols)
        for candidate in candidates:
            if candidate["ready"] and not candidate["blocked_reason"]:
                return candidate
        return candidates[0]

    def _scan_auto_trade_candidates(
        self,
        client: BinanceDemoClient,
        config: dict,
        symbols: list[str],
    ) -> list[dict]:
        if not symbols or not symbols[0]:
            raise BinanceDemoError("Set a symbol before enabling autotrade.")

        books_by_symbol: dict[str, dict] = {}
        try:
            all_books = client.get_all_book_tickers()
            if isinstance(all_books, list):
                for item in all_books:
                    if not isinstance(item, dict):
                        continue
                    item_symbol = str(item.get("symbol", "")).upper()
                    if item_symbol in symbols:
                        books_by_symbol[item_symbol] = item
        except Exception:
            books_by_symbol = {}

        missing_symbols = [symbol for symbol in symbols if symbol not in books_by_symbol]
        if missing_symbols:
            fetched_books = self._parallel_map_ordered(
                lambda item: self._fetch_book_ticker_for_symbol(client, item),
                missing_symbols,
                config,
            )
            for symbol, book, error in fetched_books:
                if book is not None:
                    books_by_symbol[symbol] = book
                elif error:
                    self.after(0, lambda s=symbol, e=error: self.log_warn(f"[AUTO-TRADE] scan skipped {s}: {e}"))

        candidates: list[dict] = []
        for symbol in symbols:
            book = books_by_symbol.get(symbol)
            if not book:
                continue
            bid, ask, mid = self._market_prices(book)
            spread_pct = self._spread_pct(bid, ask, mid)
            history, average_mid, dip_pct = self._append_auto_trade_price(symbol, mid, config["window"])
            ready = len(history) >= config["window"]
            blocked_reason = ""
            if spread_pct > config["max_spread_pct"]:
                blocked_reason = f"spread {format_decimal(spread_pct, 3)}% > max {format_decimal(config['max_spread_pct'], 3)}%"
            score = dip_pct - spread_pct - config["round_trip_fee_pct"]
            candidates.append(
                {
                    "symbol": symbol,
                    "bid": bid,
                    "ask": ask,
                    "mid": mid,
                    "spread_pct": spread_pct,
                    "history_size": len(history),
                    "average_mid": average_mid,
                    "dip_pct": dip_pct,
                    "ready": ready,
                    "blocked_reason": blocked_reason,
                    "score": score,
                }
            )

        if candidates:
            indicator_snapshots = self._parallel_map_ordered(
                lambda item: self._indicator_snapshot_for_symbol(client, item, config),
                [item["symbol"] for item in candidates],
                config,
            )
            indicators_by_symbol = {item.get("symbol"): item for item in indicator_snapshots if isinstance(item, dict)}
            for candidate in candidates:
                self._attach_candidate_filters(candidate, indicators_by_symbol.get(candidate["symbol"]), config)

        candidates.sort(key=lambda item: item["score"], reverse=True)
        top = candidates[:3]
        top_text = "; ".join(
            f"{item['symbol']} dip={format_decimal(item['dip_pct'], 2)}% "
            f"ATR={format_decimal(item['atr_pct'], 3) if isinstance(item.get('atr_pct'), Decimal) else 'n/a'}% "
            f"RSI={format_decimal(item['rsi'], 1) if isinstance(item.get('rsi'), Decimal) else 'n/a'} "
            f"vol={format_decimal(decimal_or_zero(item.get('quote_volume')), 0)} "
            f"score={format_decimal(item['score'], 2)}"
            for item in top
        )
        if top_text:
            self.after(0, lambda text=top_text: self.log_info(f"[AUTO-TRADE] scan top: {text}"))
        if not candidates:
            raise BinanceDemoError("No autotrade scan candidates were available.")
        return candidates

    def _evaluate_price_trigger(self, bid: Decimal, ask: Decimal, trigger_config: dict) -> tuple[bool, Decimal, str]:
        market_field = trigger_config["market_field"]
        operator = trigger_config["operator"]
        target = trigger_config["target"]
        market_price = ask if market_field == "ask" else bid
        if operator == "<=":
            triggered = market_price <= target
        else:
            triggered = market_price >= target
        description = f"{market_field} {operator} {format_decimal(target, 2)}"
        return triggered, market_price, description

    def _run_test_market_order(self, client: BinanceDemoClient, symbol: str, side: str, quantity: str) -> None:
        if side.upper() == "BUY":
            client.test_market_buy(symbol=symbol, quantity=quantity)
        else:
            client.test_market_sell(symbol=symbol, quantity=quantity)
        self.after(
            0,
            lambda s=symbol, side_text=side.upper(), q=quantity: self.log_ok(
                f"Test-first check passed: {s} | {side_text} MARKET | qty={q}"
            ),
        )

    def _record_entry_fill(
        self,
        symbol: str,
        reason: str,
        order: dict,
        fallback_price: Decimal,
        requested_qty: str,
    ) -> tuple[Decimal, Decimal, str]:
        buy_order_id = str(order.get("orderId", ""))
        executed_qty = decimal_or_zero(order.get("executedQty"))
        entry_price = self._extract_average_fill_price(order, fallback_price)
        entry_qty = executed_qty if executed_qty > ZERO else decimal_or_zero(requested_qty)
        commission_amount, commission_asset = self._extract_commission_details(order)

        self.auto_trade_entry_price = entry_price if entry_price > ZERO else fallback_price
        self.auto_trade_entry_qty = entry_qty
        self.position_entry_timestamp = datetime.now().isoformat(timespec="seconds")
        self.position_entry_reason = reason
        self.position_entry_order_id = buy_order_id
        self.position_entry_commission = commission_amount
        self.auto_trade_positions[symbol] = {
            "entry_price": self.auto_trade_entry_price,
            "qty": entry_qty,
            "entry_timestamp": self.position_entry_timestamp,
            "entry_reason": reason,
            "entry_order_id": buy_order_id,
            "entry_fee_estimate_quote": ZERO,
            "peak_price": self.auto_trade_entry_price,
            "trailing_stop_price": ZERO,
            "break_even_armed": False,
            "break_even_price": ZERO,
        }

        self._journal_order_event("order_update", symbol, reason, order, {"side": "BUY"})
        self._journal_trade_fill(symbol, "BUY", "entry", reason, order)

        self.after(0, lambda oid=buy_order_id: self._set_last_order_id(oid))
        self.after(
            0,
            lambda s=symbol, qty=entry_qty, px=self.auto_trade_entry_price: self._update_health_position(s, qty, px),
        )
        fill_text = f"BUY {symbol} | qty={format_decimal(entry_qty)} | avg={format_decimal(self.auto_trade_entry_price, 2)}"
        if commission_asset:
            fill_text += f" | fee={format_decimal(commission_amount)} {commission_asset}"
        self.after(0, lambda text=fill_text: self._set_health_last_fill(text))
        self.after(0, lambda: self._register_fill())
        return self.auto_trade_entry_price, entry_qty, buy_order_id

    def _record_exit_fill(
        self,
        symbol: str,
        reason: str,
        order: dict,
        fallback_price: Decimal,
        requested_qty: str,
        fee_config: dict | None = None,
    ) -> tuple[Decimal, Decimal, Decimal, str]:
        sell_order_id = str(order.get("orderId", ""))
        executed_qty = decimal_or_zero(order.get("executedQty"))
        if executed_qty <= ZERO:
            executed_qty = decimal_or_zero(requested_qty)
        exit_price = self._extract_average_fill_price(order, fallback_price)
        matched_qty = executed_qty
        portfolio_position = self.auto_trade_positions.get(symbol, {})
        portfolio_entry_qty = decimal_or_zero(portfolio_position.get("qty"))
        portfolio_entry_price = decimal_or_zero(portfolio_position.get("entry_price"))
        entry_qty_for_pnl = portfolio_entry_qty if portfolio_entry_qty > ZERO else self.auto_trade_entry_qty
        entry_price_for_pnl = portfolio_entry_price if portfolio_entry_price > ZERO else self.auto_trade_entry_price
        if entry_qty_for_pnl and entry_qty_for_pnl > ZERO:
            matched_qty = min(entry_qty_for_pnl, executed_qty)
        realized_pnl = ZERO
        if entry_price_for_pnl and entry_price_for_pnl > ZERO and matched_qty > ZERO:
            realized_pnl = (exit_price * matched_qty) - (entry_price_for_pnl * matched_qty)
            if fee_config is not None:
                realized_pnl, _entry_fee_estimate, _exit_fee_estimate = self._auto_trade_estimated_net_pnl(
                    entry_price_for_pnl,
                    exit_price,
                    matched_qty,
                    fee_config,
                )

        commission_amount, commission_asset = self._extract_commission_details(order)
        self._journal_order_event("order_update", symbol, reason, order, {"side": "SELL"})
        self._journal_trade_fill(symbol, "SELL", "exit", reason, order, pnl_quote=realized_pnl)

        self.auto_trade_entry_price = None
        self.auto_trade_entry_qty = None
        self.position_entry_timestamp = ""
        self.position_entry_reason = ""
        self.position_entry_order_id = ""
        self.position_entry_commission = ZERO
        self.auto_trade_entry_fee_estimate_quote = ZERO
        self.auto_trade_positions.pop(symbol, None)

        self.after(0, lambda oid=sell_order_id: self._set_last_order_id(oid))
        self.after(0, lambda qty=format_decimal(executed_qty): self.quantity_var.set(qty))
        self.after(0, lambda: self._update_health_position(label=None))
        sign = "+" if realized_pnl >= ZERO else ""
        fill_text = (
            f"SELL {symbol} | qty={format_decimal(executed_qty)} | avg={format_decimal(exit_price, 2)} | "
            f"P/L={sign}{format_decimal(realized_pnl, 2)}"
        )
        if commission_asset:
            fill_text += f" | fee={format_decimal(commission_amount)} {commission_asset}"
        self.after(0, lambda text=fill_text: self._set_health_last_fill(text))
        self.after(0, lambda pnl=realized_pnl: self._register_fill(pnl))
        return exit_price, executed_qty, realized_pnl, sell_order_id

    def _format_trade_history_report(
        self,
        symbol: str,
        interval_label: str,
        start_ms: int,
        end_ms: int,
        trades: list[dict],
        limit: int,
    ) -> str:
        start_text = datetime.fromtimestamp(start_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
        end_text = datetime.fromtimestamp(end_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
        buy_count = 0
        sell_count = 0
        buy_quote = ZERO
        sell_quote = ZERO
        net_qty = ZERO
        commissions: dict[str, Decimal] = {}

        lines = [
            f"symbol={symbol} | interval={interval_label} | rows={len(trades)} | limit={limit}",
            f"from={start_text} | to={end_text}",
            "",
        ]

        for trade in trades:
            side = "BUY" if trade.get("isBuyer") else "SELL"
            trade_time_ms = decimal_or_zero(trade.get("time"))
            trade_time = datetime.fromtimestamp(float(trade_time_ms / Decimal("1000"))).strftime("%Y-%m-%d %H:%M:%S")
            price = decimal_or_zero(trade.get("price"))
            qty = decimal_or_zero(trade.get("qty"))
            quote_qty = decimal_or_zero(trade.get("quoteQty"))
            commission = decimal_or_zero(trade.get("commission"))
            commission_asset = str(trade.get("commissionAsset", ""))
            maker_taker = "MAKER" if trade.get("isMaker") else "TAKER"
            order_id = trade.get("orderId", "")

            if side == "BUY":
                buy_count += 1
                buy_quote += quote_qty
                net_qty += qty
            else:
                sell_count += 1
                sell_quote += quote_qty
                net_qty -= qty

            if commission_asset:
                commissions[commission_asset] = commissions.get(commission_asset, ZERO) + commission

            lines.append(
                f"{trade_time} | {side:<4} | price={format_decimal(price)} | qty={format_decimal(qty)} | "
                f"quote={format_decimal(quote_qty)} | fee={format_decimal(commission)} {commission_asset} | {maker_taker} | orderId={order_id}"
            )

        lines.append("")
        lines.append(
            f"summary | buys={buy_count} | sells={sell_count} | buyQuote={format_decimal(buy_quote)} | "
            f"sellQuote={format_decimal(sell_quote)} | netQty={format_decimal(net_qty)}"
        )
        if commissions:
            commission_parts = [
                f"{asset}={format_decimal(total)}"
                for asset, total in sorted(commissions.items())
            ]
            lines.append("commission | " + " | ".join(commission_parts))

        if len(trades) >= limit:
            lines.append("note | result reached the current row limit; older trades inside the interval may be omitted.")

        return "\n".join(lines)

    def _position_balance_for_symbol(self, client: BinanceDemoClient, symbol: str) -> dict:
        symbol_info = client.get_exchange_info(symbol)
        base_asset = symbol_info.get("baseAsset", "")
        quote_asset = symbol_info.get("quoteAsset", "")
        account = client.get_account()
        base_balance = self._base_asset_balance(account, base_asset)
        quote_balance = self._base_asset_balance(account, quote_asset)
        return {
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            "free_qty": decimal_or_zero(base_balance.get("free") if base_balance else "0"),
            "locked_qty": decimal_or_zero(base_balance.get("locked") if base_balance else "0"),
            "quote_free": decimal_or_zero(quote_balance.get("free") if quote_balance else "0"),
        }

    def _sync_existing_auto_trade_positions(self, client: BinanceDemoClient, symbols: list[str]) -> int:
        synced = 0
        for symbol in symbols:
            try:
                position = self._position_balance_for_symbol(client, symbol)
            except Exception as exc:
                self.after(0, lambda s=symbol, e=exc: self.log_warn(f"[AUTO-TRADE] Balance sync skipped {s}: {e}"))
                continue
            if position["free_qty"] <= ZERO:
                continue
            is_sellable, sell_qty, _base_asset, _details, dust_reason = self._sellable_balance_status(client, symbol)
            if not is_sellable:
                self.auto_trade_positions.pop(symbol, None)
                self.after(
                    0,
                    lambda s=symbol, qty=position["free_qty"], reason=dust_reason: self.log_warn(
                        f"[AUTO-TRADE] Ignoring dust balance on {s}: qty={format_decimal(qty)} | {reason}"
                    ),
                )
                continue
            existing_position = self.auto_trade_positions.get(symbol)
            if existing_position:
                existing_position["qty"] = position["free_qty"]
                synced += 1
                continue
            try:
                book = client.get_book_ticker(symbol)
                _, _, mid = self._market_prices(book)
            except Exception:
                mid = ZERO
            entry_price = mid if mid > ZERO else Decimal("0")
            self.auto_trade_positions[symbol] = {
                "entry_price": entry_price,
                "qty": position["free_qty"],
                "entry_timestamp": datetime.now().isoformat(timespec="seconds"),
                "entry_reason": "Autotrade balance sync",
                "entry_order_id": "",
                "entry_fee_estimate_quote": ZERO,
                "peak_price": entry_price,
                "trailing_stop_price": ZERO,
                "break_even_armed": False,
                "break_even_price": ZERO,
            }
            if self.auto_trade_entry_price is None:
                self.auto_trade_entry_price = entry_price
                self.auto_trade_entry_qty = position["free_qty"]
                self.position_entry_reason = "Autotrade balance sync"
                self.position_entry_timestamp = datetime.now().isoformat(timespec="seconds")
            synced += 1
            self.after(
                0,
                lambda s=symbol, qty=position["free_qty"], px=entry_price, asset=position["base_asset"]: self.log_warn(
                    f"Autotrade synced existing {asset} balance on {s}: qty={format_decimal(qty)} | basis≈{format_decimal(px, 2)}."
                ),
            )
        return synced

    def _extract_filter_value(self, symbol_info: dict, filter_type: str, key: str, default: str = "0") -> Decimal:
        for item in symbol_info.get("filters", []):
            if item.get("filterType") == filter_type:
                return decimal_or_zero(item.get(key, default))
        return decimal_or_zero(default)

    def _sellable_quantity_for_symbol(self, client: BinanceDemoClient, symbol: str) -> tuple[str, str, dict]:
        symbol_info = client.get_exchange_info(symbol)
        base_asset = symbol_info.get("baseAsset", "")
        account = client.get_account()
        balance = self._base_asset_balance(account, base_asset)
        free_qty = decimal_or_zero(balance.get("free") if balance else "0")
        locked_qty = decimal_or_zero(balance.get("locked") if balance else "0")

        if free_qty <= ZERO:
            raise BinanceDemoError(
                f"Недостаточно свободного баланса для SELL. {base_asset}: free={format_decimal(free_qty)}, locked={format_decimal(locked_qty)}"
            )

        qty_filters = self._quantity_filters_for_order(symbol_info, "MARKET")
        min_qty, _max_qty, step_size = self._quantity_filter_bounds(qty_filters)
        sell_qty = self._round_quantity_for_filters(free_qty, qty_filters)
        if sell_qty <= ZERO:
            raise BinanceDemoError(
                f"После округления по stepSize доступное количество стало нулевым. {base_asset}: free={format_decimal(free_qty)}"
            )
        if min_qty > ZERO and sell_qty < min_qty:
            raise BinanceDemoError(
                f"Свободный баланс меньше минимального MARKET_LOT_SIZE. {base_asset}: free={format_decimal(free_qty)}, rounded={format_decimal(sell_qty)}, minQty={format_decimal(min_qty)}"
            )

        self._validate_quantity_filters(sell_qty, qty_filters, f"{base_asset} sell quantity")

        return format_decimal(sell_qty), base_asset, {
            "symbol_info": symbol_info,
            "free_qty": free_qty,
            "locked_qty": locked_qty,
            "step_size": step_size,
            "min_qty": min_qty,
        }

    def _sellable_balance_status(self, client: BinanceDemoClient, symbol: str) -> tuple[bool, str, str, dict, str]:
        try:
            sell_qty, base_asset, details = self._sellable_quantity_for_symbol(client, symbol)
            bid, _ask, _mid = self._current_market_prices(client, symbol)
            notional = decimal_or_zero(sell_qty) * bid
            self._validate_notional_filters(details["symbol_info"], notional, is_market=True)
            return True, sell_qty, base_asset, details, ""
        except Exception as exc:
            return False, "", "", {}, str(exc)

    def _build_pnl_snapshot(self, client: BinanceDemoClient, order: dict) -> dict | None:
        symbol = order.get("symbol", "")
        side = order.get("side", "")
        status = order.get("status", "")
        executed_qty = decimal_or_zero(order.get("executedQty"))
        cumulative_quote = decimal_or_zero(order.get("cummulativeQuoteQty"))

        if not symbol:
            return None

        bid, ask, _ = self._current_market_prices(client, symbol)

        quote_asset = "QUOTE"
        try:
            quote_asset = client.get_exchange_info(symbol).get("quoteAsset", "QUOTE")
        except Exception:
            pass

        snapshot = {
            "symbol": symbol,
            "side": side,
            "status": status,
            "executed_qty": executed_qty,
            "bid": bid,
            "ask": ask,
            "avg_price": ZERO,
            "current_value": ZERO,
            "entry_cost": cumulative_quote,
            "pnl": ZERO,
            "pnl_pct": ZERO,
            "quote_asset": quote_asset,
        }

        if side != "BUY":
            return snapshot

        if executed_qty <= ZERO or cumulative_quote <= ZERO:
            return snapshot

        avg_price = cumulative_quote / executed_qty
        current_value = executed_qty * bid
        pnl = current_value - cumulative_quote
        pnl_pct = (pnl / cumulative_quote * Decimal("100")) if cumulative_quote > ZERO else ZERO

        snapshot.update(
            {
                "avg_price": avg_price,
                "current_value": current_value,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
            }
        )
        return snapshot

    def _apply_pnl_snapshot(self, snapshot: dict | None, auto: bool = False) -> None:
        prefix = "[AUTO] " if auto else ""
        if snapshot is None:
            self._update_position_labels("Позиция: —", "P/L: —", PNL_COLOR_NEUTRAL)
            return

        symbol = snapshot["symbol"]
        side = snapshot["side"]
        status = snapshot["status"]
        executed_qty = snapshot["executed_qty"]
        bid = snapshot["bid"]
        ask = snapshot["ask"]

        if side != "BUY":
            position_text = (
                f"Позиция: {symbol} | side={side} | status={status} | executedQty={format_decimal(executed_qty)} | "
                f"bid={format_decimal(bid, 2)} | ask={format_decimal(ask, 2)}"
            )
            pnl_text = "P/L: для SELL-ордера не рассчитывается"
            self._update_position_labels(position_text, pnl_text, PNL_COLOR_WARNING)
            self.log_info(f"{prefix}{position_text}")
            self.log_pnl(f"{prefix}{pnl_text}", None)
            return

        if executed_qty <= ZERO or snapshot["entry_cost"] <= ZERO:
            position_text = (
                f"Позиция: {symbol} | BUY | status={status} | executedQty={format_decimal(executed_qty)} | "
                f"bid={format_decimal(bid, 2)} | ask={format_decimal(ask, 2)}"
            )
            pnl_text = "P/L: ордер ещё не исполнен"
            self._update_position_labels(position_text, pnl_text, PNL_COLOR_WARNING)
            self.log_info(f"{prefix}{position_text}")
            self.log_pnl(f"{prefix}{pnl_text}", None)
            return

        avg_price = snapshot["avg_price"]
        current_value = snapshot["current_value"]
        entry_cost = snapshot["entry_cost"]
        pnl = snapshot["pnl"]
        pnl_pct = snapshot["pnl_pct"]
        sign = "+" if pnl >= ZERO else ""
        sign_pct = "+" if pnl_pct >= ZERO else ""

        position_text = (
            f"Позиция: {symbol} | BUY | status={status} | qty={format_decimal(executed_qty)} | "
            f"avg={format_decimal(avg_price, 2)} | bid={format_decimal(bid, 2)} | ask={format_decimal(ask, 2)}"
        )
        quote_asset = snapshot.get("quote_asset", "QUOTE")
        pnl_text = (
            f"P/L: {sign}{format_decimal(pnl, 2)} {quote_asset} "
            f"({sign_pct}{format_decimal(pnl_pct, 2)}%) | вход={format_decimal(entry_cost, 2)} {quote_asset} | "
            f"сейчас={format_decimal(current_value, 2)} {quote_asset}"
        )

        pnl_color = PNL_COLOR_NEUTRAL
        if pnl > ZERO:
            pnl_color = PNL_COLOR_PROFIT
        elif pnl < ZERO:
            pnl_color = PNL_COLOR_LOSS

        self._update_position_labels(position_text, pnl_text, pnl_color)
        self.log_info(f"{prefix}{position_text}")
        self.log_pnl(f"{prefix}{pnl_text}", pnl)

    def check_api(self) -> None:
        self._run_async("Проверка API", self._check_api_impl)

    def _check_api_impl(self) -> None:
        client = self._client()
        server_time = client.get_server_time()
        self.after(0, lambda: self._set_health_api("connected"))
        self.after(0, lambda: self.log_ok(f"Server time получен: {server_time.get('serverTime', '—')}"))

        account = client.get_account()
        balances = account.get("balances", [])
        self.after(0, lambda: self.log_ok(f"Аккаунт прочитан. Ненулевых балансов: {len(balances)}"))
        if balances[:10]:
            self.after(0, lambda: self.log_block("Первые ненулевые балансы", balances[:10]))

        symbol = self.symbol_var.get().strip().upper()
        book = client.get_book_ticker(symbol)
        self.after(0, lambda: self.log_ok(f"{symbol} | bid={book.get('bidPrice', '—')} | ask={book.get('askPrice', '—')}"))
        self.after(0, lambda: self.log_info("Проверка API завершена успешно."))
        self.after(0, self._ensure_demo_streams)

    def send_test_order(self) -> None:
        self._run_async("Отправка TEST order", self._send_test_order_impl)

    def _send_test_order_impl(self) -> None:
        client = self._client()
        symbol = self.symbol_var.get().strip().upper()
        price = self._resolved_order_price(client, symbol)
        quantity = self._resolve_buy_quantity(client, symbol, "LIMIT", price_text=price)
        self._validate_order_filters(client, symbol, quantity, "BUY", "LIMIT", price_text=price)
        client.test_limit_buy(symbol=symbol, quantity=quantity, price=price)
        self._journal_order_event(
            "order_test",
            symbol,
            "Manual test limit buy",
            {"symbol": symbol, "side": "BUY", "type": "LIMIT", "price": price, "origQty": quantity, "status": "TEST"},
        )
        self.after(0, lambda: self.log_ok(f"TEST BUY принят: symbol={symbol} | quantity={quantity} | price={price}"))

    def send_live_order(self) -> None:
        if not messagebox.askyesno("Подтверждение", "Отправить реальный BUY-ордер в demo matching engine?"):
            return
        self._run_async("Отправка реального DEMO BUY", self._send_live_order_impl)

    def _send_live_order_impl(self) -> None:
        client = self._client()
        symbol = self.symbol_var.get().strip().upper()
        price = self._resolved_order_price(client, symbol)
        self._enforce_risk_limits(client, symbol)
        quantity = self._resolve_buy_quantity(client, symbol, "LIMIT", price_text=price)
        self._validate_order_filters(client, symbol, quantity, "BUY", "LIMIT", price_text=price)
        result = client.live_limit_buy(symbol=symbol, quantity=quantity, price=price)
        self._journal_order_event("order_submit", symbol, "Manual live limit buy", result)
        self.after(0, lambda: self._set_last_order_id(str(result.get("orderId", ""))))
        if decimal_or_zero(result.get("executedQty")) > ZERO:
            self._record_entry_fill(symbol, "Manual live limit buy", result, decimal_or_zero(price), quantity)
            self.after(0, lambda: self._update_health_orders(0, symbol))
        else:
            self.after(0, lambda: self._update_health_orders(1, symbol))
        self.after(0, lambda: self.log_ok(f"BUY-ордер отправлен: {self._format_order_summary(result)}"))
        self.after(0, lambda: self.log_block("Полный ответ Binance по BUY-ордеру", result))

    def check_order_status(self) -> None:
        self._run_async("Проверка статуса ордера", self._check_order_status_impl)

    def _check_order_status_impl(self) -> None:
        result = self._fetch_order_status()
        client = self._client()
        snapshot = self._build_pnl_snapshot(client, result)
        symbol = self.symbol_var.get().strip().upper()
        status = str(result.get("status", ""))
        side = str(result.get("side", "")).upper()
        executed_qty = decimal_or_zero(result.get("executedQty"))
        if status in {"NEW", "PARTIALLY_FILLED"}:
            self.after(0, lambda: self._update_health_orders(1, symbol))
        else:
            self.after(0, lambda: self._update_health_orders(0, symbol))
        if side == "BUY" and executed_qty > ZERO:
            entry_price = self._extract_average_fill_price(result, decimal_or_zero(result.get("price")))
            self.auto_trade_entry_price = entry_price
            self.auto_trade_entry_qty = executed_qty
            self.position_entry_order_id = str(result.get("orderId", ""))
            self.position_entry_reason = self.position_entry_reason or "Manual status sync"
            self.position_entry_timestamp = self.position_entry_timestamp or datetime.now().isoformat(timespec="seconds")
            self.auto_trade_positions[symbol] = {
                "entry_price": entry_price,
                "qty": executed_qty,
                "entry_timestamp": self.position_entry_timestamp,
                "entry_reason": self.position_entry_reason,
                "entry_order_id": self.position_entry_order_id,
                "entry_fee_estimate_quote": ZERO,
                "peak_price": entry_price,
                "trailing_stop_price": ZERO,
                "break_even_armed": False,
                "break_even_price": ZERO,
            }
            self.after(0, lambda s=symbol, qty=executed_qty, px=entry_price: self._update_health_position(s, qty, px))
        elif side == "SELL" and status == "FILLED":
            self.auto_trade_entry_price = None
            self.auto_trade_entry_qty = None
            self.position_entry_timestamp = ""
            self.position_entry_reason = ""
            self.position_entry_order_id = ""
            self.position_entry_commission = ZERO
            self.auto_trade_positions.pop(symbol, None)
            self.after(0, lambda: self._update_health_position(label=None))
        self.after(0, lambda: self.log_ok(f"Статус ордера: {self._format_order_summary(result)}"))
        self.after(0, lambda: self.log_block("Полный ответ Binance по статусу ордера", result))
        self.after(0, lambda: self._apply_pnl_snapshot(snapshot, auto=False))

    def _fetch_order_status(self) -> dict:
        client = self._client()
        symbol = self.symbol_var.get().strip().upper()
        order_id = self._selected_order_id()
        result = client.get_order(symbol=symbol, order_id=order_id)
        self.after(0, lambda: self._set_last_order_id(str(result.get("orderId", order_id))))
        return result

    def cancel_order(self) -> None:
        if not messagebox.askyesno("Подтверждение", "Отменить выбранный ордер?"):
            return
        self._run_async("Отмена ордера", self._cancel_order_impl)

    def _cancel_order_impl(self) -> None:
        client = self._client()
        symbol = self.symbol_var.get().strip().upper()
        order_id = self._selected_order_id()
        result = client.cancel_order(symbol=symbol, order_id=order_id)
        self._journal_order_event("order_cancel", symbol, "Manual cancel", result)
        self.after(0, lambda: self._update_health_orders(0, symbol))
        self.after(0, lambda: self.log_ok(f"Ордер отменён: {self._format_order_summary(result)}"))
        self.after(0, lambda: self.log_block("Полный ответ Binance по отмене ордера", result))

    def close_position_sell(self) -> None:
        if not messagebox.askyesno(
            "Подтверждение",
            "Закрыть позицию MARKET SELL по свободному балансу базового актива?",
        ):
            return
        self._run_async("Закрытие позиции SELL", self._close_position_sell_impl)

    def _close_position_sell_impl(self) -> None:
        client = self._client()
        symbol = self.symbol_var.get().strip().upper()
        sell_qty, base_asset, details = self._sellable_quantity_for_symbol(client, symbol)
        self._validate_order_filters(client, symbol, sell_qty, "SELL", "MARKET")
        self.after(
            0,
            lambda: self.log_info(
                f"Авто-SELL по свободному балансу: {base_asset} free={format_decimal(details['free_qty'])}, "
                f"locked={format_decimal(details['locked_qty'])}, step={format_decimal(details['step_size'])}, "
                f"minQty={format_decimal(details['min_qty'])}, qty_to_sell={sell_qty}"
            ),
        )
        result = client.market_sell(symbol=symbol, quantity=sell_qty)
        self._journal_order_event("order_submit", symbol, "Manual market sell", result)
        _, _, realized_pnl, _ = self._record_exit_fill(symbol, "Manual market sell", result, ZERO, sell_qty)
        self.after(0, lambda: self.log_ok(f"SELL-ордер отправлен: {self._format_order_summary(result)}"))
        self.after(
            0,
            lambda pnl=realized_pnl: self.log_pnl(
                f"Manual market sell realized P/L: {format_decimal(pnl, 2)}",
                pnl,
            ),
        )
        self.after(0, lambda: self.log_block("Полный ответ Binance по SELL-ордеру", result))

    def check_open_orders(self) -> None:
        self._run_async("Запрос открытых ордеров", self._check_open_orders_impl)

    def _check_open_orders_impl(self) -> None:
        client = self._client()
        symbol = self.symbol_var.get().strip().upper()
        result = client.get_open_orders_retry(symbol=symbol)
        self.after(0, lambda: self._update_health_orders(len(result), symbol))
        self.after(0, lambda: self.log_ok(f"Открытых ордеров по {symbol}: {len(result)}"))
        if result:
            self.after(0, lambda: self.log_block("Список открытых ордеров", result))
            self.after(0, lambda: self._set_last_order_id(str(result[-1].get("orderId", ""))))
        else:
            self.after(0, lambda: self.log_info("По этому символу открытых ордеров нет."))

    def load_trade_history(self) -> None:
        self._run_async("Load trade history", self._load_trade_history_impl)

    def run_backtest(self) -> None:
        self._run_async("Run backtest", self._run_backtest_impl)

    def _load_trade_history_impl(self) -> None:
        client = self._client()
        symbol = self.symbol_var.get().strip().upper()
        if not symbol:
            raise BinanceDemoError("Set a symbol before loading trade history.")

        interval_label, start_ms, end_ms = self._trade_history_time_range()
        limit = self._validate_trade_history_limit()
        trades = client.get_my_trades(
            symbol=symbol,
            start_time_ms=start_ms,
            end_time_ms=end_ms,
            limit=limit,
        )
        trades = sorted(trades, key=lambda item: item.get("time", 0), reverse=True)
        report = self._format_trade_history_report(
            symbol=symbol,
            interval_label=interval_label,
            start_ms=start_ms,
            end_ms=end_ms,
            trades=trades,
            limit=limit,
        )

        self.after(
            0,
            lambda count=len(trades), s=symbol, interval=interval_label: self.log_ok(
                f"Trade history loaded: {s} | interval={interval} | rows={count}"
            ),
        )
        if trades:
            self.after(
                0,
                lambda text=report, s=symbol, interval=interval_label: self.log_block(
                    f"Trade history | {s} | {interval}",
                    text,
                ),
            )
        else:
            self.after(
                0,
                lambda s=symbol, interval=interval_label: self.log_info(
                    f"No trades found for {s} over the last {interval}."
                ),
            )

    def _run_backtest_impl(self) -> None:
        client = self._client()
        config = self._validate_auto_trade_config()
        if config["strategy_mode"] != "Mean reversion":
            raise BinanceDemoError("Backtest currently uses the Mean reversion autotrade signal.")
        symbol = self.symbol_var.get().strip().upper()
        if not symbol:
            raise BinanceDemoError("Set a symbol before running backtest.")

        rows = client.get_klines(symbol, config["kline_interval"], config["kline_limit"])
        candles = self._parse_klines(rows)
        warmup = max(config["window"], EMA_SLOW_PERIOD, ATR_PERIOD + 1, RSI_PERIOD + 1)
        if len(candles) <= warmup:
            raise BinanceDemoError(
                f"Backtest needs more klines: got {len(candles)}, need more than {warmup}."
            )

        position: dict | None = None
        completed_trades: list[dict] = []
        equity = ZERO
        peak_equity = ZERO
        max_drawdown = ZERO
        skipped_signals = 0

        for index in range(warmup, len(candles)):
            candle = candles[index]
            close = candle["close"]
            spread_pct = min(config["max_spread_pct"], Decimal("0.10"))

            if position is not None:
                exit_snapshot = self._auto_trade_exit_snapshot(
                    position,
                    position["entry_price"],
                    close,
                    position["qty"],
                    spread_pct,
                    config,
                )
                floating_equity = equity + exit_snapshot["estimated_net_pnl"]
                peak_equity = max(peak_equity, floating_equity)
                max_drawdown = max(max_drawdown, peak_equity - floating_equity)
                if exit_snapshot["exit_reason"]:
                    trade = {
                        "entry_time": position["entry_time"],
                        "exit_time": candle["open_time"],
                        "entry_price": position["entry_price"],
                        "exit_price": close,
                        "qty": position["qty"],
                        "pnl": exit_snapshot["estimated_net_pnl"],
                        "pnl_pct": exit_snapshot["pnl_pct"],
                        "reason": exit_snapshot["exit_reason"],
                    }
                    completed_trades.append(trade)
                    equity += exit_snapshot["estimated_net_pnl"]
                    peak_equity = max(peak_equity, equity)
                    position = None
                else:
                    continue

            candidate = self._backtest_candidate_from_candles(symbol, candles[: index + 1], config)
            triggered, reason = self._mean_reversion_signal(candidate, config)
            if not triggered:
                if candidate.get("blocked_reason"):
                    skipped_signals += 1
                continue

            entry_price = candidate["ask"]
            qty = config["capital_per_position"] / entry_price
            position = {
                "entry_price": entry_price,
                "qty": qty,
                "entry_time": candle["open_time"],
                "entry_reason": reason,
                "peak_price": entry_price,
                "trailing_stop_price": ZERO,
                "break_even_armed": False,
                "break_even_price": ZERO,
            }

        open_pnl = ZERO
        if position is not None:
            last_close = candles[-1]["close"]
            exit_snapshot = self._auto_trade_exit_snapshot(
                position,
                position["entry_price"],
                last_close,
                position["qty"],
                min(config["max_spread_pct"], Decimal("0.10")),
                config,
            )
            open_pnl = exit_snapshot["estimated_net_pnl"]

        wins = [item for item in completed_trades if item["pnl"] > ZERO]
        losses = [item for item in completed_trades if item["pnl"] < ZERO]
        gross_profit = sum((item["pnl"] for item in wins), ZERO)
        gross_loss = -sum((item["pnl"] for item in losses), ZERO)
        profit_factor = (gross_profit / gross_loss) if gross_loss > ZERO else ZERO
        win_rate = (Decimal(len(wins)) / Decimal(len(completed_trades)) * Decimal("100")) if completed_trades else ZERO

        def candle_time(open_time: int) -> str:
            if not open_time:
                return "n/a"
            return datetime.fromtimestamp(open_time / 1000).strftime("%Y-%m-%d %H:%M")

        lines = [
            f"symbol={symbol} | interval={config['kline_interval']} | candles={len(candles)} | capital/trade={format_decimal(config['capital_per_position'], 2)} USDT",
            f"signals: SMA={config['window']} | EMA{EMA_FAST_PERIOD}/EMA{EMA_SLOW_PERIOD} trend={'on' if config['trend_filter_enabled'] else 'off'} | ATR={format_decimal(config['min_atr_pct'], 3)}-{format_decimal(config['max_atr_pct'], 3)}% | minVol={format_decimal(config['min_quote_volume'], 0)}",
            f"exits: target={format_decimal(config['take_profit_pct'], 2)}% + fees/spread | stop={format_decimal(config['stop_loss_pct'], 2)}% | trail={format_decimal(config['trailing_stop_pct'], 2)}% | BE arm={format_decimal(config['break_even_profit_pct'], 2)}%",
            "",
            f"closed trades={len(completed_trades)} | wins={len(wins)} | losses={len(losses)} | winRate={format_decimal(win_rate, 2)}%",
            f"net P/L={format_decimal(equity, 4)} USDT | open P/L={format_decimal(open_pnl, 4)} USDT | maxDD={format_decimal(max_drawdown, 4)} USDT | profitFactor={format_decimal(profit_factor, 2)} | blockedBars={skipped_signals}",
            "",
        ]
        for trade in completed_trades[-20:]:
            sign = "+" if trade["pnl"] >= ZERO else ""
            lines.append(
                f"{candle_time(trade['entry_time'])} -> {candle_time(trade['exit_time'])} | "
                f"entry={format_decimal(trade['entry_price'], 4)} | exit={format_decimal(trade['exit_price'], 4)} | "
                f"{trade['reason']} | P/L={sign}{format_decimal(trade['pnl'], 4)} ({format_decimal(trade['pnl_pct'], 2)}%)"
            )
        if position is not None:
            sign = "+" if open_pnl >= ZERO else ""
            lines.append(
                f"open simulated position | entry={format_decimal(position['entry_price'], 4)} | "
                f"last={format_decimal(candles[-1]['close'], 4)} | P/L={sign}{format_decimal(open_pnl, 4)}"
            )
        if not completed_trades and position is None:
            lines.append("No entries matched the current live signal filters.")

        report = "\n".join(lines)
        self.after(
            0,
            lambda trades=len(completed_trades), pnl=equity, s=symbol: self.log_ok(
                f"Backtest complete: {s} | trades={trades} | net={format_decimal(pnl, 4)} USDT"
            ),
        )
        self.after(0, lambda text=report, s=symbol: self.log_block(f"Backtest | {s}", text))

    def toggle_auto_refresh(self) -> None:
        if self.auto_refresh_enabled.get():
            try:
                seconds = self._validate_interval_seconds()
            except Exception as exc:
                self.auto_refresh_enabled.set(False)
                self._handle_error(exc, silence_errors=False)
                return
            self.log_info(f"Автообновление включено. Интервал: {seconds} сек.")
            self._schedule_next_auto_refresh(initial=True)
        else:
            self._cancel_auto_refresh()
            self.log_info("Автообновление выключено.")

    def _schedule_next_auto_refresh(self, initial: bool = False) -> None:
        self._cancel_auto_refresh()
        try:
            seconds = self._validate_interval_seconds()
        except Exception as exc:
            self.auto_refresh_enabled.set(False)
            self._handle_error(exc, silence_errors=False)
            return
        delay_ms = 200 if initial else seconds * 1000
        self.auto_refresh_job = self.after(delay_ms, self._run_auto_refresh)

    def _cancel_auto_refresh(self) -> None:
        if self.auto_refresh_job is not None:
            try:
                self.after_cancel(self.auto_refresh_job)
            except Exception:
                pass
            self.auto_refresh_job = None

    def _run_auto_refresh(self) -> None:
        if not self.auto_refresh_enabled.get():
            self._cancel_auto_refresh()
            return
        if self.auto_refresh_in_flight:
            self._schedule_next_auto_refresh(initial=False)
            return
        self.auto_refresh_in_flight = True

        def runner() -> None:
            try:
                result = self._fetch_order_status()
                client = self._client()
                snapshot = self._build_pnl_snapshot(client, result)
                self.after(0, lambda r=result: self.log_info(f"[AUTO] {self._format_order_summary(r)}"))
                self.after(0, lambda s=snapshot: self._apply_pnl_snapshot(s, auto=True))
            except Exception as exc:
                self.after(0, lambda e=exc: self.log_warn(f"[AUTO] Ошибка автообновления: {e}"))
            finally:
                def finish() -> None:
                    self.auto_refresh_in_flight = False
                    if self.auto_refresh_enabled.get():
                        self._schedule_next_auto_refresh(initial=False)

                self.after(0, finish)

        threading.Thread(target=runner, daemon=True).start()

    def start_auto_trading(self) -> None:
        if self.auto_trade_enabled.get():
            self._set_auto_trade_status("Autotrade is already running.", PNL_COLOR_NEUTRAL, log_level="info")
            self._sync_auto_trade_buttons()
            return
        self.auto_trade_enabled.set(True)
        self.toggle_auto_trading()

    def stop_auto_trading(self) -> None:
        if not self.auto_trade_enabled.get():
            self._set_auto_trade_status("Autotrade is already stopped.", PNL_COLOR_NEUTRAL, log_level="info")
            self._sync_auto_trade_buttons()
            return
        self.auto_trade_enabled.set(False)
        self.toggle_auto_trading()

    def toggle_auto_trading(self) -> None:
        if self.auto_trade_enabled.get():
            try:
                config = self._validate_auto_trade_config()
                client = self._client()
                symbol = self.symbol_var.get().strip().upper()
                if not symbol:
                    raise BinanceDemoError("Set a symbol before enabling autotrade.")

                self._clear_auto_trade_runtime(clear_history=True, clear_position_basis=False)
                self.auto_trade_cycle_count = 0
                sync_symbols = config["scan_symbols"] if config["auto_select"] else [symbol]
                self._sync_mode_health()
                self._set_health_api("connected")
                self._update_health_orders(0, symbol)
                self._ensure_demo_streams()
                synced_count = self._sync_existing_auto_trade_positions(client, sync_symbols)
                if synced_count:
                    self.after(0, lambda: self._update_health_position(label=None))
                else:
                    self.after(0, lambda: self._update_health_position(label="flat"))

                self._set_auto_trade_status(
                    f"Autotrade armed: {symbol} | {config['strategy_mode']} | {config['execution_mode']} | positions {len(self.auto_trade_positions)}/{config['max_positions']} | every {config['interval_seconds']}s",
                    PNL_COLOR_NEUTRAL,
                    log_level="info",
                )
                if config["strategy_mode"] == "Price trigger":
                    trigger = config["price_trigger"]
                    self.log_warn(
                        f"Autotrade armed for price trigger: {trigger['condition']} {format_decimal(trigger['target'], 2)}."
                    )
                else:
                    mode_text = "auto-select" if config["auto_select"] else "single-symbol"
                    self.log_warn(
                        f"Autotrade mean reversion uses MARKET BUY / MARKET SELL on the Binance demo API. "
                        f"Mode={mode_text} | fee={format_decimal(config['fee_pct'], 3)}%/side | "
                        f"min net={format_decimal(config['min_net_profit_pct'], 2)}% | max spread={format_decimal(config['max_spread_pct'], 3)}% | "
                        f"max positions={config['max_positions']} | capital/position={format_decimal(config['capital_per_position'], 2)} USDT | "
                        f"klines={config['kline_interval']}x{config['kline_limit']} | minVol={format_decimal(config['min_quote_volume'], 0)} | "
                        f"ATR={format_decimal(config['min_atr_pct'], 3)}-{format_decimal(config['max_atr_pct'], 3)}% | "
                        f"trail={format_decimal(config['trailing_stop_pct'], 2)}% | BE={format_decimal(config['break_even_profit_pct'], 2)}% | "
                        f"parallel={'on' if config['parallel_enabled'] else 'off'}({config['parallel_workers']} workers). Demo only."
                    )
                self._set_auto_trade_last_check("Last check: waiting for first cycle", PNL_COLOR_NEUTRAL)
                self._schedule_next_auto_trade(initial=True)
                self._sync_auto_trade_buttons()
            except Exception as exc:
                self.auto_trade_enabled.set(False)
                self._sync_auto_trade_buttons()
                self._handle_error(exc, silence_errors=False)
                return
        else:
            self._cancel_auto_trade()
            self.auto_trade_in_flight = False
            self._clear_auto_trade_runtime(clear_history=True, clear_position_basis=False)
            self._set_auto_trade_status("Autotrade: OFF", PNL_COLOR_NEUTRAL, log_level="info")
            self._set_auto_trade_last_check("Last check: stopped", PNL_COLOR_NEUTRAL)
            self._sync_auto_trade_buttons()

    def _schedule_next_auto_trade(self, initial: bool = False) -> None:
        self._cancel_auto_trade()
        try:
            config = self._validate_auto_trade_config()
        except Exception as exc:
            self.auto_trade_enabled.set(False)
            self._handle_error(exc, silence_errors=False)
            return
        delay_ms = 250 if initial else config["interval_seconds"] * 1000
        self.auto_trade_job = self.after(delay_ms, self._run_auto_trade)

    def _cancel_auto_trade(self) -> None:
        if self.auto_trade_job is not None:
            try:
                self.after_cancel(self.auto_trade_job)
            except Exception:
                pass
            self.auto_trade_job = None

    def _run_auto_trade(self) -> None:
        if not self.auto_trade_enabled.get():
            self._cancel_auto_trade()
            return
        if self.auto_trade_in_flight:
            self._schedule_next_auto_trade(initial=False)
            return
        self.auto_trade_in_flight = True

        def runner() -> None:
            try:
                self._auto_trade_cycle_impl()
            except Exception as exc:
                self.after(0, lambda e=exc: self.log_warn(f"[AUTO-TRADE] {e}"))
                self.after(
                    0,
                    lambda e=exc: self._set_auto_trade_status(
                        f"Autotrade warning: {str(e)[:90]}",
                        PNL_COLOR_WARNING,
                    ),
                )
                self.after(
                    0,
                    lambda e=exc: self._set_auto_trade_last_check(
                        f"Last check: {self._timestamp()} | warning | {str(e)[:90]}",
                        PNL_COLOR_WARNING,
                    ),
                )
            finally:
                def finish() -> None:
                    self.auto_trade_in_flight = False
                    if self.auto_trade_enabled.get():
                        self._schedule_next_auto_trade(initial=False)

                self.after(0, finish)

        threading.Thread(target=runner, daemon=True).start()

    def _auto_trade_cycle_impl(self) -> None:
        return self._auto_trade_portfolio_cycle_impl()

    def _auto_trade_portfolio_cycle_impl(self) -> None:
        client = self._client()
        config = self._validate_auto_trade_config()
        strategy_mode = config["strategy_mode"]
        execution_mode = config["execution_mode"]
        now_ts = time.time()
        self.after(0, lambda: self._set_health_api("connected"))

        scan_symbols = config["scan_symbols"] if config["auto_select"] else [self.symbol_var.get().strip().upper()]
        scan_symbols = [symbol for symbol in scan_symbols if symbol]
        if not scan_symbols:
            raise BinanceDemoError("Set at least one symbol before enabling autotrade.")

        # Keep existing positions first. This prevents entry scans from hiding exits.
        portfolio_net = ZERO
        closed_count = 0
        open_symbols = list(self.auto_trade_positions.keys())
        position_snapshots = {
            item["symbol"]: item
            for item in self._parallel_map_ordered(
                lambda item: self._position_check_snapshot(client, item),
                open_symbols,
                config,
            )
        }
        for symbol in open_symbols:
            position_state = self.auto_trade_positions.get(symbol)
            if not position_state:
                continue
            snapshot = position_snapshots.get(symbol, {"ok": False, "error": "position snapshot missing"})
            if not snapshot.get("ok"):
                self.after(
                    0,
                    lambda s=symbol, e=snapshot.get("error", "unknown error"): self.log_warn(
                        f"[AUTO-TRADE] {s} position check skipped: {e}"
                    ),
                )
                continue
            bid = snapshot["bid"]
            ask = snapshot["ask"]
            mid = snapshot["mid"]
            history, average_mid, dip_pct = self._append_auto_trade_price(symbol, mid, config["window"])
            spread_pct = self._spread_pct(bid, ask, mid)
            required_profit_pct = self._auto_trade_required_profit_pct(config, spread_pct)
            balance = snapshot["balance"]
            free_qty = balance["free_qty"]
            if free_qty <= ZERO:
                self.auto_trade_positions.pop(symbol, None)
                self.after(0, lambda s=symbol: self.log_warn(f"[AUTO-TRADE] {s} removed from portfolio: no free balance."))
                continue
            is_sellable = bool(snapshot["is_sellable"])
            sell_qty_for_exit = snapshot["sell_qty"]
            base_asset_for_exit = snapshot["base_asset"]
            sell_details = snapshot["sell_details"]
            dust_reason = snapshot["dust_reason"]
            if not is_sellable:
                self.auto_trade_positions.pop(symbol, None)
                self.after(
                    0,
                    lambda s=symbol, qty=free_qty, reason=dust_reason: self.log_warn(
                        f"[AUTO-TRADE] {s} removed from portfolio as dust: qty={format_decimal(qty)} | {reason}"
                    ),
                )
                continue

            entry_price = decimal_or_zero(position_state.get("entry_price"))
            entry_qty = decimal_or_zero(position_state.get("qty"))
            if entry_price <= ZERO:
                entry_price = mid
                position_state["entry_price"] = entry_price
            if entry_qty <= ZERO:
                entry_qty = free_qty
                position_state["qty"] = entry_qty

            exit_snapshot = self._auto_trade_exit_snapshot(
                position_state,
                entry_price,
                bid,
                min(free_qty, entry_qty),
                spread_pct,
                config,
            )
            pnl_pct = exit_snapshot["pnl_pct"]
            estimated_net_pnl = exit_snapshot["estimated_net_pnl"]
            entry_fee_estimate = exit_snapshot["entry_fee_estimate"]
            exit_fee_estimate = exit_snapshot["exit_fee_estimate"]
            required_profit_pct = exit_snapshot["required_profit_pct"]
            peak_price = exit_snapshot["peak_price"]
            trailing_stop_price = exit_snapshot["trailing_stop_price"]
            break_even_armed = exit_snapshot["break_even_armed"]
            portfolio_net += estimated_net_pnl
            status_color = PNL_COLOR_PROFIT if estimated_net_pnl > ZERO else PNL_COLOR_LOSS if estimated_net_pnl < ZERO else PNL_COLOR_NEUTRAL
            self._publish_auto_trade_heartbeat("hold", symbol, mid, average_mid, dip_pct, status_color)
            self.after(
                0,
                lambda s=symbol, qty=free_qty, entry=entry_price, b=bid, pct=pnl_pct, net=estimated_net_pnl, need=required_profit_pct, peak=peak_price, trail=trailing_stop_price, be=break_even_armed: self.log_pnl(
                    f"[AUTO-TRADE] {s} HOLD qty={format_decimal(qty)} | entry={format_decimal(entry, 2)} | "
                    f"bid={format_decimal(b, 2)} | gross={format_decimal(pct, 2)}% | net≈{format_decimal(net, 2)} | "
                    f"target={format_decimal(need, 2)}% | peak={format_decimal(peak, 2)} | "
                    f"trail={format_decimal(trail, 2) if trail > ZERO else 'off'} | BE={'on' if be else 'off'}",
                    net,
                ),
            )

            exit_reason = exit_snapshot["exit_reason"]
            if not exit_reason:
                continue

            if snapshot["open_orders_error"]:
                self.after(
                    0,
                    lambda s=symbol, e=snapshot["open_orders_error"]: self.log_warn(
                        f"[AUTO-TRADE] {s} exit open-orders check timed out/skipped: {e}"
                    ),
                )
                continue
            open_orders = snapshot["open_orders"]
            self.after(0, lambda count=len(open_orders), s=symbol: self._update_health_orders(count, s))
            if open_orders:
                self.after(0, lambda s=symbol, count=len(open_orders): self.log_warn(f"[AUTO-TRADE] {s} exit paused: {count} open order(s)."))
                continue

            sell_qty, base_asset, details = sell_qty_for_exit, base_asset_for_exit, sell_details
            self._validate_order_filters(client, symbol, sell_qty, "SELL", "MARKET")
            if execution_mode == "Manual":
                self.after(
                    0,
                    lambda reason=exit_reason, s=symbol, qty=sell_qty, asset=base_asset: self.log_warn(
                        f"[AUTO-TRADE] EXIT signal only ({reason}): {s} | qty={qty} {asset}"
                    ),
                )
                continue

            if execution_mode == "Test first":
                self._run_test_market_order(client, symbol, "SELL", sell_qty)
            result = client.market_sell(symbol=symbol, quantity=sell_qty)
            result = self._refresh_order_snapshot(client, symbol, result)
            self._journal_order_event("order_submit", symbol, f"Autotrade {exit_reason} sell", result)
            exit_price, executed_qty, realized_pnl, _sell_order_id = self._record_exit_fill(
                symbol,
                f"Autotrade {exit_reason} sell",
                result,
                bid,
                sell_qty,
                fee_config=config,
            )
            closed_count += 1
            self.auto_trade_cooldown_until = now_ts + config["cooldown_seconds"]
            self.after(
                0,
                lambda reason=exit_reason, s=symbol, qty=executed_qty, asset=base_asset, px=exit_price, pnl=realized_pnl: self.log_ok(
                    f"[AUTO-TRADE] SELL {reason}: {s} | qty={format_decimal(qty)} {asset} | avg={format_decimal(px, 2)} | net realized≈{format_decimal(pnl, 2)}"
                ),
            )
            self.after(0, lambda payload=result: self.log_block("[AUTO-TRADE] Binance AUTO SELL response", payload))

        if now_ts < self.auto_trade_cooldown_until:
            remaining = int(max(0, self.auto_trade_cooldown_until - now_ts))
            self.after(
                0,
                lambda rem=remaining, count=len(self.auto_trade_positions), net=portfolio_net: self._set_auto_trade_status(
                    f"Autotrade portfolio cooldown {rem}s | open={count} | net≈{format_decimal(net, 2)}",
                    PNL_COLOR_WARNING,
                ),
            )
            self.after(0, lambda: self._update_health_position(label=None))
            return

        candidates = self._scan_auto_trade_candidates(client, config, scan_symbols)
        entries_left = max(0, config["max_positions"] - len(self.auto_trade_positions))
        opened_count = 0
        if strategy_mode == "Price trigger":
            candidates = [item for item in candidates if item["symbol"] == self.symbol_var.get().strip().upper()]

        entry_check_symbols: list[str] = []
        for candidate in candidates:
            if len(entry_check_symbols) >= entries_left:
                break
            symbol = candidate["symbol"]
            if symbol in self.auto_trade_positions:
                continue
            if candidate["blocked_reason"] or not candidate["ready"]:
                continue
            if strategy_mode == "Mean reversion":
                should_enter, _reason = self._mean_reversion_signal(candidate, config)
                if not should_enter:
                    continue
            entry_check_symbols.append(symbol)
        entry_open_orders = {
            item["symbol"]: item
            for item in self._parallel_map_ordered(
                lambda item: self._open_orders_check_snapshot(client, item),
                entry_check_symbols,
                config,
            )
        }

        for candidate in candidates:
            if entries_left <= 0:
                break
            symbol = candidate["symbol"]
            if symbol in self.auto_trade_positions:
                continue
            if candidate["blocked_reason"] or not candidate["ready"]:
                continue
            open_order_snapshot = entry_open_orders.get(symbol)
            if not open_order_snapshot or not open_order_snapshot.get("ok"):
                error = open_order_snapshot.get("error", "open-orders check missing") if open_order_snapshot else "open-orders check missing"
                self.after(
                    0,
                    lambda s=symbol, e=error: self.log_warn(
                        f"[AUTO-TRADE] {s} entry skipped: open-orders check failed after retries: {e}"
                    ),
                )
                continue
            open_orders = open_order_snapshot["open_orders"]
            if open_orders:
                self.after(0, lambda s=symbol, count=len(open_orders): self.log_warn(f"[AUTO-TRADE] {s} entry skipped: {count} open order(s)."))
                continue

            bid = candidate["bid"]
            ask = candidate["ask"]
            mid = candidate["mid"]
            average_mid = candidate["average_mid"]
            dip_pct = candidate["dip_pct"]
            spread_pct = candidate["spread_pct"]
            required_profit_pct = self._auto_trade_required_profit_pct(config, spread_pct)

            if strategy_mode == "Price trigger":
                trigger_config = config["price_trigger"]
                triggered, trigger_price, trigger_text = self._evaluate_price_trigger(bid, ask, trigger_config)
                if not triggered:
                    continue
                signal_reason = f"Price trigger hit: {trigger_text} | current={format_decimal(trigger_price, 2)}"
            else:
                triggered, signal_reason = self._mean_reversion_signal(candidate, config)
                if not triggered:
                    continue

            self._enforce_risk_limits(client, symbol)
            quantity = (
                self._resolve_buy_quantity(client, symbol, "MARKET", apply_risk_to_field=not config["auto_select"])
                if self.use_risk_sizing_var.get()
                else self._quantity_for_capital(client, symbol, ask, config["capital_per_position"])
            )
            self._validate_order_filters(client, symbol, quantity, "BUY", "MARKET")

            if execution_mode == "Manual":
                self.after(0, lambda s=symbol, reason=signal_reason, q=quantity: self.log_warn(f"[AUTO-TRADE] ENTRY signal only: {s} | {reason} | qty={q}"))
                entries_left -= 1
                continue
            if execution_mode == "Test first":
                self._run_test_market_order(client, symbol, "BUY", quantity)

            result = client.market_buy(symbol=symbol, quantity=quantity)
            result = self._refresh_order_snapshot(client, symbol, result)
            self._journal_order_event("order_submit", symbol, signal_reason, result)
            entry_price, entry_qty, buy_order_id = self._record_entry_fill(symbol, signal_reason, result, ask, quantity)
            fee_estimate = entry_price * entry_qty * config["fee_pct"] / Decimal("100")
            if symbol in self.auto_trade_positions:
                self.auto_trade_positions[symbol]["entry_fee_estimate_quote"] = fee_estimate
            opened_count += 1
            entries_left -= 1
            self.after(
                0,
                lambda s=symbol, q=entry_qty, px=entry_price, reason=signal_reason, fee=fee_estimate, target=required_profit_pct: self.log_ok(
                    f"[AUTO-TRADE] BUY executed: {s} | {reason} | qty={format_decimal(q)} | avg={format_decimal(px, 2)} | entry fee≈{format_decimal(fee, 2)} | exit target={format_decimal(target, 2)}%"
                ),
            )
            self.after(0, lambda payload=result: self.log_block("[AUTO-TRADE] Binance AUTO BUY response", payload))

        color = PNL_COLOR_PROFIT if portfolio_net > ZERO else PNL_COLOR_LOSS if portfolio_net < ZERO else PNL_COLOR_NEUTRAL
        self.after(
            0,
            lambda open_count=len(self.auto_trade_positions), opened=opened_count, closed=closed_count, net=portfolio_net: self._set_auto_trade_status(
                f"Autotrade portfolio | open={open_count}/{config['max_positions']} | opened={opened} | closed={closed} | net≈{format_decimal(net, 2)}",
                color,
            ),
        )
        self.after(0, lambda: self._update_health_position(label=None))
        self.after(0, self._write_position_snapshot)
        return

        # Legacy single-position implementation is kept below as a fallback reference.
        client = self._client()
        config = self._validate_auto_trade_config()
        symbol = self.symbol_var.get().strip().upper()
        if not symbol:
            raise BinanceDemoError("Set a symbol before enabling autotrade.")

        self.after(0, lambda: self._set_health_api("connected"))
        has_position_basis = self.auto_trade_entry_price is not None and self.auto_trade_entry_price > ZERO
        if config["auto_select"] and config["strategy_mode"] == "Mean reversion" and not has_position_basis:
            candidate = self._select_auto_trade_candidate(client, config)
            symbol = candidate["symbol"]
            if symbol != self.symbol_var.get().strip().upper():
                self.after(0, lambda s=symbol: self.symbol_var.set(s))
            bid = candidate["bid"]
            ask = candidate["ask"]
            mid = candidate["mid"]
            history_size = candidate["history_size"]
            average_mid = candidate["average_mid"]
            dip_pct = candidate["dip_pct"]
            spread_pct = candidate["spread_pct"]
            if candidate["blocked_reason"]:
                self._publish_auto_trade_heartbeat("blocked-spread", symbol, mid, average_mid, dip_pct, PNL_COLOR_WARNING)
                self.after(
                    0,
                    lambda s=symbol, reason=candidate["blocked_reason"]: self._set_auto_trade_status(
                        f"Autotrade scan blocked {s}: {reason}",
                        PNL_COLOR_WARNING,
                    ),
                )
                return
        else:
            bid, ask, mid = self._current_market_prices(client, symbol)
            history, average_mid, dip_pct = self._append_auto_trade_price(symbol, mid, config["window"])
            history_size = len(history)
            spread_pct = self._spread_pct(bid, ask, mid)

        required_profit_pct = self._auto_trade_required_profit_pct(config, spread_pct)
        now_ts = time.time()
        strategy_mode = config["strategy_mode"]
        execution_mode = config["execution_mode"]

        position = self._position_balance_for_symbol(client, symbol)
        free_qty = position["free_qty"]
        open_orders = client.get_open_orders_retry(symbol=symbol)
        self.after(0, lambda count=len(open_orders), s=symbol: self._update_health_orders(count, s))
        if open_orders:
            self._publish_auto_trade_heartbeat("paused-open-orders", symbol, mid, average_mid, dip_pct, PNL_COLOR_WARNING)
            self.after(
                0,
                lambda count=len(open_orders), s=symbol: self._set_auto_trade_status(
                    f"Autotrade paused: {s} has {count} open order(s).",
                    PNL_COLOR_WARNING,
                    log_level="warn",
                ),
            )
            return

        if strategy_mode == "Mean reversion" and history_size < config["window"]:
            self._publish_auto_trade_heartbeat("warmup", symbol, mid, average_mid, dip_pct, PNL_COLOR_NEUTRAL)
            self.after(
                0,
                lambda s=symbol, size=history_size, need=config["window"], m=mid: self._set_auto_trade_status(
                    f"Autotrade warmup: {s} | samples {size}/{need} | mid={format_decimal(m, 2)}",
                    PNL_COLOR_NEUTRAL,
                ),
            )
            return

        if free_qty > ZERO:
            if self.auto_trade_entry_price is None or self.auto_trade_entry_price <= ZERO:
                self.auto_trade_entry_price = mid
                self.auto_trade_entry_qty = free_qty
                self.position_entry_reason = self.position_entry_reason or "Autotrade balance sync"
                self.position_entry_timestamp = self.position_entry_timestamp or datetime.now().isoformat(timespec="seconds")
                self.after(
                    0,
                    lambda qty=free_qty, px=mid, asset=position["base_asset"]: self.log_warn(
                        f"Autotrade synced an existing {asset} balance {format_decimal(qty)}. Entry basis is current mid {format_decimal(px, 2)}."
                    ),
                )
            self.after(0, lambda s=symbol, qty=free_qty, px=self.auto_trade_entry_price: self._update_health_position(s, qty, px))

            entry_price = self.auto_trade_entry_price or mid
            pnl_pct = ((bid - entry_price) / entry_price * Decimal("100")) if entry_price > ZERO else ZERO
            estimated_net_pnl, entry_fee_estimate, exit_fee_estimate = self._auto_trade_estimated_net_pnl(
                entry_price,
                bid,
                free_qty,
                config,
            )
            status_color = PNL_COLOR_NEUTRAL
            if estimated_net_pnl > ZERO:
                status_color = PNL_COLOR_PROFIT
            elif estimated_net_pnl < ZERO:
                status_color = PNL_COLOR_LOSS

            self._publish_auto_trade_heartbeat("hold", symbol, mid, average_mid, dip_pct, status_color)
            self.after(
                0,
                lambda s=symbol, qty=free_qty, entry=entry_price, b=bid, pct=pnl_pct, net=estimated_net_pnl, need=required_profit_pct: self._set_auto_trade_status(
                    f"Autotrade HOLD {s} | qty={format_decimal(qty)} | entry={format_decimal(entry, 2)} | bid={format_decimal(b, 2)} | gross={format_decimal(pct, 2)}% | net≈{format_decimal(net, 2)} | target={format_decimal(need, 2)}%",
                    status_color,
                ),
            )
            self.after(
                0,
                lambda s=symbol, net=estimated_net_pnl, ef=entry_fee_estimate, xf=exit_fee_estimate, sp=spread_pct: self.log_pnl(
                    f"[AUTO-TRADE] progress {s}: net≈{format_decimal(net, 2)} after fees≈{format_decimal(ef + xf, 2)} | spread={format_decimal(sp, 3)}%",
                    net,
                ),
            )

            exit_reason = ""
            if pnl_pct >= required_profit_pct and estimated_net_pnl > ZERO:
                exit_reason = "fee-aware take-profit"
            elif pnl_pct <= -config["stop_loss_pct"]:
                exit_reason = "stop-loss"

            if not exit_reason:
                return

            self._publish_auto_trade_heartbeat(f"exit-{exit_reason}", symbol, mid, average_mid, dip_pct, PNL_COLOR_WARNING)
            sell_qty, base_asset, details = self._sellable_quantity_for_symbol(client, symbol)
            self._validate_order_filters(client, symbol, sell_qty, "SELL", "MARKET")

            if execution_mode == "Manual":
                self.after(
                    0,
                    lambda reason=exit_reason, s=symbol, qty=sell_qty, asset=base_asset, b=bid, entry=entry_price: self.log_warn(
                        f"[AUTO-TRADE] EXIT signal only ({reason}): {s} | qty={qty} {asset} | entry={format_decimal(entry, 2)} | bid={format_decimal(b, 2)}"
                    ),
                )
                self.after(
                    0,
                    lambda reason=exit_reason, s=symbol: self._set_auto_trade_status(
                        f"Autotrade EXIT SIGNAL {s} by {reason}. Manual mode leaves the position open.",
                        PNL_COLOR_WARNING,
                    ),
                )
                return

            if execution_mode == "Test first":
                self._run_test_market_order(client, symbol, "SELL", sell_qty)

            result = client.market_sell(symbol=symbol, quantity=sell_qty)
            result = self._refresh_order_snapshot(client, symbol, result)
            self._journal_order_event("order_submit", symbol, f"Autotrade {exit_reason} sell", result)
            exit_price, executed_qty, realized_pnl, sell_order_id = self._record_exit_fill(
                symbol,
                f"Autotrade {exit_reason} sell",
                result,
                bid,
                sell_qty,
                fee_config=config,
            )
            self.auto_trade_cooldown_until = now_ts + config["cooldown_seconds"]

            self.after(
                0,
                lambda reason=exit_reason, s=symbol, qty=executed_qty, asset=base_asset, px=exit_price, pnl=realized_pnl: self.log_ok(
                    f"[AUTO-TRADE] SELL {reason}: {s} | qty={format_decimal(qty)} {asset} | avg={format_decimal(px, 2)} | realized={format_decimal(pnl, 2)}"
                ),
            )
            self.after(
                0,
                lambda info=details: self.log_info(
                    f"[AUTO-TRADE] SELL filters: free={format_decimal(info['free_qty'])}, locked={format_decimal(info['locked_qty'])}, step={format_decimal(info['step_size'])}, minQty={format_decimal(info['min_qty'])}"
                ),
            )
            self.after(
                0,
                lambda pnl=realized_pnl: self.log_pnl(
                    f"[AUTO-TRADE] Exit realized P/L: {format_decimal(pnl, 2)}",
                    pnl,
                ),
            )
            self.after(0, lambda payload=result: self.log_block("[AUTO-TRADE] Binance AUTO SELL response", payload))
            self.after(
                0,
                lambda reason=exit_reason, s=symbol, cd=config["cooldown_seconds"]: self._set_auto_trade_status(
                    f"Autotrade EXIT {s} by {reason}. Cooldown {cd}s.",
                    PNL_COLOR_WARNING,
                    log_level="ok",
                ),
            )
            return

        self.auto_trade_entry_price = None
        self.auto_trade_entry_qty = None
        self.position_entry_timestamp = ""
        self.position_entry_reason = ""
        self.position_entry_order_id = ""
        self.position_entry_commission = ZERO
        self.auto_trade_entry_fee_estimate_quote = ZERO
        self.after(0, lambda: self._update_health_position(label=None))

        if now_ts < self.auto_trade_cooldown_until:
            remaining = int(max(0, self.auto_trade_cooldown_until - now_ts))
            self._publish_auto_trade_heartbeat("cooldown", symbol, mid, average_mid, dip_pct, PNL_COLOR_WARNING)
            self.after(
                0,
                lambda s=symbol, rem=remaining, dip=dip_pct, avg=average_mid, m=mid: self._set_auto_trade_status(
                    f"Autotrade cooldown {rem}s | {s} | mid={format_decimal(m, 2)} | SMA={format_decimal(avg, 2)} | dip={format_decimal(dip, 2)}%",
                    PNL_COLOR_WARNING,
                ),
            )
            return

        signal_reason = ""
        if strategy_mode == "Price trigger":
            trigger_config = config["price_trigger"]
            triggered, trigger_price, trigger_text = self._evaluate_price_trigger(bid, ask, trigger_config)
            self._publish_auto_trade_heartbeat(
                "ready-trigger" if not triggered else "entry-trigger",
                symbol,
                mid,
                average_mid,
                dip_pct,
                PNL_COLOR_NEUTRAL if not triggered else PNL_COLOR_PROFIT,
            )
            self.after(
                0,
                lambda s=symbol, text=trigger_text, px=trigger_price: self._set_auto_trade_status(
                    f"Autotrade READY {s} | trigger {text} | current={format_decimal(px, 2)}",
                    PNL_COLOR_NEUTRAL,
                ),
            )
            if not triggered:
                return
            signal_reason = f"Price trigger hit: {trigger_text} | current={format_decimal(trigger_price, 2)}"
        else:
            self._publish_auto_trade_heartbeat("ready", symbol, mid, average_mid, dip_pct, PNL_COLOR_NEUTRAL)
            self.after(
                0,
                lambda s=symbol, dip=dip_pct, avg=average_mid, m=mid, need=config["buy_threshold_pct"], sp=spread_pct, target=required_profit_pct: self._set_auto_trade_status(
                    f"Autotrade READY {s} | mid={format_decimal(m, 2)} | SMA={format_decimal(avg, 2)} | dip={format_decimal(dip, 2)}%/{format_decimal(need, 2)}% | spread={format_decimal(sp, 3)}% | net target={format_decimal(target, 2)}%",
                    PNL_COLOR_NEUTRAL,
                ),
            )
            if dip_pct < config["buy_threshold_pct"]:
                return
            signal_reason = (
                f"Mean reversion buy: dip {format_decimal(dip_pct, 2)}% below SMA | "
                f"spread {format_decimal(spread_pct, 3)}% | target net-aware {format_decimal(required_profit_pct, 2)}%"
            )

        self._publish_auto_trade_heartbeat("entry-signal", symbol, mid, average_mid, dip_pct, PNL_COLOR_PROFIT)
        self._enforce_risk_limits(client, symbol)
        quantity = self._resolve_buy_quantity(client, symbol, "MARKET", apply_risk_to_field=True)
        self._validate_order_filters(client, symbol, quantity, "BUY", "MARKET")

        if execution_mode == "Manual":
            self.auto_trade_cooldown_until = now_ts + config["cooldown_seconds"]
            self.after(
                0,
                lambda s=symbol, reason=signal_reason, q=quantity: self.log_warn(
                    f"[AUTO-TRADE] ENTRY signal only: {s} | {reason} | qty={q}"
                ),
            )
            self.after(
                0,
                lambda s=symbol: self._set_auto_trade_status(
                    f"Autotrade SIGNAL {s} detected. Manual mode did not send an order.",
                    PNL_COLOR_WARNING,
                ),
            )
            return

        if execution_mode == "Test first":
            self._run_test_market_order(client, symbol, "BUY", quantity)

        result = client.market_buy(symbol=symbol, quantity=quantity)
        result = self._refresh_order_snapshot(client, symbol, result)
        self._journal_order_event("order_submit", symbol, signal_reason, result)
        entry_price, entry_qty, buy_order_id = self._record_entry_fill(symbol, signal_reason, result, ask, quantity)
        self.auto_trade_entry_fee_estimate_quote = entry_price * entry_qty * config["fee_pct"] / Decimal("100")
        self.after(
            0,
            lambda s=symbol, q=entry_qty, px=entry_price, reason=signal_reason, fee=self.auto_trade_entry_fee_estimate_quote, target=required_profit_pct: self.log_ok(
                f"[AUTO-TRADE] BUY executed: {s} | {reason} | qty={format_decimal(q)} | avg={format_decimal(px, 2)} | entry fee≈{format_decimal(fee, 2)} | exit target={format_decimal(target, 2)}%"
            ),
        )
        self.after(0, lambda payload=result: self.log_block("[AUTO-TRADE] Binance AUTO BUY response", payload))
        self.after(
            0,
            lambda s=symbol, q=entry_qty, px=entry_price: self._set_auto_trade_status(
                f"Autotrade ENTER {s} | qty={format_decimal(q)} | avg={format_decimal(px, 2)}",
                PNL_COLOR_PROFIT,
                log_level="ok",
            ),
        )

    def _on_close(self) -> None:
        self.auto_refresh_enabled.set(False)
        self._cancel_auto_refresh()
        self.auto_trade_enabled.set(False)
        self._cancel_auto_trade()
        self._stop_market_stream(log_message=False)
        self._stop_user_data_stream(log_message=False)
        self.destroy()


if __name__ == "__main__":
    app = App()
    app.mainloop()
