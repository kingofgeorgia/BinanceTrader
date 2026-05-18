# Binance Demo Trader

A desktop GUI for Binance Demo trading. The app lets you connect with Binance Demo API keys, inspect account data, place demo orders, track positions, and keep local order/trade history.

## Features

- Tkinter desktop interface
- Binance Demo REST API integration
- Binance Demo WebSocket support
- Market, limit, stop-loss, take-profit, break-even, and trailing-stop order workflows
- Fee-aware multi-position autotrade scanner for selected USDT pairs
- Kline-based EMA/ATR/RSI, volume, volatility, and trend filters
- Kline backtest that reuses the live autotrade signal and exit logic
- Local order and position persistence
- CSV trade journal export

## Requirements

- Python 3.10 or newer
- Dependencies from `requirements.txt`

## Installation

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Configuration

Create your local key file from the example:

```powershell
Copy-Item binance_demo_keys.example.json binance_demo_keys.json
```

Then edit `binance_demo_keys.json` and add your Binance Demo API key and secret.

`binance_demo_keys.json` is intentionally ignored by Git. Do not commit real API credentials.

## Run

```powershell
python binance_demo_gui_clipboard_v3.py
```

## Autotrade

Autotrade can scan multiple USDT pairs and keep several positions open at the same time.

Key controls:

- `Auto-select`: scans the configured symbol list and chooses candidates automatically.
- `Scan symbols`: comma-separated USDT pairs to scan.
- `Max positions`: maximum simultaneous autotrade positions.
- `USDT/position`: quote amount to allocate to each new position when risk sizing is off.
- `Fee %`: estimated fee per side.
- `Min net %`: extra target profit after estimated fees and spread.
- `Max spread %`: skips pairs with a wider spread.
- `Klines` / `Bars`: candle interval and sample size for EMA/ATR/RSI calculations.
- `Min vol USDT`: minimum quote volume over the recent candle window.
- `Trend EMA`: requires EMA20/EMA50 bullish trend before a mean-reversion entry.
- `ATR % min/max`: skips pairs that are too flat or too volatile.
- `Trail %`: trailing stop distance from the best price after entry.
- `BE arm %`: profit threshold that arms a fee-aware break-even stop.

Use `Run backtest` in the `Trade History` block to test the current symbol with the same mean-reversion signal filters and exit logic used by the live autotrade loop.

The bot saves portfolio state in `positions.json` and restores it on startup. It still cannot guarantee profit; market moves, slippage, liquidity, and API behavior can produce losses.

## Local Data

The app may create local runtime files such as:

- `orders.jsonl`
- `positions.json`
- `trades.csv`

These files are ignored by Git because they contain local trading history and app state.
