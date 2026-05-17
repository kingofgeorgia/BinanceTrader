# Binance Demo Trader

A desktop GUI for Binance Demo trading. The app lets you connect with Binance Demo API keys, inspect account data, place demo orders, track positions, and keep local order/trade history.

## Features

- Tkinter desktop interface
- Binance Demo REST API integration
- Binance Demo WebSocket support
- Market, limit, stop-loss, and take-profit order workflows
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

## Local Data

The app may create local runtime files such as:

- `orders.jsonl`
- `positions.json`
- `trades.csv`

These files are ignored by Git because they contain local trading history and app state.
