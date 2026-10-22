# Kite → AmiBroker Bridge

Live 1-minute bars from Zerodha (Kite Connect) into AmiBroker, plus history backfill. Read-only — it never places orders.

```
Zerodha live feed ──► Bridge (1-minute bars) ──► AmiBroker (imported every few seconds)
Zerodha minute history ──► Bridge ──► AmiBroker (backfill, and an end-of-day repair)
```

No AmiBroker plug-in is needed: the bridge uses AmiBroker's own automation (the same ASCII import you can do by hand),
so it works with any AmiBroker 5/6 on Windows, 32- or 64-bit.

## One-time setup

1. **Python 3.10+** from python.org — tick "Add python.exe to PATH".
2. **AmiBroker database for 1-minute data** (keep it separate from your daily database):
   File → New → Database → pick a folder (e.g. `C:\AmiBroker\Kite1m`) → **Create** →
   *Base time interval* = **1 minute**, Data source = **(local database)**, Number of bars = e.g. 500000 → OK.
   Under *Intraday settings* keep the time shift at 0 (the data is already in Indian time).
3. **Zerodha login** — pick one on the bridge page:
   * **Use NSE EOD Fetcher's login** (default) — log in to Zerodha in NSE EOD Fetcher each morning as usual;
     the bridge reads that login from `nse_eod.db` and starts by itself. One Kite app, one daily login.
   * **My own Kite app** — enter its API key and secret, set the app's redirect URL to
     `http://127.0.0.1:8770/callback`, then press *Log in to Zerodha* each morning.
   (Logging in to the *same* Kite app from two programs can sign the other one out — that is why the
   shared mode reuses NSE EOD Fetcher's login instead of logging in again.)

## Every day

1. Open AmiBroker with the 1-minute database.
2. Double-click **START.bat** — the control page opens at http://127.0.0.1:8770.
3. During market hours, once Zerodha is logged in, the bridge starts on its own: it fetches any missing history,
   then streams. AmiBroker charts update every few seconds (setting: *Send to AmiBroker every … seconds*).

If AmiBroker is closed, nothing is lost — bars wait and go in when it is opened again.

## Symbols

One per line using Zerodha's name (`RELIANCE`, `NIFTY 50`, `NIFTY BANK` …). To give it a different name in
AmiBroker write `NIFTY 50 = NIFTY`. Up to about 200 is comfortable (hard limit 400). The buttons under the list
add the stocks from NSE EOD Fetcher's three watchlists. New tickers are created in AmiBroker automatically.

## History

* **Fetch missing history** — fills each symbol from where it last stopped, up to *History to fetch (days)*.
  Zerodha serves 1-minute history in 60-day pieces at about 3 requests a second, so 200 symbols × 1 year
  takes roughly 8 minutes.
* **Re-send all history** — forgets what was sent and fetches the whole range again (use this after creating a
  new AmiBroker database).
* **3:40 PM repair** — replaces the day's live bars with Zerodha's official minute candles, so volumes match
  exactly.

## How the live bars are built

Each tick's price updates the current minute's open/high/low/close; volume is the increase in the day's total volume
traded since the previous tick. Bars are stamped with the start of the minute (09:15 = 09:15:00–09:15:59), the
same as Zerodha's history. Only 09:15–15:30 is kept (pre-open and after-market ticks are ignored). Indices have no
volume.

## Where things are kept

Settings (including the API secret), progress and the small import files live in the folder named in
`data_location.txt` (for example `C:\Users\Lenovo\Desktop\OHLC_Data\AmiBridge`), else in the bridge's own `data`
folder. Nothing private is in this repository — `data/`, `data_location.txt` and `settings.json` are ignored by git.
The import format is also saved as `kite2ami.format` there (and copied into AmiBroker's Formats folder when Windows
allows) in case you want to import a file by hand.

## Files

| File | What it does |
|---|---|
| `bridge.py` | control page, Zerodha login, symbol list, history, bar building, scheduling |
| `kitews.py` | Zerodha live WebSocket — decodes Kite's binary ticks |
| `amifeed.py` | writes AmiBroker import files and imports them through AmiBroker's automation |
| `static/index.html` | the control page |
