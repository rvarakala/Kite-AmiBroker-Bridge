"""
Zerodha Kite Connect live feed (WebSocket) — read-only.

Connects to wss://ws.kite.trade, subscribes the chosen instruments in "quote" mode and hands every tick to a callback:
    on_tick(token, last_price, day_volume)      # day_volume = cumulative volume traded today (0 for indices)
Kite's binary packet layout (all integers big-endian, prices in paise):
    message = int16 number_of_packets, then for each: int16 length + packet
    quote packet (44 bytes, stocks)  : token, ltp, last_qty, avg_price, volume, buy_qty, sell_qty, open, high, low, close
    index packet (28 / 32 bytes)     : token, ltp, high, low, open, close, change[, exchange_time]
    ltp packet (8 bytes)             : token, ltp
A 1-byte message is a heartbeat. Text messages are JSON (errors / notices).
"""
import json
import os
import struct
import threading
import time

import websocket

WS_URL = os.environ.get("KITE_WS_URL", "wss://ws.kite.trade")


def parse(msg):
    """binary message → list of (token, ltp, day_volume)"""
    out = []
    if len(msg) < 2:
        return out
    n = struct.unpack(">H", msg[:2])[0]
    j = 2
    for _ in range(n):
        if j + 2 > len(msg):
            break
        ln = struct.unpack(">H", msg[j:j + 2])[0]
        p = msg[j + 2:j + 2 + ln]
        j += 2 + ln
        if len(p) < 8:
            continue
        tok, ltp = struct.unpack(">ii", p[:8])
        div = 100.0
        seg = tok & 0xFF
        if seg == 3:                 # currency derivatives
            div = 10000000.0
        elif seg == 6:               # BCD
            div = 10000.0
        vol = 0
        if len(p) >= 44 and len(p) not in (28, 32):
            vol = struct.unpack(">i", p[16:20])[0]
        out.append((tok, ltp / div, vol))
    return out


class Ticker:
    def __init__(self, api_key, access_token, tokens, on_tick, log=print):
        self.api_key, self.access_token = api_key, access_token
        self.tokens = list(tokens)
        self.on_tick, self.log = on_tick, log
        self.ws = None
        self.stop_flag = threading.Event()
        self.connected = False
        self.ticks = 0
        self.last_tick = None
        self.error = ""
        self.auth_failed = False
        self.th = None

    def start(self):
        self.th = threading.Thread(target=self._run, daemon=True, name="kite-ws")
        self.th.start()

    def stop(self):
        self.stop_flag.set()
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass

    def _subscribe(self, ws):
        for i in range(0, len(self.tokens), 500):
            part = self.tokens[i:i + 500]
            ws.send(json.dumps({"a": "subscribe", "v": part}))
            ws.send(json.dumps({"a": "mode", "v": ["quote", part]}))

    def _run(self):
        wait = 2
        while not self.stop_flag.is_set():
            url = f"{WS_URL}?api_key={self.api_key}&access_token={self.access_token}"
            try:
                self.ws = websocket.WebSocketApp(
                    url,
                    on_open=lambda ws: self._opened(ws),
                    on_message=lambda ws, m: self._message(m),
                    on_error=lambda ws, e: self._err(e),
                    on_close=lambda ws, a, b: self._closed())
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:                # noqa: BLE001
                self._err(e)
            self.connected = False
            if self.stop_flag.is_set() or self.auth_failed:
                break
            self.log(f"Live feed disconnected — reconnecting in {wait} s")
            self.stop_flag.wait(wait)
            wait = min(wait * 2, 60)
        self.connected = False

    def _opened(self, ws):
        self.connected, self.error = True, ""
        self._subscribe(ws)
        self.log(f"Live feed connected — {len(self.tokens)} instruments")

    def _message(self, m):
        if isinstance(m, str):
            try:
                j = json.loads(m)
            except ValueError:
                return
            if j.get("type") == "error":
                self.error = str(j.get("data"))
                self.log("Zerodha: " + self.error)
            return
        if len(m) < 2:
            return                                   # heartbeat
        for tok, ltp, vol in parse(m):
            self.ticks += 1
            self.last_tick = time.time()
            try:
                self.on_tick(tok, ltp, vol)
            except Exception as e:                    # noqa: BLE001
                self.log(f"tick error: {e}")

    def _err(self, e):
        s = str(e)
        self.error = s
        if "403" in s or "Forbidden" in s or "401" in s:
            self.auth_failed = True
            self.error = "Zerodha refused the login (token expired?) — log in again."
        self.log("Live feed: " + self.error)

    def _closed(self):
        self.connected = False
