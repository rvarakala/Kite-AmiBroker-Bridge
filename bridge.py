"""
Kite → AmiBroker Bridge (read-only: it never places orders).

Zerodha live feed  →  1-minute bars  →  AmiBroker (imported every few seconds while AmiBroker is open)
plus a history backfill of 1-minute candles from Zerodha, and an end-of-day repair that replaces the day's live bars
with Zerodha's official minute candles.

Run:  python bridge.py   (or START.bat)   →   control page at http://127.0.0.1:8770
"""
import argparse
import csv
import hashlib
import io
import json
import os
import secrets
import sqlite3
import threading
import time
import webbrowser
from collections import deque
from datetime import date, datetime, timedelta
from urllib.parse import urlencode

import requests
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from pydantic import BaseModel

import amifeed as AF
import kitews as KW

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PORT = 8770
API = os.environ.get("KITE_API_BASE", "https://api.kite.trade")
LOGIN = os.environ.get("KITE_LOGIN_URL", "https://kite.zerodha.com/connect/login")
HDR = {"X-Kite-Version": "3"}
SESSION_START, SESSION_END = (9, 15), (15, 30)
REPAIR_AT = (15, 40)
CHUNK_DAYS = 55                     # Zerodha serves at most 60 days of minute candles per request
MAX_SYMBOLS = 400


# ------------------------------------------------------------------ data folder (same idea as NSE EOD Fetcher)
def _data_dir():
    want = os.environ.get("BRIDGE_DATA_DIR", "").strip()
    note = os.path.join(BASE_DIR, "data_location.txt")
    if not want and os.path.exists(note):
        try:
            with open(note, encoding="utf-8-sig") as fh:
                want = next((ln.strip().strip('"') for ln in fh if ln.strip() and not ln.strip().startswith("#")), "")
        except OSError:
            want = ""
    if want and os.name != "nt" and len(want) > 1 and want[1] == ":":
        want = ""
    if want:
        want = os.path.abspath(os.path.expandvars(os.path.expanduser(want)))
        try:
            os.makedirs(want, exist_ok=True)
            return want
        except OSError as e:
            print(f"  Data folder {want} is not usable ({e}) — using the bridge's own data folder.")
    d = os.path.join(BASE_DIR, "data")
    os.makedirs(d, exist_ok=True)
    return d


DATA_DIR = _data_dir()
FEED_DIR = os.path.join(DATA_DIR, "feed")
os.makedirs(FEED_DIR, exist_ok=True)
SETTINGS = os.path.join(DATA_DIR, "settings.json")
STATEF = os.path.join(DATA_DIR, "state.json")

LOGQ = deque(maxlen=300)


def log(msg):
    line = f"{now_ist():%H:%M:%S}  {msg}"
    LOGQ.append(line)
    print("  " + line)


def now_ist():
    return datetime.utcnow() + timedelta(hours=5, minutes=30)


def _load(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return {**default, **json.load(f)}
    except (OSError, ValueError):
        return dict(default)


def _save(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=1)
    os.replace(tmp, path)


DEFAULTS = dict(login_mode="shared", nse_db="", api_key="", secret="", own_token={},
                symbols="NIFTY 50 = NIFTY\nNIFTY BANK = BANKNIFTY\nRELIANCE\nHDFCBANK\nINFY",
                push_secs=5, backfill_days=30, auto_backfill=True, eod_repair=True)


def settings():
    return _load(SETTINGS, DEFAULTS)


def save_settings(s):
    _save(SETTINGS, s)


# ------------------------------------------------------------------ Zerodha login: shared with NSE EOD Fetcher, or own app
def find_nse_db():
    home = os.path.expanduser("~")
    for p in (os.path.join(home, "Desktop", "OHLC_Data", "nse_eod.db"),
              os.path.join(os.path.dirname(DATA_DIR), "nse_eod.db"),
              os.path.join(os.path.dirname(BASE_DIR), "NSE_EOD_FETCHER", "data", "nse_eod.db")):
        if os.path.exists(p):
            return p
    return ""


def _nse_db(s):
    p = (s.get("nse_db") or "").strip().strip('"') or find_nse_db()
    return p if p and os.path.exists(p) else ""


def _ro(path):
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)


def credentials(s=None):
    """(api_key, access_token, who, expires) or (None, None, reason, None)"""
    s = s or settings()
    if s["login_mode"] == "shared":
        p = _nse_db(s)
        if not p:
            return None, None, "NSE EOD Fetcher database not found — set its path, or use your own Kite app.", None
        try:
            con = _ro(p)
            rows = dict(con.execute("SELECT key, value FROM app_settings WHERE key IN ('kite','kite_token')"))
            con.close()
        except sqlite3.Error as e:
            return None, None, f"Could not read the NSE EOD Fetcher database: {e}", None
        k = json.loads(rows.get("kite") or "{}")
        t = json.loads(rows.get("kite_token") or "{}")
        if not k.get("api_key"):
            return None, None, "Zerodha is not set up in NSE EOD Fetcher.", None
        if not t.get("access_token") or (t.get("expires") and now_ist().isoformat() >= t["expires"]):
            return None, None, "Log in to Zerodha in NSE EOD Fetcher (Connect Zerodha) — the bridge picks it up.", None
        return k["api_key"], t["access_token"], t.get("user") or "", t.get("expires")
    t = s.get("own_token") or {}
    if not (s.get("api_key") and s.get("secret")):
        return None, None, "Enter your Kite API key and secret.", None
    if not t.get("access_token") or (t.get("expires") and now_ist().isoformat() >= t["expires"]):
        return None, None, "Press Log in to Zerodha.", None
    return s["api_key"], t["access_token"], t.get("user") or "", t.get("expires")


_PENDING = {}


def _expiry():
    n = now_ist()
    e = n.replace(hour=6, minute=0, second=0, microsecond=0)
    if n >= e:
        e += timedelta(days=1)
    return e.isoformat(timespec="seconds")


_S = requests.Session()
STATS = dict(requests=0)


def kget(path, params=None, tries=4):
    key, tok, why, _ = credentials()
    if not key:
        raise PermissionError(why)
    for k in range(tries):
        r = _S.get(API + path, params=params, headers={**HDR, "Authorization": f"token {key}:{tok}"},
                   timeout=(10, 60))
        STATS["requests"] += 1
        if r.status_code == 429:
            time.sleep(1.0 + k)
            continue
        if r.status_code == 403:
            raise PermissionError("Zerodha login expired — log in again.")
        return r
    raise RuntimeError("Zerodha keeps rate-limiting — try again later.")


# ------------------------------------------------------------------ instruments + symbol list
def instruments():
    """{'RELIANCE': (token, False), 'NIFTY 50': (token, True), ...} cached for the day"""
    path = os.path.join(DATA_DIR, "instruments.json")
    today = now_ist().strftime("%Y-%m-%d")
    c = _load(path, {})
    if c.get("day") == today and c.get("map"):
        return {k: tuple(v) for k, v in c["map"].items()}
    r = kget("/instruments/NSE")
    if r.status_code != 200:
        raise RuntimeError(f"Instrument list failed: HTTP {r.status_code}")
    m = {}
    for row in csv.DictReader(io.StringIO(r.text)):
        if row.get("segment") == "NSE" and row.get("instrument_type") == "EQ":
            m[row["tradingsymbol"].upper()] = (int(row["instrument_token"]), False)
        elif row.get("segment") == "INDICES":
            m[row["tradingsymbol"].upper()] = (int(row["instrument_token"]), True)
    _save(path, {"day": today, "map": m})
    return m


def parse_symbols(text):
    """lines 'RELIANCE' or 'NIFTY 50 = NIFTY' (Zerodha name = AmiBroker ticker). '#' starts a comment."""
    out, seen = [], set()
    for ln in (text or "").replace(",", "\n").splitlines():
        ln = ln.split("#")[0].strip()
        if not ln:
            continue
        if "=" in ln:
            a, b = (x.strip() for x in ln.split("=", 1))
        else:
            a, b = ln, ln
        a = a.upper().replace("NSE:", "")
        b = "".join(b.upper().replace("NSE:", "").split()) or "".join(a.split())
        if a not in seen:
            seen.add(a)
            out.append((a, b))
    return out


# ------------------------------------------------------------------ 1-minute bar book (today only)
class Book:
    def __init__(self):
        self.lock = threading.Lock()
        self.day = None
        self.bars = {}           # ami → {minute 'HH:MM': [o,h,l,c,v]}
        self.cum = {}            # ami → last cumulative day volume seen
        self.dirty = set()       # (ami, 'HH:MM')
        self.last_px = {}

    def _roll(self, d):
        if self.day != d:
            self.day, self.bars, self.cum, self.dirty = d, {}, {}, set()

    def seed(self, ami, candles_today):
        """after a backfill: carry on from Zerodha's candles (cumulative volume and the bar still forming)"""
        with self.lock:
            self._roll(now_ist().strftime("%Y-%m-%d"))
            if not candles_today:
                return
            self.cum[ami] = sum(c[5] for c in candles_today)
            cur = now_ist().strftime("%H:%M")
            last = candles_today[-1]
            if last[0][11:16] == cur:
                self.bars.setdefault(ami, {})[cur] = [last[1], last[2], last[3], last[4], last[5]]

    def tick(self, ami, px, cum, is_index):
        t = now_ist()
        hm = (t.hour, t.minute)
        if not (SESSION_START <= hm < SESSION_END) or t.weekday() >= 5 or px <= 0:
            return
        key = t.strftime("%H:%M")
        with self.lock:
            self._roll(t.strftime("%Y-%m-%d"))
            dv = 0
            if not is_index and cum > 0:
                prev = self.cum.get(ami)
                dv = max(0, cum - prev) if prev is not None else 0
                if prev is None or cum >= prev:
                    self.cum[ami] = cum
            b = self.bars.setdefault(ami, {}).get(key)
            if b is None:
                self.bars[ami][key] = [px, px, px, px, dv]
            else:
                b[1] = max(b[1], px); b[2] = min(b[2], px); b[3] = px; b[4] += dv
            self.dirty.add((ami, key))
            self.last_px[ami] = px

    def take(self):
        with self.lock:
            if not self.dirty:
                return []
            rows = [(a, self.day, k + ":00", *self.bars[a][k]) for a, k in sorted(self.dirty)]
            self.dirty = set()
            return rows

    def give_back(self, rows):
        with self.lock:
            for r in rows:
                if r[1] == self.day:
                    self.dirty.add((r[0], r[2][:5]))


# ------------------------------------------------------------------ engine
class Engine:
    def __init__(self):
        self.book = Book()
        self.feed = AF.Feeder(log)
        self.ticker = None
        self.map = {}            # token → (ami, is_index)
        self.syms = []           # (zerodha, ami, token, is_index)
        self.unknown = []
        self.running = False
        self.token_used = None
        self.bf = dict(running=False, done=0, total=0, symbol="", rows=0, error="", last=None)
        self.repaired_on = None
        self.n = 0
        self.lock = threading.Lock()
        threading.Thread(target=self._pusher, daemon=True, name="pusher").start()
        threading.Thread(target=self._watchdog, daemon=True, name="watchdog").start()
        AF.install_format(DATA_DIR)

    # -------------------------------------------------- start / stop
    def resolve(self):
        s = settings()
        lst = parse_symbols(s["symbols"])[:MAX_SYMBOLS]
        ins = instruments()
        self.syms, self.unknown = [], []
        for z, a in lst:
            if z in ins:
                tok, idx = ins[z]
                self.syms.append((z, a, tok, idx))
            else:
                self.unknown.append(z)
        self.map = {tok: (a, idx) for z, a, tok, idx in self.syms}
        if self.unknown:
            log("Not found on Zerodha (check the spelling): " + ", ".join(self.unknown))
        return self.syms

    def start(self, backfill=None):
        with self.lock:
            key, tok, why, _ = credentials()
            if not key:
                raise PermissionError(why)
            self.stop(quiet=True)
            self.resolve()
            if not self.syms:
                raise ValueError("No valid symbols in the list.")
            self.running = True
            self.token_used = tok
            s = settings()
            do_bf = s["auto_backfill"] if backfill is None else backfill
        if do_bf:
            self.backfill(int(s["backfill_days"]), then_live=True)
        else:
            self._go_live()

    def _go_live(self):
        key, tok, why, _ = credentials()
        if not key or not self.running:
            return
        self.ticker = KW.Ticker(key, tok, [t for _, _, t, _ in self.syms], self._on_tick, log)
        self.ticker.start()

    def stop(self, quiet=False):
        if self.ticker:
            self.ticker.stop()
            self.ticker = None
        if self.running and not quiet:
            log("Stopped.")
        self.running = False

    def _on_tick(self, tok, px, cum):
        m = self.map.get(tok)
        if m:
            self.book.tick(m[0], px, cum, m[1])

    # -------------------------------------------------- push live bars to AmiBroker
    def _pusher(self):
        while True:
            time.sleep(max(1, int(settings().get("push_secs") or 5)))
            try:
                if self.feed.q.qsize() or self.feed.state["waiting"]:
                    continue                         # the previous file is still waiting for AmiBroker
                rows = self.book.take()
                if not rows:
                    continue
                self.n += 1
                path = os.path.join(FEED_DIR, f"live_{self.n % 1000:03d}.txt")
                AF.write_file(path, rows)
                self.feed.push(path)
            except Exception as e:                   # noqa: BLE001
                log(f"push error: {e}")

    # -------------------------------------------------- history
    def backfill(self, days, then_live=False, today_only=False):
        if self.bf["running"]:
            raise RuntimeError("A backfill is already running.")
        threading.Thread(target=self._backfill, args=(days, then_live, today_only), daemon=True,
                         name="backfill").start()

    def _backfill(self, days, then_live, today_only):
        st = _load(STATEF, {"done": {}})
        self.bf.update(running=True, done=0, total=len(self.syms), rows=0, error="", symbol="")
        now = now_ist()
        today = now.strftime("%Y-%m-%d")
        log(("Repairing today's bars" if today_only else f"Backfilling up to {days} days") +
            f" for {len(self.syms)} symbols")
        batch, nbatch = [], 0
        try:
            for i, (z, a, tok, idx) in enumerate(self.syms):
                self.bf.update(symbol=a, done=i)
                start = (now - timedelta(days=int(days))).replace(hour=9, minute=15, second=0, microsecond=0)
                if today_only:
                    start = now.replace(hour=9, minute=15, second=0, microsecond=0)
                else:
                    last = st["done"].get(a)
                    if last:
                        start = max(start, datetime.strptime(last, "%Y-%m-%d %H:%M") + timedelta(minutes=1))
                candles = []
                a0 = start
                while a0 < now:
                    b0 = min(a0 + timedelta(days=CHUNK_DAYS), now)
                    r = kget(f"/instruments/historical/{tok}/minute",
                             dict(**{"from": a0.strftime("%Y-%m-%d %H:%M:%S"), "to": b0.strftime("%Y-%m-%d %H:%M:%S")}))
                    if r.status_code == 200:
                        candles += (r.json().get("data") or {}).get("candles") or []
                    else:
                        log(f"{a}: history HTTP {r.status_code}")
                    a0 = b0 + timedelta(seconds=1)
                    time.sleep(0.35)                  # Zerodha allows ~3 history requests a second
                if not self.running and then_live:
                    break
                rows = []
                for c in candles:
                    ts = str(c[0])
                    rows.append((a, ts[:10], ts[11:19], float(c[1]), float(c[2]), float(c[3]), float(c[4]),
                                 0 if idx else int(c[5] or 0)))
                today_c = [c for c in candles if str(c[0])[:10] == today]
                self.book.seed(a, [(str(c[0]), c[1], c[2], c[3], c[4], 0 if idx else int(c[5] or 0)) for c in today_c])
                if rows:
                    batch += rows
                    cur = now_ist().strftime("%Y-%m-%d %H:%M")
                    full = [r for r in rows if f"{r[1]} {r[2][:5]}" < cur]
                    if full:
                        st["done"][a] = f"{full[-1][1]} {full[-1][2][:5]}"
                self.bf["rows"] += len(rows)
                if len(batch) > 200000 or (batch and i == len(self.syms) - 1):
                    nbatch += 1
                    AF.write_file(os.path.join(FEED_DIR, f"history_{nbatch:03d}.txt"), batch)
                    self.feed.push(os.path.join(FEED_DIR, f"history_{nbatch:03d}.txt"))
                    _save(STATEF, st)
                    batch = []
            if batch:
                nbatch += 1
                AF.write_file(os.path.join(FEED_DIR, f"history_{nbatch:03d}.txt"), batch)
                self.feed.push(os.path.join(FEED_DIR, f"history_{nbatch:03d}.txt"))
            _save(STATEF, st)
            self.bf.update(done=len(self.syms), symbol="", last=now_ist().strftime("%Y-%m-%d %H:%M"))
            log(f"History done: {self.bf['rows']:,} bars sent to AmiBroker")
        except Exception as e:                       # noqa: BLE001
            self.bf["error"] = str(e)
            log(f"History stopped: {e}")
        finally:
            self.bf["running"] = False
        if then_live:
            self._go_live()

    # -------------------------------------------------- keeps things going without clicks
    def _watchdog(self):
        time.sleep(3)
        while True:
            try:
                s = settings()
                key, tok, why, _ = credentials(s)
                t = now_ist()
                market = t.weekday() < 5 and (9, 0) <= (t.hour, t.minute) < (15, 35)
                if key and s.get("autostart", True):
                    dead = self.running and self.ticker and self.ticker.auth_failed
                    if (not self.running and market and parse_symbols(s["symbols"])) or \
                            (self.running and tok != self.token_used and not self.bf["running"]) or dead:
                        log("Starting the live feed" + (" with the new Zerodha login" if self.running else ""))
                        self.start()
                if (s["eod_repair"] and self.running and t.weekday() < 5 and (t.hour, t.minute) >= REPAIR_AT
                        and self.repaired_on != t.date() and not self.bf["running"]):
                    self.repaired_on = t.date()
                    self.backfill(1, today_only=True)
            except Exception as e:                   # noqa: BLE001
                log(f"watchdog: {e}")
            time.sleep(30)

    def status(self):
        s = settings()
        key, tok, why, exp = credentials(s)
        tk = self.ticker
        return dict(login=dict(ok=bool(key), message="" if key else why, user=why if key else "", expires=exp,
                               mode=s["login_mode"], nse_db=_nse_db(s)),
                    running=self.running, live=dict(connected=bool(tk and tk.connected), ticks=tk.ticks if tk else 0,
                                                    last_tick=time.strftime("%H:%M:%S", time.localtime(tk.last_tick))
                                                    if tk and tk.last_tick else None,
                                                    error=tk.error if tk else ""),
                    symbols=len(self.syms), unknown=self.unknown, bars_today=sum(len(v) for v in self.book.bars.values()),
                    backfill=self.bf, amibroker=self.feed.state, data_dir=DATA_DIR,
                    amibroker_folder=AF.amibroker_folder(), requests=STATS["requests"],
                    now=now_ist().strftime("%a %d %b %H:%M:%S"))


ENG = None

# ------------------------------------------------------------------ web page
app = FastAPI(title="Kite → AmiBroker Bridge")


@app.get("/", response_class=HTMLResponse)
def home():
    return FileResponse(os.path.join(BASE_DIR, "static", "index.html"), headers={"Cache-Control": "no-cache"})


@app.get("/api/status")
def api_status():
    return ENG.status()


@app.get("/api/settings")
def api_settings():
    s = settings()
    s.pop("own_token", None)
    s["secret"] = "••••••••" if s.get("secret") else ""
    s["nse_db_found"] = _nse_db(settings())
    return s


class SettingsIn(BaseModel):
    login_mode: str = "shared"
    nse_db: str = ""
    api_key: str = ""
    secret: str = ""
    symbols: str = ""
    push_secs: int = 5
    backfill_days: int = 30
    auto_backfill: bool = True
    eod_repair: bool = True


@app.post("/api/settings")
def api_save(p: SettingsIn):
    s = settings()
    old_syms = s["symbols"]
    s.update(login_mode="own" if p.login_mode == "own" else "shared", nse_db=p.nse_db.strip().strip('"'),
             api_key=p.api_key.strip(), symbols=p.symbols, push_secs=max(1, min(60, p.push_secs)),
             backfill_days=max(1, min(3650, p.backfill_days)), auto_backfill=p.auto_backfill, eod_repair=p.eod_repair)
    if p.secret and "•" not in p.secret:
        s["secret"] = p.secret.strip()
        s["own_token"] = {}
    n = len(parse_symbols(s["symbols"]))
    if n > MAX_SYMBOLS:
        raise HTTPException(400, f"{n} symbols — the bridge takes up to {MAX_SYMBOLS}.")
    save_settings(s)
    if ENG.running and old_syms != s["symbols"]:
        threading.Thread(target=ENG.start, daemon=True).start()
    return {"ok": True, "symbols": n}


@app.get("/api/login")
def api_login():
    s = settings()
    if not s.get("api_key"):
        raise HTTPException(400, "Enter your Kite API key and secret first.")
    st = secrets.token_urlsafe(12)
    _PENDING[st] = time.time()
    return RedirectResponse(LOGIN + "?" + urlencode({"v": "3", "api_key": s["api_key"],
                                                      "redirect_params": urlencode({"state": st})}))


@app.get("/callback")
def callback(request_token: str = "", status: str = "", state: str = ""):
    s = settings()
    if status != "success" or not request_token:
        return HTMLResponse("<p>Zerodha login was cancelled. <a href='/'>Back</a></p>")
    if state and state not in _PENDING:
        return HTMLResponse("<p>This login link has expired — press Log in again. <a href='/'>Back</a></p>")
    _PENDING.pop(state, None)
    chk = hashlib.sha256((s["api_key"] + request_token + s["secret"]).encode()).hexdigest()
    r = _S.post(API + "/session/token", data=dict(api_key=s["api_key"], request_token=request_token, checksum=chk),
                headers=HDR, timeout=(10, 30))
    try:
        j = r.json()
    except ValueError:
        j = {}
    d = j.get("data") or {}
    if j.get("status") != "success" or not d.get("access_token"):
        return HTMLResponse(f"<p>Zerodha login failed: {j.get('message') or r.status_code}. <a href='/'>Back</a></p>")
    s["own_token"] = dict(access_token=d["access_token"], user=d.get("user_id"), expires=_expiry())
    save_settings(s)
    log(f"Logged in to Zerodha as {d.get('user_id')}")
    return RedirectResponse("/")


@app.post("/api/start")
def api_start():
    try:
        ENG.start()
    except (PermissionError, ValueError, RuntimeError) as e:
        raise HTTPException(400, str(e))
    return {"ok": True}


@app.post("/api/stop")
def api_stop():
    ENG.stop()
    return {"ok": True}


@app.post("/api/backfill")
def api_backfill(days: int = 30, full: bool = False):
    try:
        if not ENG.syms:
            ENG.resolve()
        if full:                                    # forget what was already sent and fetch the whole range again
            _save(STATEF, {"done": {}})
        ENG.backfill(max(1, min(3650, days)))
    except (PermissionError, ValueError, RuntimeError) as e:
        raise HTTPException(400, str(e))
    return {"ok": True}


@app.post("/api/test")
def api_test():
    ENG.feed.test()
    time.sleep(2.5)
    return ENG.feed.state


@app.get("/api/log")
def api_log():
    return list(LOGQ)[-150:]


@app.get("/api/nse-lists")
def nse_lists():
    """the three watchlists of NSE EOD Fetcher (read-only)"""
    p = _nse_db(settings())
    if not p:
        raise HTTPException(404, "NSE EOD Fetcher database not found.")
    con = _ro(p)
    try:
        names = ["Watchlist 1", "Watchlist 2", "Watchlist 3"]
        row = con.execute("SELECT value FROM app_settings WHERE key='watch_names'").fetchone()
        if row:
            n = json.loads(row[0]).get("names") or []
            if len(n) >= 3:
                names = [str(x) for x in n[:3]]
        cols = [r[1] for r in con.execute("PRAGMA table_info(watchlist)")]
        lists = {1: [], 2: [], 3: []}
        if cols:
            q = "SELECT symbol, lists FROM watchlist" if "lists" in cols else "SELECT symbol, '1' FROM watchlist"
            for sym, ls in con.execute(q):
                for x in str(ls or "1").split(","):
                    if x.strip().isdigit() and int(x) in lists:
                        lists[int(x)].append(sym)
        return [dict(id=i, name=names[i - 1], symbols=sorted(lists[i])) for i in (1, 2, 3)]
    finally:
        con.close()


def main():
    global ENG
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=PORT)
    ap.add_argument("--no-browser", action="store_true")
    a = ap.parse_args()
    ENG = Engine()
    url = f"http://127.0.0.1:{a.port}"
    print(f"\n  Kite → AmiBroker Bridge running at {url}\n  Data folder: {DATA_DIR}\n  Close this window to stop.\n")
    if not a.no_browser:
        threading.Timer(1.5, lambda: webbrowser.open(url)).start()
    uvicorn.run(app, host="127.0.0.1", port=a.port, log_level="warning")


if __name__ == "__main__":
    main()
