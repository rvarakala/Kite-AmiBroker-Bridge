"""
Hands bars to AmiBroker through its own automation interface (OLE, "Broker.Application") — the same ASCII import
you would do by hand (File → Import ASCII), just done every few seconds.

Every import file starts with the AmiBroker format commands, so no setup is needed in AmiBroker beyond a database
whose base time interval is 1 minute. The same commands are also saved as "kite2ami.format" in the bridge's data folder
(and copied into AmiBroker's Formats folder when Windows allows it) for manual imports.

Only attaches to an AmiBroker that is already open — it never starts AmiBroker itself. While AmiBroker is closed the
bars wait and go in as soon as it is opened again.
"""
import os
import queue
import threading
import time

FORMAT = """$FORMAT TICKER,DATE_YMD,TIME,OPEN,HIGH,LOW,CLOSE,VOLUME
$SKIPLINES 0
$SEPARATOR ,
$AUTOADD 1
$CONT 1
$DEBUG 1
"""
FORMAT_NAME = "kite2ami.format"

try:                                   # Windows with pywin32
    import pythoncom                   # type: ignore
    import win32com.client             # type: ignore
    HAVE_COM = True
except Exception:                      # Mac / Linux / pywin32 missing → files are written, nothing imported
    HAVE_COM = False


def amibroker_folder():
    """AmiBroker's install folder from the registry (where Broker.exe lives), else the usual places"""
    if os.name == "nt":
        try:
            import winreg              # type: ignore
            with winreg.OpenKey(winreg.HKEY_CLASSES_ROOT, r"Broker.Application\CLSID") as k:
                clsid = winreg.QueryValue(k, None)
            with winreg.OpenKey(winreg.HKEY_CLASSES_ROOT, rf"CLSID\{clsid}\LocalServer32") as k:
                exe = winreg.QueryValue(k, None).strip().strip('"').split('"')[0]
            exe = exe.split(" /")[0].strip('"')
            if os.path.exists(exe):
                return os.path.dirname(exe)
        except OSError:
            pass
    for p in (r"C:\Program Files\AmiBroker", r"C:\Program Files (x86)\AmiBroker", r"C:\AmiBroker"):
        if os.path.isdir(p):
            return p
    return None


def install_format(data_dir):
    """write kite2ami.format next to the data and try to copy it into AmiBroker\\Formats"""
    mine = os.path.join(data_dir, FORMAT_NAME)
    with open(mine, "w", newline="\r\n") as f:
        f.write(FORMAT)
    ab = amibroker_folder()
    if ab:
        try:
            dst = os.path.join(ab, "Formats", FORMAT_NAME)
            with open(dst, "w", newline="\r\n") as f:
                f.write(FORMAT)
            return dst
        except OSError:
            pass
    return mine


def write_file(path, rows):
    """rows: (ticker, 'YYYY-MM-DD', 'HH:MM:SS', o, h, l, c, v) → AmiBroker import file with the format on top"""
    tmp = path + ".tmp"
    with open(tmp, "w", newline="\r\n") as f:
        f.write(FORMAT)
        for t, d, tm, o, h, l, c, v in rows:
            f.write(f"{t},{d},{tm},{o:g},{h:g},{l:g},{c:g},{int(v)}\n")
    os.replace(tmp, path)
    return path


class Feeder:
    """one thread owns the COM connection; others queue files to import"""

    def __init__(self, log=print):
        self.log = log
        self.q = queue.Queue()
        self.ab = None
        self.state = dict(attached=False, version="", database="", imports=0, last_import=None, error="",
                          com=HAVE_COM, waiting=0)
        self.th = threading.Thread(target=self._run, daemon=True, name="amibroker")
        self.th.start()

    # ----------------------------------------------------------- COM
    def _attach(self):
        if not HAVE_COM:
            self.state.update(attached=False, error="AmiBroker link needs Windows (pywin32) — files are only written.")
            return None
        if self.ab is not None:
            try:
                _ = self.ab.Version
                return self.ab
            except Exception:
                self.ab = None
        try:
            self.ab = win32com.client.GetActiveObject("Broker.Application")
            self.state.update(attached=True, version=str(self.ab.Version), database=str(self.ab.DatabasePath or ""),
                              error="")
            self.log(f"Attached to AmiBroker {self.state['version']} — database {self.state['database']}")
            return self.ab
        except Exception:
            self.ab = None
            self.state.update(attached=False, error="AmiBroker is not open — bars will go in when it is.")
            return None

    def _run(self):
        if HAVE_COM:
            pythoncom.CoInitialize()
        pending = []
        while True:
            try:
                item = self.q.get(timeout=2)
                pending.append(item)
                while True:                               # take everything queued
                    try:
                        pending.append(self.q.get_nowait())
                    except queue.Empty:
                        break
            except queue.Empty:
                pass
            if item_is_test(pending):
                pending = [p for p in pending if p != "TEST"]
                self.ab = None
                self._attach()
            self.state["waiting"] = len(pending)
            if not pending:
                continue
            ab = self._attach()
            if ab is None:
                pending = pending[-500:]                  # keep the newest files; older live files are superseded
                time.sleep(3)
                continue
            done = []
            for path in pending:
                try:
                    r = ab.Import(0, path, FORMAT_NAME if self._has_format() else "default.format")
                    if r not in (0, None, True):
                        self.state["error"] = f"AmiBroker import returned {r} for {os.path.basename(path)}"
                    done.append(path)
                    self.state["imports"] += 1
                except Exception as e:                    # noqa: BLE001
                    self.state["error"] = f"Import failed: {e}"
                    self.ab = None
                    break
            for p in done:
                pending.remove(p)
                try:
                    os.remove(p)                          # imported → no longer needed
                except OSError:
                    pass
            if done:
                try:
                    ab.RefreshAll()
                except Exception:
                    pass
                self.state["last_import"] = time.strftime("%H:%M:%S")
            self.state["waiting"] = len(pending)

    def _has_format(self):
        ab = amibroker_folder()
        return bool(ab and os.path.exists(os.path.join(ab, "Formats", FORMAT_NAME)))

    # ----------------------------------------------------------- API
    def push(self, path):
        self.q.put(path)

    def test(self):
        self.q.put("TEST")


def item_is_test(pending):
    return "TEST" in pending
