# refresh_excel.py — minimal, bounded, loud logs
print(f"[boot] refresh_excel.py loaded from {__file__}", flush=True)
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import os, re, time, subprocess, traceback, threading
import boto3, xlwings as xw, pythoncom
from pywinauto import Desktop
from pywinauto.keyboard import send_keys
from uuid import uuid4
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
import json

import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

# ---------- config ----------
BUCKET = "agentic-data"
BASE_TEMP_DIR = r"C:\Temp"
WORK_DIR = os.path.join(BASE_TEMP_DIR, "work")
os.makedirs(WORK_DIR, exist_ok=True)

POST_CLICK_DWELL_SEC = 8
EXPECTED_SEC = 22       
QUIET_SECS   = 5        # quiet window to call it “done”
MAX_WAIT_SEC = 120       # absolute cap
# ribbon timing knobs
UI_READY_GRACE_SEC    = 2     # quick settle after workbook open
RIBBON_APPEAR_TIMEOUT = 12    # wait up to this for the VA tab to appear
REFRESH_APPEAR_TIMEOUT= 10    # wait up to this for the Refresh control
MENU_APPEAR_TIMEOUT   = 8     # wait up to this for menu items after clicking
RIBBON_TOGGLE_PERIOD  = 1.2   # how often to toggle Ctrl+F1 while waiting
EXTERNAL_MAX_WAIT_SEC = 300 

S3 = boto3.client("s3")
app = FastAPI()
KEEP_EXCEL_ALIVE = True
_gate = threading.Lock()

# DB URL for the query_* SQL phase (env override supported)
DB_URL = os.getenv(
    "FM_DB_URL",
    "postgresql://z1_admin:Zenger%3F5@z1-database-1.ctymuke0m20h.us-east-2.rds.amazonaws.com:5432/telecaster"
)

# ---------- small utils ----------
def log(msg): print(msg, flush=True)
def _now(): return time.time()
def _pump(): pythoncom.PumpWaitingMessages()

def unblock_file(path):
    try:
        subprocess.run(["powershell","-NoProfile","-ExecutionPolicy","Bypass","Unblock-File","-Path",path],
                       check=False, capture_output=True, creationflags=0x08000000)
    except Exception: pass

# ---------- external-data refresh (PowerQuery/Connections/QueryTables) ----------
def _count_name_errors(wb_com) -> int:
    """
    Count cells showing the Excel error #NAME? across all worksheets.
    Robust to scalar/tuple/2D tuple returns from UsedRange.Value.
    """
    total = 0
    try:
        for ws in wb_com.Worksheets:
            try:
                vals = ws.UsedRange.Value
            except Exception:
                continue

            # scalar string
            if isinstance(vals, str):
                if "#NAME?" in vals.upper():
                    total += 1
                continue

            # 1D or 2D tuple
            try:
                rows = vals if isinstance(vals, tuple) else ()
            except Exception:
                rows = ()

            if not rows:
                continue

            # normalize to iterable of rows
            if rows and not isinstance(rows[0], tuple):
                rows = (rows,)

            for row in rows:
                if not isinstance(row, tuple):
                    # sometimes a single string sneaks in
                    if isinstance(row, str) and "#NAME?" in row.upper():
                        total += 1
                    continue
                for v in row:
                    if isinstance(v, str) and "#NAME?" in v.upper():
                        total += 1
    except Exception:
        pass
    return total


def _has_external_work(wb_com) -> bool:
    """Return True if workbook has any connections/querytables/listobjects/pivots to refresh."""
    try:
        if wb_com.Connections.Count > 0:
            return True
    except Exception:
        pass
    try:
        for ws in wb_com.Worksheets:
            try:
                if ws.QueryTables.Count > 0:
                    return True
            except Exception:
                pass
            try:
                if ws.ListObjects.Count > 0:
                    return True
            except Exception:
                pass
    except Exception:
        pass
    # pivots don’t imply a remote fetch, but include them in “has work” so we can refresh caches
    try:
        pcs = wb_com.PivotCaches()
        if pcs.Count > 0:
            return True
    except Exception:
        pass
    return False

def _any_refreshing(wb_com) -> bool:
    # Connections (OLEDB/ODBC)
    try:
        conns = wb_com.Connections
        for i in range(1, conns.Count + 1):
            c = conns.Item(i)
            try:
                if hasattr(c, "OLEDBConnection") and c.OLEDBConnection and getattr(c.OLEDBConnection, "Refreshing", False):
                    return True
            except Exception: pass
            try:
                if hasattr(c, "ODBCConnection") and c.ODBCConnection and getattr(c.ODBCConnection, "Refreshing", False):
                    return True
            except Exception: pass
    except Exception: pass
    # QueryTables / ListObjects
    try:
        for ws in wb_com.Worksheets:
            try:
                qts = ws.QueryTables
                for j in range(1, qts.Count + 1):
                    if getattr(qts.Item(j), "Refreshing", False):
                        return True
            except Exception: pass
            try:
                los = ws.ListObjects
                for j in range(1, los.Count + 1):
                    try:
                        if getattr(los.Item(j).QueryTable, "Refreshing", False):
                            return True
                    except Exception: pass
            except Exception: pass
    except Exception: pass
    return False

def _wait_calc_and_external_done(app_api, wb_com, timeout=EXTERNAL_MAX_WAIT_SEC, poll=0.25):
    """Bounded wait until Excel finishes calc AND external refresh. Loud heartbeat."""
    start = _now(); last = -999
    try:
        app_api.CalculateUntilAsyncQueriesDone()
    except Exception:
        pass
    while True:
        _pump()
        # calc state: 0=Done, 1=Calculating, 2=Pending; some builds return -4135 for Done
        try:
            calc = app_api.CalculationState
        except Exception:
            calc = 0
        calc_done = (calc in (0, None, -4135))
        busy = False
        try:
            busy = _any_refreshing(wb_com)
        except Exception:
            busy = False

        now = _now()
        if now - last >= 5:
            log(f"[wait] external elapsed={int(now-start)}s calc_done={calc_done} refreshing={busy}")
            last = now

        if calc_done and not busy:
            return True
        if now - start > timeout:
            log("[wait] external_timeout_reached")
            return False
        time.sleep(poll)

def _refresh_all_connections(wb_com):
    # Workbook.Connections (incl. Power Query “Query - <Name>” connections)
    try:
        conns = wb_com.Connections; n = conns.Count
        log(f"[ext] connections={n}")
    except Exception:
        conns = None; n = 0
    for i in range(1, n + 1):
        c = conns.Item(i)
        name = getattr(c, "Name", f"conn_{i}")
        try:
            try:
                if hasattr(c, "ODBCConnection"): c.ODBCConnection.BackgroundQuery = False
                if hasattr(c, "OLEDBConnection"): c.OLEDBConnection.BackgroundQuery = False
            except Exception:
                pass
            try:
                c.RefreshWithRefreshAll = True
            except Exception:
                pass
            c.Refresh()   # synchronous when BackgroundQuery=False
            log(f"[ext] refreshed connection: {name}")
        except Exception as e:
            log(f"[ext] WARN connection '{name}' failed: {e}")

def _refresh_querytables_and_listobjects(wb_com):
    # Legacy QueryTables and ListObject.QueryTable
    try:
        for ws in wb_com.Worksheets:
            try:
                qts = ws.QueryTables
                for j in range(1, qts.Count + 1):
                    qt = qts.Item(j)
                    try: qt.BackgroundQuery = False
                    except Exception: pass
                    try:
                        qt.Refresh(False)
                        log(f"[ext] QueryTable refreshed: {ws.Name}!{qt.Name}")
                    except Exception as e:
                        log(f"[ext] WARN QueryTable {ws.Name}!{qt.Name} failed: {e}")
            except Exception:
                pass
            try:
                los = ws.ListObjects
                for j in range(1, los.Count + 1):
                    lo = los.Item(j)
                    try:
                        qt = lo.QueryTable
                        try: qt.BackgroundQuery = False
                        except Exception: pass
                        qt.Refresh(False)
                        log(f"[ext] ListObject refreshed: {ws.Name}!{lo.Name}")
                    except Exception:
                        pass
            except Exception:
                pass
    except Exception:
        pass

def _refresh_pivots(wb_com):
    # PivotCaches and PivotTables
    try:
        pcs = wb_com.PivotCaches()
        for i in range(1, pcs.Count + 1):
            try: pcs.Item(i).Refresh()
            except Exception: pass
    except Exception:
        pass
    try:
        for ws in wb_com.Worksheets:
            try:
                pts = ws.PivotTables()
                for i in range(1, pts.Count + 1):
                    try: pts.Item(i).RefreshTable()
                    except Exception: pass
            except Exception:
                pass
    except Exception:
        pass

def refresh_external_data_pre_va(app_api, wb_com):
    """Run all external-data refresh paths, then bounded-wait until quiet."""
    t0 = _now()
    log("[phase] external_refresh_begin")
    _refresh_all_connections(wb_com)
    _refresh_querytables_and_listobjects(wb_com)
    _refresh_pivots(wb_com)
    ok = _wait_calc_and_external_done(app_api, wb_com, timeout=EXTERNAL_MAX_WAIT_SEC)
    log(f"[phase] external_refresh_done ok={ok} dur={round(_now()-t0,1)}s")
    return ok


def s3_download_atomic(bucket, key, dest):
    cfg  = Config(connect_timeout=5, read_timeout=30, retries={"max_attempts":3})
    s3c  = boto3.client("s3", config=cfg)
    tcfg = TransferConfig(use_threads=False, max_concurrency=1, multipart_threshold=1024**4)
    part = f"{dest}.part_{uuid4().hex}"
    try:
        try: os.remove(part)
        except FileNotFoundError: pass
        log(f"[s3] dl_begin key={key} -> {dest}")
        t0 = _now()
        s3c.download_file(bucket, key, part, Config=tcfg)
        os.replace(part, dest)
        log(f"[s3] dl_ok bytes={os.path.getsize(dest)} in {round(_now()-t0,2)}s")
    except Exception as e:
        try: os.remove(part)
        except Exception: pass
        raise HTTPException(502, f"S3 download failed: {e}")

def kill_excel():
    try:
        subprocess.call(["taskkill","/f","/im","EXCEL.EXE"],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        log("[phase] excel_killed")
    except Exception: pass

def _wb_used_fingerprint(wb_com):
    """Coarse signal for 'sheet is filling': sum of UsedRange.Count over sheets."""
    total = 0
    try:
        for ws in wb_com.Worksheets:
            try: total += int(ws.UsedRange.Count)
            except Exception: pass
    except Exception: return -1
    return total

# ---------- query_* SQL helpers ----------
def _clean_sql_text(s: str | None) -> str:
    if not s:
        return ""
    s = str(s)
    return (s.replace("\u200b", " ")
             .replace("\u200c", " ")
             .replace("\u200d", " ")
             .replace("\u2060", " ")
             .replace("\xa0", " ")
             .strip())

def _pg_connect(dsn_or_kwargs):
    if isinstance(dsn_or_kwargs, str):
        return psycopg2.connect(dsn_or_kwargs)
    if isinstance(dsn_or_kwargs, dict):
        return psycopg2.connect(**dsn_or_kwargs)
    return psycopg2.connect(dsn_or_kwargs)

def run_query_tabs(wb, fork: str, ticker: str, db=DB_URL) -> None:
    """
    For each sheet named 'query_*':
      - read SQL from A1
      - replace {{fork}} and {{ticker}}
      - execute via psycopg2
      - write at C3 (headers + rows)
      - stamp B1/B2
    """
    sheets = [s for s in wb.sheets if str(s.name).lower().startswith("query_")]
    log(f"[sql] sheets={len(sheets)} -> {[s.name for s in sheets]}")
    if not sheets:
        return

    # make sure formulas (if any) are computed before reading A1
    try:
        wb.app.api.Calculation = -4105  # xlCalculationAutomatic
        wb.app.api.Calculate()
    except Exception:
        pass

    with _pg_connect(db) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        for sheet in sheets:
            try:
                raw = sheet.range("A1").value
                sql_txt = _clean_sql_text(raw)
                if not sql_txt:
                    try:
                        f = sheet.api.Range("A1").Formula
                        log(f"[sql] {sheet.name} A1 formula: {str(f)[:160]}")
                    except Exception:
                        pass
                    sheet.range("B1").value = f"{datetime.now():%Y-%m-%d %H:%M:%S} | skipped: empty A1"
                    sheet.range("B2").value = ""
                    log(f"[sql] skip {sheet.name}: empty A1")
                    continue

                sql = (sql_txt.replace("{{fork}}", fork or "")
                               .replace("{{ticker}}", ticker or ""))
                prev = sql.replace("\n", " ")
                log(f"[sql] {sheet.name} SQL: {prev[:240]}")

                sheet.range("B1").value = f"{datetime.now():%Y-%m-%d %H:%M:%S} | running fork={fork} ticker={ticker}"
                sheet.range("B2").value = ""

                cur.execute(sql)
                rows = cur.fetchall() if cur.description else []

                try:
                    sheet.range("C3").expand().clear()
                except Exception:
                    try:
                        sheet.range("C3").expand().clear_contents()
                    except Exception:
                        pass
                if rows:
                    cols = list(rows[0].keys())
                    data = [cols] + [[r.get(c) for c in cols] for r in rows]
                    sheet.range("C3").value = data

                sheet.range("B1").value = f"{datetime.now():%Y-%m-%d %H:%M:%S} | success"
                sheet.range("B2").value = f"rows={len(rows)}"
                log(f"[sql] {sheet.name} rows={len(rows)}")

            except Exception as e:
                sheet.range("B1").value = f"{datetime.now():%Y-%m-%d %H:%M:%S} | failed"
                sheet.range("B2").value = str(e)[:500]
                log(f"[sql] {sheet.name} ERROR: {e}")


# ---------- robust workbook open ----------
def open_workbook_robust(app: xw.App, path: str):
    log(f"[open] path={path}")
    try:
        for b in list(app.books):
            if b.name.lower().startswith("book"): b.close(save=False)
    except Exception: pass

    wb = None
    try:
        wb = app.books.open(path, update_links=False, read_only=False,
                            ignore_read_only_recommended=True)
        log("[open] xlwings_ok")
    except Exception as e:
        log(f"[open] xlwings_failed: {e}")

    try:
        has_any = int(app.api.Workbooks.Count) > 0
    except Exception:
        has_any = bool(wb)

    if not has_any:
        log("[open] com_fallback")
        try:
            app.api.AutomationSecurity = 1
            app.api.DisplayAlerts = False
            app.api.Workbooks.Open(Filename=path, UpdateLinks=0, ReadOnly=False,
                                   IgnoreReadOnlyRecommended=True)
            for b in app.books:
                if os.path.normcase(os.path.normpath(getattr(b,"fullname",""))) \
                   == os.path.normcase(os.path.normpath(path)):
                    wb = b; break
        except Exception as e:
            raise HTTPException(500, f"COM open failed: {e}")

    try: send_keys("{ESC}")  # leave Backstage if shown
    except Exception: pass

    if wb is None:
        raise HTTPException(500, "Excel launched but workbook did not open")
    log(f"[open] done wb={wb.name}")
    return wb

# ---------- UIA: VA → Refresh → Entire Workbook ----------
def click_refresh_entire_workbook(pid: int, workbook_hint: str, timeout=6):
    """
    Visible Alpha → Refresh → Entire Workbook, with robust waits:
      1) wait for VA tab (toggle Ctrl+F1 while polling)
      2) wait for Refresh control (same)
      3) open menu (invoke/click), wait for items; fallback Alt+Down and then DOWN+ENTER
    Returns (ok: bool, why: str) for clear diagnostics.
    """
    why = ""
    try:
        desk = Desktop(backend="uia")

        # 0) pick the top Excel window (prefer title with workbook_hint)
        picks = []
        for w in desk.windows(process=pid):
            try:
                if (w.element_info.class_name or "") == "XLMAIN" and w.is_visible():
                    title = (w.window_text() or "")
                    score = (1_000_000 if workbook_hint and workbook_hint.lower() in title.lower() else 0)
                    r = w.rectangle(); score += r.width() * r.height()
                    picks.append((score, w, title))
            except: 
                pass
        if not picks:
            why = "no_xlmain_window"; log(f"[uia] {why}"); return False, why
        picks.sort(key=lambda t: t[0], reverse=True)
        xl_wrap = picks[0][1]
        sel_title = picks[0][2].strip()
        log(f"[uia] window='{sel_title}' (hint='{workbook_hint}')")

        xl = desk.window(handle=xl_wrap.handle)   # spec for search
        w  = xl.wrapper_object()                  # wrapper for light focus

        # small grace period after workbook is opened
        time.sleep(UI_READY_GRACE_SEC)

        # helper: gently toggle ribbon while waiting for a predicate
        def _wait_for(fn, tmax, label):
            t0 = _now(); last_toggle = 0.0; tries = 0
            while _now() - t0 < tmax:
                _pump(); tries += 1
                obj = fn()
                if obj is not None:
                    log(f"[uia] {label} found after {round(_now()-t0,2)}s (tries={tries})")
                    return obj
                # light focus + occasional Ctrl+F1 to coax ribbon
                try: w.set_focus()
                except Exception: pass
                if _now() - last_toggle >= RIBBON_TOGGLE_PERIOD:
                    try: send_keys("^{F1}")
                    except Exception: pass
                    last_toggle = _now()
                time.sleep(0.2)
            log(f"[uia] {label} NOT found after {tmax}s")
            return None

        # 1) wait for the VA tab (spec first, then wrappers)
        def _find_va_tab():
            spec = xl.child_window(title_re=r"^\s*Visible\s+Alpha\s*$", control_type="TabItem")
            if spec.exists(): return spec.wrapper_object()
            for t in xl_wrap.descendants(control_type="TabItem"):
                try:
                    s = (t.window_text() or "").lower()
                    if "visible" in s and "alpha" in s: return t
                except: pass
            return None

        tab = _wait_for(_find_va_tab, RIBBON_APPEAR_TIMEOUT, "va_tab")
        if tab is not None:
            try: tab.invoke()
            except Exception:
                try: tab.select()
                except Exception:
                    try: tab.click_input()
                    except Exception: pass
        else:
            # not fatal: keep going, some builds show Refresh without a distinct VA tab
            pass

        # 2) wait for the Refresh control (spec, then broad)
        def _find_refresh():
            spec = xl.child_window(title_re=r"^\s*Refresh\s*$", control_type="SplitButton")
            if spec.exists(): return spec.wrapper_object()
            spec = xl.child_window(title_re=r"^\s*Refresh\s*$", control_type="Button")
            if spec.exists(): return spec.wrapper_object()
            for c in xl_wrap.descendants():
                try:
                    nm = (c.window_text() or "").lower()
                    if nm and "refresh" in nm: return c
                except: pass
            return None

        refresh = _wait_for(_find_refresh, REFRESH_APPEAR_TIMEOUT, "refresh_control")
        if refresh is None:
            why = "refresh_control_not_found"; log(f"[uia] {why}"); return False, why

        # 3) open the dropdown (invoke preferred), then wait for menu items
        opened = False
        try: refresh.invoke(); opened = True; log("[uia] refresh.invoke()")
        except Exception:
            try: refresh.click_input(); opened = True; log("[uia] refresh.click_input()")
            except Exception: pass
        if not opened:
            try: send_keys("%{DOWN}"); opened = True; log("[uia] sent Alt+Down")
            except Exception: pass
        if not opened:
            why = "refresh_open_failed"; log(f"[uia] {why}"); return False, why

        # wait for the menu & click "Entire Workbook"
        end = _now() + MENU_APPEAR_TIMEOUT
        clicked = False
        while _now() < end and not clicked:
            try: items = list(xl_wrap.descendants(control_type="MenuItem"))
            except Exception: items = []
            names = []
            for it in items:
                try:
                    txt = (it.window_text() or "").strip()
                    if txt: names.append(txt)
                    low = txt.lower() if txt else ""
                    if "entire" in low and "workbook" in low:
                        try: it.invoke()
                        except Exception:
                            try: it.select()
                            except Exception: it.click_input()
                        clicked = True; break
                except: pass
            if names: log(f"[dbg] menu_items_seen={names[:8]}{'…' if len(names)>8 else ''}")
            if not clicked: time.sleep(0.25)

        if not clicked:
            # final keyboard fallback even if UIA couldn't enumerate
            try: send_keys("{DOWN}{ENTER}"); log("[uia] fallback keys DOWN+ENTER"); return True, "fallback_keys"
            except Exception:
                why = "menu_item_not_found"; log(f"[uia] {why}"); return False, why

        log("[uia] menu_item 'Entire Workbook' clicked")
        return True, "menu_clicked"

    except Exception as e:
        why = f"uia_exception:{e}"
        log(f"[uia] {why}")
        return False, why


# ---------- bounded waiter (never hangs) ----------
def wait_refresh_complete(app, wb_api,
                          expected_sec=EXPECTED_SEC,
                          quiet_secs=QUIET_SECS,
                          max_wait=MAX_WAIT_SEC,
                          poll=0.25):
    start=_now()
    fp_last=_wb_used_fingerprint(wb_api)
    last_change=start
    changed=False
    log(f"[wait] start fp={fp_last} expected={expected_sec}s quiet={quiet_secs}s cap={max_wait}s")
    while True:
        _pump()
        fp=_wb_used_fingerprint(wb_api)
        if fp!=fp_last:
            changed=True; last_change=_now()
            log(f"[wait] fp_change {fp_last}->{fp} t={int(_now()-start)}s")
            fp_last=fp
        else:
            if changed and (_now()-last_change)>=quiet_secs:
                log("[phase] refresh_stable"); return True, fp_last
        elapsed=_now()-start
        if not changed and elapsed>=expected_sec:
            log("[phase] expected_elapsed_no_change"); return False, fp_last
        if elapsed>=max_wait:
            log("[phase] max_wait_reached"); return changed, fp_last
        time.sleep(poll)

def _fmt(s): 
    return round(float(s), 3)

# ---------- API ----------
class RefreshRequest(BaseModel):
    path: str

@app.post("/api/refresh/")
def refresh_excel_file(data: RefreshRequest, request: Request):
    pythoncom.CoInitialize()
    wb = app_excel = None
    try:
        with _gate:
            key = data.path.strip()
            log(f"\n==== /api/refresh {key} ====")
            if not key.lower().endswith((".xlsx", ".xlsm")):
                raise HTTPException(400, "Only .xlsx/.xlsm")

            base  = os.path.basename(key)
            safe  = re.sub(r"[^A-Za-z0-9._-]", "_", base)
            local = os.path.join(WORK_DIR, safe)

            # --- timings
            t0_all = _now()

            # S3 → local (no pre-emptive kill; just ensure prior Excel drains)
            t0_dl = _now()
            log(f"[phase] s3_download_start {key} -> {local}")
            s3_download_atomic(BUCKET, key, local)
            unblock_file(local)
            log("[phase] s3_download_ok")
            t1_dl = _now()

            # give a moment if prior Excel is still exiting
            t_wait = time.time()
            while time.time() - t_wait < 5:
                try:
                    procs = subprocess.check_output(["tasklist"], creationflags=0x08000000).decode("utf-8","ignore")
                    if "EXCEL.EXE" not in procs: break
                except Exception: break
                time.sleep(0.25)

            # Launch or attach to Excel so the workbook grid paints and UIA can act
            t0_open = _now()
            app_excel = None
            if KEEP_EXCEL_ALIVE:
                try:
                    # attach to an existing Excel (prefers the active one)
                    app_excel = xw.apps.active or list(xw.apps)[0]
                    app_excel.visible = True
                    log("[phase] excel_attached_keepalive")
                except Exception:
                    app_excel = xw.App(visible=True, add_book=False)
                    log("[phase] excel_started_fresh_keepalive")
            else:
                app_excel = xw.App(visible=True, add_book=False)
                log("[phase] excel_started")

            app_excel.display_alerts = False
            app_excel.screen_updating = True
            try:
                app_excel.api.CalculateBeforeSave = True
            except Exception:
                pass
            pid = getattr(app_excel, "pid", None)
            log(f"[phase] excel_pid={pid}")

            # Open workbook and bring the worksheet window to the foreground
            wb = open_workbook_robust(app_excel, local)
            log(f"[excel] opened: {wb.name}")

            try: send_keys("{ESC}")  # leave Backstage if shown
            except Exception: pass

            app_api = app_excel.api
            wb_api  = wb.api
            try:
                wb.activate()
                aw = app_api.ActiveWindow
                try:
                    aw.Visible = True
                    aw.WindowState = -4137  # xlMaximized
                except Exception:
                    pass
                log(f"[focus] active workbook={app_api.ActiveWorkbook.Name}")
            except Exception as e:
                log(f"[focus] activation failed: {e}")
            t1_open = _now()

            # --- Pre-VA: refresh all external data (Power Query / Connections / QueryTables / pivots)
            # --- Pre-VA: RUN query_* tabs (populate C3 regions and stamp B1/B2)
            name_wo_ext = os.path.splitext(base)[0]
            parts = name_wo_ext.split("_")
            ticker = parts[0] if parts else ""
            fork   = parts[-1] if len(parts) > 1 else ""
            t0_sql = _now()
            log("[phase] query_tabs_start")
            try:
                run_query_tabs(wb, fork=fork, ticker=ticker, db=DB_URL)
            except Exception as e:
                log(f"[sql] ERROR top-level: {e}")
            log("[phase] query_tabs_end")
            t1_sql = _now()

            # --- Pre-VA: refresh all external data (Power Query / Connections / QueryTables / pivots)
            t0_ext = _now()
            external_ok: str | bool = "skip"
            try:
                if _has_external_work(wb_api):
                    ok = refresh_external_data_pre_va(app_api, wb_api)
                    external_ok = bool(ok)
                else:
                    log("[phase] external_refresh_skip (no connections/queries/pivots detected)")
            except Exception as e:
                external_ok = False
                log(f"[ext] WARN external refresh phase error: {e}")
            t1_ext = _now()

            # --- UIA: Visible Alpha → Refresh → Entire Workbook
            t0_click = _now()
            hint = os.path.splitext(base)[0]
            uia_ok = False
            uia_why = ""
            ok, why = click_refresh_entire_workbook(pid, hint)
            log(f"[uia] result ok={ok} why={why}")
            uia_ok, uia_why = bool(ok), str(why)
            if not uia_ok:
                raise HTTPException(status_code=500, detail=f"VA UI click failed: {uia_why}")
            t1_click = _now()

            # --- Dwell, then bounded wait until stable calc
            t0_dwell = _now()
            fp0 = _wb_used_fingerprint(wb_api)
            log(f"[phase] dwell {POST_CLICK_DWELL_SEC}s fp0={fp0}")
            time.sleep(POST_CLICK_DWELL_SEC)
            t1_dwell = _now()

            t0_wait = _now()
            changed, fp1 = wait_refresh_complete(app_excel, wb_api)
            log(f"[result] changed={changed} fp0={fp0} fp1={fp1} delta={fp1-fp0}")
            t1_wait = _now()

            # --- HARD RECALC to ensure cached values are written before save ---
            try:
                app_api.CalculateFullRebuild()
                app_api.CalculateUntilAsyncQueriesDone()
                log("[calc] CalculateFullRebuild done")
            except Exception as e:
                log(f"[calc] WARN: {e}")

            # One more bounded settle after hard recalc (longer expected window)
            changed2, fp2 = wait_refresh_complete(app_excel, wb_api,
                                                expected_sec=45, quiet_secs=5, max_wait=180)
            log(f"[result] post-recalc changed={changed2} fp_now={fp2} delta={fp2-fp1}")
            # If nothing changed at all across the whole cycle, try VA once more; if still no change, abort to avoid ingesting stale data.
            if fp2 == fp0:
                log("[va] no-op detected; retrying VA click once")
                ok2, why2 = click_refresh_entire_workbook(pid, hint)
                log(f"[uia] retry ok={ok2} why={why2}")
                time.sleep(POST_CLICK_DWELL_SEC)

                changed3, fp3 = wait_refresh_complete(app_excel, wb_api,
                                                    expected_sec=45, quiet_secs=5, max_wait=180)
                log(f"[result] retry changed={changed3} fp_now={fp3} delta={fp3-fp2}")

                try:
                    app_api.CalculateFullRebuild()
                    app_api.CalculateUntilAsyncQueriesDone()
                    log("[calc] post-retry CalculateFullRebuild done")
                except Exception as e:
                    log(f"[calc] WARN post-retry: {e}")

                changed4, fp4 = wait_refresh_complete(app_excel, wb_api,
                                                    expected_sec=45, quiet_secs=5, max_wait=180)
                log(f"[result] post-retry changed={changed4} fp_now={fp4} delta={fp4-fp3}")

                if fp4 == fp0:
                    # Prevent upload+ingest of stale content
                    raise HTTPException(status_code=503, detail="VA refresh produced no change; aborting to avoid stale ingest")

            # hard sanity: no #NAME? anywhere
            name_errors = _count_name_errors(wb_api)

            log(f"[va] #NAME? cells after refresh: {name_errors}")
            if name_errors > 0:
                raise HTTPException(status_code=500, detail="VA cells still #NAME? after refresh")

            # --- Save & close workbook (app teardown handled in finally)
            t0_save = _now()
            wb.save(); wb.close()
            log("[phase] workbook_closed")
            t1_save = _now()

            # --- Upload back to S3
            t0_up = _now()
            log(f"[phase] s3_upload_start {local} -> {key}")
            S3.upload_file(local, BUCKET, key)
            log("[phase] s3_upload_ok")
            t1_up = _now()

            total = _fmt(_now() - t0_all)
            durations = {
                "download_s":      _fmt(t1_dl   - t0_dl),
                "open_s":          _fmt(t1_open - t0_open),
                "sql_s":           _fmt(t1_sql  - t0_sql),
                "external_s":      _fmt(t1_ext  - t0_ext),
                "click_s":         _fmt(t1_click- t0_click),
                "dwell_s":         _fmt(t1_dwell- t0_dwell),
                "wait_s":          _fmt(t1_wait - t0_wait),
                "save_close_s":    _fmt(t1_save - t0_save),
                "upload_s":        _fmt(t1_up   - t0_up),
                "total_s":         total,
            }

            resp = {
                "status":        "ok",
                "file":          key,
                "uia_ok":        uia_ok,
                "uia_why":       uia_why,
                "external_ok":   external_ok,
                "name_errors":   name_errors,
                "durations":     durations,
            }
            log(f"[summary] {json.dumps(resp, ensure_ascii=False)}")
            return resp

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))
    finally:
        try:
            if wb is not None:
                wb.close()
        except Exception: pass

        if not KEEP_EXCEL_ALIVE:
            try:
                if app_excel is not None:
                    app_excel.quit()
            except Exception: pass
            kill_excel()

        pythoncom.CoUninitialize()

class TickerRequest(BaseModel):
    ticker: str

def _find_latest_model_key(bucket: str, ticker: str) -> str:
    prefix = "user_datasets/financial_models/"
    t = ticker.upper().strip()
    # list keys starting with TICKER_
    resp = S3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    candidates = []
    for obj in resp.get("Contents", []):
        k = obj["Key"]
        base = os.path.basename(k)
        low  = base.lower()
        if not (low.endswith(".xlsx") or low.endswith(".xlsm")): continue
        if base.startswith("~$"): continue
        if not base.upper().startswith(f"{t}_"): continue
        candidates.append((obj["LastModified"], k))
    if not candidates:
        raise HTTPException(404, f"No model for ticker={t}")
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]

@app.post("/api/refresh/ticker")
def refresh_by_ticker(data: TickerRequest, request: Request):
    # resolve latest key for TICKER and forward to existing /api/refresh logic
    key = _find_latest_model_key(BUCKET, data.ticker)
    # reuse the same refresh flow by calling the function directly
    return refresh_excel_file(RefreshRequest(path=key), request)
