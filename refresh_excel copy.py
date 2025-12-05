from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import xlwings as xw
import boto3
import os
import traceback
import subprocess
import json
import time
import pandas as pd
import psycopg2
from datetime import datetime
#uvicorn refresh_excel:app --host 0.0.0.0 --port 9000 --reload
import threading
from pywinauto import Application, Desktop
from pywinauto.keyboard import send_keys
_gate = threading.Lock()
# --- CONFIG ---
BUCKET = "agentic-data"
BASE_TEMP_DIR = "C:\\Temp"
s3 = boto3.client("s3")

DB_CONN = dict(
    host="z1-database-1.ctymuke0m20h.us-east-2.rds.amazonaws.com",
    port=5432,
    dbname="telecaster",
    user="z1_admin",
    password="Zenger?5"
)

app = FastAPI()

class RefreshRequest(BaseModel):
    path: str  # e.g. "user_datasets/Model_USA 1.xlsx"

import pythoncom

TIMEOUT_SEC = 900  # 10 minutes

import re
from psycopg2.extras import RealDictCursor

# top of file (with other imports/consts)
from typing import Optional

FM_PREFIX = "user_datasets/financial_models/"

class TickerRequest(BaseModel):
    ticker: str

def _latest_key_for_ticker(ticker: str) -> Optional[str]:
    t = (ticker or "").upper().strip()
    best = None
    token = None
    while True:
        kwargs = {"Bucket": BUCKET, "Prefix": FM_PREFIX}
        if token: kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            base = os.path.basename(k)
            low  = base.lower()
            if not (low.endswith(".xlsx") or low.endswith(".xlsm")):
                continue
            if base.startswith("~$") or base.startswith("._"):
                continue
            if not base.upper().startswith(f"{t}_"):
                continue
            lm = obj["LastModified"]
            if (best is None) or (lm > best[0]):
                best = (lm, k)
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return best[1] if best else None


def _clean_sql_text(s: str | None) -> str:
    if not s:
        return ""
    s = str(s)
    # strip zero-width / NBSP / etc.
    return (s.replace("\u200b", " ")
             .replace("\u200c", " ")
             .replace("\u200d", " ")
             .replace("\u2060", " ")
             .replace("\xa0", " ")
             .strip())

def run_query_tabs(wb, fork: str, ticker: str, db_conn_dict: dict) -> None:
    """Execute SQL from A1 for every sheet named 'query_*', write to C3, stamp B1/B2."""
    sheets = [s for s in wb.sheets if str(s.name).lower().startswith("query_")]
    print(f"[sql] sheets={len(sheets)} -> {[s.name for s in sheets]}", flush=True)

    if not sheets:
        return

    # ensure formulas (if any) are evaluated before reading A1
    try:
        wb.app.api.Calculation = -4105  # xlCalculationAutomatic
    except Exception:
        pass
    try:
        wb.app.api.Calculate()
    except Exception:
        pass

    with psycopg2.connect(**db_conn_dict) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        for sheet in sheets:
            try:
                sql_raw = sheet.range("A1").value
                sql_txt = _clean_sql_text(sql_raw)
                if not sql_txt:
                    # try to log formula if present
                    try:
                        f = sheet.api.Range("A1").Formula
                        print(f"[sql] {sheet.name} A1 formula: {str(f)[:160]}", flush=True)
                    except Exception:
                        pass
                    sheet.range("B1").value = f"{datetime.now():%Y-%m-%d %H:%M:%S} | skipped: empty A1"
                    sheet.range("B2").value = ""
                    print(f"[sql] skip {sheet.name}: empty A1", flush=True)
                    continue

                # token substitution
                sql = (sql_txt.replace("{{fork}}", fork or "")
                              .replace("{{ticker}}", ticker or ""))

                prev = sql.replace("\n", " ")
                print(f"[sql] {sheet.name} SQL: {prev[:240]}", flush=True)

                sheet.range("B1").value = f"{datetime.now():%Y-%m-%d %H:%M:%S} | running fork={fork} ticker={ticker}"
                sheet.range("B2").value = ""

                cur.execute(sql)
                rows = cur.fetchall() if cur.description else []
                # clear output zone and write at C3
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
                print(f"[sql] {sheet.name} rows={len(rows)}", flush=True)

            except Exception as e:
                sheet.range("B1").value = f"{datetime.now():%Y-%m-%d %H:%M:%S} | failed"
                sheet.range("B2").value = str(e)[:500]
                print(f"[sql] {sheet.name} ERROR: {e}", flush=True)


def start_excel_attached(timeout=30):
    """
    Pre-launch Excel via shell and attach with xlwings, avoiding App() creation hangs.
    Returns an xlwings.App handle.
    """
    # Start Excel without opening any file; /automation minimizes UI prompts
    try:
        subprocess.Popen(
            ['cmd', '/c', 'start', '""', 'excel.exe', '/automation'],
            creationflags=0x08000000  # CREATE_NO_WINDOW
        )
        print("[excel] prelaunch via shell: excel.exe /automation", flush=True)
    except Exception as e:
        print(f"[excel] prelaunch warn: {e}", flush=True)

    # Poll until an Excel instance becomes attachable
    t0 = time.time()
    app_excel = None
    while time.time() - t0 < timeout:
        try:
            app_excel = xw.apps.active  # attach to the active Excel instance
            if app_excel:
                break
        except Exception:
            pass
        time.sleep(0.5)

    if not app_excel:
        raise HTTPException(status_code=500, detail="Failed to attach to Excel instance within timeout")

    # Bring it into a good automation state
    try:
        app_excel.visible = True
        app_excel.display_alerts = False
        app_excel.screen_updating = True
        try:
            app_excel.api.ScreenUpdating = True
            app_excel.api.DisplayStatusBar = True
            app_excel.api.AskToUpdateLinks = False
        except Exception:
            pass

        # Close any stray Book* the instance may have created
        try:
            for b in list(app_excel.books):
                if b.name.lower().startswith("book"):
                    print(f"[excel] closing stray book: {b.name}", flush=True)
                    b.close(save=False)
        except Exception:
            pass
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Attached to Excel but could not configure instance: {e}")

    pid = getattr(app_excel, "pid", None)
    print(f"[excel] attached pid={pid}", flush=True)
    return app_excel


def ui_click_va_refresh_entire(pid: int, timeout=25) -> bool:
    """
    UIA fallback: in Excel process `pid`, activate 'Visible Alpha' tab and
    click 'Refresh' → 'Entire Workbook'. Robust against multiple XLMAIN windows.
    """
    try:
        desk = Desktop(backend="uia")

        # -- pick the largest visible XLMAIN window for this PID
        xl_wins = []
        for w in desk.windows(process=pid):
            try:
                if (w.element_info.class_name or "") == "XLMAIN" and w.is_visible():
                    r = w.rectangle()
                    area = max(1, r.width() * r.height())
                    xl_wins.append((area, w))
            except Exception:
                continue
        if not xl_wins:
            print("[uia] no visible XLMAIN windows for pid", pid); return False
        xl_wins.sort(key=lambda t: t[0], reverse=True)
        win = xl_wins[0][1]

        # bring to front & ensure ribbon visible
        try:
            win.set_focus()
            win.restore()
            win.maximize()
        except Exception:
            pass
        try:
            send_keys("^{F1}")   # show ribbon if collapsed
        except Exception:
            pass

        # -- select 'Visible Alpha' tab (exact, then fuzzy)
        tab = None
        try:
            tab = win.child_window(title="Visible Alpha", control_type="TabItem")
            if not tab.exists(): tab = None
        except Exception:
            tab = None

        if not tab:
            for t in win.descendants(control_type="TabItem"):
                try:
                    txt = (t.window_text() or "").lower()
                    if "visible" in txt and "alpha" in txt:
                        tab = t; break
                except Exception:
                    continue
        if not tab:
            print("[uia] VA tab not found"); return False

        try:
            tab.select()
        except Exception:
            tab.click_input()
        time.sleep(0.3)

        # -- find the 'Refresh' control on the VA tab (SplitButton/Button)
        refresh = None
        for ctl_type in ("SplitButton", "Button"):
            try:
                r = win.child_window(title_re=r"^\s*Refresh\s*$", control_type=ctl_type)
                if r.exists(): refresh = r; break
            except Exception:
                continue
        if not refresh:
            # looser search: any descendant containing 'refresh'
            for c in win.descendants():
                try:
                    if "refresh" in (c.window_text() or "").lower():
                        refresh = c; break
                except Exception:
                    continue
        if not refresh:
            print("[uia] Refresh control not found"); return False

        # click refresh; if a menu opens, pick 'Entire Workbook'
        try:
            refresh.click_input()
        except Exception:
            try:
                refresh.invoke()
            except Exception:
                print("[uia] cannot click refresh"); return False

        time.sleep(0.4)
        entire = None
        try:
            entire = win.child_window(title_re=r".*Entire.*Workbook.*", control_type="MenuItem")
            if entire.exists(): entire.click_input()
        except Exception:
            pass

        print("[uia] invoked Visible Alpha → Refresh → Entire Workbook")
        return True

    except Exception as e:
        print(f"[uia] UI automation failed: {e}")
        return False


def hunt_and_register_va_xlls_from_dll_folder(app_com) -> int:
    """
    If the VA AddIn shows a DLL path (adxloader64.VAExcelPlugin.dll),
    scan its directory (and subdirs) for any .xll and RegisterXLL them.
    Returns how many were registered.
    """
    registered = 0
    try:
        addins = app_com.AddIns
        for i in range(1, addins.Count + 1):
            ai = addins.Item(i)
            name = (getattr(ai, "Name", "") or "") + " " + (getattr(ai, "Title", "") or "")
            low = name.lower()
            if "visiblealpha" in low or "vaexcelplugin" in low or "visible alpha" in low:
                try:
                    dll_path = getattr(ai, "FullName", "") or ""
                except Exception:
                    dll_path = ""
                if dll_path and os.path.exists(dll_path):
                    root = os.path.dirname(dll_path)
                    for dirpath, dirnames, filenames in os.walk(root):
                        for fn in filenames:
                            if fn.lower().endswith(".xll"):
                                full = os.path.join(dirpath, fn)
                                try:
                                    app_com.RegisterXLL(full)
                                    print(f"[va] RegisterXLL (scan): {full}")
                                    registered += 1
                                except Exception as e:
                                    print(f"[va] RegisterXLL failed: {full} -> {e}")
    except Exception as e:
        print(f"[va] hunt XLL failed: {e}")
    return registered

def _used_cells_snapshot(wb_com):
    """Coarse signal for 'sheet is filling': sum of UsedRange.Count over sheets."""
    total = 0
    try:
        for ws in wb_com.Worksheets:
            try:
                total += int(ws.UsedRange.Count)
            except Exception:
                pass
    except Exception:
        return -1
    return total

def try_register_va_xlls(app_com) -> int:
    """Scan AddIns/AddIns2 for VA/ADX XLLs and call RegisterXLL on them."""
    registered = 0
    def _try(path):
        nonlocal registered
        if not path: 
            return
        p = str(path).lower()
        if not p.endswith(".xll"):
            return
        if ("visible" in p) or ("vaexcel" in p) or ("va" in p) or ("adx" in p):
            try:
                app_com.RegisterXLL(path)
                print(f"[va] RegisterXLL: {path}")
                registered += 1
            except Exception as e:
                print(f"[va] RegisterXLL failed: {path} -> {e}")

    for coll_name in ("AddIns", "AddIns2"):
        try:
            coll = getattr(app_com, coll_name)
            for i in range(1, coll.Count + 1):
                ai = coll.Item(i)
                try:
                    _try(getattr(ai, "FullName", None))
                except Exception:
                    continue
        except Exception as e:
            print(f"[va] enumerate {coll_name} failed: {e}")
    return registered


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
    # pivots don’t imply a remote fetch, but keep them out of the “must wait” gate
    return False

def _now():
    return time.time()

def _fmt(seconds):
    return round(float(seconds), 3)  # e.g., 1.234

def _pump():
    pythoncom.PumpWaitingMessages()

def _any_refreshing(wb_com) -> bool:
    # check Connections (OLEDB/ODBC)
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
    # check QueryTables / ListObjects
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

def wait_quiet(app_com, wb_com, timeout=TIMEOUT_SEC, poll=0.25):
    """Wait until Excel finishes calc and no connections are refreshing, with heartbeat."""
    try:
        app_com.CalculateUntilAsyncQueriesDone()
    except Exception:
        pass

    start = time.time()
    last_beat = -999
    while True:
        _pump()
        # calc state
        try:
            calc = app_com.CalculationState  # 0=xlDone, 1=xlCalculating, 2=xlPending
        except Exception:
            calc = 0  # assume done if unreadable

        # treat both 0 and legacy -4135 as 'done'
        calc_done = (calc in (0, None, -4135))

        # refresh state
        try:
            busy = _any_refreshing(wb_com)
        except Exception:
            busy = False

        # heartbeat every ~5s
        now = time.time()
        if now - last_beat >= 5:
            state_str = {0: "Done", 1: "Calculating", 2: "Pending"}.get(calc, str(calc))
            print(f"[wait] elapsed={int(now-start)}s calc_state={state_str} calc_done={calc_done} refreshing={busy}", flush=True)
            last_beat = now

        if calc_done and not busy:
            return True
        if now - start > timeout:
            print("[⏳] Timeout waiting for Excel to finish (calc/refresh).", flush=True)
            return False
        time.sleep(poll)


def refresh_all_connections(wb_com):
    """Refresh Workbook.Connections (incl. Power Query) synchronously."""
    try:
        conns = wb_com.Connections
        n = conns.Count
    except Exception:
        conns, n = None, 0
    print(f"[ℹ️] Workbook connections: {n}")
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
            c.Refresh()
            print(f"[🔄] Refreshed connection: {name}")
        except Exception as e:
            print(f"[⚠️] Connection '{name}' refresh failed: {e}")

def refresh_power_query(wb_com):
    """Refresh Workbook.Queries via their 'Query - <Name>' connections."""
    try:
        qs = wb_com.Queries
        m = qs.Count
    except Exception:
        return
    for i in range(1, m + 1):
        q = qs.Item(i)
        conn_name = f"Query - {q.Name}"
        try:
            c = wb_com.Connections(conn_name)
            try:
                if hasattr(c, "ODBCConnection"): c.ODBCConnection.BackgroundQuery = False
                if hasattr(c, "OLEDBConnection"): c.OLEDBConnection.BackgroundQuery = False
            except Exception:
                pass
            c.Refresh()
            print(f"[🔄] PowerQuery refreshed: {q.Name}")
        except Exception as e:
            print(f"[⚠️] PowerQuery '{q.Name}' failed: {e}")

def refresh_querytables_and_listobjects(wb_com):
    """Refresh legacy QueryTables and ListObject.QueryTable."""
    for ws in wb_com.Worksheets:
        # QueryTables
        try:
            qts = ws.QueryTables
            for j in range(1, qts.Count + 1):
                qt = qts.Item(j)
                try: qt.BackgroundQuery = False
                except Exception: pass
                try:
                    qt.Refresh(False)
                    print(f"[🔄] QueryTable refreshed: {ws.Name}!{qt.Name}")
                except Exception as e:
                    print(f"[⚠️] QueryTable {ws.Name}!{qt.Name} failed: {e}")
        except Exception:
            pass
        # ListObject.QueryTable
        try:
            los = ws.ListObjects
            for j in range(1, los.Count + 1):
                lo = los.Item(j)
                try:
                    qt = lo.QueryTable
                    try: qt.BackgroundQuery = False
                    except Exception: pass
                    qt.Refresh(False)
                    print(f"[🔄] ListObject refreshed: {ws.Name}!{lo.Name}")
                except Exception:
                    pass
        except Exception:
            pass

def refresh_pivots(wb_com):
    """Refresh pivot caches/tables if present."""
    try:
        pcs = wb_com.PivotCaches()
        for i in range(1, pcs.Count + 1):
            try: pcs.Item(i).Refresh()
            except Exception: pass
    except Exception:
        pass
    for ws in wb_com.Worksheets:
        try:
            pts = ws.PivotTables()
            for i in range(1, pts.Count + 1):
                try: pts.Item(i).RefreshTable()
                except Exception: pass
        except Exception:
            pass


def kill_excel_if_lingering():
    try:
        subprocess.call(["taskkill", "/f", "/im", "EXCEL.EXE"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("[🧹] Force-killed lingering Excel processes.")
    except Exception:
        print("[⚠️] Unable to force-kill Excel.")

def _count_name_errors(wb_com) -> int:
    total = 0
    try:
        for ws in wb_com.Worksheets:
            try:
                vals = ws.UsedRange.Value
                # vals can be scalar, tuple, or tuple-of-tuples
                if isinstance(vals, str):
                    if "#NAME?" in vals.upper(): total += 1
                elif isinstance(vals, tuple):
                    for row in vals:
                        if isinstance(row, tuple):
                            for v in row:
                                if isinstance(v, str) and "#NAME?" in v.upper():
                                    total += 1
                        elif isinstance(row, str) and "#NAME?" in row.upper():
                            total += 1
            except Exception:
                continue
    except Exception:
        pass
    return total


def ensure_va_addin(app_com) -> bool:
    """Force-load the Visible Alpha COM/XLL add-in and print registration status."""
    ok = False
    # COM add-in (Add-in Express) by ProgID
    try:
        va = app_com.COMAddIns(".VisibleAlpha.ExcelAddin")
        if not va.Connect:
            va.Connect = True
        print(f"[va] COMAddIn .VisibleAlpha.ExcelAddin connected={va.Connect}")
        ok = ok or bool(va.Connect)
    except Exception as e:
        print(f"[va] COMAddIns('.VisibleAlpha.ExcelAddin') failed: {e}")

    # Legacy AddIns list (XLL shim), often named adxloader64.VAExcelPlugin.dll
    try:
        addins = app_com.AddIns
        for i in range(1, addins.Count + 1):
            ai = addins.Item(i)
            name = f"{ai.Name} {ai.Title}".lower()
            if "visiblealpha" in name or "vaexcelplugin" in name:
                if not ai.Installed:
                    ai.Installed = True
                print(f"[va] AddIns '{ai.Name}' installed={ai.Installed}")
                ok = ok or bool(ai.Installed)
    except Exception as e:
        print(f"[va] AddIns enumeration failed: {e}")

    # Verify UDFs registered
    try:
        rf = app_com.RegisteredFunctions  # 2-col rows [lib, func]
        cnt = 0
        if rf is not None:
            for row in rf:
                if not row or len(row) < 2:
                    continue
                lib = str(row[0]).lower()
                if "visiblealpha" in lib or "vaexcel" in lib or "va" in lib:
                    cnt += 1
        print(f"[va] RegisteredFunctions (VA) count={cnt}")
        ok = ok or cnt > 0
    except Exception as e:
        print(f"[va] RegisteredFunctions read failed: {e}")

    return ok


@app.post("/api/refresh/")
def refresh_excel_file(data: RefreshRequest, request: Request):
    with _gate:
        t0 = _now()
        client_ip = request.client.host if request and request.client else "unknown"
        print(f"\n====== /api/refresh start ip={client_ip} path={data.path} ======", flush=True)

        model_key = data.path.strip()
        if not model_key.endswith((".xlsx", ".xlsm")):
            raise HTTPException(status_code=400, detail="Only .xlsx or .xlsm files are supported.")

        # Detect fork/ticker from filename, e.g. "DDS_Daloopa_v1.xlsx" -> ticker="DDS", fork="v1"
        base_filename = os.path.basename(model_key)
        name_wo_ext   = os.path.splitext(base_filename)[0]
        parts         = name_wo_ext.split("_")
        ticker        = parts[0] if parts else ""
        try:
            fork = base_filename.rsplit("_", 1)[-1].split(".")[0]
        except Exception:
            fork = ""
        print(f"[meta] file={base_filename} fork='{fork or '-'}' ticker='{ticker or '-'}'", flush=True)

        try:
            from uuid import uuid4
            import shutil

            #kill_excel_if_lingering()
            tmp_dir = os.path.join(BASE_TEMP_DIR, f"job_{int(time.time())}_{os.getpid()}_{uuid4().hex[:6]}")
            os.makedirs(tmp_dir, exist_ok=True)
            local_path = os.path.join(tmp_dir, base_filename)

            t_dl = _now()
            print(f"[s3] download {model_key} → {local_path}", flush=True)
            s3.download_file(BUCKET, model_key, local_path)
            try:
                fsz = os.path.getsize(local_path)
                fmtime = time.ctime(os.path.getmtime(local_path))
                print(f"[s3] ok size={fsz} mtime={fmtime}", flush=True)
            except Exception:
                pass

            # --- Open Excel workbook
            t_open = _now()
            t_wait = time.time()
            while time.time() - t_wait < 5:  # up to 5s
                try:
                    procs = subprocess.check_output(["tasklist"], creationflags=0x08000000).decode("utf-8", "ignore")
                    if "EXCEL.EXE" not in procs:
                        break
                except Exception:
                    break
                time.sleep(0.25)

            app_excel = xw.App(visible=True)
            app_excel.display_alerts = False
            app_excel.screen_updating = False
            try:
                app_excel.api.EnableEvents = True
                app_excel.api.DisplayAlerts = False
            except Exception:
                pass
            macro_called = False
            uia_ok = True

            try:
                try:
                    pid = getattr(app_excel, "pid", None)
                    print(f"[excel] started pid={pid}", flush=True)
                except Exception:
                    pass

                wb = app_excel.books.open(local_path)

                # Make sure the workbook is the active, visible foreground window
                try:
                    app_excel.visible = True
                    app_com = app_excel.api
                    wb.api.Activate()
                    try:
                        wb.api.Windows(1).Visible = True
                        wb.api.Windows(1).Activate()
                    except Exception:
                        pass
                    try:
                        app_com.WindowState = -4137  # xlMaximized
                    except Exception:
                        pass
                    print("[focus] workbook activated/maximized", flush=True)
                except Exception as e:
                    print(f"[focus] activation failed: {e}", flush=True)

                # ---------- BUSINESS-CRITICAL: run query_* tabs FIRST ----------
                print("[guard] RUN_QUERY_TABS entry", flush=True)
                t_sql = _now()
                run_query_tabs(wb, fork=fork, ticker=ticker, db_conn_dict=DB_CONN)
                print("[guard] RUN_QUERY_TABS done", flush=True)
                # ---------------------------------------------------------------

                # ---- COM objects (for robust refresh/timeouts)
                app_com = app_excel.api        # Excel.Application
                wb_com  = wb.api               # Excel.Workbook

                # --- Enumerate add-ins (unchanged) ---
                try:
                    coms = app_com.COMAddIns
                    print(f"[va] COMAddIns.Count={coms.Count}")
                    for i in range(1, coms.Count + 1):
                        ci = coms.Item(i)
                        print(f"[va] COMAddIn[{i}] Desc='{ci.Description}' ProgId='{ci.ProgId}' Connect={ci.Connect}")
                except Exception as e:
                    print(f"[va] COMAddIns enum failed: {e}")

                try:
                    addins = app_com.AddIns
                    print(f"[va] AddIns.Count={addins.Count}")
                    for i in range(1, addins.Count + 1):
                        ai = addins.Item(i)
                        name = getattr(ai, "Name", "")
                        title = getattr(ai, "Title", "")
                        full = ""
                        try:
                            full = ai.FullName
                        except Exception:
                            pass
                        print(f"[va] AddIn[{i}] Name='{name}' Title='{title}' Installed={ai.Installed} FullName='{full}'")
                except Exception as e:
                    print(f"[va] AddIns enum failed: {e}")

                try:
                    addins2 = app_com.AddIns2
                    print(f"[va] AddIns2.Count={addins2.Count}")
                    for i in range(1, addins2.Count + 1):
                        ai = addins2.Item(i)
                        full = ""
                        try:
                            full = ai.FullName
                        except Exception:
                            pass
                        print(f"[va] AddIns2[{i}] Name='{ai.Name}' Installed={ai.Installed} FullName='{full}'")
                except Exception as e:
                    print(f"[va] AddIns2 enum failed: {e}")

                # 0) Don’t block macros via policy
                try: app_com.AutomationSecurity = 1
                except Exception: pass

                # 1) Ensure VA COM add-in & AddIns toggled
                va_ok = ensure_va_addin(app_com)

                # 2) Try XLL registration via AddIns/AddIns2
                xll_registered = try_register_va_xlls(app_com)

                # 3) Try XLL registration by scanning the VA DLL folder
                xll_registered += hunt_and_register_va_xlls_from_dll_folder(app_com)

                # 4) NOW check RegisteredFunctions
                cnt = 0
                try:
                    rf = app_com.RegisteredFunctions
                    if rf is not None:
                        lim = min(len(rf), 500) if hasattr(rf, "__len__") else 200
                        for i in range(lim):
                            try: row = rf[i]
                            except Exception: break
                            if row and len(row) >= 2:
                                lib = str(row[0]).lower()
                                if ("visiblealpha" in lib) or ("vaexcel" in lib) or ("vaexcelplugin" in lib):
                                    cnt += 1
                except Exception as e:
                    print(f"[va] RegisteredFunctions read failed: {e}", flush=True)

                print(f"[va] RegisteredFunctions count={cnt} ...", flush=True)

                if cnt == 0:
                    pid = getattr(app_excel, "pid", None)
                    uia_ok = False
                    if pid:
                        uia_ok = ui_click_va_refresh_entire(pid, timeout=25)
                    if not uia_ok:
                        raise HTTPException(status_code=500, detail="VA UDFs not registered and UI refresh failed")

                # Ensure Automatic calc
                try:
                    app_com.Calculation = -4105   # xlCalculationAutomatic
                except Exception:
                    pass

                # 1) Refresh all external data first (sync)
                t_ext = _now()
                refresh_all_connections(wb_com)
                refresh_power_query(wb_com)
                refresh_querytables_and_listobjects(wb_com)
                refresh_pivots(wb_com)
                if _has_external_work(wb_com):
                    wait_quiet(app_com, wb_com, timeout=TIMEOUT_SEC // 2)

                # 2) Rebuild VA UDFs via PERSONAL macro (or fallback)
                t_macro = _now()
                try:
                    print("[macro] Run PERSONAL.XLSB!ForceVARecalc", flush=True)
                    app_com.Run("PERSONAL.XLSB!ForceVARecalc")
                    macro_called = True
                except Exception as e:
                    print(f"[macro] primary run failed: {e} -> trying qualified form", flush=True)
                    try:
                        app_com.Run("'PERSONAL.XLSB'!MVAHooks.ForceVARecalc")
                        macro_called = True
                    except Exception as e2:
                        print(f"[macro] WARN macro not callable: {e2} -> full rebuild", flush=True)
                        try:
                            app_com.CalculateFullRebuild()
                        except Exception:
                            app_com.Calculate()

                # --- Post-macro settle ---
                POST_MACRO_DWELL_SEC = 8
                before_used = _used_cells_snapshot(wb_com)
                print(f"[macro] dwell {POST_MACRO_DWELL_SEC}s (used_cells_before={before_used})", flush=True)
                t_dwell = _now()
                while _now() - t_dwell < POST_MACRO_DWELL_SEC:
                    _pump()
                    try:
                        app_com.CalculateUntilAsyncQueriesDone()
                    except Exception:
                        pass
                    time.sleep(0.25)
                after_used = _used_cells_snapshot(wb_com)
                print(f"[macro] dwell done (used_cells_after={after_used})", flush=True)

                try:
                    app_com.CalculateFullRebuild()
                except Exception:
                    app_com.Calculate()

                # 3) Final bounded wait; then save and close
                if not wait_quiet(app_com, wb_com, timeout=TIMEOUT_SEC):
                    raise HTTPException(status_code=500, detail="Timeout waiting for Excel to finish calc/queries")
                
                err_cnt = _count_name_errors(wb_com)
                print(f"[va] #NAME? cells after refresh: {err_cnt}", flush=True)
                if err_cnt > 0:
                    raise HTTPException(status_code=500, detail="VA add-in not active: consensus cells remain #NAME?")

                t_save = _now()
                wb.save()
                wb.close()

            finally:
                try:
                    app_excel.quit()
                except Exception:
                    pass
                kill_excel_if_lingering()

            # Upload back to S3
            t_up = _now()
            print(f"[s3] upload {local_path} → {model_key}", flush=True)
            s3.upload_file(local_path, BUCKET, model_key)

            # timings
            total = _fmt(_now() - t0)
            dur = {
                "download_s": _fmt(t_open - t_dl),
                "open_s": _fmt(t_sql - t_open),
                "sql_s": _fmt(t_ext - t_sql),
                "external_refresh_s": _fmt(t_macro - t_ext),
                "macro_phase_s": _fmt(t_save - t_macro),
                "save_close_s": _fmt(t_up - t_save),
                "upload_s": _fmt(_now() - t_up),
                "total_s": total,
            }
            print(f"[summary] macro={'yes' if macro_called else 'fallback'} durations={json.dumps(dur)}", flush=True)
            print(f"====== /api/refresh done {base_filename} total={total}s ======\n", flush=True)

            try:
                return {
                    "status": "ok",
                    "file": model_key,                 # backend-friendly
                    "uia_ok": bool(uia_ok),
                    "name_errors": int(err_cnt),
                    "refreshed_file": model_key,       # legacy
                    "va_udf_rebuild": "macro" if macro_called else "full_rebuild_fallback",
                    "durations": dur,
                }
            finally:
                try:
                    shutil.rmtree(tmp_dir, ignore_errors=True)
                except Exception:
                    pass

        except HTTPException:
            raise
        except Exception as e:
            print("❌ Excel refresh failed:")
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/refresh/ticker")
def refresh_excel_ticker(data: TickerRequest, request: Request):
    tkr = (data.ticker or "").upper().strip()
    if not tkr:
        raise HTTPException(status_code=400, detail="ticker required")
    key = _latest_key_for_ticker(tkr)
    if not key:
        raise HTTPException(status_code=404, detail=f"No model found for ticker={tkr}")
    print(f"[route] /api/refresh/ticker -> resolved key={key}", flush=True)
    # Reuse the same business-critical function so query_* runs
    return refresh_excel_file(RefreshRequest(path=key), request)
