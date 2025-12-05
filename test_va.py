# va_refresh_final.py
# Deterministic Visible Alpha refresh via Excel COM:
# - Avoids VA.Object.OnAddInsUpdate()
# - Ribbon Refresh All (ExecuteMso)
# - Synchronous refresh of all connections (BackgroundQuery=False)
# - Full calc rebuild
# - Hard timeouts + COM message pump (no infinite hangs)

import sys, time, os
import pythoncom
from win32com.client import DispatchEx

VA_PROGID = ".VisibleAlpha.ExcelAddin"
TIMEOUT_SEC = 300  # 5 min cap per stage

def pump(): pythoncom.PumpWaitingMessages()

def wait_quiet(app, timeout=TIMEOUT_SEC, poll=0.25):
    # Try to flush async queries first (if present)
    try: app.CalculateUntilAsyncQueriesDone()
    except Exception: pass
    start = time.time()
    while True:
        pump()
        try: calc_state = app.CalculationState  # -4135 done, -4136 calculating
        except Exception: calc_state = None
        bq = getattr(app, "BackgroundQueryCount", 0)
        if (calc_state in (None, -4135)) and (bq == 0):
            return True
        if time.time() - start > timeout:
            print("[TIMEOUT] Excel did not become quiet.")
            return False
        time.sleep(poll)

def refresh_all_connections(wb):
    """Refresh every connection synchronously; never enqueue background jobs."""
    try:
        conns = wb.Connections
        n = conns.Count
    except Exception:
        n = 0
    print(f"[INFO] Workbook connections: {n}")
    for i in range(1, n + 1):
        c = conns.Item(i)
        print(f"[RUN] Refreshing connection: {c.Name}")
        try:
            # Force sync mode where possible (ADO/ODBC/OLEDB)
            try:
                if hasattr(c, "ODBCConnection"):
                    c.ODBCConnection.BackgroundQuery = False
                if hasattr(c, "OLEDBConnection"):
                    c.OLEDBConnection.BackgroundQuery = False
            except Exception:
                pass
            # Some providers support RefreshWithRefreshAll; fallback to Refresh
            try:
                c.RefreshWithRefreshAll = True
            except Exception:
                pass
            c.Refresh()
        except Exception as e:
            print(f"[WARN] Connection {c.Name} refresh failed: {e}")

def main(path=None):
    pythoncom.CoInitialize()
    app = None
    try:
        app = DispatchEx("Excel.Application")
        app.Visible = True   # first runs: True helps SSO; set False later under Scheduler
        app.DisplayAlerts = False

        # Ensure VA add-in is connected (don’t call its Object methods)
        try:
            va = app.COMAddIns(VA_PROGID)
            if not va.Connect:
                va.Connect = True
            print("[INFO] Visible Alpha add-in connected.")
        except Exception as e:
            print(f"[WARN] Could not access/enable VA add-in: {e}")

        # Open workbook (or a throwaway one if none provided)
        if path:
            wb = app.Workbooks.Open(os.path.abspath(path))
        else:
            wb = app.Workbooks.Add()

        try:
            # 1) Ribbon “Refresh All”
            try:
                print("[RUN] CommandBars.ExecuteMso('ConnectionsRefreshAll')")
                app.CommandBars.ExecuteMso("ConnectionsRefreshAll")
            except Exception as e:
                print(f"[WARN] ExecuteMso failed: {e}")

            # 2) Hard-refresh every connection synchronously
            refresh_all_connections(wb)

            # 3) Calculation: flush async queries, then full rebuild
            try:
                print("[RUN] Application.CalculateUntilAsyncQueriesDone()")
                app.CalculateUntilAsyncQueriesDone()
            except Exception:
                pass
            try:
                print("[RUN] Application.CalculateFullRebuild()")
                app.CalculateFullRebuild()
            except Exception:
                app.Calculate()

            # 4) Bounded wait for quiet
            wait_quiet(app, timeout=TIMEOUT_SEC)

            # Optional: stamp a cell for your own validation
            try:
                ws = wb.Worksheets(1)
                ws.Range("B2").Value = time.strftime("%Y-%m-%d %H:%M:%S") + " | post-refresh checkpoint"
            except Exception:
                pass

            # Save if you opened a real file
            if path:
                wb.Save()
            print("[DONE] Refresh sequence finished.")
            return 0
        finally:
            wb.Close(SaveChanges=bool(path))
    except Exception as e:
        print("[ERROR]", e)
        return 2
    finally:
        try:
            if app: app.Quit()
        except Exception:
            pass
        pythoncom.CoUninitialize()

if __name__ == "__main__":
    # Optional: pass a workbook path as arg
    code = main(sys.argv[1] if len(sys.argv) > 1 else None)
    sys.exit(code)
