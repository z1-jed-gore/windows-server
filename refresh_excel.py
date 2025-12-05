# refresh_excel.py — minimal, bounded, loud logs (refactored, Always-Rebuild, VA autodetect)
print(f"[boot] refresh_excel.py loaded from {__file__}", flush=True)

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import os, re, time, subprocess, traceback, threading
import boto3, xlwings as xw, pythoncom
from pywinauto.keyboard import send_keys
from uuid import uuid4
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- config ----------
BUCKET = "agentic-data"
BASE_TEMP_DIR = r"C:\Temp"
WORK_DIR = os.path.join(BASE_TEMP_DIR, "work")
os.makedirs(WORK_DIR, exist_ok=True)

S3 = boto3.client("s3")
app = FastAPI()

KEEP_EXCEL_ALIVE = False          # fresh Excel each request
_gate = threading.Lock()

# Hard-coded backend target
BACKEND_BASE = "http://3.133.166.140:9000"

# ---- HARD-CODED REBUILD POLICY (no envs) ----
# We will ALWAYS rebuild VA (Snowflake -> Postgres) when the workbook looks like a VA model
# (i.e., has VA-style periods + codes). Toggle REQUIRE to fail on rebuild errors.
VA_REBUILD_REQUIRE = False        # set True to fail run if rebuild errors
VA_AS_OF = None                   # e.g., "2025-10-20" to backtest; keep None for live/latest

# DB URL for the query_* SQL phase (hard-coded)
DB_URL = (
    "postgresql://z1_admin:Zenger%3F5@z1-database-1.ctymuke0m20h.us-east-2.rds.amazonaws.com:5432/telecaster"
)

# ---------- small utils ----------
def log(msg):
    print(msg, flush=True)

def _now():
    return time.time()

def _col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def unblock_file(path):
    try:
        subprocess.run(
            ["powershell","-NoProfile","-ExecutionPolicy","Bypass","Unblock-File","-Path",path],
            check=False, capture_output=True, creationflags=0x08000000
        )
    except Exception:
        pass

def kill_excel():
    try:
        subprocess.call(
            ["taskkill","/f","/im","EXCEL.EXE"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        log("[phase] excel_killed")
    except Exception:
        pass

# ---------- external-data refresh (PowerQuery/Connections/QueryTables) ----------
import re as _reva

def _find_period_cell(ws, header_row: int, col: int) -> str | None:
    for rr in ([header_row] +
               [header_row - 1, header_row - 2, header_row - 3, header_row - 4,
                header_row + 1, header_row + 2, header_row + 3, header_row + 4]):
        if rr < 1:
            continue
        try:
            v = ws.range(rr, col).value
        except Exception:
            v = None
        lab = _canon_period_label(v)
        if lab:
            return f"{_col_letter(col)}${rr}"
    return None

def _find_va_relq_row(ws, label_col: int, search_rows: range) -> int | None:
    for rr in search_rows:
        try:
            v = ws.range(rr, label_col).value
        except Exception:
            v = None
        if isinstance(v, str) and _reva.search(r'^\s*Relative\s+Qtr\s+VA\s*$', v, _reva.I):
            return rr
    best_row, best_hits = None, -1
    try:
        last_col = int(ws.used_range.last_cell.column)
    except Exception:
        last_col = 200
    for rr in search_rows:
        hits = 0
        for cc in range(3, last_col + 1):
            try:
                val = ws.range(rr, cc).value
            except Exception:
                val = None
            if isinstance(val, str) and _reva.search(r'^FQ[+\-]?\d+$', val.strip(), _reva.I):
                hits += 1
        if hits > best_hits:
            best_hits, best_row = hits, rr
    return best_row

def _find_fq_cell_for_col(ws, header_row: int, col: int) -> tuple[str | None, str | None]:
    scan_rows = [header_row + 2, header_row + 1, header_row + 3,
                 header_row,     header_row + 4, header_row - 1,
                 header_row + 5, header_row - 2, header_row + 6,
                 header_row + 7, header_row + 8]
    for rr in scan_rows:
        if rr < 1:
            continue
        try:
            v = ws.range(rr, col).value
        except Exception:
            v = None
        if isinstance(v, str) and _reva.fullmatch(r'(?:FQ|F)[+\-]?\d+', v.strip(), flags=_reva.I):
            return f"{_col_letter(col)}${rr}", v.strip().upper()
    return None, None

def _canon_period_from_headers(ws, header_row: int, col: int) -> str | None:
    p = _canon_period_label(ws.range(header_row, col).value)
    if p:
        return p
    for rr in (header_row - 1, header_row - 2, header_row - 3, header_row - 4):
        if rr < 1:
            continue
        try:
            v = ws.range(rr, col).value
        except Exception:
            v = None
        p = _canon_period_label(v)
        if p:
            return p
        if isinstance(v, str):
            s = v.strip()
            m = _reva.search(r'(?i)FY[-\s]?(\d{4})', s)
            if m:
                return f"FY-{m.group(1)}"
            m = _reva.search(r'(?i)Q([1-4])[-\s]?(\d{4})', s)
            if m:
                return f"{m.group(1)}QFY-{m.group(2)}"
    return None

def ensure_model_formulas(
    wb, sheet_name: str, header_row: int, periods_canon: list[str], codes_rows: list[tuple[int, str]],
    max_scan_cols: int = 500, trace_samples: int = 10,
) -> int:
    ws = wb.sheets[sheet_name]
    try:
        last_col_row4 = int(ws.api.Cells(header_row, ws.api.Columns.Count).End(-4159).Column)
    except Exception:
        last_col_row4 = 0
    try:
        used_last_col = int(ws.used_range.last_cell.column)
    except Exception:
        used_last_col = 0
    scan_last_col = max(used_last_col, last_col_row4, max_scan_cols)

    period_by_col: dict[int, str] = {}
    period_cell_by_col: dict[int, str] = {}
    relq_row = _find_va_relq_row(ws, label_col=5, search_rows=range(header_row-2, header_row+8)) or (header_row + 2)

    for c in range(3, scan_last_col + 1):
        ref_col, lab_col = _find_fq_cell_for_col(ws, header_row, c)
        if ref_col and lab_col:
            period_by_col[c] = lab_col
            period_cell_by_col[c] = ref_col
            continue
        try:
            v = ws.range(relq_row, c).value
        except Exception:
            v = None
        if isinstance(v, str) and _reva.fullmatch(r'(?:FQ|F)[+\-]?\d+', v.strip(), flags=_reva.I):
            lab = v.strip().upper()
            ref = f"{_col_letter(c)}${relq_row}"
            period_by_col[c] = lab
            period_cell_by_col[c] = ref
        else:
            ref = _find_period_cell(ws, header_row, c)
            if ref:
                try:
                    vv = ws.range(ref).value
                except Exception:
                    vv = None
                lab = _canon_period_label(vv)
                if lab:
                    period_by_col[c] = lab
                    period_cell_by_col[c] = ref

    if not period_by_col:
        log("[convert] WARN: no period labels detected across scanned columns")

    va_rx   = _reva.compile(r'VADATA', _reva.I)
    #cons_rx = _reva.compile(r'CONSENSUS\.MEDIAN', _reva.I)
    cons_rx = _reva.compile(r'CONSENSUS(?:\.MEDIAN)?', _reva.I)
    xl_rx   = _reva.compile(r'XLOOKUP', _reva.I)
    hardcoded_period_in_key = _reva.compile(r'\$B\d+\s*&\s*"\|"\s*&\s*"', _reva.I)
    arg2_rx = _reva.compile(r'VADATA\(\s*[^,]+,\s*([A-Z]{1,3}\$?\d+)', _reva.I)
    code_rx = _reva.compile(r'^\s*(?:N_[A-Z0-9]+|VA_[A-Z0-9_]+|[A-Z][A-Z0-9_]{2,})\s*$', _reva.I)
    sumifs_ok_rx = _reva.compile(
        r'SUMIFS\(\s*query_va_refresh!\$E:\$E\s*,\s*query_va_refresh!\$D:\$D\s*,\s*\$?[A-Z]{1,3}\$[1-9]\d*\s*,\s*query_va_refresh!\$C:\$C\s*,',
        _reva.I
    )

    findings = []
    replaced = 0

    for r, _code in codes_rows:
        try:
            bval = ws.range(r, 2).value
        except Exception:
            bval = None
        if not (isinstance(bval, str) and code_rx.match(bval.strip())):
            continue

        for c in range(3, scan_last_col + 1):
            try:
                f0 = ws.range(r, c).formula or ""
            except Exception:
                f0 = ""
            if not f0:
                continue
            if sumifs_ok_rx.search(f0):
                continue

            fu = f0.upper()
            mode = None
            if va_rx.search(fu) and cons_rx.search(fu):
                mode = "vadata"
            elif xl_rx.search(fu) and hardcoded_period_in_key.search(f0):
                mode = "xlookup_hardcoded"
            else:
                continue

            p = period_by_col.get(c) or _canon_period_from_headers(ws, header_row, c)
            if not p and mode == "vadata":
                m = arg2_rx.search(f0)
                if m:
                    ref = m.group(1).replace("$", "")
                    try:
                        raw = ws.range(ref).value
                    except Exception:
                        raw = None
                    p = _canon_period_label(raw)

            findings.append({"r": r, "c": c, "mode": mode, "p": p, "old": f0})

    va_cols = sorted({it["c"] for it in findings})
    log("[convert] va_cols=" + ", ".join(_col_letter(c) for c in va_cols))

    if period_by_col and va_cols:
        log("[convert] period_by_col (VA cols head): " +
            ", ".join(f"{_col_letter(c)}={period_by_col.get(c, '')}" for c in va_cols[:16]))
        log("[convert] period_by_col (VA cols tail): " +
            ", ".join(f"{_col_letter(c)}={period_by_col.get(c, '')}" for c in va_cols[-16:]))

    period_known_cols = []
    for c in range(3, scan_last_col + 1):
        if period_by_col.get(c) or _canon_period_from_headers(ws, header_row, c):
            period_known_cols.append(c)
    rightmost_known = max(period_known_cols) if period_known_cols else None

    if findings:
        samp = []
        for it in findings[:trace_samples]:
            r, c, p = it["r"], it["c"], it["p"]
            try:
                hdr4 = ws.range(header_row, c).value
            except Exception:
                hdr4 = None
            samp.append(f"R{r}C{c} col={c} hdr4='{hdr4}' -> period='{p}'")
        log(f"[convert] scan_cols=3..{scan_last_col} rightmost_period_col={rightmost_known}")
        log(f"[convert] findings={len(findings)} sample={samp}")
    else:
        log("[convert] no VAData/XLOOKUP hard-coded cells found — likely already SUMIFS; skipping conversion")
        return 0

    app_api = wb.app.api
    old_calc   = getattr(app_api, "Calculation", None)
    old_screen = getattr(app_api, "ScreenUpdating", None)
    old_events = getattr(app_api, "EnableEvents", None)
    old_status = getattr(app_api, "DisplayStatusBar", None)

    try:
        try: app_api.Calculation = -4135  # xlCalculationManual
        except Exception: pass
        try: app_api.ScreenUpdating = False
        except Exception: pass
        try: app_api.EnableEvents = False
        except Exception: pass
        try: app_api.DisplayStatusBar = False
        except Exception: pass

        for it in findings:
            r, c, mode, p, f0 = it["r"], it["c"], it["mode"], it["p"], it["old"]
            period_cell_ref = period_cell_by_col.get(c) or _find_period_cell(ws, header_row, c)
            if not period_cell_ref:
                continue
            f_new = _replace_vadata_subexpr(f0, r, period_cell_ref)
            if f_new == f0:
                f_new = (
                    f'=IFERROR('
                    f'SUMIFS('
                    f'query_va_refresh!$E:$E,'
                    f'query_va_refresh!$D:$D,{period_cell_ref},'
                    f'query_va_refresh!$C:$C,$B{r}'
                    f'),"")'
                )
            ws.range(r, c).formula = f_new
            replaced += 1
    finally:
        try: app_api.Calculate()
        except Exception: pass
        try:
            if old_calc   is not None: app_api.Calculation      = old_calc
            if old_screen is not None: app_api.ScreenUpdating   = old_screen
            if old_events is not None: app_api.EnableEvents     = old_events
            if old_status is not None: app_api.DisplayStatusBar = old_status
        except Exception:
            pass

    log(f"[convert] replaced_formulas={replaced}")
    return replaced

def ensure_query_va_refresh(wb, vaticker: str):
    target = None
    for s in wb.sheets:
        if s.name.lower() == "query_va_refresh":
            target = s
            break
    if not target:
        idx = None
        for i, s in enumerate(wb.sheets):
            if s.name.lower() == "model":
                idx = i + 1
                break
        target = wb.sheets.add(name="query_va_refresh", after=wb.sheets[idx] if idx else wb.sheets[-1])
        log("[sql] created sheet 'query_va_refresh'")
    sql_a1 = (
        "SELECT\n"
        "  code,\n"
        "  period_label,\n"
        "  value,\n"
        "  (code || '|' || period_label) AS k\n"
        "FROM va_refresh_data\n"
        "WHERE ticker = '{{ticker}}' AND metric = 'consensus'\n"
        "ORDER BY code, period_label;"
    )
    try:
        a1 = target.range("A1").value
    except Exception:
        a1 = None
    if not a1 or "va_refresh_data" not in str(a1):
        target.range("A1").value = sql_a1
        log("[sql] wrote A1 in 'query_va_refresh'")
    return target

def _replace_vadata_subexpr(formula: str, row_idx: int, period_cell_ref: str | None) -> str:
    if not period_cell_ref:
        return formula
    fu = formula.upper()
    ix = fu.find("VADATA(")
    if ix == -1:
        return formula
    start = ix - 1 if ix > 0 and formula[ix - 1] == "@" else ix
    k = ix
    while k < len(formula) and formula[k] != '(':
        k += 1
    if k >= len(formula):
        return formula
    depth, j = 1, k + 1
    while j < len(formula) and depth > 0:
        ch = formula[j]
        depth += (ch == '(') - (ch == ')')
        j += 1
    if depth != 0:
        return formula
    sumifs = (
        f'SUMIFS('
        f'query_va_refresh!$E:$E,'
        f'query_va_refresh!$D:$D,{period_cell_ref},'
        f'query_va_refresh!$C:$C,$B{row_idx}'
        f')'
    )
    return formula[:start] + sumifs + formula[j:]

def _canon_period_label(s: str) -> str | None:
    if s is None:
        return None
    s = str(s).replace("\xa0", " ").strip()
    s = _reva.sub(r'(?i)^\s*(consensus|actuals|estimate|estimates?)\s+', '', s).strip()
    u = s.upper()
    if u in {"NTM","LTM","STM","STMU","NTMU"}:
        return u
    m = _reva.match(r'^\s*(FQ|F)\s*([+\-]?\d+)\s*$', u, _reva.I)
    if m:
        return f"{m.group(1).upper()}{m.group(2)}"
    m = _reva.match(r'^\s*([+\-]?\d+)\s*FQ\s*$', u, _reva.I)
    if m:
        n = m.group(1)
        if not n.startswith(('+', '-')):
            n = f'+{n}'
        return f"{n}FQ"
    m = _reva.match(r'(?i)^FY[-\s]?(\d{4})E?$', s)
    if m:
        return f"FY-{m.group(1)}"
    m = _reva.match(r'(?i)^FY[-\s]?(\d{2})E?$', s)
    if m:
        yy = int(m.group(1)); yyyy = 2000 + yy if yy < 70 else 1900 + yy
        return f"FY-{yyyy}"
    m = _reva.match(r'(?i)^(?:Q([1-4])(?:FY)?)[-\s]?(\d{4})E?$', s)
    if m:
        return f"{m.group(1)}QFY-{m.group(2)}"
    m = _reva.match(r'(?i)^([1-4])Q(?:FY)?[-\s]?(\d{2})E?$', s)
    if m:
        yy = int(m.group(2)); yyyy = 2000 + yy if yy < 70 else 1900 + yy
        return f"{m.group(1)}QFY-{yyyy}"
    return None

def discover_model_params(wb, sheet_name="Model", guess_rows=8):
    ws = wb.sheets[sheet_name]
    header_row = 4

    try:
        last_col_row4 = int(ws.api.Cells(header_row, ws.api.Columns.Count).End(-4159).Column)
    except Exception:
        last_col_row4 = 0
    try:
        used_last_col = int(ws.used_range.last_cell.column)
    except Exception:
        used_last_col = 0
    last_col = max(used_last_col, last_col_row4, 500)

    last_row = ws.used_range.last_cell.row

    if last_col < 3:
        raise HTTPException(422, "No period columns found (need col C or beyond).")
    hdr_rng = ws.range((header_row, 3), (header_row, last_col)).value
    if isinstance(hdr_rng, (list, tuple)) and hdr_rng and isinstance(hdr_rng[0], (list, tuple)):
        row_vals = list(hdr_rng[0])
    elif isinstance(hdr_rng, (list, tuple)):
        row_vals = list(hdr_rng)
    else:
        row_vals = [hdr_rng]

    periods, seen = [], set()
    for v in row_vals:
        lab = _canon_period_label(v)
        if lab and lab not in seen:
            periods.append(lab); seen.add(lab)
    if not periods:
        raise HTTPException(422, "Row 4 did not yield any canonical VA period labels (NTM/FY/Q).")

    if last_row <= header_row:
        raise HTTPException(422, "No data rows under header.")
    colb_rng = ws.range((header_row + 1, 2), (last_row, 2)).value
    if isinstance(colb_rng, (list, tuple)) and colb_rng and isinstance(colb_rng[0], (list, tuple)):
        colb_vals = [row[0] for row in colb_rng]
    elif isinstance(colb_rng, (list, tuple)):
        colb_vals = list(colb_rng)
    else:
        colb_vals = [colb_rng]

    code_rx = _reva.compile(r'^\s*(?:N_[A-Z0-9]+|VA_[A-Z0-9_]+|[A-Z][A-Z0-9_]{2,})\s*$', _reva.I)
    codes_rows, ignored_rows = [], []
    r0 = header_row + 1

    for i, v in enumerate(colb_vals):
        r = r0 + i
        if not isinstance(v, str):
            continue
        s = v.strip()
        if not s:
            continue
        if code_rx.match(s):
            codes_rows.append((r, s))
        else:
            ignored_rows.append((r, s))

    log(f"[model] header_row={header_row} VA_codes={len(codes_rows)} ignored_non_va={len(ignored_rows)} periods={len(periods)}")
    return header_row, periods, codes_rows

# ---------- S3 ----------
def s3_download_atomic(bucket, key, dest):
    cfg  = Config(connect_timeout=5, read_timeout=30, retries={"max_attempts":3})
    s3c  = boto3.client("s3", config=cfg)
    tcfg = TransferConfig(use_threads=False, max_concurrency=1, multipart_threshold=1024**4)
    part = f"{dest}.part_{uuid4().hex}"
    try:
        try: os.remove(part)
        except FileNotFoundError:
            pass
        log(f"[s3] dl_begin key={key} -> {dest}")
        t0 = _now()
        s3c.download_file(bucket, key, part, Config=tcfg)
        os.replace(part, dest)
        log(f"[s3] dl_ok bytes={os.path.getsize(dest)} in {round(_now()-t0,2)}s")
    except Exception as e:
        try: os.remove(part)
        except Exception:
            pass
        raise HTTPException(502, f"S3 download failed: {e}")

# ---------- SQL tabs ----------
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

def log_excel_refresh_run_start(ticker: str, file_key: str, source: str) -> int | None:
    """
    Insert a 'running' row into excel_refresh_runs.
    Returns run_id or None on failure (best-effort).
    """
    try:
        with _pg_connect(DB_URL) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO excel_refresh_runs (ticker, file_key, source, status)
                VALUES (%s, %s, %s, 'running')
                RETURNING id
                """,
                (ticker, file_key, source),
            )
            run_id = cur.fetchone()[0]
            conn.commit()
            log(f"[db] excel_refresh_runs start id={run_id} ticker={ticker} source={source}")
            return run_id
    except Exception as e:
        log(f"[db] excel_refresh_runs start FAILED: {e}")
        return None


def log_excel_refresh_run_finish(
    run_id: int | None,
    *,
    status: str,
    va_used: bool | None = None,
    va_rebuild: str | None = None,
    codes: int | None = None,
    periods: int | None = None,
    durations: dict | None = None,
    error_message: str | None = None,
) -> None:
    """
    Update an existing excel_refresh_runs row (best-effort).
    """
    if not run_id:
        return
    try:
        dur_json = json.dumps(durations) if isinstance(durations, dict) else None
        with _pg_connect(DB_URL) as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE excel_refresh_runs
                SET finished_at   = now(),
                    status        = %s,
                    va_used       = COALESCE(%s, va_used),
                    va_rebuild    = COALESCE(%s, va_rebuild),
                    codes         = COALESCE(%s, codes),
                    periods       = COALESCE(%s, periods),
                    durations     = COALESCE(%s::jsonb, durations),
                    error_message = COALESCE(%s, error_message)
                WHERE id = %s
                """,
                (
                    status,
                    va_used,
                    va_rebuild,
                    codes,
                    periods,
                    dur_json,
                    error_message,
                    run_id,
                ),
            )
            conn.commit()
        log(f"[db] excel_refresh_runs finish id={run_id} status={status}")
    except Exception as e:
        log(f"[db] excel_refresh_runs finish FAILED id={run_id}: {e}")


def run_query_tabs(
    wb, fork: str, ticker: str, db=DB_URL,
    include: list[str] | None = None, parallel: bool = True,
    max_workers: int = 3, stmt_timeout_ms: int = 15000
) -> None:
    """
    For each sheet whose name starts with 'query_':

      - Read SQL from A1
      - Execute it against Postgres
      - Write results starting at C3 (headers in row 3)
      - Stamp B1/B2 with status
      - On sheet 'query_llm_outputs_work', format the 'value' column to 4 decimals

    This preserves the old "jobs from A1" behaviour, with the newer 4dp formatting.
    """
    # 1) Find candidate sheets
    all_sheets = [s for s in wb.sheets if str(s.name).lower().startswith("query_")]
    if include:
        want = {n.lower() for n in include}
        sheets = [s for s in all_sheets if s.name.lower() in want]
    else:
        sheets = all_sheets

    log(f"[sql] sheets={len(sheets)} -> {[s.name for s in sheets]}")
    if not sheets:
        return

    # Make sure Excel is in automatic calc mode before we start
    try:
        wb.app.api.Calculation = -4105  # xlCalculationAutomatic
        wb.app.api.Calculate()
    except Exception:
        pass

    # 2) Build jobs from A1 (this is the piece that was effectively removed)
    jobs: list[tuple[str, str]] = []
    for sheet in sheets:
        name = sheet.name
        try:
            raw_sql = sheet.range("A1").value
        except Exception:
            raw_sql = None

        base_sql = _clean_sql_text(raw_sql)
        if not base_sql:
            # No SQL → mark as skipped and move on
            stamp = f"{datetime.now():%Y-%m-%d %H:%M:%S}"
            try:
                sheet.range("B1").value = f"{stamp} | skipped"
                sheet.range("B2").value = "no SQL in A1"
            except Exception:
                pass
            log(f"[sql] {name} skipped: no SQL in A1")
            continue

        # --- TEMPLATE SUBSTITUTION ---
        # Replace {{ticker}} / {{TICKER}} with the workbook's ticker (e.g. M_US)
        sql = base_sql.replace("{{ticker}}", ticker.upper()).replace("{{TICKER}}", ticker.upper())

        # Optional: support {{fork}} tokens if present
        if fork:
            sql = sql.replace("{{fork}}", str(fork)).replace("{{FORK}}", str(fork))

        # If we *still* see {{ticker}} in the SQL, log it loudly
        if "{{ticker}}" in sql or "{{TICKER}}" in sql:
            log(f"[sql] WARN {name}: unresolved {{ticker}} token in SQL after templating")

        jobs.append((name, sql))


    log(f"[sql] jobs={len(jobs)}")
    if not jobs:
        return

    # 3) Helper to execute a single query
    def _fetch_one(name_sql: tuple[str, str]) -> tuple[str, list[str], list[list], str]:
        name, sql = name_sql
        t0 = time.time()
        try:
            with psycopg2.connect(db) as conn, conn.cursor() as cur:
                if stmt_timeout_ms:
                    cur.execute("SET lock_timeout = '2s'")
                    cur.execute("SET statement_timeout = %s", (f"{stmt_timeout_ms}ms",))
                cur.execute(sql)
                rows = cur.fetchall() if cur.description else []
                cols = [d.name for d in cur.description] if cur.description else []
            ms = int((time.time() - t0) * 1000)
            return name, cols, rows, f"ok {ms}ms"
        except Exception as e:
            return name, [], [], f"err {type(e).__name__}: {e}"

    # 4) Run all jobs (optionally in parallel)
    results: dict[str, tuple[list[str], list[list], str]] = {}
    if parallel and len(jobs) > 1:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(_fetch_one, j): j[0] for j in jobs}
            for fut in as_completed(futs):
                name = futs[fut]
                try:
                    name_r, cols, rows, why = fut.result()
                except Exception as e:
                    cols, rows, why = [], [], f"err {type(e).__name__}: {e}"
                results[name] = (cols, rows, why)
    else:
        for j in jobs:
            name, cols, rows, why = _fetch_one(j)
            results[name] = (cols, rows, why)

    # 5) Write results back into each sheet
    for sheet in sheets:
        name = sheet.name
        cols, rows, why = results.get(name, ([], [], "skipped"))

        try:
            # Clear existing data block starting at C3
            try:
                sheet.range("C3").expand().clear()
            except Exception:
                try:
                    sheet.range("C3").expand().clear_contents()
                except Exception:
                    pass

            if rows and cols:
                data = [cols] + rows
                sheet.range("C3").value = data

                # NEW: Force 'value' column to 4 decimals on query_llm_outputs_work
                if name.lower() == "query_llm_outputs_work":
                    try:
                        value_idx = next(
                            (i for i, c in enumerate(cols) if str(c).lower() == "value"),
                            None,
                        )
                        if value_idx is not None and rows:
                            # C (col 3) is where headers start; value_idx is 0-based
                            col_idx    = 3 + value_idx
                            header_row = 3
                            first_row  = header_row + 1
                            last_row   = header_row + len(rows)
                            rng = sheet.range((first_row, col_idx), (last_row, col_idx))
                            rng.number_format = "0.0000"
                            log(f"[sql] {name} formatted Value column to 4 decimals")
                    except Exception as fe:
                        log(f"[sql] {name} format Value 4dp FAILED: {fe}")

            # Status stamp in B1/B2
            stamp = f"{datetime.now():%Y-%m-%d %H:%M:%S}"
            if why.startswith("ok"):
                sheet.range("B1").value = f"{stamp} | success"
                sheet.range("B2").value = f"rows={len(rows)} ({why})"
            else:
                sheet.range("B1").value = f"{stamp} | failed"
                sheet.range("B2").value = why[:500]

            log(f"[sql] {name} rows={len(rows)} {why}")
        except Exception as e:
            log(f"[sql] {name} WRITE ERROR: {e}")

# ---------- robust workbook open ----------
def open_workbook_robust(app: xw.App, path: str):
    log(f"[open] path={path}")
    try:
        for b in list(app.books):
            if b.name.lower().startswith("book"):
                b.close(save=False)
    except Exception:
        pass

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
                    wb = b
                    break
        except Exception as e:
            raise HTTPException(500, f"COM open failed: {e}")

    try:
        send_keys("{ESC}")
    except Exception:
        pass

    if wb is None:
        raise HTTPException(500, "Excel launched but workbook did not open")
    log(f"[open] done wb={wb.name}")
    return wb

def _fmt(s):
    return round(float(s), 3)

# ---------- backend VA utilities ----------
def _backend_healthy(base: str) -> bool:
    try:
        r = requests.get(base.rstrip("/") + "/health", timeout=(3, 3))
        return r.status_code == 200
    except Exception:
        return False

def backend_rebuild_va_pg(ticker: str, codes: list[str], periods: list[str], endpoint_base: str, require: bool = False):
    url = endpoint_base.rstrip("/") + "/api/va_refresh/rebuild"
    body = {"ticker": ticker.upper(), "codes": codes, "period_labels": periods}

    TIMEOUT = (5, 120)  # (connect, read)
    retry = Retry(total=3, connect=3, read=1, backoff_factor=0.5,
                  status_forcelist=(502, 503, 504), allowed_methods=frozenset(["GET","POST"]))
    s = requests.Session()
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    try:
        r = s.post(url, json=body, timeout=TIMEOUT)
        r.raise_for_status()
        resp = r.json()
        if resp.get("status") != "ok":
            raise RuntimeError(f"unexpected resp: {resp}")
        return {"status": "ok", **resp}
    except Exception as e:
        if require:
            raise
        log(f"[backend] WARN rebuild skipped (best-effort): {type(e).__name__}: {e}")
        return {"status": "skipped", "reason": str(e)}

# ---- helper with as_of (used when VA_AS_OF is set) ----
def backend_rebuild_va_pg_with_asof(ticker, codes, periods, endpoint_base, as_of: str, require=False):
    url = endpoint_base.rstrip("/") + "/api/va_refresh/rebuild"
    body = {
        "ticker": ticker.upper(),
        "codes": codes,
        "period_labels": periods,
        "as_of": as_of
    }
    TIMEOUT = (5, 120)
    retry = Retry(total=3, connect=3, read=1, backoff_factor=0.5,
                  status_forcelist=(502, 503, 504), allowed_methods=frozenset(["GET","POST"]))
    s = requests.Session()
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    try:
        r = s.post(url, json=body, timeout=TIMEOUT)
        r.raise_for_status()
        resp = r.json()
        if resp.get("status") != "ok":
            raise RuntimeError(f"unexpected resp: {resp}")
        return {"status": "ok", **resp}
    except Exception as e:
        if require:
            raise
        log(f"[backend] WARN rebuild (as_of) skipped: {type(e).__name__}: {e}")
        return {"status": "skipped", "reason": str(e)}

# ---------- API ----------
class RefreshRequest(BaseModel):
    path: str

@app.post("/api/refresh/")
@app.post("/api/refresh/")
def refresh_excel_file(
    data: RefreshRequest,
    request: Request,
    source: str = "manual",   # 'manual' by default; override when called internally
):
    pythoncom.CoInitialize()
    wb = None
    app_excel = None
    va_used = False
    va_rebuild_status = "skipped"
    run_id = None  # best-effort logging; may stay None if we fail very early

    try:
        # snapshot of S3 object before/after (for race/stale detection)
        s3_lastmod_before = None
        s3_etag_before    = None
        s3_lastmod_after  = None
        s3_etag_after     = None

        with _gate:
            key = data.path.strip()
            log(f"\n==== /api/refresh {key} ====")
            if not key.lower().endswith((".xlsx", ".xlsm")):
                raise HTTPException(400, "Only .xlsx/.xlsm")

            # Proactive cleanup so we never inherit zombies
            kill_excel()
            time.sleep(0.25)

            base  = os.path.basename(key)
            safe  = re.sub(r"[^A-Za-z0-9._-]", "_", base)
            local = os.path.join(WORK_DIR, safe)

            t0_all = _now()

            # 0) S3 → local
            t0_dl = _now()
            log(f"[phase] s3_download_start {key} -> {local}")

            # NEW: snapshot S3 state before we download
            try:
                head_before = S3.head_object(Bucket=BUCKET, Key=key)
                s3_lastmod_before = head_before.get("LastModified")
                s3_etag_before    = head_before.get("ETag")
                log(f"[s3] before LastModified={s3_lastmod_before} ETag={s3_etag_before}")
            except Exception as e:
                log(f"[s3] head_before FAILED: {e}")

            s3_download_atomic(BUCKET, key, local)
            unblock_file(local)
            log("[phase] s3_download_ok")
            t1_dl = _now()

            # 1) Launch Excel (fresh)
            t0_open = _now()
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

            wb = open_workbook_robust(app_excel, local)
            log(f"[excel] opened: {wb.name}")
            try:
                send_keys("{ESC}")
            except Exception:
                pass

            app_api = app_excel.api
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

            # 2) Ticker naming from filename
            name_wo_ext = os.path.splitext(base)[0]
            parts = name_wo_ext.split("_")
            base_ticker = parts[0].upper() if parts else ""
            if not base_ticker:
                raise HTTPException(400, "Cannot infer ticker from filename")
            vatkr = base_ticker if re.search(r'_[A-Z]{2}$', base_ticker) else f"{base_ticker}_US"

            # ---- log run start (best-effort) ----
            run_id = log_excel_refresh_run_start(vatkr, key, source)

            # 3) Discover params (Model sheet) and detect VA-ness from workbook (no allow-list)
            t0_discover = _now()
            hdr_row, periods, codes_rows = discover_model_params(wb, sheet_name="Model")
            codes_all = [c for _, c in codes_rows]
            codes = [c for c in codes_all if re.fullmatch(r'\s*N_\d+\s*', str(c) or "", flags=re.I)]
            rows_node = [(r, c) for (r, c) in codes_rows if re.fullmatch(r'\s*N_\d+\s*', str(c) or "", flags=re.I)]

            # Autodetect VA: must have period labels AND at least one NodeCode in col B
            has_va = bool(periods) and len(codes) > 0

            log(f"[model] header_row={hdr_row} codes={len(codes)} periods={len(periods)} has_va={has_va}")
            # Only raise if periods missing; codes may be empty for some non-VA models
            if not periods:
                raise HTTPException(422, "No period labels discovered from Model (row 4).")
            t1_discover = _now()

            # Convert 'FQ+N' → '+NFQ' for backend compatibility (leave FY-YYYY / 1QFY-YYYY / NTM/LTM/STM unchanged)
            def _to_backend_period(p: str) -> str:
                s = str(p).strip().upper()
                m = re.fullmatch(r'FQ([+\-]?\d+)', s)
                if m:
                    n = m.group(1)
                    if not n.startswith(('+', '-')) and n:
                        n = f'+{n}'
                    return f"{n}FQ"
                m = re.fullmatch(r'F([+\-]?\d+)', s)
                if m:
                    n = m.group(1)
                    if not n.startswith(('+', '-')) and n:
                        n = f'+{n}'
                    return f"{n}F"
                return s

            periods_backend = [_to_backend_period(p) for p in periods]

            # 4) Backend rebuild (ALWAYS when workbook looks like a VA model AND codes present)
            t0_rebuild = _now()
            if has_va and codes:
                va_used = True
                try:
                    if VA_AS_OF:
                        log(f"[backend] REBUILD ticker={vatkr} codes={len(codes)} periods={len(periods)} as_of={VA_AS_OF}")
                        resp = backend_rebuild_va_pg_with_asof(
                            vatkr, codes, periods_backend, BACKEND_BASE, as_of=VA_AS_OF, require=VA_REBUILD_REQUIRE
                        )
                    else:
                        log(f"[backend] REBUILD ticker={vatkr} codes={len(codes)} periods={len(periods)} live")
                        resp = backend_rebuild_va_pg(
                            vatkr, codes, periods_backend, BACKEND_BASE, require=VA_REBUILD_REQUIRE
                        )
                    va_rebuild_status = resp.get("status", "ok")
                    log(f"[backend] va_refresh/rebuild {va_rebuild_status}")
                except Exception as e:
                    va_rebuild_status = f"error:{type(e).__name__}"
                    log(f"[backend] ERROR rebuild failed: {e}")
                    if VA_REBUILD_REQUIRE:
                        raise
            else:
                log("[backend] skip va_refresh/rebuild (non-VA workbook or no codes)")
            t1_rebuild = _now()

            # 5) Query evidence sheet(s)
            t0_query = _now()
            try:
                includes = ["query_llm_outputs_work"]
                if va_used:
                    ensure_query_va_refresh(wb, vatkr)
                    includes.insert(0, "query_va_refresh")
                run_query_tabs(
                    wb, fork="", ticker=vatkr, db=DB_URL,
                    include=includes, parallel=True, max_workers=5, stmt_timeout_ms=15000
                )
            except Exception as e:
                log(f"[sql] ERROR query_tabs: {e}")
            t1_query = _now()

            # 6) One-time conversion (only for VA flows)
            try:
                if va_used:
                    replaced = ensure_model_formulas(wb, "Model", hdr_row, periods, rows_node)
                    if replaced:
                        log(f"[convert] converted {replaced} VA cells to SUMIFS")
                else:
                    log("[convert] skip ensure_model_formulas (non-VA workbook)")
            except Exception as e:
                log(f"[convert] ERROR: {e}")

            # 7) Recalc, save, upload
            t0_recalc = _now()
            try:
                app_api.Calculate()
            except Exception:
                pass
            t1_recalc = _now()

            t0_save = _now()
            wb.save()
            wb.close()
            log("[phase] workbook_closed")
            t1_save = _now()

            t0_up = _now()
            log(f"[phase] s3_upload_start {local} -> {key}")
            S3.upload_file(local, BUCKET, key)
            log("[phase] s3_upload_ok")
            t1_up = _now()

            # : snapshot S3 state after upload
            try:
                head_after = S3.head_object(Bucket=BUCKET, Key=key)
                s3_lastmod_after = head_after.get("LastModified")
                s3_etag_after    = head_after.get("ETag")
                log(f"[s3] after LastModified={s3_lastmod_after} ETag={s3_etag_after}")
            except Exception as e:
                log(f"[s3] head_after FAILED: {e}")

            # 8) Durations + response
            total = _fmt(_now() - t0_all)
            durations = {
                "download_s":   _fmt(t1_dl - t0_dl),
                "open_s":       _fmt(t1_open - t0_open),
                "discover_s":   _fmt(t1_discover - t0_discover),
                "rebuild_s":    _fmt(t1_rebuild - t0_rebuild),
                "query_s":      _fmt(t1_query - t0_query),
                "recalc_s":     _fmt(t1_recalc - t0_recalc),
                "save_close_s": _fmt(t1_save - t0_save),
                "upload_s":     _fmt(t1_up - t0_up),
                "total_s":      total,
                # NEW: S3 metadata snapshots for post-mortems
                "s3_lastmod_before": (
                    s3_lastmod_before.isoformat() if s3_lastmod_before else None
                ),
                "s3_etag_before":    s3_etag_before,
                "s3_lastmod_after": (
                    s3_lastmod_after.isoformat() if s3_lastmod_after else None
                ),
                "s3_etag_after":     s3_etag_after,
                "s3_changed_during_run": (
                    True
                    if (s3_etag_before is not None
                        and s3_etag_after is not None
                        and s3_etag_before != s3_etag_after)
                    else False
                ),
            }

            resp = {
                "status":    "ok",
                "file":      key,
                "ticker":    vatkr,
                "va_used":   bool(va_used),
                "va_rebuild": va_rebuild_status,  # "ok" | "skipped" | "error:..."
                "codes":     len(codes),
                "periods":   len(periods),
                "durations": durations,
            }

            # ---- log run finish (success) ----
            log_excel_refresh_run_finish(
                run_id,
                status="ok",
                va_used=bool(va_used),
                va_rebuild=str(va_rebuild_status),
                codes=len(codes),
                periods=len(periods),
                durations=durations,
                error_message=None,
            )
            log(f"[summary] {json.dumps(resp, ensure_ascii=False)}")
            return JSONResponse(resp, status_code=200)

    except HTTPException as e:
        # log as error, then re-raise
        try:
            if run_id is not None:
                log_excel_refresh_run_finish(
                    run_id,
                    status="error",
                    error_message=str(e.detail if hasattr(e, "detail") else e),
                )
        except Exception:
            pass
        raise
    except Exception as e:
        traceback.print_exc()
        try:
            if run_id is not None:
                log_excel_refresh_run_finish(
                    run_id,
                    status="error",
                    error_message=str(e),
                )
        except Exception:
            pass
        raise HTTPException(500, str(e))
    finally:
        # Always cleanup Excel + COM, regardless of success/failure
        try:
            if wb is not None:
                wb.close()
        except Exception:
            pass
        try:
            if app_excel is not None:
                app_excel.quit()
        except Exception:
            pass
        kill_excel()
        pythoncom.CoUninitialize()

# ---------- convenience: refresh by ticker (latest file on S3) ----------
class TickerRequest(BaseModel):
    ticker: str

def _find_latest_model_key(bucket: str, ticker: str) -> str:
    """
    Pick the MOST RECENT upload for a ticker.
    - Looks only under user_datasets/financial_models/
    - Matches filenames that start with '<TICKER>_' (case-insensitive)
    - Ignores temp files like '~$*.xlsx'
    - Uses paginator (handles >1,000 objects)
    - Sorts by S3 LastModified descending
    """
    import boto3, datetime as dt
    s3 = boto3.client("s3")
    prefix_root = "user_datasets/financial_models/"
    t_upper = ticker.upper().strip()

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix_root)

    candidates = []
    for page in pages:
        for obj in page.get("Contents", []) or []:
            key = obj["Key"]
            if not key.lower().endswith((".xlsx", ".xlsm")): 
                continue
            base = os.path.basename(key)
            if base.startswith("~$"):                       # skip temporary Excel locks
                continue
            # must start with "<TICKER>_"
            if not base.upper().startswith(f"{t_upper}_"):
                continue
            candidates.append((obj["LastModified"], key))

    if not candidates:
        raise HTTPException(404, f"No model for ticker={t_upper}")

    # Sort by S3's LastModified (already timezone-aware), newest first
    candidates.sort(key=lambda x: x[0], reverse=True)
    chosen = candidates[0][1]

    # helpful logging
    try:
        log("[s3] candidates (newest→oldest): " + " | ".join(
            f"{os.path.basename(k)}@{ts.strftime('%Y-%m-%d %H:%M:%S %Z')}" for ts, k in candidates[:5]
        ))
        log(f"[s3] chosen={chosen}")
    except Exception:
        pass
    return chosen

@app.post("/api/refresh/ticker")
def refresh_by_ticker(data: TickerRequest, request: Request):
    key = _find_latest_model_key(BUCKET, data.ticker)
    # This path is used by nightly ingest job → mark as nightly
    return refresh_excel_file(RefreshRequest(path=key), request, source="nightly_finmodels")
