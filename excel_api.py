from fastapi import FastAPI, APIRouter, HTTPException
from pydantic import BaseModel
import xlwings as xw
import boto3
import os
from datetime import datetime
import json
import subprocess
import traceback

from fastapi.middleware.cors import CORSMiddleware
from dateutil.parser import parse

s3 = boto3.client('s3')
TEMP_DIR = "C:\\Temp"

router = APIRouter()
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ExcelUpdateRequest(BaseModel):
    model_key: str
    task: str
    sheet: str
    anchor_cell: str
    metric_row_header: str
    period_column_header: str
    value: float
@router.post("/excel/update")
def update_excel_from_s3(data: ExcelUpdateRequest):
    try:
        model_key = data.model_key
        task = data.task
        sheet_name = data.sheet
        anchor_cell = data.anchor_cell
        metric_row_header = data.metric_row_header.strip()
        period_column_header = data.period_column_header.strip()
        update_value = data.value

        original_model = os.path.splitext(os.path.basename(model_key))[0]
        output_filename = f"{original_model}_{task}.xlsx"
        output_key = f"models/base/{output_filename}"

        input_path = os.path.join(TEMP_DIR, "input.xlsx")
        output_path = os.path.join(TEMP_DIR, "output.xlsx")
        if os.path.exists(input_path):
            try:
                os.remove(input_path)
                print("🧹 Deleted stale input.xlsx before download")
            except Exception as e:
                print("⚠️ Failed to delete input.xlsx:", e)
                raise HTTPException(status_code=500, detail=f"Could not delete old input file: {e}")

        print("📥 Downloading model:", model_key)
        s3.download_file("agentic-data", model_key, input_path)

        app_excel = xw.App(visible=False, add_book=False)

        try:
            wb = app_excel.books.open(input_path)
            sheet_map = {
                "Sheet1": 0,
                "Sheet2": 1,
                "Sheet3": 2,
            }

            if sheet_name in sheet_map:
                try:
                    ws = wb.sheets[sheet_map[sheet_name]]
                    print(f"✅ Fallback to sheet by index: {sheet_map[sheet_name]}")
                except IndexError:
                    raise HTTPException(status_code=400, detail=f"Sheet index {sheet_map[sheet_name]} not found in workbook.")
            else:
                ws = wb.sheets[sheet_name]

            col_letter = ''.join([c for c in anchor_cell if c.isalpha()])
            row_number = int(''.join([c for c in anchor_cell if c.isdigit()]))
            start_row = row_number
            start_col = col_letter_to_index(col_letter)

            print(f"🔎 Anchor at row {start_row}, col {start_col}")

            metric_candidates = ws.range((start_row + 1, start_col)).expand('down').value
            if not isinstance(metric_candidates, list):
                metric_candidates = [metric_candidates]
            metric_candidates = [str(m).strip() for m in metric_candidates if m]

            print("🧪 Raw metric header column (↓):", metric_candidates)

            def normalize_label(s):
                s = str(s).strip().replace("T", " ").replace(":00", "")
                try:
                    dt = parse(s, fuzzy=True, dayfirst=False)
                    return dt.strftime("%Y-%m-%d")
                except Exception:
                    pass
                try:
                    serial = float(s)
                    if serial > 59:
                        serial -= 1
                    base_date = datetime(1899, 12, 31)
                    dt = base_date + timedelta(days=serial)
                    return dt.strftime("%Y-%m-%d")
                except Exception:
                    return s

            metric_candidates_normalized = [normalize_label(m) for m in metric_candidates]
            normalized_metric = normalize_label(metric_row_header)

            try:
                metric_idx = metric_candidates_normalized.index(normalized_metric)
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Metric row '{metric_row_header}' not found below {anchor_cell}."
                )

            # ✅ NEW: Fetch full period row, preserving blanks
            period_range = ws.range((start_row, 1), (start_row, start_col + 40)).value
            if not isinstance(period_range, list):
                period_range = [period_range]
            period_candidates = [str(p).strip() if p else "" for p in period_range]
            period_candidates_normalized = [normalize_label(p) for p in period_candidates]
            normalized_period = normalize_label(period_column_header)

            try:
                period_col_index = period_candidates_normalized.index(normalized_period)
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Period column '{period_column_header}' not found in row {start_row}."
                )

            target_row = start_row + 1 + metric_idx
            target_col = period_col_index + 1  # absolute column position
            target_cell = index_to_cell_address(target_row, target_col)

            print(f"📌 Matched metric index: {metric_idx}, period index: {period_col_index}")
            print(f"🧮 Calculated cell → Row {target_row}, Col {target_col}")
            print(f"🎯 Targeting cell {target_cell} with value {update_value}")

            ws.range(target_cell).value = update_value
            wb.app.calculate()
            wb.save(output_path)

        finally:
            try:
                wb.close()
            except Exception as e:
                print("⚠️ Failed to close workbook:", e)
            try:
                app_excel.quit()
            except Exception as e:
                print("⚠️ Failed to quit Excel app cleanly:", e)
                try:
                    subprocess.run(["taskkill", "/f", "/im", "EXCEL.EXE"], check=True)
                except Exception as kill_err:
                    print("💥 Failed to kill Excel.exe:", kill_err)

        s3.upload_file(output_path, "agentic-data", output_key)

        audit = {
            "timestamp": datetime.utcnow().isoformat(),
            "model_key": model_key,
            "output_key": output_key,
            "sheet": sheet_name,
            "anchor_cell": anchor_cell,
            "metric_row_header": metric_row_header,
            "period_column_header": period_column_header,
            "target_cell": target_cell,
            "value": update_value,
            "task": task,
            "updated_by": "llm_excel_api",
        }

        audit_path = output_path.replace(".xlsx", ".audit.json")
        with open(audit_path, "w") as f:
            json.dump(audit, f, indent=2)
        audit_key = audit_path.split("/")[-1]

        s3.upload_file(audit_path, "agentic-data", audit_key)

        s3_url = s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": "agentic-data", "Key": output_key},
            ExpiresIn=3600
        )

        return {
            "status": "success",
            "output_key": s3_url,
            "audit_key": audit_key,
            "target_cell": target_cell,
        }

    except Exception as e:
        print("❌ Unhandled error:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))



@router.get("/healthcheck")
def healthcheck():
    return {"status": "ok"}

app.include_router(router, prefix="/api")

def index_to_cell_address(row: int, col: int) -> str:
    col_str = ""
    while col > 0:
        col, remainder = divmod(col - 1, 26)
        col_str = chr(65 + remainder) + col_str
    return f"{col_str}{row}"

def col_letter_to_index(letter: str) -> int:
    index = 0
    for c in letter.upper():
        index = index * 26 + (ord(c) - ord('A') + 1)
    return index
