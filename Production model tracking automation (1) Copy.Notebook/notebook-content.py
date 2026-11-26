# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Cell 1: Setup (imports + auth + workspace id)

import os
import datetime as dt

from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

FABRIC_API_ROOT = "https://api.fabric.microsoft.com/v1"

ctx = notebookutils.runtime.context
workspace_id = os.environ.get("WORKSPACE_ID")

if not workspace_id:
    try:
        workspace_id = ctx.get("currentWorkspaceId")
    except Exception:
        workspace_id = getattr(ctx, "currentWorkspaceId", None)

if not workspace_id:
    raise ValueError("Workspace ID not found. Set WORKSPACE_ID env var or run in Fabric workspace context.")

token = notebookutils.credentials.getToken("pbi")
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

print(f"workspace_id={workspace_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: Fetch all items from workspace (with pagination)

import requests

def api_get(path, params=None, timeout=60):
    url = f"{FABRIC_API_ROOT}{path}"
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"GET {url} failed: {r.status_code} {r.text}")
    return r.json() if r.text else {}

def list_workspace_items(ws_id):
    all_items = []
    continuation = None

    while True:
        params = {}
        if continuation:
            params["continuationToken"] = continuation

        payload = api_get(f"/workspaces/{ws_id}/items", params=params)
        all_items.extend(payload.get("value", []))

        continuation = payload.get("continuationToken")
        if not continuation:
            break

    return all_items

items = list_workspace_items(workspace_id)
print(f"items fetched: {len(items)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3: Keep only semantic models and display them

schema = StructType([
    StructField("item_id", StringType(), True),
    StructField("item_name", StringType(), True),
    StructField("item_type", StringType(), True),
    StructField("workspace_id", StringType(), True),
    StructField("fetched_utc", TimestampType(), True)
])

now_utc = dt.datetime.now(dt.timezone.utc)
semantic_model_name_filter = os.environ.get("SEMANTIC_MODEL_NAME_FILTER")

semantic_models = [i for i in items if i.get("type") == "SemanticModel"]
if semantic_model_name_filter:
    semantic_models = [
        i for i in semantic_models
        if semantic_model_name_filter.lower() in (i.get("displayName") or "").lower()
    ]
    print(f"semantic model name filter applied: {semantic_model_name_filter}")

print(f"semantic models found: {len(semantic_models)}")

rows = []
for i in semantic_models:
    rows.append((
        i.get("id"),
        i.get("displayName"),
        i.get("type"),
        workspace_id,
        now_utc
    ))

semantic_models_df = spark.createDataFrame(rows, schema=schema)

display(semantic_models_df.orderBy("item_name"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4: Get latest refresh status for each semantic model

import json
import datetime as dt
import requests
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType

def normalize_status(raw):
    if not raw:
        return "Unknown"
    s = str(raw).strip().lower()
    if s in {"succeeded", "success", "completed"}:
        return "Succeeded"
    if s in {"failed", "error"}:
        return "Failed"
    if s in {"cancelled", "canceled"}:
        return "Cancelled"
    if s in {"running", "inprogress", "in_progress", "queued", "notstarted", "unknown"}:
        return "Running"
    return raw

def extract_refresh_error(raw_error):
    if not raw_error:
        return None
    if isinstance(raw_error, dict):
        return raw_error.get("errorDescription") or raw_error.get("message") or str(raw_error)
    try:
        parsed = json.loads(raw_error)
    except Exception:
        return str(raw_error)[:1000]
    return parsed.get("errorDescription") or parsed.get("message") or str(parsed)[:1000]

def get_scheduled_refresh_enabled(item_id):
    schedule_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{item_id}/refreshSchedule"
    r = requests.get(schedule_url, headers=headers, timeout=60)

    if r.status_code == 404:
        return (False, "Refresh schedule not found")
    if r.status_code >= 400:
        return (False, f"Refresh schedule API failed: {r.status_code} {r.text[:300]}")

    payload = r.json() if r.text else {}
    return (bool(payload.get("enabled", False)), None)

def get_latest_semantic_model_refresh(item_id):
    pbi_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{item_id}/refreshes?$top=1"
    r = requests.get(pbi_url, headers=headers, timeout=60)

    if r.status_code == 404:
        return {
            "refresh_status": "NotSupported",
            "refresh_message": "Semantic model refresh history not found",
            "error_message": None,
            "start_time": None,
            "end_time": None
        }
    if r.status_code >= 400:
        return {
            "refresh_status": "Unknown",
            "refresh_message": f"PBI refresh API failed: {r.status_code} {r.text[:300]}",
            "error_message": None,
            "start_time": None,
            "end_time": None
        }

    payload = r.json() if r.text else {}
    runs = payload.get("value", [])
    if not runs:
        return {
            "refresh_status": "NoRuns",
            "refresh_message": "No refresh history found",
            "error_message": None,
            "start_time": None,
            "end_time": None
        }

    latest = runs[0]
    status_raw = latest.get("status")
    status = normalize_status(status_raw)
    request_id = latest.get("requestId")
    error_message = extract_refresh_error(latest.get("serviceExceptionJson"))
    return {
        "refresh_status": status,
        "refresh_message": f"requestId={request_id} status={status_raw}",
        "error_message": error_message,
        "start_time": latest.get("startTime"),
        "end_time": latest.get("endTime")
    }

status_rows = []
checked_utc = dt.datetime.now(dt.timezone.utc)

for i in semantic_models:
    item_id = i.get("id")
    model_name = i.get("displayName")

    try:
        is_scheduled, schedule_msg = get_scheduled_refresh_enabled(item_id)
        if not is_scheduled:
            status_rows.append((
                item_id,
                model_name,
                "SkippedNoSchedule",
                schedule_msg or "Scheduled refresh is not enabled",
                None,
                None,
                None,
                checked_utc,
                False
            ))
            continue

        refresh = get_latest_semantic_model_refresh(item_id)
        status_rows.append((
            item_id,
            model_name,
            refresh["refresh_status"],
            refresh["refresh_message"],
            refresh["error_message"],
            refresh["start_time"],
            refresh["end_time"],
            checked_utc,
            True
        ))
    except Exception as ex:
        status_rows.append((item_id, model_name, "Unknown", str(ex)[:500], None, None, None, checked_utc, None))

run_schema = StructType([
    StructField("item_id", StringType(), True),
    StructField("semantic_model_name", StringType(), True),
    StructField("refresh_status", StringType(), True),
    StructField("refresh_message", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("checked_utc", TimestampType(), True),
    StructField("scheduled_refresh_enabled", BooleanType(), True)
])

semantic_refresh_df = spark.createDataFrame(status_rows, schema=run_schema)
display(semantic_refresh_df.orderBy("semantic_model_name"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5: Build pipeline output for failed semantic model refreshes

import json
import html
import datetime as dt
from pyspark.sql import functions as F

MAX_MODELS_IN_ALERT = 20
MAX_ERROR_LENGTH = 100
SUCCESS_MESSAGE = "All models refreshed successfully"

display(
    semantic_refresh_df.groupBy("refresh_status", "scheduled_refresh_enabled")
    .agg(F.count("*").alias("count"))
    .orderBy("refresh_status", "scheduled_refresh_enabled")
)

failed_refresh_df = semantic_refresh_df.filter(
    (F.col("refresh_status") == "Failed") &
    (F.col("scheduled_refresh_enabled") == True)
)
failed_refreshes = failed_refresh_df.orderBy("semantic_model_name").collect()
failed_count = len(failed_refreshes)

display(failed_refresh_df.orderBy("semantic_model_name"))

def sanitize_table_value(value):
    if value is None:
        return ""
    return str(value).replace("\n", " ").replace("\r", " ").replace("|", "/").strip()

def truncate_error(value, max_len=MAX_ERROR_LENGTH):
    text = sanitize_table_value(value)
    if len(text) <= max_len:
        return text
    return text[:max_len] + ".."

def format_datetime_value(value):
    text = sanitize_table_value(value)
    if not text:
        return ""

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(normalized)
        return parsed.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return text

def get_refresh_date(row):
    return format_datetime_value(row.end_time or row.start_time or row.checked_utc)

def append_unique(parts, value):
    cleaned = sanitize_table_value(value)
    if cleaned and cleaned not in parts:
        parts.append(cleaned)

def extract_display_error(raw_error, fallback_message):
    if not raw_error:
        return truncate_error(fallback_message or "No error details returned")

    text = str(raw_error)
    try:
        parsed = json.loads(text)
    except Exception:
        return truncate_error(text)

    parts = []
    error_obj = parsed.get("error") or {}
    pbi_error = error_obj.get("pbi.error") or {}

    append_unique(parts, error_obj.get("code"))
    append_unique(parts, pbi_error.get("code"))

    parameters = pbi_error.get("parameters") or {}
    for key, value in parameters.items():
        append_unique(parts, f"{key}: {value}")

    details = pbi_error.get("details") or []
    for detail in details:
        detail_obj = detail or {}
        append_unique(parts, detail_obj.get("code"))
        detail_value = ((detail_obj.get("detail") or {}).get("value"))
        append_unique(parts, detail_value)

    append_unique(parts, pbi_error.get("exceptionCulprit"))
    append_unique(parts, fallback_message)

    if parts:
        return truncate_error("; ".join(parts))

    return truncate_error(text)

def build_message_text():
    if failed_count == 0:
        return SUCCESS_MESSAGE

    lines = [
        "Below data models are failing in production:",
        "",
        "Data model | Refresh Date | Error Message",
        "--- | --- | ---"
    ]

    for row in failed_refreshes[:MAX_MODELS_IN_ALERT]:
        error_text = extract_display_error(row.error_message, row.refresh_message)
        lines.append(
            f"{sanitize_table_value(row.semantic_model_name)} | {get_refresh_date(row)} | {error_text}"
        )

    if failed_count > MAX_MODELS_IN_ALERT:
        remaining = failed_count - MAX_MODELS_IN_ALERT
        lines.append(f"Additional failed models not shown | {remaining} more | Output truncated")

    return "\n".join(lines)

def build_message_html():
    if failed_count == 0:
        return f"<div>{html.escape(SUCCESS_MESSAGE)}</div>"

    rows_html = []
    for row in failed_refreshes[:MAX_MODELS_IN_ALERT]:
        error_text = extract_display_error(row.error_message, row.refresh_message)
        rows_html.append(
            "<tr>"
            f"<td>{html.escape(sanitize_table_value(row.semantic_model_name))}</td>"
            f"<td>{html.escape(get_refresh_date(row))}</td>"
            f"<td>{html.escape(error_text)}</td>"
            "</tr>"
        )

    if failed_count > MAX_MODELS_IN_ALERT:
        remaining = failed_count - MAX_MODELS_IN_ALERT
        rows_html.append(
            "<tr>"
            "<td>Additional failed models not shown</td>"
            f"<td>{remaining} more</td>"
            "<td>Output truncated</td>"
            "</tr>"
        )

    return (
        "<div>Below data models are failing in production:</div>"
        "<br/>"
        "<table border='1' style='border-collapse:collapse;'>"
        "<thead><tr><th>Data model</th><th>Refresh Date</th><th>Error Message</th></tr></thead>"
        f"<tbody>{''.join(rows_html)}</tbody>"
        "</table>"
    )

message_text = build_message_text()
message_html = build_message_html()

pipeline_output = {
    "shouldAlert": failed_count > 0,
    "failedCount": failed_count,
    "workspaceId": workspace_id,
    "message": message_text,
    "messageHtml": message_html,
    "failures": [
        {
            "itemId": row.item_id,
            "semanticModelName": row.semantic_model_name,
            "refreshStatus": row.refresh_status,
            "refreshMessage": row.refresh_message,
            "errorMessage": extract_display_error(row.error_message, row.refresh_message),
            "startTime": row.start_time,
            "endTime": row.end_time,
            "checkedUtc": str(row.checked_utc),
            "scheduledRefreshEnabled": row.scheduled_refresh_enabled
        }
        for row in failed_refreshes[:MAX_MODELS_IN_ALERT]
    ]
}

pipeline_output_json = json.dumps(pipeline_output)
print(message_text)
print(message_html)
print(pipeline_output_json)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6: Return the pipeline output as notebook exit value

if "mssparkutils" in globals():
    mssparkutils.notebook.exit(pipeline_output_json)
else:
    notebookutils.notebook.exit(pipeline_output_json)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
