# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Workspace semantic model data availability
# 
# Discover every semantic model visible to the notebook identity in one workspace and check whether each included table contains at least one row. Skip models with a Google Sheets data source. Exclude tables whose names contain `measure`, configured measure-only tables, and automatic local date tables. Display results only in the final summary, including the names of empty tables and their semantic models.
# 
# ## Run in Microsoft Fabric
# 1. Import this `.ipynb` into a Fabric workspace and select a supported **PySpark (Python)** runtime with Semantic Link installed. No Lakehouse attachment is required.
# 2. Leave `WORKSPACE_ID` empty to automatically use the notebook's current Fabric workspace from `notebookutils.runtime.context`. This does not use an attached Lakehouse's workspace. To check a different workspace, optionally set `WORKSPACE_ID` to its GUID.
# 3. Run all cells. Authentication uses the notebook execution identity; there are no embedded credentials.
# 4. Run through the final summary cell to see overall status, model counts, table totals, per-model summaries, and empty-table names with their model names/IDs. Query errors are also listed there. No progress tables or per-check output are emitted during scanning; results remain available as `table_results_df`, `model_summary_df`, `workspace_summary_df`, `empty_tables_df`, and `error_tables_df`.
# 5. For a scheduled Fabric pipeline, mark the configuration cell as a **parameter cell** if it is not recognized on import and set `FAIL_ON_ISSUES = True` to fail the run after displaying results if any table is empty or any check fails. `WORKSPACE_ID` is optional; omit it or leave it empty for automatic detection. Confirm the scheduled identity has the same access as the interactive identity.
# 6. The resolved workspace GUID appears in the final summary. If runtime context is unavailable or its workspace ID is invalid, the notebook stops with an error rather than guessing another workspace; supply an explicit `WORKSPACE_ID` if needed.
# 7. Any table whose name contains `measure` is excluded case-insensitively (for example, `Measures`, `Sales Measures`, or `MeasurementData`). `MEASURE_ONLY_TABLE_NAMES` defaults to `["Measures"]`; add exact names of additional measure-only tables, such as `KPI Storage`, to this list. Tables named `LocalDateTable` or starting with `LocalDateTable_` are also excluded. All matching is case-insensitive and name-based, not inferred from the presence of measures or hidden columns.
# 8. Performance defaults: `TABLES_PER_QUERY = 25` combines up to 25 table-existence checks into one DAX request; `MAX_PARALLEL_MODELS = 4` checks up to four models concurrently. Batches within each model stay sequential, so concurrent model API calls are bounded. Reduce concurrency to 1 if capacity is throttled; use `TABLES_PER_QUERY = 1` to isolate table-level failures. Supported ranges are 1-100 tables per query and 1-8 parallel models.
# 9. Before table discovery, one Power BI REST metadata request checks each model's direct data sources. Any Google Sheets connector or recognized Sheets URL skips the entire model, including models with mixed sources. Skipped models appear in the final summary with a reason, no table discovery, and zero DAX requests. Their table counts remain unknown rather than being reported as zero discovered tables.
# 10. Displayed `Error` values are limited to 100 characters, including `...` when truncated. Full error text is retained in `model_summary_df`, `table_results_df`, and `error_tables_df` for troubleshooting.
# 11. `ANALYZE_REPORT_USAGE` and `INCLUDE_SOURCE_QUERIES` default to `True`. Install `semantic-link-labs` in the Fabric Environment (or run `%pip install semantic-link-labs` in a separate setup cell and restart if required). These optional metadata analyses run after availability checks; set either flag to `False` to reduce execution time. They do not execute source queries or refresh/change models.
# 12. Final output includes `table_usage_df` (availability plus scoped reference status), `report_usage_df` (report/page/visual/filter references), `source_queries_df` (one row per table partition), `shared_expressions_df` (supporting model expressions), and `analysis_errors_df`. Report discovery is restricted to accessible reports in the current target workspace. Each report definition is loaded once; model metadata/dependencies are fetched once per eligible model. Google Sheets models and excluded tables are not analyzed, but measures stored in excluded measure tables can be traversed as dependencies.
# 13. Report analysis requires accessible PBIR definitions and uses Semantic Link Labs in read-only mode. `REFERENCED` means a static direct or dependency reference was found. `NO_REFERENCE_FOUND` applies only to successfully inspected accessible reports in this workspace; it does not mean unused. `UNKNOWN` means coverage is incomplete. Positive references are retained with `PARTIAL` coverage if another report/dependency fails. Relationship propagation, dynamic field parameters, calculation groups, visual calculations, upstream composite models, and runtime-generated queries are not fully resolved. This is not visual execution or a complete lineage proof.
# 14. Source queries are the stored Power Query M, SQL/provider query, or calculated-table DAX for each partition, not the notebook's availability DAX or a visual's generated query. Direct Lake/entity partitions expose source descriptors rather than a fabricated SQL query. Shared expressions/parameters are listed separately, not expanded; incremental-refresh partitions can have different queries. Source text may contain sensitive endpoints or literals: restrict notebook output access and do not embed credentials in model queries.
# 15. Metadata/report errors are shown separately and never convert availability results to PASS/EMPTY. `FAIL_ON_ISSUES` continues to gate availability only. Analysis seconds and errors are included in the final summary; source-query metadata and report-definition retrieval add runtime. Displayed error values remain capped at 100 characters; query text is not truncated.
# 
# ## Prerequisites and boundaries
# - Use a Fabric/Premium capacity supporting XMLA access, with XMLA read access enabled. The current SemPy documentation requires **ReadWrite model permissions** for TOM-based `list_tables`; Contributor/Member/Admin access is a typical setup. This notebook only performs read operations and never refreshes or changes a model.
# - The data-source REST endpoint also requires Write permissions on the model. Source-metadata access failures or malformed responses are recorded as model `ERROR` and no data queries are sent to that model. Google Sheets exclusions are identified from connector kind/type or URLs under `docs.google.com/spreadsheets`, `sheets.google.com`, or `sheets.googleapis.com`; model/table names are not used. This does not trace upstream dataflows, Lakehouses, or hidden/dynamic URLs not exposed in direct source metadata.
# - Semantic Link is included in Fabric Runtime 1.2 and newer. If the import fails or your installed version does not support `include_internal`, install/update `semantic-link-sempy` through your Fabric Environment or run `%pip install -U semantic-link-sempy` in a separate setup cell, restart the session if prompted, then rerun.
# - Discovery is limited to models and metadata visible to the execution identity. For workspace-wide coverage, grant that identity access to every intended model. Unsupported or inaccessible discovered models are recorded as `ERROR`, not silently skipped.
# - `PASS` means the table has at least one visible row; `EMPTY` means no rows; `ERROR` means availability could not be determined; `SKIPPED` means an excluded table was not queried. Errors never become empty results. A model with no discoverable tables is an error; a model containing only excluded tables is `SKIPPED`, not `PASS`.
# - Skipped tables are retained with a reason in `table_results_df` and counted separately in the final summary; they do not affect PASS/EMPTY/ERROR counts or fail a scheduled run. All other discovered tables, including hidden/internal tables and calculation groups, are attempted. Add any additional measure-only table names to the exclusion list.
# - Overall status is `ERROR` if any model has an error, otherwise `EMPTY` if any checked table is empty, otherwise `PASS` if at least one table passed, or `SKIPPED` if no eligible tables were checked. Discovery errors are counted separately from table-query errors; discovered-table totals cover only returned metadata.
# - This is not a freshness, report-filter, RLS-consumer, or visual-rendering test. It uses the execution identity's context without report filters; elevated workspace roles can bypass consumer RLS.
# - DAX uses `ISEMPTY` for each included table, returning only a table index and availability flag per table, not business records or exact row counts. Batching reduces connection/request overhead; it does not eliminate underlying source queries. DirectQuery/Direct Lake queries can still access data sources and consume capacity.
# - A failed or malformed batch marks every table in that batch `ERROR`, with the shared failure reason. It is not retried or split into individual queries, and no table in a failed batch is reported as `PASS` or `EMPTY`. Other batches and models continue. Set `TABLES_PER_QUERY = 1` for a diagnostic rerun if individual error isolation is needed.
# - The final summary includes wall-clock scan seconds, DAX request counts, and per-model discovery/query/total timings. `Batch Elapsed ms` is the shared client-observed time for a batch, repeated on its table rows; do not sum it as per-table execution time. Timings are **not Performance Analyzer timing**. No cache clearing or automatic retry is performed by this notebook.
# - Runtime in minutes cannot be guaranteed: capacity load, workspace size, authentication, model metadata retrieval, and slow source queries still matter. SemPy `evaluate_dax` has no public per-call timeout parameter; this notebook does not pretend to cancel running calls with a Python thread timeout. Service-side timeouts still apply. Scan seconds excludes Spark session startup and final summary rendering.
# - Results are displayed in notebook output and retained in memory for the current session. This notebook does not write history to a Lakehouse or send alerts.
# 
# References: [SemPy API](https://learn.microsoft.com/en-us/python/api/semantic-link-sempy/sempy.fabric?view=semantic-link-python), [Semantic Link connectivity](https://learn.microsoft.com/en-us/fabric/data-science/semantic-link-power-bi), [NotebookUtils runtime context](https://learn.microsoft.com/en-us/fabric/data-engineering/notebookutils/notebookutils-runtime), [Get data sources in group](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-datasources-in-group).
# 


# CELL ********************

%pip install semantic-link-labs


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

WORKSPACE_ID = ""  # Automatically use the current notebook workspace; optionally override with a GUID.
FAIL_ON_ISSUES = False
MEASURE_ONLY_TABLE_NAMES = ["Measures"]
TABLES_PER_QUERY = 25
MAX_PARALLEL_MODELS = 4
ANALYZE_REPORT_USAGE = True
INCLUDE_SOURCE_QUERIES = True


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from numbers import Integral
from time import perf_counter
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import pandas as pd
import sempy.fabric as fabric
from IPython.display import HTML, display

logger = logging.getLogger("semantic_model_data_check")

TABLE_COLUMNS = [
    "Run ID", "Workspace ID", "Model ID", "Model Name", "Table Name",
    "Has Data", "Status", "Skip Reason", "Batch Elapsed ms", "Checked At UTC", "Error",
]
SUMMARY_COLUMNS = [
    "Run ID", "Workspace ID", "Model ID", "Model Name", "Status", "Skip Reason",
    "Tables Discovered", "Tables Attempted", "SKIPPED", "PASS", "EMPTY", "ERROR",
    "DAX Requests", "Discovery ms", "Query ms", "Elapsed ms", "Checked At UTC", "Error",
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def resolve_workspace_id(workspace_id):
    if workspace_id is not None and not isinstance(workspace_id, str):
        raise ValueError("WORKSPACE_ID must be a GUID string, empty, or None for automatic detection.")
    workspace_id = workspace_id.strip() if workspace_id is not None else ""
    source = "WORKSPACE_ID override"
    if not workspace_id:
        import notebookutils

        workspace_id = notebookutils.runtime.context.get("currentWorkspaceId")
        source = "Fabric runtime currentWorkspaceId"
    if not isinstance(workspace_id, str) or not workspace_id.strip():
        raise ValueError(f"{source} is missing or empty. Set WORKSPACE_ID to a workspace GUID.")
    try:
        resolved_id = str(UUID(workspace_id.strip()))
    except ValueError as exc:
        raise ValueError(f"{source} is not a valid workspace GUID. Set WORKSPACE_ID to a workspace GUID.") from exc
    return resolved_id


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def find_column(frame, aliases):
    # REST and XMLA metadata labels can differ across SemPy versions.
    normalized = {alias.casefold().replace(" ", "").replace("_", "") for alias in aliases}
    matches = [
        column for column in frame.columns
        if str(column).casefold().replace(" ", "").replace("_", "") in normalized
    ]
    if len(matches) != 1:
        raise ValueError(
            f"Expected one column matching {aliases}; found {matches}. "
            f"Available columns: {list(frame.columns)}"
        )
    return matches[0]


def require_names(values, label):
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise ValueError(f"{label} contains a missing, blank, or non-string name.")


def availability_query(table_names):
    rows = []
    for index, table_name in enumerate(table_names):
        quoted_table = "'" + table_name.replace("'", "''") + "'"
        rows.append(f'ROW("TableIndex", {index}, "HasData", IF(ISEMPTY({quoted_table}), 0, 1))')
    return "EVALUATE " + (rows[0] if len(rows) == 1 else "UNION(\n" + ",\n".join(rows) + "\n)")


def read_has_data(result, expected_count):
    if result.shape != (expected_count, 2):
        raise ValueError(f"Expected {expected_count} availability rows and two columns; received shape {result.shape}.")
    index_column = find_column(result, ["TableIndex", "[TableIndex]"])
    value_column = find_column(result, ["HasData", "[HasData]"])
    flags = {}
    for index, value in result[[index_column, value_column]].itertuples(index=False, name=None):
        if isinstance(index, bool) or not isinstance(index, Integral) or not 0 <= index < expected_count or index in flags:
            raise ValueError(f"Unexpected or duplicate table index: {index!r}.")
        if pd.isna(value) or value not in (0, 1):
            raise ValueError(f"Expected availability 0 or 1; received {value!r}.")
        flags[index] = bool(value)
    return flags


def short_error(message):
    return message if len(message) <= 100 else message[:97] + "..."


def display_frame(frame):
    if "Error" in frame.columns:
        frame = frame.copy()
        frame["Error"] = frame["Error"].map(short_error)
    # Render every row, escaping names/errors so model metadata is never HTML.
    display(HTML(frame.to_html(index=False, escape=True)))


def is_google_sheets_source(source):
    if not isinstance(source, dict) or not isinstance(source.get("datasourceType"), str):
        raise ValueError("Data source metadata is missing a valid datasourceType.")
    details = source.get("connectionDetails", {})
    if not isinstance(details, dict):
        raise ValueError("Data source connectionDetails must be an object.")
    for kind in (source["datasourceType"], details.get("kind"), details.get("extensionDataSourceKind")):
        if kind is not None:
            if not isinstance(kind, str):
                raise ValueError("Data source connector kind must be a string.")
            if "".join(char for char in kind.casefold() if char.isalnum()) == "googlesheets":
                return True
    for key in ("url", "path", "extensionDataSourcePath"):
        value = details.get(key)
        if value is None:
            continue
        if not isinstance(value, str):
            raise ValueError(f"Data source {key} must be a string.")
        url = urlsplit(value.strip())
        if url.scheme.casefold() not in ("http", "https"):
            continue
        host = (url.hostname or "").casefold()
        path = url.path.casefold()
        if host in ("sheets.google.com", "sheets.googleapis.com"):
            return True
        if host == "docs.google.com" and (path == "/spreadsheets" or path.startswith("/spreadsheets/")):
            return True
    return False


def model_uses_google_sheets(workspace_id, model_id):
    client = fabric.PowerBIRestClient(retry_config={"total": 0})
    response = client.get(f"/v1.0/myorg/groups/{workspace_id}/datasets/{model_id}/datasources", timeout=30)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or not isinstance(payload.get("value"), list):
        raise ValueError("Data source discovery must return an object containing a value list.")
    return any(is_google_sheets_source(source) for source in payload["value"])


def table_skip_reason(table_name, measure_only_names):
    normalized = table_name.casefold()
    if "measure" in normalized:
        return "Table name contains 'measure'"
    if normalized in measure_only_names:
        return "Configured measure-only table"
    if normalized == "localdatetable" or normalized.startswith("localdatetable_"):
        return "Automatic local date table"
    return ""


def overall_status(pass_count, empty_count, error_count):
    return "ERROR" if error_count else "EMPTY" if empty_count else "PASS" if pass_count else "SKIPPED"


def summarize_workspace(model_summary, scan_seconds):
    model_counts = model_summary["Status"].value_counts()
    return pd.DataFrame([{
        "Run ID": model_summary.iloc[0]["Run ID"],
        "Workspace ID": model_summary.iloc[0]["Workspace ID"],
        "Status": overall_status(model_counts.get("PASS", 0), model_counts.get("EMPTY", 0), model_counts.get("ERROR", 0)),
        "Models Discovered": len(model_summary),
        "Models PASS": int(model_counts.get("PASS", 0)),
        "Models EMPTY": int(model_counts.get("EMPTY", 0)),
        "Models ERROR": int(model_counts.get("ERROR", 0)),
        "Models SKIPPED": int(model_counts.get("SKIPPED", 0)),
        "Google Sheets Models SKIPPED": int(model_summary["Skip Reason"].eq("Uses Google Sheets").sum()),
        "Models With Discovery Errors": int(model_summary["Error"].ne("").sum()),
        "Tables Discovered (known)": int(model_summary["Tables Discovered"].sum()),
        "Tables Attempted": int(model_summary["Tables Attempted"].sum()),
        "Tables SKIPPED": int(model_summary["SKIPPED"].sum()),
        "Tables PASS": int(model_summary["PASS"].sum()),
        "Tables EMPTY": int(model_summary["EMPTY"].sum()),
        "Tables ERROR": int(model_summary["ERROR"].sum()),
        "DAX Requests": int(model_summary["DAX Requests"].sum()),
        "Scan seconds": round(scan_seconds, 2),
        "Checked At UTC": utc_now(),
    }])


def scan_workspace(workspace_id, measure_only_table_names, tables_per_query=25, max_parallel_models=4):
    if not isinstance(measure_only_table_names, list):
        raise ValueError("MEASURE_ONLY_TABLE_NAMES must be a list of table names.")
    require_names(measure_only_table_names, "MEASURE_ONLY_TABLE_NAMES")
    for name, value, limit in (("TABLES_PER_QUERY", tables_per_query, 1000), ("MAX_PARALLEL_MODELS", max_parallel_models, 8)):
        if type(value) is not int or not 1 <= value <= limit:
            raise ValueError(f"{name} must be an integer from 1 to {limit}.")
    measure_only_names = {name.casefold() for name in measure_only_table_names}
    workspace_id = str(UUID(workspace_id))
    run_id = str(uuid4())
    table_rows = []
    summaries = []

    # Workspace discovery failures abort instead of returning a success-shaped empty scan.
    models = fabric.list_datasets(workspace=workspace_id, mode="rest")
    if models.empty:
        raise RuntimeError(
            "No semantic models were returned. Verify the workspace ID, identity access, "
            "and that the workspace contains semantic models. Nothing was checked."
        )
    id_column = find_column(models, ["Dataset ID", "ID", "Semantic Model ID"])
    name_column = find_column(models, ["Dataset Name", "Name", "Display Name", "Semantic Model Name"])
    model_records = models[[id_column, name_column]].copy()
    model_records[id_column] = model_records[id_column].map(lambda value: str(UUID(str(value))))
    require_names(model_records[name_column].tolist(), "Model discovery")
    if model_records[id_column].duplicated().any():
        raise ValueError("Model discovery returned duplicate model IDs; review the API response.")
    model_records = model_records.sort_values([name_column, id_column])

    def scan_model(model_record):
        model_id, model_name = model_record
        model_start = perf_counter()
        summary = {
            "Run ID": run_id, "Workspace ID": workspace_id,
            "Model ID": model_id, "Model Name": model_name,
            "Status": "ERROR", "Tables Discovered": None, "Tables Attempted": 0,
            "Skip Reason": "",
            "SKIPPED": 0, "PASS": 0, "EMPTY": 0, "ERROR": 0, "Checked At UTC": utc_now(), "Error": "",
            "DAX Requests": 0, "Discovery ms": 0.0, "Query ms": 0.0, "Elapsed ms": 0.0,
        }
        discovery_start = perf_counter()
        discovery_stage = "Data source discovery"
        try:
            if model_uses_google_sheets(workspace_id, model_id):
                summary.update({"Status": "SKIPPED", "Skip Reason": "Uses Google Sheets", "Checked At UTC": utc_now()})
                summary["Discovery ms"] = round((perf_counter() - discovery_start) * 1000, 2)
                summary["Elapsed ms"] = round((perf_counter() - model_start) * 1000, 2)
                return summary, []
            discovery_stage = "Table discovery"
            tables = fabric.list_tables(
                dataset=model_id, workspace=workspace_id,
                extended=False, include_internal=True,
            )
            if tables.empty:
                summary["Tables Discovered"] = 0
                raise ValueError("No tables were discovered; no table availability checks were run.")
            table_column = find_column(tables, ["Table Name", "Name"])
            table_names = tables[table_column].tolist()
            require_names(table_names, "Table discovery")
            if len(set(table_names)) != len(table_names):
                raise ValueError("Table discovery returned duplicate table names.")
            summary["Tables Discovered"] = len(table_names)
        except Exception as exc:
            # Isolate model-level API/metadata failures, report them, and continue the inventory.
            summary["Error"] = f"{discovery_stage} failed: {type(exc).__name__}: {exc}"
            summary["Checked At UTC"] = utc_now()
            summary["Discovery ms"] = round((perf_counter() - discovery_start) * 1000, 2)
            summary["Elapsed ms"] = round((perf_counter() - model_start) * 1000, 2)
            return summary, []
        summary["Discovery ms"] = round((perf_counter() - discovery_start) * 1000, 2)

        model_rows = []
        for table_name in sorted(table_names, key=str.casefold):
            row = {
                "Run ID": run_id, "Workspace ID": workspace_id,
                "Model ID": model_id, "Model Name": model_name, "Table Name": table_name,
                "Has Data": None, "Status": "ERROR", "Skip Reason": "", "Batch Elapsed ms": None,
                "Checked At UTC": "", "Error": "",
            }
            skip_reason = table_skip_reason(table_name, measure_only_names)
            if skip_reason:
                row.update({"Status": "SKIPPED", "Skip Reason": skip_reason, "Checked At UTC": utc_now()})
            model_rows.append(row)

        eligible_rows = [row for row in model_rows if row["Status"] != "SKIPPED"]
        for offset in range(0, len(eligible_rows), tables_per_query):
            batch = eligible_rows[offset:offset + tables_per_query]
            start = perf_counter()
            summary["DAX Requests"] += 1
            try:
                result = fabric.evaluate_dax(
                    dataset=model_id, workspace=workspace_id,
                    dax_string=availability_query([row["Table Name"] for row in batch]),
                    verbose=0,
                )
                flags = read_has_data(result, len(batch))
                for index, row in enumerate(batch):
                    row["Has Data"] = flags[index]
                    row["Status"] = "PASS" if flags[index] else "EMPTY"
            except Exception as exc:
                # A failed batch cannot identify which individual table caused the failure.
                for row in batch:
                    row["Error"] = f"Batch of {len(batch)} table(s) failed; availability unknown: {type(exc).__name__}: {exc}"
            elapsed_ms = (perf_counter() - start) * 1000
            summary["Query ms"] += elapsed_ms
            checked_at = utc_now()
            for row in batch:
                row["Batch Elapsed ms"] = round(elapsed_ms, 2)
                row["Checked At UTC"] = checked_at

        for row in model_rows:
            summary[row["Status"]] += 1

        summary["Tables Attempted"] = summary["PASS"] + summary["EMPTY"] + summary["ERROR"]
        summary["Status"] = overall_status(summary["PASS"], summary["EMPTY"], summary["ERROR"])
        if summary["Status"] == "SKIPPED":
            summary["Skip Reason"] = "All tables excluded"
        summary["Checked At UTC"] = utc_now()
        summary["Query ms"] = round(summary["Query ms"], 2)
        summary["Elapsed ms"] = round((perf_counter() - model_start) * 1000, 2)
        return summary, model_rows

    # Workers own their metadata/results; only the main thread combines them, in model order.
    with ThreadPoolExecutor(max_workers=min(max_parallel_models, len(model_records))) as executor:
        for summary, model_rows in executor.map(scan_model, model_records.itertuples(index=False, name=None)):
            summaries.append(summary)
            table_rows.extend(model_rows)

    table_results = pd.DataFrame(table_rows, columns=TABLE_COLUMNS)
    table_results["Has Data"] = table_results["Has Data"].astype("boolean")
    model_summary = pd.DataFrame(summaries, columns=SUMMARY_COLUMNS)
    model_summary["Tables Discovered"] = model_summary["Tables Discovered"].astype("Int64")
    return model_summary, table_results


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

USAGE_COLUMNS = ["Model ID", "Model Name", "Table Name", "Report ID", "Report Name", "Page", "Visual", "Scope", "Usage", "Object Table", "Object Name", "Object Type"]
SOURCE_COLUMNS = ["Model ID", "Model Name", "Table Name", "Partition", "Source Type", "Source Query", "Source Details", "Status", "Error"]
EXPRESSION_COLUMNS = ["Model ID", "Model Name", "Expression Name", "Kind", "Expression"]
ANALYSIS_ERROR_COLUMNS = ["Model ID", "Report ID", "Report Name", "Stage", "Error"]
DEPENDENCY_QUERY = "SELECT [TABLE], [OBJECT], [OBJECT_TYPE], [REFERENCED_TABLE], [REFERENCED_OBJECT], [REFERENCED_OBJECT_TYPE] FROM $SYSTEM.DISCOVER_CALC_DEPENDENCY"


def dependency_graph(frame):
    columns = [find_column(frame, [name, f"[{name}]"]) for name in (
        "TABLE", "OBJECT", "OBJECT_TYPE", "REFERENCED_TABLE", "REFERENCED_OBJECT", "REFERENCED_OBJECT_TYPE"
    )]
    graph = {}
    for table, obj, kind, ref_table, ref_obj, ref_kind in frame[columns].itertuples(index=False, name=None):
        require_names([table, obj, kind, ref_kind], "Dependency metadata")
        ref_table = ref_table if isinstance(ref_table, str) else ""
        ref_obj = ref_obj if isinstance(ref_obj, str) else ""
        key = (table.casefold(), obj.casefold(), dependency_kind(kind))
        graph.setdefault(key, set()).add((ref_table.casefold(), ref_obj.casefold(), dependency_kind(ref_kind)))
    return graph


def dependency_kind(kind):
    return {"calc_column": "column", "calc_table": "table"}.get(kind.casefold(), kind.casefold())


def referenced_tables(table, obj, kind, graph):
    pending = [(table.casefold(), obj.casefold(), dependency_kind(kind))]
    visited, tables = set(), set()
    complete = True
    while pending:
        key = pending.pop()
        if key in visited:
            continue
        visited.add(key)
        table_name, _, object_type = key
        if object_type in ("column", "table", "hierarchy", "calc_column", "calc_table") and table_name:
            tables.add(table_name)
        elif object_type != "measure":
            complete = False
        if key in graph:
            pending.extend(graph[key])
        elif object_type in ("measure", "calc_column", "calc_table"):
            complete = False
    return tables, complete


def report_objects(wrapper):
    rows = []
    for scope, method in (("Visual", wrapper.list_visual_objects), ("Visual Filter", wrapper.list_visual_filters),
                          ("Page Filter", wrapper.list_page_filters), ("Report Filter", wrapper.list_report_filters)):
        frame = method()
        required = ["Table Name", "Object Name", "Object Type"]
        if scope != "Report Filter":
            required.append("Page Display Name")
        if scope in ("Visual", "Visual Filter"):
            required.append("Visual Name")
        for column in required:
            if column not in frame.columns:
                raise ValueError(f"{scope} metadata missing {column}.")
        for record in frame.to_dict("records"):
            require_names([record[column] for column in required], f"{scope} metadata")
            rows.append({**record, "Scope": scope, "Page": record.get("Page Display Name", ""), "Visual": record.get("Visual Name", "")})
    return rows


def partition_source(partition):
    source_type = str(partition.SourceType)
    source = partition.Source
    if source_type in ("M", "Calculated"):
        query = source.Expression
    elif source_type == "Query":
        query = source.Query
    elif source_type == "Entity":
        details = f"Entity: {source.SchemaName}.{source.EntityName}; expression source: {source.ExpressionSource}"
        return source_type, "", details, "NO_QUERY"
    else:
        raise ValueError(f"Unsupported partition source type: {source_type}")
    if not isinstance(query, str) or not query.strip():
        raise ValueError(f"{source_type} partition has no accessible query text.")
    return source_type, query, "", "AVAILABLE"


def collect_source_queries(tom, model_id, model_name, table_names):
    rows, expressions = [], []
    tables = {table.Name.casefold(): table for table in tom.model.Tables}
    for table_name in table_names:
        base = {"Model ID": model_id, "Model Name": model_name, "Table Name": table_name,
                "Partition": "", "Source Type": "", "Source Query": "", "Source Details": "", "Status": "ERROR", "Error": ""}
        table = tables.get(table_name.casefold())
        if table is None:
            rows.append({**base, "Error": "Table no longer exists in the model metadata."})
            continue
        partitions = list(table.Partitions)
        if not partitions:
            rows.append({**base, "Error": "No partitions were returned for this table."})
        for partition in partitions:
            row = {**base, "Partition": partition.Name}
            try:
                source_type, query, details, status = partition_source(partition)
                row.update({"Source Type": source_type, "Source Query": query, "Source Details": details, "Status": status})
            except Exception as exc:
                row["Error"] = f"{type(exc).__name__}: {exc}"
            rows.append(row)
    for expression in tom.model.Expressions:
        expressions.append({"Model ID": model_id, "Model Name": model_name, "Expression Name": expression.Name,
                            "Kind": str(expression.Kind), "Expression": expression.Expression})
    return rows, expressions


def analyze_tables(workspace_id, table_results, analyze_usage, include_sources, max_workers):
    eligible = table_results.loc[table_results["Status"] == "EMPTY"].copy()
    usage_rows, source_rows, expression_rows, errors, table_rows = [], [], [], [], []
    if not eligible.empty:
        try:
            if analyze_usage:
                from sempy_labs.report import ReportWrapper
            if include_sources:
                from sempy_labs.tom import connect_semantic_model
        except ImportError as exc:
            raise ImportError(
                "Empty-table analysis requires semantic-link-labs. Run the dependency-setup cell "
                "(%pip install semantic-link-labs) or install it in the attached Fabric Environment. "
                "Restart the session if prompted and rerun from the parameter cell."
            ) from exc
    reports, discovery_error = [], ""
    if analyze_usage and not eligible.empty:
        try:
            client = fabric.PowerBIRestClient(retry_config={"total": 0})
            response = client.get(f"/v1.0/myorg/groups/{workspace_id}/reports", timeout=30)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or not isinstance(payload.get("value"), list):
                raise ValueError("Report discovery did not return a value list.")
            for report in payload["value"]:
                if not isinstance(report, dict):
                    raise ValueError("Invalid report metadata.")
                if report.get("reportType") == "PaginatedReport" and not report.get("datasetId"):
                    continue
                require_names([report.get(key) for key in ("id", "name", "datasetId")], "Report discovery")
                reports.append({**report, "datasetId": str(UUID(report["datasetId"])), "id": str(UUID(report["id"]))})
            if len({report["id"] for report in reports}) != len(reports):
                raise ValueError("Duplicate report IDs in discovery.")
        except Exception as exc:
            discovery_error = f"{type(exc).__name__}: {exc}"
            errors.append({"Model ID": "", "Report ID": "", "Report Name": "", "Stage": "Report discovery", "Error": discovery_error})

    def analyze_model(group):
        model_id, model_tables = group
        model_name = model_tables.iloc[0]["Model Name"]
        names = model_tables["Table Name"].tolist()
        canonical = {name.casefold(): name for name in names}
        found, sources, expressions, model_errors, summaries = [], [], [], [], []
        graph, dependencies_loaded = {}, False
        incomplete = bool(discovery_error)
        linked = [report for report in reports if report["datasetId"] == model_id]

        def record_error(stage, exc, report=None):
            report = report or {}
            model_errors.append({"Model ID": model_id, "Report ID": report.get("id", ""), "Report Name": report.get("name", ""),
                                 "Stage": stage, "Error": f"{type(exc).__name__}: {exc}"})

        if include_sources:
            try:
                with connect_semantic_model(dataset=model_id, workspace=workspace_id, readonly=True) as tom:
                    sources, expressions = collect_source_queries(tom, model_id, model_name, names)
            except Exception as exc:
                record_error("Source queries", exc)
                sources = [{"Model ID": model_id, "Model Name": model_name, "Table Name": name, "Partition": "", "Source Type": "",
                            "Source Query": "", "Source Details": "", "Status": "ERROR", "Error": f"{type(exc).__name__}: {exc}"} for name in names]
        if analyze_usage:
            for report in linked:
                try:
                    if report.get("reportType", "PowerBIReport") != "PowerBIReport":
                        raise ValueError("This report type is not supported for PBIR visual analysis.")
                    wrapper = ReportWrapper(report=report["id"], workspace=workspace_id, readonly=True)
                    objects = report_objects(wrapper)
                    if objects and not dependencies_loaded:
                        dependencies_loaded = True
                        try:
                            graph = dependency_graph(fabric.evaluate_dax(dataset=model_id, workspace=workspace_id, dax_string=DEPENDENCY_QUERY, verbose=0))
                        except Exception as exc:
                            incomplete = True
                            record_error("Dependencies", exc)
                    unresolved = False
                    for obj in objects:
                        tables, complete = referenced_tables(obj["Table Name"], obj["Object Name"], obj["Object Type"], graph)
                        visual_calc = obj.get("Visual Calc", False)
                        unresolved |= not complete or (isinstance(visual_calc, bool) and visual_calc)
                        for name in sorted(tables & canonical.keys()):
                            direct = name == obj["Table Name"].casefold() and obj["Object Type"].casefold() in ("column", "table", "hierarchy")
                            found.append({"Model ID": model_id, "Model Name": model_name, "Table Name": canonical[name],
                                          "Report ID": report["id"], "Report Name": report["name"], "Page": obj["Page"], "Visual": obj["Visual"],
                                          "Scope": obj["Scope"], "Usage": "Direct" if direct else "Dependency",
                                          "Object Table": obj["Table Name"], "Object Name": obj["Object Name"], "Object Type": obj["Object Type"]})
                    if unresolved:
                        incomplete = True
                        record_error("Dependencies", ValueError("Some references could not be fully resolved (including measures without dependency rows or visual calculations)."), report)
                except Exception as exc:
                    incomplete = True
                    record_error("Report definition", exc, report)
        for row in model_tables.to_dict("records"):
            table_found = [item for item in found if item["Table Name"] == row["Table Name"]]
            status = "DISABLED" if not analyze_usage else "REFERENCED" if table_found else "UNKNOWN" if incomplete else "NO_REFERENCE_FOUND"
            summaries.append({"Model ID": model_id, "Model Name": model_name, "Table Name": row["Table Name"], "Data Status": row["Status"],
                              "Usage Status": status, "Coverage": "DISABLED" if not analyze_usage else "PARTIAL" if incomplete else "SCOPED_STATIC",
                              "Linked Reports": len(linked), "Referencing Reports": len({item["Report ID"] for item in table_found})})
        return summaries, found, sources, expressions, model_errors

    if not eligible.empty:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for summaries, found, sources, expressions, model_errors in executor.map(analyze_model, eligible.groupby("Model ID", sort=True)):
                table_rows.extend(summaries)
                usage_rows.extend(found)
                source_rows.extend(sources)
                expression_rows.extend(expressions)
                errors.extend(model_errors)
    return (
        pd.DataFrame(table_rows, columns=["Model ID", "Model Name", "Table Name", "Data Status", "Usage Status", "Coverage", "Linked Reports", "Referencing Reports"]),
        pd.DataFrame(usage_rows, columns=USAGE_COLUMNS).drop_duplicates(ignore_index=True),
        pd.DataFrame(source_rows, columns=SOURCE_COLUMNS), pd.DataFrame(expression_rows, columns=EXPRESSION_COLUMNS),
        pd.DataFrame(errors, columns=ANALYSIS_ERROR_COLUMNS),
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for parameter, value in (("FAIL_ON_ISSUES", FAIL_ON_ISSUES), ("ANALYZE_REPORT_USAGE", ANALYZE_REPORT_USAGE), ("INCLUDE_SOURCE_QUERIES", INCLUDE_SOURCE_QUERIES)):
    if not isinstance(value, bool):
        raise ValueError(f"{parameter} must be a Python boolean (True or False), not a string.")

resolved_workspace_id = resolve_workspace_id(WORKSPACE_ID)
scan_started = perf_counter()
model_summary_df, table_results_df = scan_workspace(
    resolved_workspace_id, MEASURE_ONLY_TABLE_NAMES,
    tables_per_query=TABLES_PER_QUERY, max_parallel_models=MAX_PARALLEL_MODELS,
)
workspace_summary_df = summarize_workspace(model_summary_df, perf_counter() - scan_started)
analysis_started = perf_counter()
table_usage_df, report_usage_df, source_queries_df, shared_expressions_df, analysis_errors_df = analyze_tables(
    resolved_workspace_id, table_results_df, ANALYZE_REPORT_USAGE, INCLUDE_SOURCE_QUERIES, MAX_PARALLEL_MODELS,
)
workspace_summary_df["Analysis seconds"] = round(perf_counter() - analysis_started, 2)
workspace_summary_df["Analysis Errors"] = len(analysis_errors_df) + int(source_queries_df["Status"].eq("ERROR").sum())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(HTML("<h2>Overall workspace summary</h2>"))
overall = workspace_summary_df.iloc[0]
print(
    f"Overall: {overall['Status']} | Models: {overall['Models Discovered']} | "
    f"Tables checked: {overall['Tables Attempted']} | "
    f"PASS: {overall['Tables PASS']} | EMPTY: {overall['Tables EMPTY']} | "
    f"ERROR: {overall['Tables ERROR']} | SKIPPED: {overall['Tables SKIPPED']} | "
    f"Discovery errors: {overall['Models With Discovery Errors']} | "
    f"DAX requests: {overall['DAX Requests']} | Scan seconds: {overall['Scan seconds']}"
)
display_frame(overall.rename_axis("Metric").reset_index(name="Value"))

display(HTML("<h2>Summary by semantic model</h2>"))
display_frame(model_summary_df[[
    "Model Name", "Model ID", "Status", "Skip Reason", "Tables Discovered", "Tables Attempted",
    "PASS", "EMPTY", "ERROR", "SKIPPED", "DAX Requests", "Discovery ms", "Query ms", "Elapsed ms", "Error",
]])
print("PASS/EMPTY/ERROR count tables, not DAX requests; a failed batch marks all its tables ERROR without retries. Skipped models/tables were not queried. Source/table discovery errors are separate; table totals exclude undiscovered tables in skipped models. Timings are client-observed milliseconds. Displayed errors are capped at 100 characters; full details remain in the result DataFrames.")

empty_tables_df = table_results_df.loc[
    table_results_df["Status"] == "EMPTY", ["Model Name", "Model ID", "Table Name"]
].reset_index(drop=True)
error_tables_df = table_results_df.loc[
    table_results_df["Status"] == "ERROR", ["Model Name", "Model ID", "Table Name", "Error"]
].reset_index(drop=True)
display(HTML("<h2>Empty tables</h2>"))
if empty_tables_df.empty:
    print("No checked tables returned EMPTY. See errors and skipped counts for tables not verified.")
else:
    display_frame(empty_tables_df)
if not error_tables_df.empty:
    display(HTML("<h2>Table query errors</h2>"))
    display_frame(error_tables_df)

if empty_tables_df.empty and (ANALYZE_REPORT_USAGE or INCLUDE_SOURCE_QUERIES):
    print("No EMPTY tables: report-usage and source-query analysis was skipped.")
if ANALYZE_REPORT_USAGE and not empty_tables_df.empty:
    display(HTML("<h2>Empty-table usage in current-workspace reports</h2>"))
    print("Static references only. NO_REFERENCE_FOUND does not mean unused. UNKNOWN/PARTIAL indicates incomplete coverage; see analysis errors.")
    display_frame(table_usage_df)
    if not report_usage_df.empty:
        display(HTML("<h3>Report, page, visual and filter references for EMPTY tables</h3>"))
        display_frame(report_usage_df)
    else:
        print("No report references were resolved for EMPTY tables. Review usage status and analysis errors; this does not prove the tables are unused.")
if INCLUDE_SOURCE_QUERIES and not empty_tables_df.empty:
    display(HTML("<h2>Source queries for EMPTY tables</h2>"))
    print("Stored source queries only; not executed. Each partition is listed separately. NO_QUERY means an entity source descriptor, not missing data. Treat query text as sensitive metadata.")
    display_frame(source_queries_df)
if not analysis_errors_df.empty:
    display(HTML("<h2>Report/source analysis errors</h2>"))
    display_frame(analysis_errors_df)
if int(workspace_summary_df.iloc[0]["Analysis Errors"]):
    logger.warning("Report/source analysis is incomplete; review analysis errors and source-query statuses. Availability results are unchanged.")

issues_df = model_summary_df.loc[model_summary_df["Status"].isin(["EMPTY", "ERROR"])]
if not issues_df.empty:
    message = (
        f"Availability scan found {len(issues_df)} model(s) with empty tables or errors. "
        "Review the final summary, empty-table list, and error details above."
    )
    logger.warning(message)
    if FAIL_ON_ISSUES:
        raise RuntimeError(message)
elif overall["Status"] == "SKIPPED":
    print("No eligible tables were checked; all discovered models or tables were excluded.")
else:
    print("All checked tables returned at least one visible row. Excluded tables were not evaluated.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
