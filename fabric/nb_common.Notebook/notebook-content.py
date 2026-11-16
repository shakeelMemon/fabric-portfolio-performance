# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_common — shared configuration and helpers
# Loaded by every notebook with `%run nb_common`. Nothing here writes data.
# * Resolves the OneLake path of `lh_portfolio` (no default lakehouse needed, so notebooks
#   behave the same when run interactively, from `nb_99_run_all`, or from a pipeline).
# * Table helpers: overwrite / append / MERGE (Delta in Fabric, parquet emulation locally).
# * File helpers over `notebookutils.fs` (Fabric) or the local filesystem (tests).
# * Shared finance maths: XIRR, quarter calendar, FX conversion.

# CELL ********************

import os
import io
import json
import math
import uuid
import shutil
import hashlib
import datetime as dt

import pandas as pd
from pyspark.sql import functions as F, types as T, Window

LAKEHOUSE_NAME = "lh_portfolio"
SEMANTIC_MODEL_NAME = "PortfolioPerformance"

FIRST_QUARTER = "2022Q1"
LAST_QUARTER = "2026Q3"           # latest quarter the administrators have delivered
REPORTING_CURRENCIES = ["USD", "CAD"]
AMOUNT_TOLERANCE = 1.00           # currency units allowed on reconciliations (rounding)
QUARANTINE_RATE_LIMIT = 0.02      # quality gate fails if >2% of bronze rows are rejected

try:
    notebookutils  # noqa: F821  (pre-loaded in every Fabric notebook session)
    IS_FABRIC = True
except NameError:
    IS_FABRIC = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _resolve_root():
    """OneLake root of the lakehouse, e.g. abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh-id>."""
    if not IS_FABRIC:
        return os.environ.get("PORTFOLIO_LOCAL_ROOT", os.path.abspath("_local_lakehouse"))
    try:
        lh = notebookutils.lakehouse.getWithProperties(LAKEHOUSE_NAME)  # noqa: F821
        props = lh["properties"] if isinstance(lh, dict) else lh.properties
        return props["abfsPath"]
    except Exception:
        lh = notebookutils.lakehouse.get(LAKEHOUSE_NAME)  # noqa: F821
        lh_id = lh["id"] if isinstance(lh, dict) else lh.id
        ws_id = notebookutils.runtime.context["currentWorkspaceId"]  # noqa: F821
        return f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh_id}"


ROOT = _resolve_root()
FILES = f"{ROOT}/Files"
TABLES = f"{ROOT}/Tables"
LANDING = f"{FILES}/landing"
REFERENCE = f"{FILES}/reference"
TABLE_FORMAT = "delta" if IS_FABRIC else "parquet"   # local tests: Delta jars are not available

RUN_ID = globals().get("RUN_ID") or (
    dt.datetime.utcnow().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
)

if IS_FABRIC:
    from delta.tables import DeltaTable
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")  # noqa: F821
    spark.conf.set("spark.sql.parquet.vorder.enabled", "true")                 # noqa: F821

print(f"lakehouse root : {ROOT}")
print(f"table format   : {TABLE_FORMAT}   run_id: {RUN_ID}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## File helpers

# CELL ********************

def fs_write_text(path, text):
    if IS_FABRIC:
        notebookutils.fs.put(path, text, True)  # noqa: F821
    else:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="") as fh:
            fh.write(text)


def fs_read_text(path):
    if IS_FABRIC:
        return notebookutils.fs.head(path, 50 * 1024 * 1024)  # noqa: F821  (source files are small)
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def fs_ls(path):
    """Recursive list of files under `path` -> list of dicts(path, name, size)."""
    out = []
    if IS_FABRIC:
        if not notebookutils.fs.exists(path):  # noqa: F821
            return out
        for f in notebookutils.fs.ls(path):  # noqa: F821
            if f.isDir:
                out.extend(fs_ls(f.path))
            else:
                out.append({"path": f.path, "name": f.name, "size": f.size})
        return out
    if not os.path.exists(path):
        return out
    for dirpath, _, names in os.walk(path):
        for n in sorted(names):
            p = os.path.join(dirpath, n)
            out.append({"path": p, "name": n, "size": os.path.getsize(p)})
    return out


def fs_rm(path):
    if IS_FABRIC:
        if notebookutils.fs.exists(path):  # noqa: F821
            notebookutils.fs.rm(path, True)  # noqa: F821
    elif os.path.exists(path):
        shutil.rmtree(path) if os.path.isdir(path) else os.remove(path)


def sha256_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Table helpers
# Tables live in `Tables/<name>` of the lakehouse and are discovered automatically by the
# SQL analytics endpoint, which is what the Direct Lake semantic model binds to.

# CELL ********************

def table_path(name):
    return f"{TABLES}/{name}"


def table_exists(name):
    p = table_path(name)
    if IS_FABRIC:
        return DeltaTable.isDeltaTable(spark, p)  # noqa: F821
    return os.path.exists(p) and any(f.endswith(".parquet") for f in os.listdir(p))


def read_table(name):
    return spark.read.format(TABLE_FORMAT).load(table_path(name))  # noqa: F821


def _local_swap_write(df, name):
    """Local only: write to a temp folder then swap (Spark cannot overwrite a path it is reading)."""
    p = table_path(name)
    tmp = p + "__tmp"
    if os.path.exists(tmp):
        shutil.rmtree(tmp)
    df.write.format(TABLE_FORMAT).mode("overwrite").save(tmp)
    if os.path.exists(p):
        shutil.rmtree(p)
    os.rename(tmp, p)


def write_table(df, name):
    """Full overwrite (dimensions, gold aggregates, rebuilt facts)."""
    if IS_FABRIC:
        (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
           .save(table_path(name)))
    else:
        _local_swap_write(df, name)
    return df


def append_table(df, name):
    if IS_FABRIC:
        df.write.format("delta").mode("append").option("mergeSchema", "true").save(table_path(name))
    else:
        df.write.format(TABLE_FORMAT).mode("append").save(table_path(name))


def merge_table(df, name, keys):
    """Upsert on business keys: Delta MERGE in Fabric; anti-join + union emulation locally."""
    if not table_exists(name):
        write_table(df, name)
        return
    if IS_FABRIC:
        cond = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
        (DeltaTable.forPath(spark, table_path(name)).alias("t")  # noqa: F821
            .merge(df.alias("s"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    else:
        existing = read_table(name)
        kept = existing.join(df.select(*keys).distinct(), on=keys, how="left_anti")
        _local_swap_write(kept.unionByName(df, allowMissingColumns=True), name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Operational logging — `ops_load_audit`

# CELL ********************

AUDIT_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("notebook", T.StringType()),
    T.StructField("layer", T.StringType()),
    T.StructField("target_table", T.StringType()),
    T.StructField("rows_in", T.LongType()),
    T.StructField("rows_out", T.LongType()),
    T.StructField("rows_rejected", T.LongType()),
    T.StructField("status", T.StringType()),
    T.StructField("detail", T.StringType()),
    T.StructField("logged_at_utc", T.TimestampType()),
])


def log_audit(notebook, layer, target_table, rows_in, rows_out, rows_rejected=0,
              status="SUCCEEDED", detail=""):
    row = [(RUN_ID, notebook, layer, target_table, int(rows_in), int(rows_out),
            int(rows_rejected), status, detail[:4000], dt.datetime.utcnow())]
    append_table(spark.createDataFrame(row, AUDIT_SCHEMA), "ops_load_audit")  # noqa: F821
    print(f"[audit] {layer:<7} {target_table:<26} in={rows_in:>7} out={rows_out:>7} "
          f"rejected={rows_rejected:>5} {status}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Quarter calendar

# CELL ********************

def quarter_list(first=FIRST_QUARTER, last=LAST_QUARTER):
    y, q = int(first[:4]), int(first[-1])
    ly, lq = int(last[:4]), int(last[-1])
    out = []
    while (y, q) <= (ly, lq):
        out.append(f"{y}Q{q}")
        q += 1
        if q == 5:
            y, q = y + 1, 1
    return out


def quarter_end(qlabel):
    y, q = int(qlabel[:4]), int(qlabel[-1])
    m = q * 3
    nxt = dt.date(y + (m == 12), (m % 12) + 1, 1)
    return nxt - dt.timedelta(days=1)


def quarter_start(qlabel):
    y, q = int(qlabel[:4]), int(qlabel[-1])
    return dt.date(y, (q - 1) * 3 + 1, 1)


def quarter_of(d):
    return f"{d.year}Q{(d.month - 1) // 3 + 1}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## XIRR — the single Python definition of IRR
# ACT/365 compounding, same convention as Excel `XIRR` and DAX `XIRR`, so the precomputed
# `gold_performance.irr` and the report's `Net IRR` measure agree.

# CELL ********************

def xirr(dates, amounts, guess=0.10, tol=1e-9, max_iter=200):
    """Annualised IRR of dated cashflows. Investor perspective: calls < 0, distributions/NAV > 0.
    Returns None when there is no sign change (IRR undefined)."""
    flows = [(d, float(a)) for d, a in zip(dates, amounts) if a is not None and abs(float(a)) > 1e-9]
    if not flows or not any(a > 0 for _, a in flows) or not any(a < 0 for _, a in flows):
        return None
    t0 = min(d for d, _ in flows)
    ts = [((d - t0).days / 365.0, a) for d, a in flows]

    def npv(r):
        return sum(a / (1.0 + r) ** t for t, a in ts)

    def dnpv(r):
        return sum(-t * a / (1.0 + r) ** (t + 1.0) for t, a in ts)

    r = guess
    for _ in range(max_iter):                       # Newton–Raphson
        try:
            f, df_ = npv(r), dnpv(r)
        except (OverflowError, ZeroDivisionError):
            break
        if df_ == 0:
            break
        step = f / df_
        r_new = r - step
        if r_new <= -0.999999:
            break
        if abs(step) < tol:
            return r_new
        r = r_new

    lo, hi = -0.999999, 1.0                         # bisection fallback
    try:
        f_lo = npv(lo)
        while npv(hi) * f_lo > 0 and hi < 1e6:
            hi *= 2.0
        if npv(hi) * f_lo > 0:
            return None
        for _ in range(500):
            mid = (lo + hi) / 2.0
            f_mid = npv(mid)
            if abs(f_mid) < 1e-7 or (hi - lo) < tol:
                return mid
            if f_mid * f_lo < 0:
                hi = mid
            else:
                lo, f_lo = mid, f_mid
    except OverflowError:
        return None
    return (lo + hi) / 2.0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## FX conversion
# `silver_fx_rate` holds one row per calendar day and currency: `usd_per_unit`.
# Cross rate A→B = usd_per_unit(A) / usd_per_unit(B). Flows convert at the flow-date rate,
# balances at the period-end rate, P&L at the quarter-average rate (see docs/DATA_CONTRACT.md).

# CELL ********************

def add_fx_rate(df, fx, date_col, from_ccy_col, to_ccy, out_col):
    """Adds `out_col` = rate converting `from_ccy_col` → `to_ccy` (literal code or column name)."""
    f = fx.select(F.col("rate_date").alias("_fd"), F.col("currency").alias("_fc"),
                  F.col("usd_per_unit").alias("_from"))
    t = fx.select(F.col("rate_date").alias("_td"), F.col("currency").alias("_tc"),
                  F.col("usd_per_unit").alias("_to"))
    to_expr = F.col(to_ccy) if to_ccy in df.columns else F.lit(to_ccy)
    out = (df.withColumn("_to_ccy", to_expr)
             .join(F.broadcast(f), (F.col(date_col) == F.col("_fd")) & (F.col(from_ccy_col) == F.col("_fc")), "left")
             .join(F.broadcast(t), (F.col(date_col) == F.col("_td")) & (F.col("_to_ccy") == F.col("_tc")), "left")
             .withColumn(out_col, F.col("_from") / F.col("_to")))
    return out.drop("_fd", "_fc", "_from", "_td", "_tc", "_to", "_to_ccy")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
