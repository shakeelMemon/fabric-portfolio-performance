# %% [markdown]
# # nb_02_bronze — land new administrator files as-is
# Metadata-driven by `ops_source_mapping`. For each source:
# 1. list `Files/landing/<subfolder>` and keep files matching the source's regex;
# 2. skip files already processed with the same SHA-256 (`ops_processed_files`) — reruns are no-ops,
#    a re-sent file with changed content is picked up again;
# 3. read every column as string (no type coercion in bronze), add lineage columns, append.

# %%
# Parameters (injected by nb_99_run_all; blank = generate a new run id)
RUN_ID = ""

# %%
# MAGIC %run nb_common

# %%
import re

NOTEBOOK = "nb_02_bronze"
mapping = read_table("ops_source_mapping").filter("is_active").toPandas()
processed = read_table("ops_processed_files").select("file_path", "sha256").toPandas()
seen = set(zip(processed.file_path, processed.sha256))

# %%
def ingest_source(m):
    rx = re.compile(m.file_regex)
    candidates = [f for f in fs_ls(f"{LANDING}/{m.landing_subfolder}") if rx.match(f["name"])]
    new = []
    for f in candidates:
        digest = sha256_text(fs_read_text(f["path"]))
        if (f["path"], digest) not in seen:
            new.append({**f, "sha256": digest})
    if not new:
        print(f"{m.source_id:<28} no new files ({len(candidates)} already processed)")
        return
    paths = [f["path"] for f in new]
    df = (spark.read.option("header", True).option("inferSchema", False)  # noqa: F821
               .option("quote", '"').option("escape", '"').csv(paths))
    df = (df.withColumn("_source_file", F.regexp_extract(F.col("_metadata.file_path"), r"([^/\\]+)$", 1))
            .withColumn("_file_version", F.coalesce(
                F.expr(r"try_cast(regexp_extract(_source_file, '_v(\\d+)\\.csv$', 1) as int)"), F.lit(1)))
            .withColumn("_source_id", F.lit(m.source_id))
            .withColumn("_run_id", F.lit(RUN_ID))
            .withColumn("_ingested_at_utc", F.current_timestamp()))
    df = df.cache()
    counts = {r["_source_file"]: r["count"] for r in df.groupBy("_source_file").count().collect()}
    append_table(df, m.bronze_table)
    pf = pd.DataFrame([{"source_id": m.source_id, "file_path": f["path"], "file_name": f["name"],
                        "sha256": f["sha256"], "row_count": int(counts.get(f["name"], 0)), "run_id": RUN_ID,
                        "processed_at_utc": dt.datetime.utcnow()} for f in new])
    append_table(spark.createDataFrame(pf, read_table("ops_processed_files").schema), "ops_processed_files")  # noqa: F821
    total = sum(counts.values())
    log_audit(NOTEBOOK, "bronze", m.bronze_table, total, total,
              detail=json.dumps({"files": len(new), "source_id": m.source_id}))
    df.unpersist()


for m in mapping.itertuples():
    ingest_source(m)
