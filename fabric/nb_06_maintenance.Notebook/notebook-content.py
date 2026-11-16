# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_06_maintenance — OPTIMIZE (V-Order) and VACUUM
# Schedule weekly, separately from the load. Small Delta files hurt Direct Lake framing and
# SQL-endpoint performance; VACUUM keeps 7 days of history for time travel / rollback.

# CELL ********************

# Parameters
RUN_ID = ""
retain_hours = 168

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_common

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not IS_FABRIC:
    print("Local run: OPTIMIZE/VACUUM need Delta — skipped.")
else:
    names = sorted({f.name.rstrip("/") for f in notebookutils.fs.ls(TABLES)})  # noqa: F821
    for name in names:
        p = table_path(name)
        if not DeltaTable.isDeltaTable(spark, p):  # noqa: F821
            continue
        dtab = DeltaTable.forPath(spark, p)  # noqa: F821
        dtab.optimize().executeCompaction()
        dtab.vacuum(retain_hours)
        print(f"optimized + vacuumed {name}")
    log_audit("nb_06_maintenance", "ops", "*", len(names), len(names), detail="OPTIMIZE + VACUUM")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
