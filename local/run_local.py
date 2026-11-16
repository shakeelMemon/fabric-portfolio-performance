"""Runs the Fabric notebooks (fabric/*.Notebook/notebook-content.py) on a local Spark session.

This executes the exact files that Git-sync into Fabric. Differences from Fabric:
  * tables are written as parquet (Delta jars are not available offline) — merge_table() emulates MERGE;
  * nb_06_maintenance (OPTIMIZE/VACUUM) and the semantic-model refresh are skipped.

Usage:  python local/run_local.py [--root ./_local_lakehouse] [--deliver-through 2026Q3] [--fresh]
"""
import argparse
import json
import os
import pathlib
import re
import shutil
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
FABRIC = ROOT / "fabric"

PIPELINE = ["nb_00_setup", "nb_01_generate_source_files", "nb_02_bronze", "nb_03_silver",
            "nb_04_gold", "nb_05_quality_gate"]


def parse_cells(name):
    text = (FABRIC / f"{name}.Notebook" / "notebook-content.py").read_text(encoding="utf-8")
    blocks = re.split(r"^# (CELL|METADATA|MARKDOWN) \*+\s*$", text, flags=re.M)
    cells, i = [], 1
    while i < len(blocks):
        kind, body = blocks[i], blocks[i + 1]
        if kind == "CELL":
            meta = {}
            if i + 2 < len(blocks) and blocks[i + 2] == "METADATA":
                raw = "\n".join(ln[7:] if ln.startswith("# META ") else "" for ln in blocks[i + 3].splitlines())
                meta = json.loads(raw) if raw.strip() else {}
            cells.append((body.strip("\n"), meta))
        i += 2
    return cells


def run_notebook(name, spark, params=None, shared=None):
    g = {"__name__": f"nb_{name}", "spark": spark}
    g.update(shared or {})
    for code, meta in parse_cells(name):
        m = re.match(r"^(?:# MAGIC )?%run (\S+)\s*$", code.strip())
        if m:
            for sub_code, _ in parse_cells(m.group(1)):
                exec(compile(sub_code, f"{m.group(1)}", "exec"), g)
            continue
        exec(compile(code, name, "exec"), g)
        is_params = "parameters" in meta.get("tags", []) or code.lstrip().startswith("# Parameters")
        if is_params and params:
            g.update(params)
    return g


def get_spark():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.master("local[2]").appName("portfolio-local")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.memory", "3g")
            # Arrow (applyInPandas) on Java 17/21 with Spark 3.5
            .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
            .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
            .getOrCreate())


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=str(ROOT / "_local_lakehouse"))
    ap.add_argument("--deliver-through", default="2026Q3")
    ap.add_argument("--fresh", action="store_true", help="delete the local lakehouse first")
    ap.add_argument("--only", nargs="*", help="run only these notebooks")
    a = ap.parse_args(argv)
    if a.fresh and os.path.exists(a.root):
        shutil.rmtree(a.root)
    os.environ["PORTFOLIO_LOCAL_ROOT"] = os.path.abspath(a.root)
    spark = get_spark()
    run_id = time.strftime("%Y%m%dT%H%M%S") + "-local"
    for nb in a.only or PIPELINE:
        t = time.time()
        print(f"\n===== {nb} =====")
        params = {"RUN_ID": run_id}
        if nb == "nb_01_generate_source_files":
            params["deliver_through"] = a.deliver_through
        run_notebook(nb, spark, params=params)
        print(f"----- {nb} finished in {time.time() - t:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
