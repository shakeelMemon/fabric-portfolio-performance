"""Builds Fabric Git-format notebook items from notebooks_src/*.py.

notebooks_src/<name>.py uses `# %%` / `# %% [markdown]` cell markers (VS Code / Jupytext style).
Output: fabric/<name>.Notebook/{.platform, notebook-content.py} — the layout Fabric Git integration
syncs into a workspace. Logical IDs are stable (uuid5 of the item name) so rebuilding does not
create duplicate items in the workspace.

Use it once to bootstrap a new workspace. After that, Fabric is the source of truth:
commit from Fabric Source control and run tools/sync_from_fabric.py.

Usage:  python tools/build_fabric_items.py
"""
import json
import pathlib
import re
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT / "notebooks_src"
OUT = ROOT / "fabric"
NS = uuid.UUID("6f1c2a10-6b1f-4d0e-9a51-1f0e8a6c2b11")

PLATFORM_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json"


def meta_block(obj):
    lines = json.dumps(obj, indent=2).splitlines()
    return "\n".join("# META " + ln if ln else "# META" for ln in lines)


def split_cells(text):
    cells, cur, kind = [], [], None
    for line in text.splitlines():
        m = re.match(r"^# %%( \[markdown\])?\s*$", line)
        if m:
            if kind is not None:
                cells.append((kind, "\n".join(cur).strip("\n")))
            kind, cur = ("markdown" if m.group(1) else "code"), []
        else:
            cur.append(line)
    if kind is not None:
        cells.append((kind, "\n".join(cur).strip("\n")))
    return [c for c in cells if c[1].strip()]


def build_notebook(path):
    name = path.stem
    cells = split_cells(path.read_text(encoding="utf-8"))
    parts = ["# Fabric notebook source", "",
             "# METADATA ********************", "",
             meta_block({"kernel_info": {"name": "synapse_pyspark"}, "dependencies": {}}), ""]
    for kind, body in cells:
        if kind == "markdown":
            parts += ["# MARKDOWN ********************", "", body, ""]
            continue
        body = re.sub(r"^# MAGIC %run (\S+)\s*$", r"%run \1", body, flags=re.M)
        cell_meta = {"language": "python", "language_group": "synapse_pyspark"}
        if body.lstrip().startswith("# Parameters"):
            cell_meta["tags"] = ["parameters"]
        parts += ["# CELL ********************", "", body, "",
                  "# METADATA ********************", "", meta_block(cell_meta), ""]
    item = OUT / f"{name}.Notebook"
    item.mkdir(parents=True, exist_ok=True)
    (item / "notebook-content.py").write_text("\n".join(parts), encoding="utf-8")
    platform = {"$schema": PLATFORM_SCHEMA,
                "metadata": {"type": "Notebook", "displayName": name,
                             "description": first_heading(cells)},
                "config": {"version": "2.0", "logicalId": str(uuid.uuid5(NS, name))}}
    (item / ".platform").write_text(json.dumps(platform, indent=2), encoding="utf-8")
    return name


def first_heading(cells):
    for kind, body in cells:
        if kind == "markdown":
            for ln in body.splitlines():
                if ln.startswith("# # "):
                    return ln[4:].split(" — ")[-1][:250]
    return ""


def build_lakehouse(name="lh_portfolio"):
    item = OUT / f"{name}.Lakehouse"
    item.mkdir(parents=True, exist_ok=True)
    (item / "lakehouse.metadata.json").write_text("{}", encoding="utf-8")
    platform = {"$schema": PLATFORM_SCHEMA,
                "metadata": {"type": "Lakehouse", "displayName": name,
                             "description": "Bronze/silver/gold Delta tables and landing files for portfolio reporting"},
                "config": {"version": "2.0", "logicalId": str(uuid.uuid5(NS, name))}}
    (item / ".platform").write_text(json.dumps(platform, indent=2), encoding="utf-8")


if __name__ == "__main__":
    build_lakehouse()
    for p in sorted(SRC.glob("nb_*.py")):
        print("built", build_notebook(p))
