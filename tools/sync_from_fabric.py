"""Regenerates notebooks_src/*.py from the Fabric Git items (fabric/*.Notebook/notebook-content.py).

The Fabric workspace is the source of truth: edit a notebook in Fabric, commit it from Source control,
then run this script so the readable `# %%` copies in notebooks_src stay identical. CI runs it and fails
if notebooks_src is out of date.

Usage:  python tools/sync_from_fabric.py
"""
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[1]
FABRIC = ROOT / "fabric"
SRC = ROOT / "notebooks_src"


def to_source(text):
    blocks = re.split(r"^# (CELL|METADATA|MARKDOWN) \*+\s*$", text, flags=re.M)
    cells = []
    for kind, body in zip(blocks[1::2], blocks[2::2]):
        body = body.strip("\n")
        if kind == "MARKDOWN" and body:
            cells.append("# %% [markdown]\n" + body)
        elif kind == "CELL" and body:
            body = re.sub(r"^%run (\S+)\s*$", r"# MAGIC %run \1", body, flags=re.M)
            cells.append("# %%\n" + body)
    return "\n\n".join(cells) + "\n"


if __name__ == "__main__":
    for item in sorted(FABRIC.glob("nb_*.Notebook")):
        name = item.name[: -len(".Notebook")]
        (SRC / f"{name}.py").write_text(to_source((item / "notebook-content.py").read_text(encoding="utf-8")),
                                        encoding="utf-8", newline="\n")
        print("synced", name)
