"""Points the Power BI project at your lakehouse's SQL analytics endpoint.

The committed PBIP keeps placeholders ({{SQL_ENDPOINT}}, {{SQL_ENDPOINT_ID}}) so no tenant details are
stored in Git. This writes a configured copy:

  python tools/configure_powerbi.py --server <xxxx.datawarehouse.fabric.microsoft.com> --endpoint-id <guid>
      -> powerbi_configured/PortfolioPerformance.pbip   (open in Power BI Desktop, publish to the workspace)

  add --into-fabric to also copy the configured SemanticModel + Report items into fabric/ so the next
  Git "Update all" in the workspace creates them alongside the lakehouse and notebooks.
"""
import argparse
import pathlib
import re
import shutil

ROOT = pathlib.Path(__file__).resolve().parents[1]
NAME = "PortfolioPerformance"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", required=True, help="SQL analytics endpoint connection string (host name)")
    ap.add_argument("--endpoint-id", required=True, help="SQL analytics endpoint ID (GUID)")
    ap.add_argument("--into-fabric", action="store_true")
    a = ap.parse_args()
    if not re.fullmatch(r"[0-9a-fA-F-]{36}", a.endpoint_id):
        raise SystemExit("--endpoint-id must be a GUID")
    server = a.server.replace("tcp:", "").split(",")[0].strip()
    out = ROOT / "powerbi_configured"
    if out.exists():
        shutil.rmtree(out)
    shutil.copytree(ROOT / "powerbi", out)
    expr = out / f"{NAME}.SemanticModel" / "definition" / "expressions.tmdl"
    text = expr.read_text(encoding="utf-8").replace("{{SQL_ENDPOINT}}", server).replace("{{SQL_ENDPOINT_ID}}", a.endpoint_id)
    expr.write_text(text, encoding="utf-8")
    print(f"configured project: {out / (NAME + '.pbip')}")
    if a.into_fabric:
        for item in (f"{NAME}.SemanticModel", f"{NAME}.Report"):
            dst = ROOT / "fabric" / item
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(out / item, dst, ignore=shutil.ignore_patterns(".pbi"))
            print(f"copied {item} -> fabric/  (commit, then Source control > Update all in the workspace)")


if __name__ == "__main__":
    main()
