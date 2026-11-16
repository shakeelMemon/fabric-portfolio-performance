"""Generates the Power BI project (PBIP): a Direct Lake TMDL semantic model + a report.

Columns are read from the gold tables of a local pipeline run (so the model always matches what the
notebooks produce); measures, relationships, roles and report pages are declared below — this file
is the single place the reporting layer is defined. docs/METRICS.md is generated from the same
MEASURES list.

Usage:  python local/run_local.py --fresh && python tools/build_powerbi.py
"""
import json
import pathlib
import shutil
import uuid

import pyarrow.parquet as pq

ROOT = pathlib.Path(__file__).resolve().parents[1]
LAKE = ROOT / "_local_lakehouse" / "Tables"
OUT = ROOT / "powerbi"
NAME = "PortfolioPerformance"
NS = uuid.UUID("2a7f0d3e-5c61-4b8e-9d3f-7e1b6c4a9f20")
PLATFORM_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json"


def tag(*parts):
    return str(uuid.uuid5(NS, "/".join(parts)))


# ----------------------------------------------------------------------------------------------
# Tables in the model (gold + ops). Key/technical columns are hidden; amounts summarise by sum.
# ----------------------------------------------------------------------------------------------
TABLES = ["dim_date", "dim_fund", "dim_investor", "dim_investment", "dim_legal_entity", "dim_report_currency",
          "fact_commitment", "fact_cashflow", "fact_snapshot", "fact_position", "fact_accrual", "fact_irr_flow",
          "gold_nav_bridge", "ops_dq_results", "sec_user_investor", "measure_table"]
HIDDEN_TABLES = {"fact_irr_flow", "sec_user_investor"}
SORT_BY = {("dim_date", "quarter_label"): "quarter_sort", ("dim_date", "month_label"): "month_sort",
           ("gold_nav_bridge", "component"): "component_order",
           ("dim_report_currency", "currency_label"): "sort_order"}
VISIBLE_ID_COLUMNS = {("ops_dq_results", "run_id"), ("ops_dq_results", "check_id"),
                      ("dim_report_currency", "currency_code")}

RELATIONSHIPS = []
for fact in ["fact_commitment", "fact_cashflow", "fact_snapshot", "fact_irr_flow", "gold_nav_bridge"]:
    RELATIONSHIPS += [(fact, "fund_id", "dim_fund", "fund_id"), (fact, "investor_id", "dim_investor", "investor_id"),
                      (fact, "legal_entity_id", "dim_legal_entity", "legal_entity_id")]
for fact in ["fact_position", "fact_accrual", "fact_irr_flow"]:
    RELATIONSHIPS += [(fact, "investment_id", "dim_investment", "investment_id")]
for fact in ["fact_position", "fact_accrual"]:
    RELATIONSHIPS += [(fact, "fund_id", "dim_fund", "fund_id")]
for fact in ["fact_cashflow", "fact_snapshot", "fact_position", "fact_accrual", "gold_nav_bridge"]:
    RELATIONSHIPS += [(fact, "date_key", "dim_date", "date_key")]
RELATIONSHIPS += [("fact_irr_flow", "as_of_date_key", "dim_date", "date_key")]

# ----------------------------------------------------------------------------------------------
# Measures — thin wrappers over gold columns. (name, folder, format, description, DAX)
# ----------------------------------------------------------------------------------------------
AMT = "#,0;(#,0)"
MULT = '0.00"x"'
PCT = "0.0%;-0.0%"


def cur_switch(table, stem, valid_local=None, valid_le=None, le=True):
    """SUM of the amount column for the selected reporting currency, blank when LOCAL/LE would mix currencies."""
    local = f"IF ( DISTINCTCOUNT ( {table}[local_currency] ) = 1, SUM ( {table}[{stem}_local] ) )"
    le_expr = (f"IF ( DISTINCTCOUNT ( {table}[le_currency] ) = 1, SUM ( {table}[{stem}_le] ) )" if le else "BLANK ()")
    return (f'SWITCH ( _ccy, "USD", SUM ( {table}[{stem}_usd] ), "CAD", SUM ( {table}[{stem}_cad] ), '
            f'"LOCAL", {local}, "LE", {le_expr} )')


def flow_irr(level):
    return f"""VAR _asof = [As Of Date]
VAR _ccy = [Selected Currency]
VAR _flows =
    CALCULATETABLE (
        fact_irr_flow,
        fact_irr_flow[level] = "{level}",
        REMOVEFILTERS ( dim_date ),
        fact_irr_flow[as_of_date] = _asof
    )
VAR _ok =
    SWITCH (
        _ccy,
        "LOCAL", COUNTROWS ( DISTINCT ( SELECTCOLUMNS ( _flows, "c", fact_irr_flow[local_currency] ) ) ) = 1,
        "LE", COUNTROWS ( DISTINCT ( SELECTCOLUMNS ( _flows, "c", fact_irr_flow[le_currency] ) ) ) = 1,
        TRUE ()
    )
VAR _t =
    ADDCOLUMNS (
        _flows,
        "@amt",
            SWITCH (
                _ccy,
                "USD", fact_irr_flow[amount_usd],
                "CAD", fact_irr_flow[amount_cad],
                "LOCAL", fact_irr_flow[amount_local],
                "LE", fact_irr_flow[amount_le]
            )
    )
RETURN
    IF ( _ok && NOT ISEMPTY ( _flows ), IFERROR ( XIRR ( _t, [@amt], fact_irr_flow[flow_date] ), BLANK () ) )"""


MEASURES = [
    ("Selected Currency", "Selection", None, "Reporting currency picked on the Currency slicer (default USD).",
     'SELECTEDVALUE ( dim_report_currency[currency_code], "USD" )'),
    ("As Of Date", "Selection", "yyyy-mm-dd",
     "Latest quarter end in the current date filter, capped at the latest quarter the administrators delivered.",
     """VAR _picked = MAX ( dim_date[quarter_end_date] )
VAR _latest = CALCULATE ( MAX ( fact_snapshot[period_end] ), REMOVEFILTERS ( dim_date ) )
RETURN
    IF ( ISBLANK ( _picked ), _latest, MIN ( _picked, _latest ) )"""),
    ("Report Title", "Selection", None, "Dynamic subtitle for page headers.",
     '"As of " & FORMAT ( [As Of Date], "mmmm d, yyyy" ) & "  ·  " & SWITCH ( [Selected Currency], "LOCAL", "fund currency", "LE", "legal-entity currency", [Selected Currency] )'),

    ("Commitment", "Capital", AMT, "Commitments made on or before the as-of date (converted at commitment-date FX).",
     f"""VAR _asof = [As Of Date]
VAR _ccy = [Selected Currency]
RETURN
    CALCULATE ( {cur_switch("fact_commitment", "commitment")}, fact_commitment[commitment_date] <= _asof )"""),
    ("Paid In", "Capital", AMT, "Capital called including fees, from inception to the as-of date.",
     f"""VAR _asof = [As Of Date]
VAR _ccy = [Selected Currency]
RETURN
    CALCULATE (
        {cur_switch("fact_cashflow", "amount")},
        fact_cashflow[flow_category] = "PAID_IN",
        REMOVEFILTERS ( dim_date ),
        fact_cashflow[cashflow_date] <= _asof
    )"""),
    ("Distributions", "Capital", AMT, "Return of capital + realised gains + income distributed, inception to as-of date.",
     f"""VAR _asof = [As Of Date]
VAR _ccy = [Selected Currency]
RETURN
    CALCULATE (
        {cur_switch("fact_cashflow", "amount")},
        fact_cashflow[flow_category] = "DISTRIBUTED",
        REMOVEFILTERS ( dim_date ),
        fact_cashflow[cashflow_date] <= _asof
    )"""),
    ("NAV", "Capital", AMT, "Capital-account ending balance at the as-of quarter end (period-end FX).",
     f"""VAR _asof = [As Of Date]
VAR _ccy = [Selected Currency]
RETURN
    CALCULATE ( {cur_switch("fact_snapshot", "nav")}, REMOVEFILTERS ( dim_date ), fact_snapshot[period_end] = _asof )"""),
    ("Unfunded", "Capital", AMT, "Commitment − Paid In.", "[Commitment] - [Paid In]"),
    ("Total Value", "Capital", AMT, "Distributions + NAV.", "[Distributions] + [NAV]"),

    ("DPI", "Performance", MULT, "Distributed to paid-in.", "DIVIDE ( [Distributions], [Paid In] )"),
    ("RVPI", "Performance", MULT, "Residual value (NAV) to paid-in.", "DIVIDE ( [NAV], [Paid In] )"),
    ("TVPI", "Performance", MULT, "Total value to paid-in = DPI + RVPI.", "DIVIDE ( [Distributions] + [NAV], [Paid In] )"),
    ("Net IRR", "Performance", PCT,
     "LP net IRR: XIRR over calls (−), distributions (+) and NAV (+) at the as-of date, ACT/365.", flow_irr("INVESTOR")),
    ("Gross IRR", "Performance", PCT,
     "Deal-level gross IRR: XIRR over investments (−), proceeds and income (+) and fair value + accrued (+).",
     flow_irr("INVESTMENT")),
    ("Gross MOIC", "Performance", MULT, "Deal-level (proceeds + income + fair value) / invested.",
     """VAR _asof = [As Of Date]
VAR _ccy = [Selected Currency]
VAR _t =
    ADDCOLUMNS (
        CALCULATETABLE (
            fact_irr_flow,
            fact_irr_flow[level] = "INVESTMENT",
            REMOVEFILTERS ( dim_date ),
            fact_irr_flow[as_of_date] = _asof
        ),
        "@amt",
            SWITCH (
                _ccy,
                "USD", fact_irr_flow[amount_usd],
                "CAD", fact_irr_flow[amount_cad],
                "LOCAL", fact_irr_flow[amount_local]
            )
    )
RETURN
    DIVIDE ( SUMX ( FILTER ( _t, [@amt] > 0 ), [@amt] ), - SUMX ( FILTER ( _t, [@amt] < 0 ), [@amt] ) )"""),

    ("Cost Basis", "Investments", AMT, "Remaining cost of investments held at the as-of date.",
     f"""VAR _asof = [As Of Date]
VAR _ccy = [Selected Currency]
RETURN
    CALCULATE ( {cur_switch("fact_position", "cost", le=False)}, REMOVEFILTERS ( dim_date ), fact_position[as_of_date] = _asof )"""),
    ("Fair Value", "Investments", AMT, "Fair value of investments held at the as-of date.",
     f"""VAR _asof = [As Of Date]
VAR _ccy = [Selected Currency]
RETURN
    CALCULATE ( {cur_switch("fact_position", "fair_value", le=False)}, REMOVEFILTERS ( dim_date ), fact_position[as_of_date] = _asof )"""),
    ("Unrealized Gain", "Investments", AMT, "Fair Value − Cost Basis.", "[Fair Value] - [Cost Basis]"),
    ("Mark to Cost", "Investments", MULT, "Fair Value / Cost Basis.", "DIVIDE ( [Fair Value], [Cost Basis] )"),
    ("Principal Outstanding", "Private Credit", AMT, "Loan principal outstanding at the as-of date (incl. PIK).",
     f"""VAR _asof = [As Of Date]
VAR _ccy = [Selected Currency]
RETURN
    CALCULATE ( {cur_switch("fact_accrual", "principal", le=False)}, REMOVEFILTERS ( dim_date ), fact_accrual[as_of_date] = _asof )"""),
    ("Accrued Interest", "Private Credit", AMT, "Interest earned but not yet received at the as-of date.",
     f"""VAR _asof = [As Of Date]
VAR _ccy = [Selected Currency]
RETURN
    CALCULATE ( {cur_switch("fact_accrual", "accrued_interest", le=False)}, REMOVEFILTERS ( dim_date ), fact_accrual[as_of_date] = _asof )"""),
    ("Weighted Avg Coupon", "Private Credit", "0.00%", "Principal-weighted coupon of loans held at the as-of date.",
     """VAR _asof = [As Of Date]
RETURN
    CALCULATE (
        DIVIDE ( SUMX ( fact_accrual, fact_accrual[principal_usd] * fact_accrual[coupon_rate] ), SUM ( fact_accrual[principal_usd] ) ),
        REMOVEFILTERS ( dim_date ),
        fact_accrual[as_of_date] = _asof
    )"""),
    ("Non-Performing %", "Private Credit", "0.0%", "Share of principal on non-performing (PIK / impaired) loans.",
     """VAR _asof = [As Of Date]
RETURN
    CALCULATE (
        DIVIDE (
            CALCULATE ( SUM ( fact_accrual[principal_usd] ), fact_accrual[loan_status] = "NON_PERFORMING" ),
            SUM ( fact_accrual[principal_usd] )
        ),
        REMOVEFILTERS ( dim_date ),
        fact_accrual[as_of_date] = _asof
    )"""),

    ("Bridge Amount", "NAV Bridge", AMT,
     "NAV bridge component in the selected currency for the selected quarter (closing NAV is the waterfall total).",
     """VAR _ccy = [Selected Currency]
VAR _asof = [As Of Date]
VAR _ok =
    SWITCH (
        _ccy,
        "LOCAL", CALCULATE ( DISTINCTCOUNT ( fact_snapshot[local_currency] ), REMOVEFILTERS ( dim_date ) ) = 1,
        "LE", CALCULATE ( DISTINCTCOUNT ( fact_snapshot[le_currency] ), REMOVEFILTERS ( dim_date ) ) = 1,
        TRUE ()
    )
RETURN
    IF (
        _ok,
        CALCULATE (
            SUM ( gold_nav_bridge[amount] ),
            gold_nav_bridge[currency_code] = _ccy,
            gold_nav_bridge[is_closing] = FALSE (),
            REMOVEFILTERS ( dim_date ),
            gold_nav_bridge[period_end] = _asof
        )
    )"""),

    ("Checks Passed (latest run)", "Data Quality", "0", "Quality-gate checks that passed in the most recent run.",
     """VAR _run = CALCULATE ( MAX ( ops_dq_results[run_id] ), ALL ( ops_dq_results ) )
RETURN
    CALCULATE ( COUNTROWS ( ops_dq_results ), ops_dq_results[run_id] = _run, ops_dq_results[passed] = TRUE () )"""),
    ("Checks Failed (latest run)", "Data Quality", "0", "Quality-gate checks that failed in the most recent run.",
     """VAR _run = CALCULATE ( MAX ( ops_dq_results[run_id] ), ALL ( ops_dq_results ) )
RETURN
    CALCULATE ( COUNTROWS ( ops_dq_results ), ops_dq_results[run_id] = _run, ops_dq_results[passed] = FALSE () ) + 0"""),
]

ROLES = {
    "LP Investor": {
        "dim_investor": "dim_investor[investor_id] IN CALCULATETABLE ( VALUES ( sec_user_investor[investor_id] ), "
                        "sec_user_investor[user_upn] = USERPRINCIPALNAME () )",
        "sec_user_investor": "sec_user_investor[user_upn] = USERPRINCIPALNAME ()",
        "ops_dq_results": "FALSE ()",
    },
    "Portfolio Team": {},
}

# ----------------------------------------------------------------------------------------------
# TMDL writer
# ----------------------------------------------------------------------------------------------
TYPE_MAP = {"string": "string", "large_string": "string", "int32": "int64", "int64": "int64", "double": "double",
            "float": "double", "bool": "boolean", "date32[day]": "dateTime"}


RESERVED = {"date", "level", "currency", "measure", "table", "column"}


def q(name):
    if name.lower() in RESERVED:
        return "'" + name + "'"
    return name if name.replace("_", "").isalnum() and not name[0].isdigit() else "'" + name.replace("'", "''") + "'"


def indent(text, n):
    return "\n".join(("\t" * n + ln) if ln.strip() else "" for ln in text.splitlines())


def schema(table):
    files = sorted((LAKE / table).glob("*.parquet"))
    if not files:
        raise SystemExit(f"no local parquet for {table}: run `python local/run_local.py --fresh` first")
    s = pq.read_schema(files[0])
    return [(f.name, str(f.type)) for f in s]


def column_tmdl(table, col, arrow_type):
    dtype = TYPE_MAP.get(arrow_type.split("[")[0] if arrow_type.startswith("timestamp") else arrow_type, "string")
    if arrow_type.startswith("timestamp"):
        dtype = "dateTime"
    hidden = (table == "measure_table" or table in HIDDEN_TABLES
              or ((col.endswith("_id") or col.endswith("_key") or col.startswith("fx_") or col.startswith("_")
                   or col in ("source_id", "source_file", "quarter_sort", "month_sort", "component_order", "sort_order",
                              "file_version", "is_closing"))
                  and (table, col) not in VISIBLE_ID_COLUMNS))
    amount = dtype == "double" and col not in ("coupon_rate", "mgmt_fee_rate", "mark_to_cost", "usd_per_unit")
    lines = [f"column {q(col)}", f"\tdataType: {dtype}"]
    if dtype == "dateTime":
        lines.append("\tformatString: yyyy-mm-dd")
    elif amount:
        lines.append("\tformatString: #,0.00")
    elif col in ("coupon_rate", "mgmt_fee_rate"):
        lines.append("\tformatString: 0.00%")
    if hidden:
        lines.append("\tisHidden")
    if table == "dim_date" and col == "date":
        lines.append("\tisKey")
    lines += [f"\tlineageTag: {tag(table, col)}", f"\tsourceLineageTag: {col}",
              f"\tsummarizeBy: {'sum' if amount and not hidden else 'none'}", f"\tsourceColumn: {col}"]
    if (table, col) in SORT_BY:
        lines.append(f"\tsortByColumn: {SORT_BY[(table, col)]}")
    lines += ["", "\tannotation SummarizationSetBy = Automatic"]
    return "\n".join(lines)


def measure_tmdl(name, folder, fmt, desc, dax):
    out = [f"/// {desc}", f"measure {q(name)} ="]
    out.append(indent(dax, 2))
    if fmt:
        out.append(f"\tformatString: {fmt}")
    out.append(f"\tdisplayFolder: {folder}")
    out.append(f"\tlineageTag: {tag('measure', name)}")
    return "\n".join(out)


def table_tmdl(table):
    lines = [f"table {table}"]
    if table == "dim_date":
        lines.append("\tdataCategory: Time")
    if table in HIDDEN_TABLES:
        lines.append("\tisHidden")
    lines += [f"\tlineageTag: {tag(table)}", f"\tsourceLineageTag: [dbo].[{table}]", ""]
    if table == "measure_table":
        for m in MEASURES:
            lines += [indent(measure_tmdl(*m), 1), ""]
    for col, typ in schema(table):
        lines += [indent(column_tmdl(table, col, typ), 1), ""]
    lines += [f"\tpartition {table} = entity", "\t\tmode: directLake", "\t\tsource",
              f"\t\t\tentityName: {table}", "\t\t\tschemaName: dbo", "\t\t\texpressionSource: DatabaseQuery", ""]
    return "\n".join(lines)


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def platform(kind):
    return json.dumps({"$schema": PLATFORM_SCHEMA, "metadata": {"type": kind, "displayName": NAME},
                       "config": {"version": "2.0", "logicalId": tag("item", kind)}}, indent=2)


def build_model():
    sm = OUT / f"{NAME}.SemanticModel"
    if sm.exists():
        shutil.rmtree(sm)
    d = sm / "definition"
    write(sm / "definition.pbism", json.dumps({"version": "4.0", "settings": {}}, indent=2))
    write(sm / ".platform", platform("SemanticModel"))
    write(d / "database.tmdl", "database\n\tcompatibilityLevel: 1604\n")
    model = ["model Model", "\tculture: en-US", "\tdefaultPowerBIDataSourceVersion: powerBI_V3",
             "\tdiscourageImplicitMeasures", "\tsourceQueryCulture: en-US", "\tdataAccessOptions",
             "\t\tlegacyRedirects", "\t\treturnErrorValuesAsNull", "",
             'annotation PBI_QueryOrder = ["DatabaseQuery"]', "", "annotation __PBI_TimeIntelligenceEnabled = 0", ""]
    model += [f"ref table {t}" for t in TABLES] + [""] + [f"ref role {q(r)}" for r in ROLES]
    write(d / "model.tmdl", "\n".join(model))
    write(d / "expressions.tmdl", "\n".join([
        "expression DatabaseQuery =",
        "\t\tlet",
        '\t\t    database = Sql.Database("{{SQL_ENDPOINT}}", "{{SQL_ENDPOINT_ID}}")',
        "\t\tin",
        "\t\t    database",
        f"\tlineageTag: {tag('expr', 'DatabaseQuery')}", "",
        "\tannotation PBI_IncludeFutureArtifacts = False"]))
    for t in TABLES:
        write(d / "tables" / f"{t}.tmdl", table_tmdl(t))
    rel = []
    for f, fc, t, tc in RELATIONSHIPS:
        rel += [f"relationship {tag('rel', f, fc, t, tc)}", f"\tfromColumn: {f}.{fc}", f"\ttoColumn: {t}.{tc}", ""]
    write(d / "relationships.tmdl", "\n".join(rel))
    for role, perms in ROLES.items():
        lines = [f"role {q(role)}", "\tmodelPermission: read", ""]
        for tbl, expr in perms.items():
            lines += [f"\ttablePermission {tbl} = {expr}", ""]
        write(d / "roles" / f"{role}.tmdl", "\n".join(lines))


# ----------------------------------------------------------------------------------------------
# Report (legacy report.json layout, 1280×720 pages)
# ----------------------------------------------------------------------------------------------
ALIAS = {}


def src(entity):
    return ALIAS.setdefault(entity, "t" + str(len(ALIAS)))


def field(ref):
    """'measure_table.TVPI' or 'dim_fund.fund_name' → (entity, prop, is_measure)."""
    ent, prop = ref.split(".", 1)
    return ent, prop, ent == "measure_table"


def select_item(ref):
    ent, prop, is_m = field(ref)
    key = "Measure" if is_m else "Column"
    return {key: {"Expression": {"SourceRef": {"Source": src(ent)}}, "Property": prop}, "Name": ref,
            "NativeReferenceName": prop}


def visual(vid, vtype, x, y, w, h, roles=None, title=None, objects=None, order_by=None):
    ALIAS.clear()
    roles = roles or {}
    refs = [r for refs in roles.values() for r in refs]
    selects = [select_item(r) for r in dict.fromkeys(refs)]
    sv = {"visualType": vtype, "projections": {k: [{"queryRef": r} for r in v] for k, v in roles.items()},
          "drillFilterOtherVisuals": True}
    if refs:
        sv["prototypeQuery"] = {"Version": 2, "From": [{"Name": a, "Entity": e, "Type": 0} for e, a in ALIAS.items()],
                                "Select": selects}
        if order_by:
            ent, prop, is_m = field(order_by)
            sv["prototypeQuery"]["OrderBy"] = [{"Direction": 1, "Expression": {
                ("Measure" if is_m else "Column"): {"Expression": {"SourceRef": {"Source": src(ent)}}, "Property": prop}}}]
    objs = dict(objects or {})
    vc_objects = {}
    if title:
        vc_objects["title"] = [{"properties": {"show": lit("true"), "text": lit(f"'{title}'")}}]
    if vc_objects:
        sv["vcObjects"] = vc_objects
    if objs:
        sv["objects"] = objs
    cfg = {"name": vid, "layouts": [{"id": 0, "position": {"x": x, "y": y, "z": 0, "width": w, "height": h}}],
           "singleVisual": sv}
    return {"x": x, "y": y, "z": 0, "width": w, "height": h, "config": json.dumps(cfg), "filters": "[]"}


def lit(v):
    return {"expr": {"Literal": {"Value": v}}}


def textbox(vid, text, x, y, w, h, size="20pt", subtitle_measure=None):
    cfg = {"name": vid, "layouts": [{"id": 0, "position": {"x": x, "y": y, "z": 0, "width": w, "height": h}}],
           "singleVisual": {"visualType": "textbox", "drillFilterOtherVisuals": True, "objects": {"general": [
               {"properties": {"paragraphs": [{"textRuns": [{"value": text, "textStyle": {
                   "fontWeight": "bold", "fontSize": size}}]}]}}]}}}
    return {"x": x, "y": y, "z": 0, "width": w, "height": h, "config": json.dumps(cfg), "filters": "[]"}


def slicer(vid, ref, x, y, w=200, h=56, dropdown=True):
    objs = {"data": [{"properties": {"mode": lit("'Dropdown'" if dropdown else "'Basic'")}}]}
    return visual(vid, "slicer", x, y, w, h, {"Values": [ref]}, objects=objs)


def card(vid, ref, x, y, w=148, h=88):
    return visual(vid, "card", x, y, w, h, {"Values": [f"measure_table.{ref}"]}, title=ref)


SLICERS = [("dim_date.quarter_label", "Quarter"), ("dim_report_currency.currency_code", "Currency")]


def header(page, title):
    vs = [textbox(f"{page}_title", title, 16, 8, 560, 44),
          visual(f"{page}_sub", "card", 16, 52, 560, 40, {"Values": ["measure_table.Report Title"]})]
    for i, (ref, _) in enumerate(SLICERS):
        vs.append(slicer(f"{page}_s{i}", ref, 640 + i * 210, 12))
    return vs


def page(i, name, display, visuals):
    return {"name": name, "displayName": display, "displayOption": 1, "width": 1280, "height": 720,
            "ordinal": i, "config": "{}", "filters": "[]", "visualContainers": visuals}


def build_report():
    rp = OUT / f"{NAME}.Report"
    if rp.exists():
        shutil.rmtree(rp)
    write(rp / ".platform", platform("Report"))
    write(rp / "definition.pbir", json.dumps(
        {"version": "4.0", "datasetReference": {"byPath": {"path": f"../{NAME}.SemanticModel"}}}, indent=2))
    kpis = ["Commitment", "Paid In", "Distributions", "NAV", "Unfunded", "DPI", "TVPI", "Net IRR"]
    p1 = header("p1", "Portfolio Overview") + [
        slicer("p1_ac", "dim_fund.asset_class", 1060, 12, 200),
        *[card(f"p1_k{i}", k, 16 + i * 157, 104) for i, k in enumerate(kpis)],
        visual("p1_tbl", "tableEx", 16, 206, 760, 498, {"Values": [
            "dim_fund.fund_name", "dim_fund.asset_class", "dim_fund.vintage_year", "measure_table.Commitment",
            "measure_table.Paid In", "measure_table.Distributions", "measure_table.NAV", "measure_table.DPI",
            "measure_table.TVPI", "measure_table.Net IRR"]}, title="Fund performance (net to LPs)"),
        visual("p1_col", "clusteredColumnChart", 792, 206, 472, 498, {
            "Category": ["dim_fund.vintage_year"],
            "Y": ["measure_table.Paid In", "measure_table.Distributions", "measure_table.NAV"]},
            title="Paid-in, distributed and NAV by vintage"),
    ]
    p2 = header("p2", "Investor Performance") + [
        slicer("p2_inv", "dim_investor.investor_name", 1060, 12, 200),
        visual("p2_mx", "pivotTable", 16, 104, 1248, 600, {
            "Rows": ["dim_investor.investor_name", "dim_fund.fund_name"],
            "Values": ["measure_table.Commitment", "measure_table.Paid In", "measure_table.Unfunded",
                       "measure_table.Distributions", "measure_table.NAV", "measure_table.DPI", "measure_table.RVPI",
                       "measure_table.TVPI", "measure_table.Net IRR"]}, title="Investor × fund (net)"),
    ]
    p3 = header("p3", "NAV Bridge") + [
        slicer("p3_f", "dim_fund.fund_name", 16, 104, 300),
        slicer("p3_i", "dim_investor.investor_name", 332, 104, 300),
        visual("p3_wf", "waterfallChart", 16, 176, 820, 528, {
            "Category": ["gold_nav_bridge.component"], "Y": ["measure_table.Bridge Amount"]},
            title="Opening NAV → closing NAV for the quarter", order_by="gold_nav_bridge.component"),
        visual("p3_tbl", "tableEx", 852, 176, 412, 528, {
            "Values": ["gold_nav_bridge.component", "measure_table.Bridge Amount"]},
            title="Bridge components", order_by="gold_nav_bridge.component"),
    ]
    credit = ["Principal Outstanding", "Accrued Interest", "Weighted Avg Coupon", "Non-Performing %"]
    p4 = header("p4", "Investments") + [
        slicer("p4_ac", "dim_fund.asset_class", 1060, 12, 200),
        *[card(f"p4_k{i}", k, 16 + i * 200, 104, 190) for i, k in enumerate(["Cost Basis", "Fair Value", "Gross MOIC", "Gross IRR"])],
        *[card(f"p4_c{i}", k, 816 + (i % 2) * 226, 104 + (i // 2) * 96, 216, 88) for i, k in enumerate(credit)],
        visual("p4_tbl", "tableEx", 16, 300, 1248, 404, {"Values": [
            "dim_investment.investment_name", "dim_investment.fund_name", "dim_investment.instrument",
            "dim_investment.sector", "dim_investment.country", "measure_table.Cost Basis", "measure_table.Fair Value",
            "measure_table.Mark to Cost", "measure_table.Gross MOIC", "measure_table.Gross IRR"]},
            title="Investments held and realised (gross)"),
    ]
    p5 = [textbox("p5_title", "Data Quality", 16, 8, 560, 44),
          slicer("p5_run", "ops_dq_results.run_id", 640, 12, 420),
          card("p5_ok", "Checks Passed (latest run)", 16, 64, 240),
          card("p5_ko", "Checks Failed (latest run)", 272, 64, 240),
          visual("p5_tbl", "tableEx", 16, 168, 1248, 536, {"Values": [
              "ops_dq_results.check_id", "ops_dq_results.check_name", "ops_dq_results.layer",
              "ops_dq_results.severity", "ops_dq_results.passed", "ops_dq_results.failed_rows",
              "ops_dq_results.detail", "ops_dq_results.checked_at_utc"]}, title="Quality-gate results",
              order_by="ops_dq_results.check_id")]
    report = {
        "config": json.dumps({"version": "5.59", "activeSectionIndex": 0, "defaultDrillFilterOtherVisuals": True,
                              "settings": {"useStylableVisualContainerHeader": True, "exportDataMode": 1}}),
        "layoutOptimization": 0,
        "sections": [page(0, "ReportSection01", "Portfolio Overview", p1),
                     page(1, "ReportSection02", "Investor Performance", p2),
                     page(2, "ReportSection03", "NAV Bridge", p3),
                     page(3, "ReportSection04", "Investments", p4),
                     page(4, "ReportSection05", "Data Quality", p5)],
    }
    write(rp / "report.json", json.dumps(report, indent=2))
    write(OUT / f"{NAME}.pbip", json.dumps({"version": "1.0", "artifacts": [{"report": {"path": f"{NAME}.Report"}}],
                                            "settings": {"enableAutoRecovery": True}}, indent=2))
    write(OUT / ".gitignore", "**/.pbi/localSettings.json\n**/.pbi/cache.abf\n")


def build_metrics_doc():
    lines = ["# Metric catalogue", "",
             "Generated by `tools/build_powerbi.py` from the same definitions that produce the semantic model — "
             "edit there, not here.", "",
             "Python equivalents (used for `gold_performance` and the quality gate) live in `nb_04_gold` / `nb_common.xirr`.", ""]
    folder = None
    for name, fold, fmt, desc, dax in MEASURES:
        if fold != folder:
            lines += [f"## {fold}", ""]
            folder = fold
        lines += [f"### {name}", "", desc, "", f"Format: `{fmt or 'text'}`", "", "```dax", f"{name} =", dax, "```", ""]
    write(ROOT / "docs" / "METRICS.md", "\n".join(lines))


if __name__ == "__main__":
    build_model()
    build_report()
    build_metrics_doc()
    print(f"wrote {OUT}/{NAME}.SemanticModel, {NAME}.Report, {NAME}.pbip and docs/METRICS.md "
          f"({len(MEASURES)} measures, {len(RELATIONSHIPS)} relationships, {len(TABLES)} tables)")
