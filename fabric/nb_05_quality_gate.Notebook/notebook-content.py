# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_05_quality_gate — 18 reconciliation checks that gate every load
# Results go to `ops_dq_results` (shown on the report's *Data Quality* page). If any **ERROR** check
# fails, the notebook raises: `nb_99_run_all` stops and the Direct Lake model is **not** reframed, so
# report users keep seeing the last good quarter instead of a half-reconciled one.

# CELL ********************

# Parameters (injected by nb_99_run_all; blank = generate a new run id)
RUN_ID = ""

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

NOTEBOOK = "nb_05_quality_gate"
results = []


def check(check_id, name, layer, severity="ERROR"):
    def deco(fn):
        try:
            passed, failed_rows, detail = fn()
        except Exception as exc:  # a check that cannot run is a failed check
            passed, failed_rows, detail = False, -1, f"check errored: {type(exc).__name__}: {exc}"[:3900]
        results.append((RUN_ID, check_id, name, layer, severity, bool(passed), int(failed_rows), str(detail)[:3900],
                        dt.datetime.utcnow()))
        print(f"{'PASS' if passed else 'FAIL':<4} {severity:<5} {check_id} {name} — {detail}")
        return fn
    return deco


def failures(df, limit=5):
    n = df.count()
    sample = [r.asDict() for r in df.limit(limit).collect()] if n else []
    return n == 0, n, f"{n} failing rows" + (f"; e.g. {json.dumps(sample, default=str)[:600]}" if n else "")


TOL = AMOUNT_TOLERANCE
audit = read_table("ops_load_audit").filter(F.col("run_id") == RUN_ID)
s_ca, s_cf = read_table("silver_capital_account"), read_table("silver_cashflow")
fc, fs, fp = read_table("fact_cashflow"), read_table("fact_snapshot"), read_table("fact_position")
perf, bridge = read_table("gold_performance"), read_table("gold_nav_bridge")
latest = s_ca.agg(F.max("period_end")).first()[0]   # latest quarter delivered

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Ingestion and silver

# CELL ********************

@check("DQ01", "Bronze rows = silver + quarantined + superseded (this run)", "silver")
def _():
    rows = audit.filter((F.col("layer") == "silver") & F.col("detail").contains("superseded_or_duplicate")).collect()
    bad = [r.target_table for r in rows
           if r.rows_in != r.rows_out + r.rows_rejected + json.loads(r.detail)["superseded_or_duplicate"]]
    return not bad, len(bad), f"{len(rows)} silver loads balanced" if not bad else f"unbalanced: {bad}"


@check("DQ02", f"Quarantine rate ≤ {QUARANTINE_RATE_LIMIT:.0%} (this run)", "silver")
def _():
    rows = audit.filter((F.col("layer") == "silver") & F.col("detail").contains("superseded_or_duplicate")).collect()
    rin, rej = sum(r.rows_in for r in rows), sum(r.rows_rejected for r in rows)
    rate = rej / rin if rin else 0.0
    return rate <= QUARANTINE_RATE_LIMIT, rej, f"{rej} of {rin} rows quarantined ({rate:.2%})"


@check("DQ03", "No duplicate business keys in silver", "silver")
def _():
    d1 = s_ca.groupBy("fund_id", "investor_id", "period_end").count().filter("count > 1")
    d2 = s_cf.groupBy("cashflow_id").count().filter("count > 1")
    n = d1.count() + d2.count()
    return n == 0, n, f"{n} duplicated keys"


@check("DQ04", "Capital accounts roll forward (beg + flows + P&L = end)", "silver")
def _():
    roll = (F.col("beginning_capital") + F.col("contributions") - F.col("distributions") - F.col("management_fees")
            + F.col("net_income") + F.col("realized_gl") + F.col("unrealized_gl") - F.col("ending_capital"))
    return failures(s_ca.filter(F.abs(roll) > TOL).select("fund_id", "investor_id", "period_end"))


@check("DQ05", "Opening capital = prior quarter closing capital", "silver")
def _():
    w = Window.partitionBy("fund_id", "investor_id").orderBy("period_end")
    df = s_ca.withColumn("prev_end", F.lag("ending_capital").over(w))
    return failures(df.filter(F.col("prev_end").isNotNull() & (F.abs(F.col("beginning_capital") - F.col("prev_end")) > TOL))
                      .select("fund_id", "investor_id", "period_end", "beginning_capital", "prev_end"))


def notice_recon(category, ca_col):
    n = (s_cf.filter(F.col("flow_category") == category).groupBy("fund_id", "investor_id", "quarter")
         .agg(F.sum("amount_local").alias("notices")))
    j = s_ca.select("fund_id", "investor_id", "quarter", ca_col).join(n, ["fund_id", "investor_id", "quarter"], "full")
    return failures(j.filter(F.abs(F.coalesce(F.col(ca_col), F.lit(0.0)) - F.coalesce("notices", F.lit(0.0))) > TOL))


@check("DQ06", "Capital-account contributions = Σ call notices", "silver")
def _():
    return notice_recon("PAID_IN", "contributions")


@check("DQ07", "Capital-account distributions = Σ distribution notices", "silver")
def _():
    return notice_recon("DISTRIBUTED", "distributions")


@check("DQ08", "Fund NAV (Σ LP capital) = Σ investment fair value + accrued", "silver")
def _():
    lp = s_ca.groupBy("fund_id", "period_end").agg(F.sum("ending_capital").alias("lp_nav"), F.count("*").alias("n_lps"))
    pos = (read_table("silver_position").groupBy("fund_id", F.col("as_of_date").alias("period_end"))
           .agg(F.sum(F.col("fair_value") + F.coalesce("accrued_interest", F.lit(0.0))).alias("portfolio_nav")))
    j = lp.join(pos, ["fund_id", "period_end"], "full")
    return failures(j.filter(F.abs(F.coalesce("lp_nav", F.lit(0.0)) - F.coalesce("portfolio_nav", F.lit(0.0)))
                             > F.coalesce("n_lps", F.lit(1)) * 0.01 + TOL))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold

# CELL ********************

@check("DQ09", "Every amount has an FX rate (USD, CAD, legal entity)", "gold")
def _():
    n = (fc.filter(F.col("amount_usd").isNull() | F.col("amount_cad").isNull() | F.col("amount_le").isNull()).count()
         + fs.filter(F.col("nav_usd").isNull() | F.col("nav_cad").isNull() | F.col("nav_le").isNull()).count()
         + read_table("fact_commitment").filter(F.col("commitment_usd").isNull()).count()
         + fp.filter(F.col("fair_value_usd").isNull()).count())
    return n == 0, n, f"{n} rows without a rate"


@check("DQ10", "Referential integrity: facts → dimensions", "gold")
def _():
    funds, invs = read_table("dim_fund").select("fund_id"), read_table("dim_investor").select("investor_id")
    invest, dates = read_table("dim_investment").select("investment_id"), read_table("dim_date").select("date_key")
    orphans = 0
    for t in ["fact_cashflow", "fact_snapshot", "fact_commitment", "fact_position", "fact_accrual", "gold_nav_bridge"]:
        df = read_table(t)
        orphans += df.join(funds, "fund_id", "left_anti").count()
        orphans += df.join(dates, "date_key", "left_anti").count()
        if "investor_id" in df.columns:
            orphans += df.join(invs, "investor_id", "left_anti").count()
        if "investment_id" in df.columns:
            orphans += df.join(invest, "investment_id", "left_anti").count()
    return orphans == 0, orphans, f"{orphans} orphan keys"


@check("DQ11", "Gold cashflow totals = silver cashflow totals", "gold")
def _():
    a = s_cf.agg(F.sum("amount_local")).first()[0] or 0.0
    b = fc.agg(F.sum("amount_local")).first()[0] or 0.0
    return abs(a - b) <= TOL, int(abs(a - b) > TOL), f"silver {a:,.2f} vs gold {b:,.2f}"


@check("DQ12", "NAV bridge closes: closing = snapshot NAV and opening = prior closing", "gold")
def _():
    loc = bridge.filter(F.col("currency_code") == "LOCAL")
    closing = loc.filter("is_closing").select("fund_id", "investor_id", "period_end", F.col("amount").alias("closing"))
    c1 = closing.join(fs.select("fund_id", "investor_id", "period_end", "nav_local"), ["fund_id", "investor_id", "period_end"])
    bad1 = c1.filter(F.abs(F.col("closing") - F.col("nav_local")) > TOL)
    total = loc.groupBy("fund_id", "investor_id", "period_end").agg(
        F.sum(F.when(~F.col("is_closing"), F.col("amount"))).alias("sum_components"),
        F.sum(F.when(F.col("is_closing"), F.col("amount"))).alias("closing"))
    bad2 = total.filter(F.abs(F.col("sum_components") - F.col("closing")) > TOL)
    ok1, n1, _d = failures(bad1)
    ok2, n2, _d = failures(bad2)
    return ok1 and ok2, n1 + n2, f"{n1} closing mismatches, {n2} bridges that do not sum"


@check("DQ13", "FX translation is zero in fund currency and plausible in others", "gold", severity="WARN")
def _():
    fxr = bridge.filter(F.col("component") == "FX translation")
    local_nonzero = fxr.filter((F.col("currency_code") == "LOCAL") & (F.abs("amount") > TOL)).count()
    closing = bridge.filter("is_closing").select("fund_id", "investor_id", "period_end", "currency_code",
                                                 F.col("amount").alias("closing"))
    big = (fxr.join(closing, ["fund_id", "investor_id", "period_end", "currency_code"])
              .filter((F.col("closing") > 0) & (F.abs(F.col("amount") / F.col("closing")) > 0.15)).count())
    return local_nonzero + big == 0, local_nonzero + big, f"{local_nonzero} non-zero LOCAL, {big} > 15% of NAV"


@check("DQ14", "TVPI = DPI + RVPI", "gold")
def _():
    return failures(perf.filter(F.col("tvpi").isNotNull() & (F.abs(F.col("tvpi") - F.col("dpi") - F.col("rvpi")) > 1e-9))
                        .select("grain", "fund_id", "investor_id", "as_of_date"))


@check("DQ15", "Net IRR solved and in a sane range for every LP position (latest quarter)", "gold")
def _():
    df = perf.filter((F.col("grain") == "FUND_INVESTOR") & (F.col("as_of_date") == F.lit(latest)) & (F.col("paid_in") > 0))
    return failures(df.filter(F.col("irr").isNull() | (F.col("irr") < -0.5) | (F.col("irr") > 1.0))
                      .select("fund_id", "investor_id", "currency_code", "irr"))


@check("DQ16", "Paid-in does not exceed commitment (fund currency)", "gold", severity="WARN")
def _():
    # LOCAL only: in USD/CAD the commitment is at the commitment-date rate and calls at call-date rates,
    # so small negative unfunded amounts there are FX, not over-calling.
    return failures(perf.filter((F.col("currency_code") == "LOCAL") & (F.col("unfunded") < -TOL))
                        .select("grain", "fund_id", "investor_id", "as_of_date", "unfunded"))


@check("DQ17", "Freshness: every fund delivered the latest quarter", "gold")
def _():
    have = {r.fund_id for r in fs.filter(F.col("period_end") == F.lit(latest)).select("fund_id").distinct().collect()}
    expected = {r.fund_id for r in read_table("dim_fund").select("fund_id").collect()}
    missing = sorted(expected - have)
    return not missing, len(missing), (f"missing {latest}: {missing}" if missing
                                       else f"{len(have)} funds current at {latest}")


@check("DQ18", "Metric parity: gold_performance = facts (paid-in, distributed, NAV)", "gold")
def _():
    p = perf.filter((F.col("grain") == "FUND_INVESTOR") & (F.col("currency_code") == "USD") & (F.col("as_of_date") == F.lit(latest)))
    flows = (fc.filter(F.col("cashflow_date") <= F.lit(latest)).groupBy("fund_id", "investor_id")
             .agg(F.sum(F.when(F.col("flow_category") == "PAID_IN", F.col("amount_usd"))).alias("f_paid"),
                  F.sum(F.when(F.col("flow_category") == "DISTRIBUTED", F.col("amount_usd"))).alias("f_dist")))
    nav = fs.filter(F.col("period_end") == F.lit(latest)).select("fund_id", "investor_id", F.col("nav_usd").alias("f_nav"))
    j = p.join(flows, ["fund_id", "investor_id"], "left").join(nav, ["fund_id", "investor_id"], "left")
    return failures(j.filter((F.abs(F.col("paid_in") - F.coalesce("f_paid", F.lit(0.0))) > TOL)
                             | (F.abs(F.col("distributed") - F.coalesce("f_dist", F.lit(0.0))) > TOL)
                             | (F.abs(F.col("nav") - F.col("f_nav")) > TOL)).select("fund_id", "investor_id"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dq = spark.createDataFrame(results, read_table("ops_dq_results").schema)  # noqa: F821
append_table(dq, "ops_dq_results")
errors = [r for r in results if r[4] == "ERROR" and not r[5]]
warns = [r for r in results if r[4] == "WARN" and not r[5]]
log_audit(NOTEBOOK, "gate", "ops_dq_results", len(results), len(results) - len(errors) - len(warns), len(errors),
          status="FAILED" if errors else "SUCCEEDED",
          detail=json.dumps({"errors": [r[1] for r in errors], "warnings": [r[1] for r in warns]}))
if errors:
    raise RuntimeError(f"Quality gate failed: {[r[1] + ' ' + r[2] for r in errors]}")
print(f"Quality gate passed: {len(results)} checks, {len(warns)} warnings")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
