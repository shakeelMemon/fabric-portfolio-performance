# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_04_gold — star schema, IRR flows, performance and NAV bridge
# Rebuilt in full from silver every run (small, deterministic, easy to reconcile).
# **Defined once.** Every performance figure in the platform comes from the columns built here:
# sign convention, flow categories, FX rule and terminal value live in `fact_irr_flow`; the Power BI
# measures only SUM / DIVIDE / XIRR over them, and `gold_performance` (used for LP statements and the
# quality gate) is computed from the same table with the same XIRR convention.
# | Metric | Definition (investor perspective) |
# |---|---|
# | Paid-in | Σ capital calls incl. fees (`flow_category = PAID_IN`) to as-of date |
# | Distributed | Σ distributions (ROC + gain + income) to as-of date |
# | NAV | capital-account ending capital at the as-of quarter end |
# | DPI / RVPI / TVPI | Distributed / Paid-in · NAV / Paid-in · (Distributed + NAV) / Paid-in |
# | Net IRR | XIRR(calls −, distributions +, NAV + at as-of), ACT/365 |
# | Gross IRR / MOIC | same, on deal-level flows (investment −, proceeds/income +, FV + accrued) |
# | Unfunded | Commitment − Paid-in |
# **FX rule:** flows at the flow-date rate, balances at the period-end rate, P&L at the quarter-average
# rate; the difference in the NAV bridge is reported as *FX translation*. Currencies: `LOCAL` (fund),
# `USD`, `CAD`, `LE` (the investor's legal-entity functional currency).


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

NOTEBOOK = "nb_04_gold"
CURRENCY_CODES = ["LOCAL", "USD", "CAD", "LE"]


def date_key(c):
    return F.date_format(c, "yyyyMMdd").cast("int")


fx = read_table("silver_fx_rate").select("rate_date", "currency", "usd_per_unit").cache()
fx_qavg = (fx.withColumn("quarter", F.concat(F.year("rate_date"), F.lit("Q"), F.quarter("rate_date")))
             .groupBy("currency", "quarter").agg(F.avg("usd_per_unit").alias("usd_per_unit_avg")))

s_fund = read_table("silver_fund")
s_inv = read_table("silver_investor")
s_le = read_table("silver_legal_entity")
s_investment = read_table("silver_investment")
s_cmt = read_table("silver_commitment")
s_ca = read_table("silver_capital_account")
s_cf = read_table("silver_cashflow")
s_pos = read_table("silver_position")
s_dtx = read_table("silver_deal_transaction")

investor_le = (s_inv.select("investor_id", "legal_entity_id")
               .join(s_le.select("legal_entity_id", F.col("functional_currency").alias("le_currency")), "legal_entity_id"))


def with_fx(df, date_col, local_col, le_col=None):
    df = add_fx_rate(df, fx, date_col, local_col, "USD", "fx_usd")
    df = add_fx_rate(df, fx, date_col, local_col, "CAD", "fx_cad")
    if le_col:
        df = add_fx_rate(df, fx, date_col, local_col, le_col, "fx_le")
    return df


def converted(amount_col, prefix):
    return [F.col(amount_col).alias(f"{prefix}_local"),
            F.round(F.col(amount_col) * F.col("fx_usd"), 2).alias(f"{prefix}_usd"),
            F.round(F.col(amount_col) * F.col("fx_cad"), 2).alias(f"{prefix}_cad"),
            F.round(F.col(amount_col) * F.col("fx_le"), 2).alias(f"{prefix}_le")]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dimensions

# CELL ********************

latest_period_end = s_ca.agg(F.max("period_end")).first()[0]
dim_date = (spark.sql("SELECT explode(sequence(to_date('2022-01-01'), to_date('2026-12-31'), interval 1 day)) AS date")  # noqa: F821
            .withColumn("date_key", date_key("date"))
            .withColumn("year", F.year("date"))
            .withColumn("quarter_number", F.quarter("date"))
            .withColumn("quarter_label", F.concat(F.year("date"), F.lit(" Q"), F.quarter("date")))
            .withColumn("quarter_sort", F.year("date") * 10 + F.quarter("date"))
            .withColumn("quarter_end_date", F.last_day(F.make_date(F.year("date"), F.quarter("date") * 3, F.lit(1))))
            .withColumn("month_number", F.month("date"))
            .withColumn("month_label", F.date_format("date", "MMM yyyy"))
            .withColumn("month_sort", F.year("date") * 100 + F.month("date"))
            .withColumn("is_quarter_end", F.col("date") == F.col("quarter_end_date"))
            .withColumn("is_reported_quarter", F.col("quarter_end_date") <= F.lit(latest_period_end)))

fund_size = s_cmt.groupBy("fund_id").agg(F.sum("commitment_amount").alias("fund_size_local"))
dim_fund = (s_fund.join(fund_size, "fund_id", "left")
            .select("fund_id", "fund_name", "asset_class", "strategy", "vintage_year", "fund_currency",
                    "administrator", "first_close_date", "mgmt_fee_rate", "fund_size_local"))
dim_legal_entity = s_le.select("legal_entity_id", "legal_entity_name", "jurisdiction", "functional_currency")
dim_investor = (s_inv.join(dim_legal_entity, "legal_entity_id", "left")
                .select("investor_id", "investor_name", "investor_type", "domicile", "legal_entity_id",
                        "legal_entity_name", F.col("functional_currency").alias("legal_entity_currency")))
dim_investment = (s_investment.join(s_fund.select("fund_id", "fund_name"), "fund_id", "left")
                  .select("investment_id", "investment_name", "fund_id", "fund_name", "asset_class", "instrument",
                          "sector", "country", "currency", "investment_date", "maturity_date", "coupon_rate"))
dim_report_currency = spark.createDataFrame(  # noqa: F821
    [("USD", "US dollar", 1), ("CAD", "Canadian dollar", 2), ("LOCAL", "Fund currency", 3),
     ("LE", "Legal-entity currency", 4)], "currency_code string, currency_label string, sort_order int")
measure_table = spark.createDataFrame([(1, "Measures live on this table")], "id int, note string")  # noqa: F821
# Row-level security: which investor(s) each LP-portal user may see. Replace with your real UPNs.
sec_user_investor = spark.createDataFrame(  # noqa: F821
    [("lp.maple@contoso.com", "INV001"), ("lp.prairie@contoso.com", "INV002"),
     ("lp.greatlakes@contoso.com", "INV005"), ("lp.nordlys@contoso.com", "INV007")],
    "user_upn string, investor_id string")

for df_, name in [(dim_date, "dim_date"), (dim_fund, "dim_fund"), (dim_legal_entity, "dim_legal_entity"),
                  (dim_investor, "dim_investor"), (dim_investment, "dim_investment"),
                  (dim_report_currency, "dim_report_currency"), (measure_table, "measure_table"),
                  (sec_user_investor, "sec_user_investor")]:
    write_table(df_, name)
    n = read_table(name).count()
    log_audit(NOTEBOOK, "gold", name, n, n)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Facts — commitments, cashflows, capital-account snapshots

# CELL ********************

fact_commitment = (with_fx(s_cmt.join(investor_le.drop("legal_entity_id"), "investor_id")
                                .withColumnRenamed("currency", "local_currency"),
                           "commitment_date", "local_currency", "le_currency")
                   .select("commitment_id", "investor_id", "fund_id", "legal_entity_id", "commitment_date",
                           date_key("commitment_date").alias("date_key"), "local_currency", "le_currency",
                           *converted("commitment_amount", "commitment")))

fact_cashflow = (with_fx(s_cf.join(investor_le, "investor_id").withColumnRenamed("currency", "local_currency"),
                         "cashflow_date", "local_currency", "le_currency")
                 .select("cashflow_id", "fund_id", "investor_id", "legal_entity_id", "cashflow_date",
                         date_key("cashflow_date").alias("date_key"), "quarter", "cashflow_type", "flow_category",
                         "local_currency", "le_currency", *converted("amount_local", "amount"),
                         "fx_usd", "fx_cad", "fx_le", F.col("_source_id").alias("source_id"),
                         F.col("_source_file").alias("source_file")))

fact_snapshot = (with_fx(s_ca.join(investor_le, "investor_id").withColumnRenamed("currency", "local_currency"),
                         "period_end", "local_currency", "le_currency")
                 .select("fund_id", "investor_id", "legal_entity_id", "period_end",
                         date_key("period_end").alias("date_key"), "quarter", "local_currency", "le_currency",
                         "beginning_capital", "contributions", "distributions", "management_fees", "net_income",
                         "realized_gl", "unrealized_gl", *converted("ending_capital", "nav"),
                         F.col("_source_id").alias("source_id"), F.col("_source_file").alias("source_file"),
                         F.col("_file_version").alias("file_version")))

for df_, name in [(fact_commitment, "fact_commitment"), (fact_cashflow, "fact_cashflow"),
                  (fact_snapshot, "fact_snapshot")]:
    write_table(df_, name)
    n = read_table(name).count()
    log_audit(NOTEBOOK, "gold", name, n, n)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Facts — investment positions and private-credit accruals

# CELL ********************

fund_ccy = s_fund.select("fund_id", F.col("fund_currency").alias("local_currency"))
interest_qtd = (s_dtx.filter(F.col("txn_type") == "INCOME")
                .withColumn("as_of_date", F.last_day(F.make_date(F.year("txn_date"), F.quarter("txn_date") * 3, F.lit(1))))
                .groupBy("investment_id", "as_of_date").agg(F.sum("amount").alias("cash_income_qtd")))
pos = (with_fx(s_pos.drop("currency").join(fund_ccy, "fund_id"), "as_of_date", "local_currency")
       .withColumn("fx_le", F.lit(None).cast("double"))
       .join(interest_qtd, ["investment_id", "as_of_date"], "left")
       .withColumn("cash_income_qtd", F.coalesce("cash_income_qtd", F.lit(0.0)))
       .withColumn("total_value", F.col("fair_value") + F.coalesce("accrued_interest", F.lit(0.0))))
fact_position = pos.select("investment_id", "fund_id", "as_of_date", date_key("as_of_date").alias("date_key"),
                           "local_currency", *converted("cost_basis", "cost"), *converted("fair_value", "fair_value"),
                           *converted("total_value", "total_value"),
                           (F.col("fair_value") - F.col("cost_basis")).alias("unrealized_gain_local"),
                           F.round(F.col("fair_value") / F.col("cost_basis"), 4).alias("mark_to_cost"),
                           "fx_usd", "fx_cad").drop("cost_le", "fair_value_le", "total_value_le")
fact_accrual = (pos.filter(F.col("principal_outstanding").isNotNull())
                .select("investment_id", "fund_id", "as_of_date", date_key("as_of_date").alias("date_key"),
                        "local_currency", "loan_status", "coupon_rate", *converted("principal_outstanding", "principal"),
                        *converted("accrued_interest", "accrued_interest"), *converted("cash_income_qtd", "interest_received_qtd"),
                        F.col("pik_interest_qtd").alias("pik_interest_qtd_local"))
                .drop("principal_le", "accrued_interest_le", "interest_received_qtd_le"))
for df_, name in [(fact_position, "fact_position"), (fact_accrual, "fact_accrual")]:
    write_table(df_, name)
    n = read_table(name).count()
    log_audit(NOTEBOOK, "gold", name, n, n)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## `fact_irr_flow` — one row per flow per as-of quarter
# For every as-of quarter end Q: all flows dated ≤ Q, plus the terminal value at Q. This lets DAX
# `XIRR` answer any slice (fund, investor, asset class, legal entity, currency) at any as-of date.

# CELL ********************

fc, fs, fp = read_table("fact_cashflow"), read_table("fact_snapshot"), read_table("fact_position")
sign = F.when(F.col("flow_category") == "PAID_IN", -1.0).otherwise(1.0)
amt_cols = ["amount_local", "amount_usd", "amount_cad", "amount_le"]

as_of = fs.select("fund_id", "investor_id", F.col("period_end").alias("as_of_date"))
lp_flows = (fc.join(as_of, ["fund_id", "investor_id"]).filter(F.col("cashflow_date") <= F.col("as_of_date"))
            .select("as_of_date", F.lit("INVESTOR").alias("level"), "fund_id", "investor_id", "legal_entity_id",
                    F.lit(None).cast("string").alias("investment_id"), F.col("cashflow_date").alias("flow_date"),
                    F.col("flow_category").alias("flow_kind"), "local_currency", "le_currency",
                    *[(F.col(c) * sign).alias(c) for c in amt_cols]))
lp_terminal = fs.select(F.col("period_end").alias("as_of_date"), F.lit("INVESTOR").alias("level"), "fund_id",
                        "investor_id", "legal_entity_id", F.lit(None).cast("string").alias("investment_id"),
                        F.col("period_end").alias("flow_date"), F.lit("TERMINAL_NAV").alias("flow_kind"),
                        "local_currency", "le_currency", F.col("nav_local").alias("amount_local"), F.col("nav_usd").alias("amount_usd"),
                        F.col("nav_cad").alias("amount_cad"), F.col("nav_le").alias("amount_le"))

quarter_ends = fs.select(F.col("period_end").alias("as_of_date")).distinct()
deal = with_fx(s_dtx.join(fund_ccy, "fund_id"), "txn_date", "local_currency").withColumn("fx_le", F.lit(None).cast("double"))
deal_sign = F.when(F.col("txn_type") == "INVESTMENT", -1.0).otherwise(1.0)
deal = deal.withColumn("amount", F.col("amount") * deal_sign)
deal_flows = (deal.crossJoin(F.broadcast(quarter_ends)).filter(F.col("txn_date") <= F.col("as_of_date"))
              .select("as_of_date", F.lit("INVESTMENT").alias("level"), "fund_id",
                      F.lit(None).cast("string").alias("investor_id"), F.lit(None).cast("string").alias("legal_entity_id"),
                      "investment_id", F.col("txn_date").alias("flow_date"), F.col("txn_type").alias("flow_kind"),
                      "local_currency", F.lit(None).cast("string").alias("le_currency"), *converted("amount", "amount")))
deal_terminal = fp.select(F.col("as_of_date"), F.lit("INVESTMENT").alias("level"), "fund_id",
                          F.lit(None).cast("string").alias("investor_id"), F.lit(None).cast("string").alias("legal_entity_id"),
                          "investment_id", F.col("as_of_date").alias("flow_date"), F.lit("TERMINAL_VALUE").alias("flow_kind"),
                          "local_currency", F.lit(None).cast("string").alias("le_currency"),
                          F.col("total_value_local").alias("amount_local"),
                          F.col("total_value_usd").alias("amount_usd"), F.col("total_value_cad").alias("amount_cad"),
                          F.lit(None).cast("double").alias("amount_le"))

fact_irr_flow = (lp_flows.unionByName(lp_terminal).unionByName(deal_flows).unionByName(deal_terminal)
                 .withColumn("as_of_date_key", date_key("as_of_date"))
                 .withColumn("is_terminal", F.col("flow_kind").startswith("TERMINAL")))
write_table(fact_irr_flow, "fact_irr_flow")
n = read_table("fact_irr_flow").count()
log_audit(NOTEBOOK, "gold", "fact_irr_flow", n, n)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## `gold_performance` — precomputed metrics at standard grains (XIRR via `applyInPandas`)

# CELL ********************

irr = read_table("fact_irr_flow")
long = None
for code in CURRENCY_CODES:
    part = irr.select("level", "as_of_date", "fund_id", "investor_id", "investment_id", "local_currency", "le_currency",
                      "flow_date", "flow_kind", "is_terminal", F.lit(code).alias("currency_code"),
                      F.col(f"amount_{code.lower()}").alias("amount")).filter(F.col("amount").isNotNull())
    long = part if long is None else long.unionByName(part)

GRAINS = {   # grain -> (level, keys kept; others set to 'ALL')
    "FUND_INVESTOR": ("INVESTOR", ["fund_id", "investor_id"]),
    "FUND_NET": ("INVESTOR", ["fund_id"]),
    "INVESTOR": ("INVESTOR", ["investor_id"]),
    "PORTFOLIO_NET": ("INVESTOR", []),
    "INVESTMENT": ("INVESTMENT", ["fund_id", "investment_id"]),
    "FUND_GROSS": ("INVESTMENT", ["fund_id"]),
}
grain_frames = []
for grain, (level, keys) in GRAINS.items():
    g = long.filter(F.col("level") == level).withColumn("grain", F.lit(grain))
    for k in ["fund_id", "investor_id", "investment_id"]:
        if k not in keys:
            g = g.withColumn(k, F.lit("ALL"))
    grain_frames.append(g)
perf_input = grain_frames[0]
for g in grain_frames[1:]:
    perf_input = perf_input.unionByName(g)

PERF_SCHEMA = ("grain string, as_of_date date, fund_id string, investor_id string, investment_id string, "
               "currency_code string, currency_count int, paid_in double, distributed double, nav double, irr double")


def perf_udf(pdf):
    paid_in = -pdf.loc[(~pdf.is_terminal) & (pdf.amount < 0), "amount"].sum()
    distributed = pdf.loc[(~pdf.is_terminal) & (pdf.amount > 0), "amount"].sum()
    nav = pdf.loc[pdf.is_terminal, "amount"].sum()
    rate = xirr(list(pdf.flow_date), list(pdf.amount))
    first = pdf.iloc[0]
    return pd.DataFrame([{"grain": first.grain, "as_of_date": first.as_of_date, "fund_id": first.fund_id,
                          "investor_id": first.investor_id, "investment_id": first.investment_id,
                          "currency_code": first.currency_code,
                          "currency_count": int((pdf.le_currency if first.currency_code == "LE" else pdf.local_currency).nunique()),
                          "paid_in": float(paid_in), "distributed": float(distributed), "nav": float(nav),
                          "irr": None if rate is None else float(rate)}])


perf = (perf_input.groupBy("grain", "as_of_date", "fund_id", "investor_id", "investment_id", "currency_code")
        .applyInPandas(perf_udf, schema=PERF_SCHEMA)
        # LOCAL / LE are meaningless when a grain mixes currencies: drop rather than report a wrong number.
        .filter(~(F.col("currency_code").isin("LOCAL", "LE") & (F.col("currency_count") > 1))))

# commitments by grain (investor-level grains only), converted at commitment-date rates
cmt = read_table("fact_commitment")
cmt_long = None
for code in CURRENCY_CODES:
    part = cmt.select("fund_id", "investor_id", "commitment_date", F.lit(code).alias("currency_code"),
                      F.col(f"commitment_{code.lower()}").alias("commitment"))
    cmt_long = part if cmt_long is None else cmt_long.unionByName(part)
cmt_frames = []
for grain, (level, keys) in GRAINS.items():
    if level != "INVESTOR":
        continue
    c = cmt_long
    for k in ["fund_id", "investor_id"]:
        if k not in keys:
            c = c.withColumn(k, F.lit("ALL"))
    cmt_frames.append(c.withColumn("grain", F.lit(grain)).withColumn("investment_id", F.lit("ALL")))
cmt_grain = cmt_frames[0]
for c in cmt_frames[1:]:
    cmt_grain = cmt_grain.unionByName(c)
cmt_asof = (perf.select("grain", "as_of_date", "fund_id", "investor_id", "investment_id", "currency_code")
            .join(cmt_grain, ["grain", "fund_id", "investor_id", "investment_id", "currency_code"])
            .filter(F.col("commitment_date") <= F.col("as_of_date"))
            .groupBy("grain", "as_of_date", "fund_id", "investor_id", "investment_id", "currency_code")
            .agg(F.sum("commitment").alias("commitment")))

gold_performance = (perf.join(cmt_asof, ["grain", "as_of_date", "fund_id", "investor_id", "investment_id", "currency_code"], "left")
                    .withColumn("unfunded", F.when(F.col("commitment").isNotNull(), F.col("commitment") - F.col("paid_in")))
                    .withColumn("dpi", F.when(F.col("paid_in") > 0, F.col("distributed") / F.col("paid_in")))
                    .withColumn("rvpi", F.when(F.col("paid_in") > 0, F.col("nav") / F.col("paid_in")))
                    .withColumn("tvpi", F.when(F.col("paid_in") > 0, (F.col("distributed") + F.col("nav")) / F.col("paid_in")))
                    .withColumn("as_of_date_key", date_key("as_of_date"))
                    .drop("currency_count"))
write_table(gold_performance, "gold_performance")
n = read_table("gold_performance").count()
log_audit(NOTEBOOK, "gold", "gold_performance", n, n)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## `gold_nav_bridge` — opening NAV → closing NAV per fund × investor × quarter × currency

# CELL ********************

snap = read_table("fact_snapshot")
flows_q = (read_table("fact_cashflow").groupBy("fund_id", "investor_id", "quarter")
           .pivot("flow_category", ["PAID_IN", "DISTRIBUTED"])
           .agg(*[F.sum(f"amount_{c.lower()}").alias(c) for c in CURRENCY_CODES]))
b = (snap.join(flows_q, ["fund_id", "investor_id", "quarter"], "left")
         .withColumn("prev_qe", F.last_day(F.add_months("period_end", -3))))
b = add_fx_rate(b, fx, "prev_qe", "local_currency", "USD", "open_usd")
b = add_fx_rate(b, fx, "prev_qe", "local_currency", "CAD", "open_cad")
b = add_fx_rate(b, fx, "prev_qe", "local_currency", "le_currency", "open_le")
qa = fx_qavg.select(F.col("currency").alias("_c"), F.col("quarter").alias("_q"), F.col("usd_per_unit_avg").alias("_a"))
b = (b.join(qa.withColumnRenamed("_a", "avg_local_usd"), (F.col("_c") == F.col("local_currency")) & (F.col("_q") == F.col("quarter")), "left").drop("_c", "_q")
      .join(qa.filter("_c = 'CAD'").select("_q", F.col("_a").alias("avg_cad_usd")), F.col("_q") == F.col("quarter"), "left").drop("_q")
      .join(qa.select(F.col("_c").alias("_lc"), "_q", F.col("_a").alias("avg_le_usd")),
            (F.col("_lc") == F.col("le_currency")) & (F.col("_q") == F.col("quarter")), "left").drop("_lc", "_q"))

RATES = {"LOCAL": (F.lit(1.0), F.lit(1.0)),
         "USD": (F.col("open_usd"), F.col("avg_local_usd")),
         "CAD": (F.col("open_cad"), F.col("avg_local_usd") / F.col("avg_cad_usd")),
         "LE": (F.col("open_le"), F.col("avg_local_usd") / F.col("avg_le_usd"))}
bridge_parts = []
for code, (open_rate, avg_rate) in RATES.items():
    sfx = code.lower()
    opening = F.round(F.col("beginning_capital") * F.coalesce(open_rate, F.lit(0.0)), 2)
    contrib = F.coalesce(F.col(f"PAID_IN_{code}"), F.lit(0.0)) if code != "LOCAL" else F.col("contributions")
    distrib = -(F.coalesce(F.col(f"DISTRIBUTED_{code}"), F.lit(0.0)) if code != "LOCAL" else F.col("distributions"))
    fees = F.round(-F.col("management_fees") * avg_rate, 2)
    income = F.round(F.col("net_income") * avg_rate, 2)
    real = F.round(F.col("realized_gl") * avg_rate, 2)
    unreal = F.round(F.col("unrealized_gl") * avg_rate, 2)
    closing = F.col(f"nav_{sfx}")
    fx_tr = F.round(closing - (opening + contrib + distrib + fees + income + real + unreal), 2)
    comps = [("Opening NAV", 1, opening, False), ("Contributions", 2, contrib, False),
             ("Distributions", 3, distrib, False), ("Management fees", 4, fees, False),
             ("Net income", 5, income, False), ("Realized gain/loss", 6, real, False),
             ("Unrealized gain/loss", 7, unreal, False), ("FX translation", 8, fx_tr, False),
             ("Closing NAV", 9, closing, True)]
    arr = F.array(*[F.struct(F.lit(n).alias("component"), F.lit(o).alias("component_order"),
                             v.cast("double").alias("amount"), F.lit(t).alias("is_closing")) for n, o, v, t in comps])
    bridge_parts.append(b.select("fund_id", "investor_id", "legal_entity_id", "period_end", "date_key", "quarter",
                                 F.lit(code).alias("currency_code"), F.explode(arr).alias("c"))
                         .select("*", "c.*").drop("c"))
gold_nav_bridge = bridge_parts[0]
for p in bridge_parts[1:]:
    gold_nav_bridge = gold_nav_bridge.unionByName(p)
write_table(gold_nav_bridge, "gold_nav_bridge")
n = read_table("gold_nav_bridge").count()
log_audit(NOTEBOOK, "gold", "gold_nav_bridge", n, n)
fx.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
