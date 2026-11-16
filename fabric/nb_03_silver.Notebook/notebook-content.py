# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_03_silver — conform, validate, quarantine, MERGE
# * Reference data (funds, investors, legal entities, investments, commitments) → typed `silver_*` snapshots.
# * Each bronze batch from this run is renamed to the canonical contract with `ops_source_mapping`
#   (column map, date format, amount format, transaction-type map, sign convention).
# * Rule failures go to `silver_quarantine` with the reason and the raw record — never silently dropped.
# * Restatements: within a key, the highest file version (`_vN`) wins; MERGE on business keys makes the
#   latest delivery authoritative and reruns idempotent.
# All parsing is ANSI-safe (`try_cast`, `try_to_timestamp`) so a bad value is quarantined instead of
# failing the run.

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

NOTEBOOK = "nb_03_silver"
mapping = read_table("ops_source_mapping").filter("is_active").toPandas()


def read_reference(name):
    return spark.read.option("header", True).csv(f"{REFERENCE}/{name}.csv")  # noqa: F821


ref_fund = (read_reference("funds")
            .select("fund_id", "fund_name", "asset_class", "strategy",
                    F.col("vintage_year").cast("int").alias("vintage_year"), "fund_currency", "administrator",
                    "first_quarter", F.col("mgmt_fee_rate").cast("double").alias("mgmt_fee_rate"),
                    F.to_date("first_close_date").alias("first_close_date")))
ref_investor = read_reference("investors")
ref_le = read_reference("legal_entities")
ref_investment = (read_reference("investments")
                  .withColumn("investment_date", F.to_date("investment_date"))
                  .withColumn("maturity_date", F.to_date("maturity_date"))
                  .withColumn("coupon_rate", F.expr("try_cast(coupon_rate as double)")))
ref_commitment = (read_reference("commitments")
                  .withColumn("commitment_date", F.to_date("commitment_date"))
                  .withColumn("commitment_amount", F.col("commitment_amount").cast("double")))

for df_, name in [(ref_fund, "silver_fund"), (ref_investor, "silver_investor"), (ref_le, "silver_legal_entity"),
                  (ref_investment, "silver_investment"), (ref_commitment, "silver_commitment")]:
    write_table(df_.withColumn("_silver_loaded_at_utc", F.current_timestamp()), name)
    n = read_table(name).count()
    log_audit(NOTEBOOK, "silver", name, n, n, detail="reference snapshot")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Generic conformance driven by `ops_source_mapping`

# CELL ********************

LINEAGE = ["_source_id", "_source_file", "_file_version", "_run_id", "_ingested_at_utc"]
AMOUNT_COLS = {
    "capital_account": ["beginning_capital", "contributions", "distributions", "management_fees", "net_income",
                        "realized_gl", "unrealized_gl", "ending_capital"],
    "cashflow": ["amount_local"],
    "position": ["cost_basis", "fair_value", "principal_outstanding", "coupon_rate", "accrued_interest",
                 "pik_interest_qtd"],
    "deal_transaction": ["amount"],
    "fx_rate": ["usd_per_unit"],
}
DATE_COLS = {"capital_account": ["period_end"], "cashflow": ["cashflow_date"], "position": ["as_of_date"],
             "deal_transaction": ["txn_date"], "fx_rate": ["rate_date"]}


def conform(m, bronze):
    cmap, tmap = json.loads(m.column_map), json.loads(m.type_map)
    cols = [F.trim(F.col(f"`{src}`")).alias(dst) for src, dst in cmap.items()] + [F.col(c) for c in LINEAGE]
    df = bronze.select(*cols)
    for c in DATE_COLS[m.entity]:
        df = df.withColumn(c, F.to_date(F.expr(f"try_to_timestamp({c}, '{m.date_format}')")))
    for c in AMOUNT_COLS[m.entity]:
        cleaned = f"regexp_replace({c}, ',', '')" if m.amount_format == "thousands" else c
        df = (df.withColumn(f"_{c}_raw", F.col(c))
                .withColumn(c, F.expr(f"try_cast({cleaned} as double)")))
        if m.sign_convention == "absolute":
            df = df.withColumn(c, F.abs(c))
    if tmap:
        tm = F.create_map(*[F.lit(x) for kv in tmap.items() for x in kv])
        df = df.withColumn("cashflow_type", tm[F.col("source_txn_type")])
    return df


def reasons_col(rules):
    """rules: list of (condition_that_means_failure, reason). Returns array<string> of failed reasons."""
    arr = F.array(*[F.when(cond, F.lit(reason)) for cond, reason in rules])
    return F.filter(arr, lambda x: x.isNotNull())


def raw_nonnull_but_unparsed(entity):
    return [(F.col(f"_{c}_raw").isNotNull() & F.col(c).isNull(), f"non-numeric {c}") for c in AMOUNT_COLS[entity]]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Entity rules

# CELL ********************

funds_k = ref_fund.select(F.col("fund_id").alias("_rf"), F.col("fund_currency").alias("_fund_ccy"))
inv_k = ref_investor.select(F.col("investor_id").alias("_ri"))
cmt_k = ref_commitment.select(F.col("fund_id").alias("_cf"), F.col("investor_id").alias("_ci"))
invest_k = ref_investment.select(F.col("investment_id").alias("_rinv"))
TOL = AMOUNT_TOLERANCE


def validate(entity, df):
    if entity in ("capital_account", "cashflow"):
        df = (df.join(F.broadcast(funds_k), F.col("fund_id") == F.col("_rf"), "left")
                .join(F.broadcast(inv_k), F.col("investor_id") == F.col("_ri"), "left")
                .join(F.broadcast(cmt_k), (F.col("fund_id") == F.col("_cf")) & (F.col("investor_id") == F.col("_ci")), "left"))
        common = [(F.col("fund_id").isNull() | F.col("investor_id").isNull(), "missing business key"),
                  (F.col("fund_id").isNotNull() & F.col("_rf").isNull(), "unknown fund"),
                  (F.col("investor_id").isNotNull() & F.col("_ri").isNull(), "unknown investor"),
                  (F.col("_ri").isNotNull() & F.col("_rf").isNotNull() & F.col("_cf").isNull(), "no commitment for fund/investor"),
                  (F.col("_rf").isNotNull() & (F.col("currency") != F.col("_fund_ccy")), "currency differs from fund currency")]
    if entity == "capital_account":
        roll = (F.col("beginning_capital") + F.col("contributions") - F.col("distributions") - F.col("management_fees")
                + F.col("net_income") + F.col("realized_gl") + F.col("unrealized_gl") - F.col("ending_capital"))
        rules = common + raw_nonnull_but_unparsed(entity) + [
            (F.col("period_end").isNull(), "missing or invalid period_end"),
            (F.col("period_end") != F.last_day("period_end"), "period_end is not a month end"),
            (~F.month("period_end").isin(3, 6, 9, 12), "period_end is not a quarter end"),
            (F.abs(roll) > TOL, "capital account does not roll forward")]
    elif entity == "cashflow":
        rules = common + raw_nonnull_but_unparsed(entity) + [
            (F.col("cashflow_id").isNull() | (F.col("cashflow_id") == "TOTAL"), "footer/total row"),
            (F.col("cashflow_date").isNull(), "missing or invalid cashflow_date"),
            (F.col("cashflow_type").isNull(), "unmapped transaction type"),
            (F.col("amount_local").isNull(), "missing amount")]
    elif entity == "position":
        df = df.join(F.broadcast(invest_k), F.col("investment_id") == F.col("_rinv"), "left")
        rules = raw_nonnull_but_unparsed(entity) + [
            (F.col("_rinv").isNull(), "unknown investment"),
            (F.col("fair_value").isNull() | (F.col("fair_value") < 0), "missing or negative fair value"),
            (F.col("as_of_date").isNull(), "missing as_of_date")]
    elif entity == "deal_transaction":
        df = df.join(F.broadcast(invest_k), F.col("investment_id") == F.col("_rinv"), "left")
        rules = raw_nonnull_but_unparsed(entity) + [
            (F.col("_rinv").isNull(), "unknown investment"),
            (~F.col("txn_type").isin("INVESTMENT", "PROCEEDS", "INCOME"), "unknown deal txn type"),
            (F.col("txn_date").isNull(), "missing txn_date")]
    else:  # fx_rate
        rules = [(F.col("usd_per_unit").isNull() | (F.col("usd_per_unit") <= 0), "invalid rate"),
                 (F.col("rate_date").isNull(), "missing rate_date")]
    df = df.withColumn("_reasons", reasons_col(rules))
    return df.drop(*[c for c in df.columns if c in ("_rf", "_fund_ccy", "_ri", "_cf", "_ci", "_rinv")])


def finalize(entity, df):
    out = df.drop(*[c for c in df.columns if c.endswith("_raw") or c == "_reasons"])
    if entity == "cashflow":
        out = (out.withColumn("flow_category", F.when(F.col("cashflow_type").startswith("CONTRIBUTION"), "PAID_IN")
                                                 .otherwise("DISTRIBUTED"))
                  .withColumn("quarter", F.concat(F.year("cashflow_date"), F.lit("Q"), F.quarter("cashflow_date"))))
    if entity == "capital_account":
        out = out.withColumn("quarter", F.concat(F.year("period_end"), F.lit("Q"), F.quarter("period_end")))
    return out.withColumn("_silver_loaded_at_utc", F.current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Process this run's bronze batches

# CELL ********************

WM_SCHEMA = T.StructType([T.StructField("source_id", T.StringType()),
                          T.StructField("last_ingested_at_utc", T.TimestampType()),
                          T.StructField("run_id", T.StringType())])
watermarks = ({r.source_id: r.last_ingested_at_utc for r in read_table("ops_silver_watermark").collect()}
              if table_exists("ops_silver_watermark") else {})

for m in mapping.itertuples():
    if not table_exists(m.bronze_table):
        print(f"{m.source_id:<28} no bronze table yet")
        continue
    batch = read_table(m.bronze_table)
    if watermarks.get(m.source_id) is not None:
        batch = batch.filter(F.col("_ingested_at_utc") > F.lit(watermarks[m.source_id]))
    batch = batch.cache()
    rows_in = batch.count()
    if rows_in == 0:
        print(f"{m.source_id:<28} nothing new in bronze since the last silver load")
        continue
    new_wm = batch.agg(F.max("_ingested_at_utc")).first()[0]
    keys = json.loads(m.key_columns)
    checked = validate(m.entity, conform(m, batch)).cache()
    bad = checked.filter(F.size("_reasons") > 0)
    good = checked.filter(F.size("_reasons") == 0)
    n_bad = bad.count()
    if n_bad:
        raw_cols = [c for c in checked.columns if not c.startswith("_") or c in ("_source_file",)]
        q = bad.select(F.lit(RUN_ID).alias("run_id"), F.lit(m.source_id).alias("source_id"),
                       F.col("_source_file").alias("source_file"),
                       F.concat_ws("|", *[F.coalesce(F.col(k).cast("string"), F.lit("<null>")) for k in keys]).alias("business_key"),
                       F.array_join("_reasons", "; ").alias("reason"),
                       F.to_json(F.struct(*[F.col(c) for c in raw_cols])).alias("record_json"),
                       F.current_timestamp().alias("quarantined_at_utc"))
        append_table(q, "silver_quarantine")
    w = Window.partitionBy(*keys).orderBy(F.col("_file_version").desc(), F.col("_ingested_at_utc").desc(),
                                          F.col("_source_file").desc())
    deduped = good.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    final = finalize(m.entity, deduped)
    n_good, n_out = good.count(), final.count()
    merge_table(final, m.silver_table, keys)
    log_audit(NOTEBOOK, "silver", m.silver_table, rows_in, n_out, n_bad,
              detail=json.dumps({"source_id": m.source_id, "superseded_or_duplicate": n_good - n_out}))
    merge_table(spark.createDataFrame([(m.source_id, new_wm, RUN_ID)], WM_SCHEMA),
                "ops_silver_watermark", ["source_id"])
    checked.unpersist()
    batch.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
