# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_00_setup — operational tables and the source-mapping contract
# Idempotent. Creates the `ops_*` tables and (re)publishes `ops_source_mapping`, the metadata that
# drives bronze ingestion and silver conformance. Onboarding a new administrator = adding a row
# here, not writing a new notebook.

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

SOURCE_MAPPING = [
    {
        "source_id": "tpa1_capital_account", "administrator": "TPA1", "entity": "capital_account",
        "landing_subfolder": "tpa1", "file_regex": r"^TPA1_CAPACCT_(F\d+)_(\d{4}Q\d)_v(\d+)\.csv$",
        "bronze_table": "bronze_tpa1_capital_account", "silver_table": "silver_capital_account",
        "date_format": "yyyyMMdd", "amount_format": "plain", "sign_convention": "as_is",
        "column_map": {"FUND_CODE": "fund_id", "INVESTOR_ID": "investor_id", "PERIOD_END": "period_end",
                       "BEG_CAP": "beginning_capital", "CONTRIB": "contributions", "DISTRIB": "distributions",
                       "MGMT_FEE": "management_fees", "INCOME": "net_income", "REALIZED_GL": "realized_gl",
                       "UNREALIZED_GL": "unrealized_gl", "END_CAP": "ending_capital", "CCY": "currency"},
        "type_map": {}, "key_columns": ["fund_id", "investor_id", "period_end"],
    },
    {
        "source_id": "tpa2_capital_account", "administrator": "TPA2", "entity": "capital_account",
        "landing_subfolder": "tpa2", "file_regex": r"^tpa2_capital_account_(f\d+)_(\d{4}q\d)_v(\d+)\.csv$",
        "bronze_table": "bronze_tpa2_capital_account", "silver_table": "silver_capital_account",
        "date_format": "yyyy-MM-dd", "amount_format": "thousands", "sign_convention": "as_is",
        "column_map": {"fund_id": "fund_id", "lp_id": "investor_id", "as_of_date": "period_end",
                       "opening_balance": "beginning_capital", "contributions": "contributions",
                       "distributions": "distributions", "fees": "management_fees", "net_income": "net_income",
                       "realised": "realized_gl", "unrealised": "unrealized_gl", "closing_balance": "ending_capital",
                       "currency": "currency"},
        "type_map": {}, "key_columns": ["fund_id", "investor_id", "period_end"],
    },
    {
        "source_id": "tpa1_cashflow", "administrator": "TPA1", "entity": "cashflow",
        "landing_subfolder": "tpa1", "file_regex": r"^TPA1_CASHFLOW_(F\d+)_(\d{4}Q\d)_v(\d+)\.csv$",
        "bronze_table": "bronze_tpa1_cashflow", "silver_table": "silver_cashflow",
        "date_format": "yyyyMMdd", "amount_format": "plain", "sign_convention": "absolute",
        "column_map": {"NOTICE_ID": "cashflow_id", "FUND_CODE": "fund_id", "INVESTOR_ID": "investor_id",
                       "VALUE_DATE": "cashflow_date", "TXN_CODE": "source_txn_type", "AMOUNT": "amount_local",
                       "CCY": "currency"},
        "type_map": {"CALL": "CONTRIBUTION", "CALL_FEE": "CONTRIBUTION_FEES", "DIST_ROC": "DISTRIBUTION_ROC",
                     "DIST_GAIN": "DISTRIBUTION_GAIN", "DIST_INC": "DISTRIBUTION_INCOME"},
        "key_columns": ["cashflow_id"],
    },
    {
        "source_id": "tpa2_transactions", "administrator": "TPA2", "entity": "cashflow",
        "landing_subfolder": "tpa2", "file_regex": r"^tpa2_transactions_(f\d+)_(\d{4}q\d)_v(\d+)\.csv$",
        "bronze_table": "bronze_tpa2_transactions", "silver_table": "silver_cashflow",
        "date_format": "yyyy-MM-dd", "amount_format": "thousands", "sign_convention": "absolute",
        "column_map": {"txn_ref": "cashflow_id", "fund_id": "fund_id", "lp_id": "investor_id",
                       "settle_date": "cashflow_date", "txn_type": "source_txn_type", "amount": "amount_local",
                       "currency": "currency"},
        "type_map": {"Capital Call": "CONTRIBUTION", "Capital Call - Management Fees": "CONTRIBUTION_FEES",
                     "Distribution - Return of Capital": "DISTRIBUTION_ROC",
                     "Distribution - Realized Gain": "DISTRIBUTION_GAIN", "Distribution - Income": "DISTRIBUTION_INCOME"},
        "key_columns": ["cashflow_id"],
    },
    {
        "source_id": "pm_valuations", "administrator": "PORTFOLIO_MONITORING", "entity": "position",
        "landing_subfolder": "portfolio_monitoring", "file_regex": r"^valuations_(\d{4}Q\d)\.csv$",
        "bronze_table": "bronze_pm_valuations", "silver_table": "silver_position",
        "date_format": "yyyy-MM-dd", "amount_format": "plain", "sign_convention": "as_is",
        "column_map": {c: c for c in ["investment_id", "fund_id", "as_of_date", "currency", "cost_basis", "fair_value",
                                      "principal_outstanding", "coupon_rate", "accrued_interest", "pik_interest_qtd",
                                      "loan_status"]},
        "type_map": {}, "key_columns": ["investment_id", "as_of_date"],
    },
    {
        "source_id": "pm_deal_transactions", "administrator": "PORTFOLIO_MONITORING", "entity": "deal_transaction",
        "landing_subfolder": "portfolio_monitoring", "file_regex": r"^deal_transactions_(\d{4}Q\d)\.csv$",
        "bronze_table": "bronze_pm_deal_transactions", "silver_table": "silver_deal_transaction",
        "date_format": "yyyy-MM-dd", "amount_format": "plain", "sign_convention": "absolute",
        "column_map": {c: c for c in ["txn_id", "investment_id", "txn_date", "txn_type", "amount", "fund_id", "currency"]},
        "type_map": {}, "key_columns": ["txn_id"],
    },
    {
        "source_id": "fx_rates", "administrator": "MARKET_DATA", "entity": "fx_rate",
        "landing_subfolder": "fx", "file_regex": r"^fx_rates_(\d{4}Q\d)\.csv$",
        "bronze_table": "bronze_fx_rates", "silver_table": "silver_fx_rate",
        "date_format": "yyyy-MM-dd", "amount_format": "plain", "sign_convention": "as_is",
        "column_map": {"rate_date": "rate_date", "currency": "currency", "usd_per_unit": "usd_per_unit"},
        "type_map": {}, "key_columns": ["rate_date", "currency"],
    },
]

mapping_rows = [{**{k: v for k, v in m.items() if k not in ("column_map", "type_map", "key_columns")},
                 "column_map": json.dumps(m["column_map"]), "type_map": json.dumps(m["type_map"]),
                 "key_columns": json.dumps(m["key_columns"]), "is_active": True}
                for m in SOURCE_MAPPING]
write_table(spark.createDataFrame(pd.DataFrame(mapping_rows)), "ops_source_mapping")  # noqa: F821
print(f"ops_source_mapping: {len(mapping_rows)} sources")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

OPS_TABLES = {
    "ops_load_audit": AUDIT_SCHEMA,
    "ops_processed_files": T.StructType([
        T.StructField("source_id", T.StringType()), T.StructField("file_path", T.StringType()),
        T.StructField("file_name", T.StringType()), T.StructField("sha256", T.StringType()),
        T.StructField("row_count", T.LongType()), T.StructField("run_id", T.StringType()),
        T.StructField("processed_at_utc", T.TimestampType())]),
    "ops_dq_results": T.StructType([
        T.StructField("run_id", T.StringType()), T.StructField("check_id", T.StringType()),
        T.StructField("check_name", T.StringType()), T.StructField("layer", T.StringType()),
        T.StructField("severity", T.StringType()), T.StructField("passed", T.BooleanType()),
        T.StructField("failed_rows", T.LongType()), T.StructField("detail", T.StringType()),
        T.StructField("checked_at_utc", T.TimestampType())]),
    "silver_quarantine": T.StructType([
        T.StructField("run_id", T.StringType()), T.StructField("source_id", T.StringType()),
        T.StructField("source_file", T.StringType()), T.StructField("business_key", T.StringType()),
        T.StructField("reason", T.StringType()), T.StructField("record_json", T.StringType()),
        T.StructField("quarantined_at_utc", T.TimestampType())]),
}
for name, schema in OPS_TABLES.items():
    if not table_exists(name):
        write_table(spark.createDataFrame([], schema), name)  # noqa: F821
        print(f"created {name}")
    else:
        print(f"exists  {name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## SQL analytics endpoint — values for `tools/configure_powerbi.py`
# The Direct Lake semantic model binds to the lakehouse's SQL analytics endpoint. Copy the two values
# printed below into the configure script (see docs/RUNBOOK.md › Deploy the Power BI project).

# CELL ********************

if IS_FABRIC:
    try:
        _lh = notebookutils.lakehouse.getWithProperties(LAKEHOUSE_NAME)  # noqa: F821
        _props = _lh["properties"] if isinstance(_lh, dict) else _lh.properties
        _sql = _props.get("sqlEndpointProperties", {})
        print("SQL endpoint server :", _sql.get("connectionString"))
        print("SQL endpoint id     :", _sql.get("id"))
    except Exception as exc:
        print(f"Could not read SQL endpoint properties ({exc}); copy them from the endpoint's Settings pane.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
