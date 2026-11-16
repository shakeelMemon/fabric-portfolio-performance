# Data contract — gold layer for the Power BI team

**Producer:** data engineering (`nb_04_gold`) · **Consumers:** Power BI semantic model `PortfolioPerformance`, LP statement extracts
**Location:** lakehouse `lh_portfolio`, schema `dbo` (SQL analytics endpoint) · **Refresh:** quarterly delivery + ad-hoc restatements
**Release rule:** tables are rebuilt in place, but the semantic model is reframed only after `nb_05_quality_gate` passes for the same `run_id`.

## 1. Multi-currency cashflow contract — `fact_cashflow`

**Grain:** one row per capital-call or distribution notice line, fund × investor × date × type (`cashflow_id` is unique).

| Column | Type | Rule |
|---|---|---|
| `cashflow_id` | string, PK | Administrator notice/transaction reference (Citco `NOTICE_ID`, Alter Domus `txn_ref`) |
| `fund_id`, `investor_id`, `legal_entity_id` | string, FK | Must exist in `dim_fund`, `dim_investor`, `dim_legal_entity` (DQ10) |
| `cashflow_date`, `date_key` | date, int `yyyyMMdd` | Value/settlement date; `date_key` → `dim_date` |
| `quarter` | string `YYYYQn` | Calendar quarter of `cashflow_date` |
| `cashflow_type` | enum | `CONTRIBUTION`, `CONTRIBUTION_FEES`, `DISTRIBUTION_ROC`, `DISTRIBUTION_GAIN`, `DISTRIBUTION_INCOME` |
| `flow_category` | enum | `PAID_IN` (both contribution types) or `DISTRIBUTED` (all distribution types) |
| `local_currency` | ISO 4217 | Fund currency; equals `dim_fund.fund_currency` (enforced in silver) |
| `le_currency` | ISO 4217 | Functional currency of the investor's legal entity (feeder) |
| `amount_local` | double, **always ≥ 0** | Direction comes from `flow_category`, never from the sign. Administrators that send signed amounts are normalised in silver |
| `amount_usd`, `amount_cad`, `amount_le` | double ≥ 0 | `amount_local × fx_*`, rounded to 2 dp |
| `fx_usd`, `fx_cad`, `fx_le` | double | Rate applied: local → target on **`cashflow_date`** |
| `source_id`, `source_file` | string | Lineage back to the delivered file |

**FX policy (applies to every gold table)**

| Measure type | Rate | Why |
|---|---|---|
| Flows (calls, distributions) | spot on the flow date | What the LP actually paid or received in that currency |
| Balances (NAV, fair value, accrued, principal) | spot on the period end | Reporting-date valuation |
| P&L inside the NAV bridge (fees, income, realised, unrealised) | quarter-average | Standard translation practice; the residual is shown as **FX translation** |
| Commitments | spot on the commitment date | Fixed when the commitment is signed. Unfunded in USD/CAD therefore includes FX drift (DQ16 checks over-calling in fund currency only) |

Rates come from `silver_fx_rate` (`usd_per_unit`, one row per calendar day and currency). Cross rate A→B = usd_per_unit(A) / usd_per_unit(B).
A missing rate fails DQ09; the pipeline never defaults a rate to 1.

**Aggregation rules for consumers**
* `amount_usd` and `amount_cad` can be summed across any slice.
* `amount_local` can be summed only where the slice has one `local_currency`, and `amount_le` only where it has one `le_currency`. The DAX measures return BLANK otherwise, and `gold_performance` omits those rows.

## 2. Other gold tables

| Table | Grain | Notes |
|---|---|---|
| `fact_snapshot` | fund × investor × quarter end | Capital account: `beginning_capital … unrealized_gl` (local) + `nav_local/usd/cad/le` at period-end spot. Rolls forward exactly (DQ04), opening = prior closing (DQ05) |
| `fact_commitment` | commitment | `commitment_local/usd/cad/le` at commitment-date spot |
| `fact_position` | investment × quarter end | `cost_*`, `fair_value_*`, `total_value_*` (fair value + accrued), `mark_to_cost`; LE not applicable (investment level) |
| `fact_accrual` | loan × quarter end (private credit) | `principal_*` (incl. PIK), `accrued_interest_*`, `interest_received_qtd_*`, `coupon_rate`, `loan_status` |
| `fact_irr_flow` | as-of quarter × flow | **Investor perspective, signed:** calls < 0, distributions > 0, terminal NAV > 0 dated at `as_of_date`. `level = INVESTOR` (net, LP) or `INVESTMENT` (gross, deal). Every as-of quarter repeats its full history, so XIRR at any as-of date is a filter, not a recalculation |
| `gold_performance` | grain × as-of × currency | Precomputed paid-in, distributed, NAV, commitment, unfunded, DPI, RVPI, TVPI, IRR for `FUND_INVESTOR`, `FUND_NET`, `INVESTOR`, `PORTFOLIO_NET`, `INVESTMENT`, `FUND_GROSS`. Keys not in the grain are `'ALL'`. Used for LP statements and DQ18 parity |
| `gold_nav_bridge` | fund × investor × quarter × currency × component | 9 components (`component_order` 1–9); `is_closing = true` marks Closing NAV, which equals the sum of components 1–8 (DQ12) |
| `dim_report_currency` | currency option | `USD`, `CAD`, `LOCAL`, `LE`, used by the Currency slicer |
| `sec_user_investor` | user × investor | RLS mapping (`user_upn` = Entra UPN) |

## 3. Change policy

* **Additive changes** (new column, new `cashflow_type` mapped to an existing `flow_category`) are released with notice in the PR description.
* **Breaking changes** (rename, type change, new sign convention, new FX rule) need a version bump of this document and sign-off from the Power BI owner. Both semantic-model and notebook changes ship in the same PR.
* **Restatements:** an administrator's `_vN+1` file replaces `_vN` for the same business key. Prior quarters can change, and the report always shows the latest delivered view. `fact_snapshot.file_version` shows which version is live.
