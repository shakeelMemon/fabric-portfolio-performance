# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_01_generate_source_files — synthetic fund-administrator deliveries
# Simulates what lands in OneLake from the outside world, so the platform can be demoed end to end
# without client data:
# | Source | Format quirks | Lands in |
# |---|---|---|
# | **TPA 1** (private equity funds) — capital account + cashflow notices | UPPERCASE headers, `YYYYMMDD` dates, distributions signed negative, `_vN` restatements | `Files/landing/tpa1/` |
# | **TPA 2** (private credit funds) — capital account + transactions | snake_case headers, ISO dates, `"1,234.56"` amounts, wordy txn types, trailing TOTAL row | `Files/landing/tpa2/` |
# | **Portfolio monitoring** — valuations, loan tape, deal transactions | one file per quarter | `Files/landing/portfolio_monitoring/` |
# | **FX** — daily `usd_per_unit` | one file per quarter | `Files/landing/fx/` |
# | **Reference** — funds, investors, legal entities, investments, commitments | CSV | `Files/reference/` |
# Deliberate data-quality defects (the quality gate must catch or absorb them): a duplicated TPA 1
# notice, an unknown LP (`INV999`), TOTAL footer rows, and a mis-stated 2026Q2 F01 capital account
# that is restated (`_v2`) with the 2026Q3 delivery.
# The economics are internally consistent: fund NAV = Σ investment fair value + accrued interest;
# each LP's capital account rolls forward exactly; notices sum to capital-account contributions.


# CELL ********************

# Parameters (override from nb_99_run_all or a pipeline)
deliver_through = "2026Q3"   # write administrator files up to and including this quarter
seed = 20261114
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

import numpy as np

rng = np.random.default_rng(seed)
QUARTERS = quarter_list(FIRST_QUARTER, LAST_QUARTER)
M = 1_000_000

LEGAL_ENTITIES = pd.DataFrame([
    ("LE01", "Aurora Canada Feeder LP", "Ontario, Canada", "CAD"),
    ("LE02", "Aurora Global Feeder LP", "Cayman Islands", "USD"),
    ("LE03", "Aurora Europe SCSp", "Luxembourg", "EUR"),
], columns=["legal_entity_id", "legal_entity_name", "jurisdiction", "functional_currency"])

FUNDS = pd.DataFrame([
    ("F01", "Aurora Global Buyout III",    "Private Equity", "Buyout",                2022, "USD", "TPA1", "2022Q1", 0.0175),
    ("F02", "Aurora Canadian Growth II",   "Private Equity", "Growth Equity",         2022, "CAD", "TPA1", "2022Q1", 0.0200),
    ("F03", "Aurora Secondaries I",        "Private Equity", "Secondaries",           2023, "USD", "TPA1", "2023Q1", 0.0125),
    ("F04", "Aurora Senior Credit I",      "Private Credit", "Senior Direct Lending", 2022, "USD", "TPA2", "2022Q1", 0.0100),
    ("F05", "Aurora Canadian Credit II",   "Private Credit", "Unitranche",            2023, "CAD", "TPA2", "2023Q2", 0.0125),
    ("F06", "Aurora European Credit I",    "Private Credit", "Senior Direct Lending", 2024, "EUR", "TPA2", "2024Q1", 0.0100),
], columns=["fund_id", "fund_name", "asset_class", "strategy", "vintage_year", "fund_currency",
            "administrator", "first_quarter", "mgmt_fee_rate"])
FUNDS["first_close_date"] = FUNDS["first_quarter"].map(lambda q: quarter_start(q) + dt.timedelta(days=14))

INVESTORS = pd.DataFrame([
    ("INV001", "Maple Teachers' Pension Plan",       "Public Pension",  "Canada",         "LE01"),
    ("INV002", "Prairie Municipal Pension Fund",     "Public Pension",  "Canada",         "LE01"),
    ("INV003", "Laurentian University Endowment",    "Endowment",       "Canada",         "LE01"),
    ("INV004", "Harbourfront Family Office",         "Family Office",   "Canada",         "LE01"),
    ("INV005", "Great Lakes Mutual Life",            "Insurance",       "United States",  "LE02"),
    ("INV006", "Cascadia Community Foundation",      "Foundation",      "United States",  "LE02"),
    ("INV007", "Nordlys Occupational Pension",       "Corporate Pension", "Norway",       "LE03"),
    ("INV008", "Rhine Valley Insurance Group",       "Insurance",       "Germany",        "LE03"),
    ("INV009", "Pacific Ridge University Endowment", "Endowment",       "United States",  "LE02"),
    ("INV010", "Aurora Staff Co-Investment Plan",    "Employee Plan",   "Canada",         "LE01"),
], columns=["investor_id", "investor_name", "investor_type", "domicile", "legal_entity_id"])

FUND_LPS = {
    "F01": ["INV001", "INV002", "INV003", "INV005", "INV006", "INV008", "INV009", "INV010"],
    "F02": ["INV001", "INV002", "INV003", "INV004", "INV010"],
    "F03": ["INV001", "INV005", "INV006", "INV009"],
    "F04": ["INV002", "INV004", "INV005", "INV006", "INV007", "INV008"],
    "F05": ["INV001", "INV002", "INV003", "INV004", "INV010"],
    "F06": ["INV005", "INV007", "INV008", "INV009"],
}

commit_rows = []
for f in FUNDS.itertuples():
    for lp in FUND_LPS[f.fund_id]:
        amt = 10 * M if lp == "INV010" else float(rng.integers(5, 31) * 5 * M)
        le = INVESTORS.set_index("investor_id").loc[lp, "legal_entity_id"]
        commit_rows.append((f"CMT-{f.fund_id}-{lp}", lp, f.fund_id, le, f.first_close_date, amt, f.fund_currency))
COMMITMENTS = pd.DataFrame(commit_rows, columns=["commitment_id", "investor_id", "fund_id", "legal_entity_id",
                                                 "commitment_date", "commitment_amount", "currency"])
FUND_SIZE = COMMITMENTS.groupby("fund_id")["commitment_amount"].sum().to_dict()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Daily FX (USD per unit), calendar days

# CELL ********************

fx_days = pd.date_range("2022-01-01", quarter_end(LAST_QUARTER), freq="D")
fx_frames = [pd.DataFrame({"rate_date": fx_days, "currency": "USD", "usd_per_unit": 1.0})]
for ccy, start, drift in [("CAD", 0.790, -0.000015), ("EUR", 1.135, -0.000010)]:
    shocks = rng.normal(drift, 0.0035, len(fx_days))
    path = start * np.exp(np.cumsum(shocks))
    fx_frames.append(pd.DataFrame({"rate_date": fx_days, "currency": ccy, "usd_per_unit": np.round(path, 6)}))
FX = pd.concat(fx_frames, ignore_index=True)
FX["rate_date"] = FX["rate_date"].dt.date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Portfolio simulation (fund currency)

# CELL ********************

PREFIX = ["Northwind", "Bluepeak", "Granite", "Cedarline", "Silverbirch", "Ironwood", "Harbor", "Summit",
          "Clearwater", "Redrock", "Frontier", "Lakeshore", "Westgate", "Evergreen", "Polaris", "Keystone",
          "Riverbend", "Stonebridge", "Brightpath", "Oakridge", "Copperleaf", "Highland", "Meridian", "Tidewater",
          "Aspenwood", "Bayview", "Crescent", "Driftwood", "Falcon", "Glacier", "Juniper", "Kingsway",
          "Larkspur", "Mapleton", "Northstar", "Orchard", "Pinecrest", "Quarry", "Ravenwood", "Sablewood",
          "Thornbury", "Upland", "Vantage", "Willowbrook", "Yorkdale", "Zephyr", "Alder", "Beacon"]
SECTORS = [("Software", "Technology"), ("Health Services", "Healthcare"), ("Logistics", "Industrials"),
           ("Foods", "Consumer Staples"), ("Payments", "Financials"), ("Manufacturing", "Industrials"),
           ("Dental Partners", "Healthcare"), ("Data Centres", "Technology"), ("Insurance Brokers", "Financials"),
           ("Environmental", "Utilities"), ("Education", "Consumer Discretionary"), ("Pet Care", "Consumer Discretionary")]
COUNTRY = {"USD": ["United States", "United States", "Canada"], "CAD": ["Canada"], "EUR": ["Germany", "France", "Netherlands", "Spain"]}

name_pool = list(rng.permutation(PREFIX))
deals = []
for f in FUNDS.itertuples():
    qs = [q for q in QUARTERS if q >= f.first_quarter]
    is_credit = f.asset_class == "Private Credit"
    n = {"F01": 7, "F02": 6, "F03": 6, "F04": 10, "F05": 8, "F06": 7}[f.fund_id]
    size = FUND_SIZE[f.fund_id]
    deploy_qs = qs[1:11] if len(qs) > 11 else qs[1:max(2, len(qs) - 2)]
    target = {"F01": 0.80, "F02": 0.78, "F03": 0.86, "F04": 0.86, "F05": 0.85, "F06": 0.86}[f.fund_id]
    weights = rng.uniform(0.7, 1.3, n)
    sizes = np.round(size * target * weights / weights.sum(), -3)     # deploy ~target of commitments
    for i in range(n):
        q_inv = deploy_qs[min(int(i * len(deploy_qs) / n), len(deploy_qs) - 1)]
        inv_date = quarter_start(q_inv) + dt.timedelta(days=int(rng.integers(5, 80)))
        sector, gics = SECTORS[int(rng.integers(0, len(SECTORS)))]
        d = dict(investment_id=f"{f.fund_id}-I{i + 1:02d}", fund_id=f.fund_id, currency=f.fund_currency,
                 investment_name=f"{name_pool.pop()} {sector}", sector=gics,
                 country=COUNTRY[f.fund_currency][int(rng.integers(0, len(COUNTRY[f.fund_currency])))],
                 investment_quarter=q_inv, investment_date=inv_date, asset_class=f.asset_class)
        if is_credit:
            principal = float(sizes[i])
            term_q = int(rng.integers(10, 21))           # some repay inside the window, some after
            exit_idx = QUARTERS.index(q_inv) + term_q
            d.update(instrument="Unitranche Loan" if f.fund_id == "F05" else "Senior Secured Loan",
                     cost=principal, coupon_rate=round(float(rng.uniform(0.085, 0.118)), 4),
                     exit_quarter=QUARTERS[exit_idx] if exit_idx < len(QUARTERS) else None,
                     prepay=bool(rng.random() < 0.5), maturity_date=quarter_end(q_inv) + dt.timedelta(days=365 * 5),
                     impaired_from=None)
        else:
            cost = float(sizes[i])
            hold_q = int(rng.integers(8, 13)) if f.fund_id == "F03" else int(rng.integers(12, 21))
            exit_idx = QUARTERS.index(q_inv) + hold_q
            moic = float(rng.choice([0.55, 1.15, 1.45, 1.7, 2.0, 2.3, 2.8, 3.2])) if f.fund_id != "F03" \
                else float(rng.uniform(1.25, 1.75))
            d.update(instrument="Common Equity", cost=cost, coupon_rate=None, target_moic=moic, hold_q=hold_q,
                     exit_quarter=QUARTERS[exit_idx] if exit_idx < len(QUARTERS) else None,
                     dividend_yield=float(rng.choice([0.0, 0.0, 0.01, 0.02])), maturity_date=None)
        deals.append(d)

# One stressed loan in F04: marked down and switched to PIK interest from 2025Q1.
for d in deals:
    if d["investment_id"] == "F04-I03":
        d.update(impaired_from="2025Q1", exit_quarter=None)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def simulate_fund(fund):
    """Quarter-by-quarter fund events and position snapshots in fund currency."""
    fid, ccy = fund.fund_id, fund.fund_currency
    size = FUND_SIZE[fid]
    fdeals = [dict(d) for d in deals if d["fund_id"] == fid]
    qs = [q for q in QUARTERS if q >= fund.first_quarter]
    events, positions, deal_txns, fund_q = [], [], [], []
    for qi, q in enumerate(qs):
        qs_d, qe_d = quarter_start(q), quarter_end(q)
        fees = income = realized = 0.0
        # management fee: on commitments during the 4-year investment period, then on invested cost
        held_cost = sum(d["cost"] for d in fdeals if d.get("_held"))
        fee_base = size if qi < 16 else held_cost
        fee = round(fee_base * fund.mgmt_fee_rate / 4, 2)
        events.append((qs_d + dt.timedelta(days=14), "CONTRIBUTION_FEES", fee, None))
        fees += fee
        accrued_begin = sum(d.get("_accrued", 0.0) for d in fdeals if d.get("_held"))
        for d in fdeals:
            if d["investment_quarter"] == q:                                   # new investment
                d["_held"] = True
                d["_fv"] = d["cost"]
                d["_principal"] = d["cost"]
                events.append((d["investment_date"], "CONTRIBUTION", d["cost"], d["investment_id"]))
                deal_txns.append((d["investment_id"], d["investment_date"], "INVESTMENT", d["cost"]))
            if not d.get("_held"):
                continue
            age_q = qs.index(q) - qs.index(d["investment_quarter"])
            exiting = d["exit_quarter"] == q
            if fund.asset_class == "Private Equity":
                if d["dividend_yield"] and age_q > 0 and not exiting:
                    div = round(d["cost"] * d["dividend_yield"] / 4, 2)
                    ddate = qe_d - dt.timedelta(days=10)
                    events.append((ddate, "DISTRIBUTION_INCOME", div, d["investment_id"]))
                    deal_txns.append((d["investment_id"], ddate, "INCOME", div))
                    income += div
                progress = min(age_q / d["hold_q"], 1.0)
                fv = d["cost"] * d["target_moic"] ** progress * (1 + rng.normal(0, 0.035)) if age_q > 0 else d["cost"]
                if exiting:
                    proceeds = round(d["cost"] * d["target_moic"] * (1 + rng.normal(0, 0.03)), 2)
                    xdate = qs_d + dt.timedelta(days=int(rng.integers(20, 80)))
                    roc, gain = min(d["cost"], proceeds), max(proceeds - d["cost"], 0.0)
                    events.append((xdate, "DISTRIBUTION_ROC", roc, d["investment_id"]))
                    if gain:
                        events.append((xdate, "DISTRIBUTION_GAIN", round(gain, 2), d["investment_id"]))
                    deal_txns.append((d["investment_id"], xdate, "PROCEEDS", proceeds))
                    realized += proceeds - d["cost"]
                    d["_held"], d["_fv"] = False, 0.0
                    continue
                d["_fv"] = round(fv, 2)
            else:                                                              # private credit
                impaired = d.get("impaired_from") and q >= d["impaired_from"]
                if age_q > 0 and not impaired:
                    intr = round(d["_principal"] * d["coupon_rate"] / 4, 2)
                    idate = qe_d - dt.timedelta(days=15)
                    events.append((idate, "DISTRIBUTION_INCOME", intr, d["investment_id"]))
                    deal_txns.append((d["investment_id"], idate, "INCOME", intr))
                    income += intr
                d["_pik"] = 0.0
                if impaired:
                    pik = round(d["_principal"] * d["coupon_rate"] / 4, 2)
                    d["_principal"] += pik
                    d["cost"] += pik
                    d["_pik"] = pik
                    income += pik
                if exiting:
                    fee_pct = 0.01 if d["prepay"] else 0.0
                    proceeds = round(d["_principal"] * (1 + fee_pct), 2)
                    xdate = qe_d - dt.timedelta(days=int(rng.integers(20, 60)))
                    events.append((xdate, "DISTRIBUTION_ROC", d["_principal"], d["investment_id"]))
                    if fee_pct:
                        events.append((xdate, "DISTRIBUTION_GAIN", round(proceeds - d["_principal"], 2), d["investment_id"]))
                    deal_txns.append((d["investment_id"], xdate, "PROCEEDS", proceeds))
                    realized += proceeds - d["_principal"]
                    d["_held"], d["_fv"], d["_accrued"] = False, 0.0, 0.0
                    continue
                mark = 0.62 if impaired else float(rng.uniform(0.975, 1.004))
                d["_fv"] = round(d["_principal"] * mark, 2)
                d["_accrued"] = 0.0 if impaired else round(d["_principal"] * d["coupon_rate"] / 12, 2)
            positions.append(dict(investment_id=d["investment_id"], fund_id=fid, as_of_date=qe_d, currency=ccy,
                                  cost_basis=round(d["cost"], 2), fair_value=d["_fv"],
                                  principal_outstanding=round(d["_principal"], 2) if fund.asset_class == "Private Credit" else None,
                                  coupon_rate=d["coupon_rate"], accrued_interest=d.get("_accrued", 0.0),
                                  pik_interest_qtd=d.get("_pik", 0.0),
                                  loan_status=("NON_PERFORMING" if (d.get("impaired_from") and q >= d["impaired_from"]) else "PERFORMING")
                                  if fund.asset_class == "Private Credit" else None))
        accrued_end = sum(d.get("_accrued", 0.0) for d in fdeals if d.get("_held"))
        income += accrued_end - accrued_begin            # accrual accounting for loan interest
        nav = sum(d["_fv"] + d.get("_accrued", 0.0) for d in fdeals if d.get("_held"))
        fund_q.append(dict(quarter=q, nav=round(nav, 2), fees=fees, income=round(income, 2), realized=round(realized, 2)))
    return events, positions, deal_txns, fund_q


SIM = {f.fund_id: simulate_fund(f) for f in FUNDS.itertuples()}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Allocate to LPs (pro rata to commitment) and build capital accounts

# CELL ********************

notices, capital = [], []
for f in FUNDS.itertuples():
    events, positions, deal_txns, fund_q = SIM[f.fund_id]
    lps = COMMITMENTS[COMMITMENTS.fund_id == f.fund_id]
    shares = dict(zip(lps.investor_id, lps.commitment_amount / FUND_SIZE[f.fund_id]))
    ev = pd.DataFrame(events, columns=["date", "type", "amount", "investment_id"])
    ev["quarter"] = ev["date"].map(quarter_of)
    ev = ev.groupby(["quarter", "date", "type"], as_index=False)["amount"].sum()   # one notice per date/type
    seq = 0
    for r in ev.itertuples():
        for lp, s in shares.items():
            seq += 1
            notices.append(dict(fund_id=f.fund_id, investor_id=lp, quarter=r.quarter, date=r.date, type=r.type,
                                amount=round(r.amount * s, 2), seq=seq, currency=f.fund_currency))
    nt = pd.DataFrame([n for n in notices if n["fund_id"] == f.fund_id])
    prev_end = {lp: 0.0 for lp in shares}
    for fq in fund_q:
        for lp, s in shares.items():
            sub = nt[(nt.investor_id == lp) & (nt.quarter == fq["quarter"])]
            contrib = round(sub[sub.type.str.startswith("CONTRIBUTION")].amount.sum(), 2)
            distrib = round(sub[sub.type.str.startswith("DISTRIBUTION")].amount.sum(), 2)
            fees, inc, rgl = (round(fq[k] * s, 2) for k in ("fees", "income", "realized"))
            end = round(fq["nav"] * s, 2)
            beg = prev_end[lp]
            ugl = round(end - (beg + contrib - distrib - fees + inc + rgl), 2)
            capital.append(dict(fund_id=f.fund_id, investor_id=lp, quarter=fq["quarter"],
                                period_end=quarter_end(fq["quarter"]), beginning_capital=beg, contributions=contrib,
                                distributions=distrib, management_fees=fees, net_income=inc, realized_gl=rgl,
                                unrealized_gl=ugl, ending_capital=end, currency=f.fund_currency))
            prev_end[lp] = end
NOTICES = pd.DataFrame(notices)
CAPITAL = pd.DataFrame(capital)
POSITIONS = pd.concat([pd.DataFrame(SIM[f][1]) for f in SIM], ignore_index=True)
DEAL_TXNS = pd.concat([pd.DataFrame(SIM[f][2], columns=["investment_id", "txn_date", "txn_type", "amount"]).assign(fund_id=f)
                       for f in SIM], ignore_index=True)
DEAL_TXNS["quarter"] = DEAL_TXNS["txn_date"].map(quarter_of)
INVESTMENTS = pd.DataFrame([{k: d[k] for k in ("investment_id", "fund_id", "investment_name", "asset_class", "instrument",
                                                "sector", "country", "currency", "investment_date", "coupon_rate",
                                                "maturity_date")} for d in deals])
print(f"capital-account rows: {len(CAPITAL)}  notices: {len(NOTICES)}  positions: {len(POSITIONS)}  deals: {len(INVESTMENTS)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Render administrator files (with their quirks) and write to OneLake

# CELL ********************

def to_csv(df):
    return df.to_csv(index=False, lineterminator="\n")


def ymd(d):
    return d.strftime("%Y%m%d")


def money(x):
    return f"{x:,.2f}"


TPA2_TYPES = {"CONTRIBUTION": "Capital Call", "CONTRIBUTION_FEES": "Capital Call - Management Fees",
            "DISTRIBUTION_ROC": "Distribution - Return of Capital", "DISTRIBUTION_GAIN": "Distribution - Realized Gain",
            "DISTRIBUTION_INCOME": "Distribution - Income"}
TPA1_TYPES = {"CONTRIBUTION": "CALL", "CONTRIBUTION_FEES": "CALL_FEE", "DISTRIBUTION_ROC": "DIST_ROC",
               "DISTRIBUTION_GAIN": "DIST_GAIN", "DISTRIBUTION_INCOME": "DIST_INC"}


def render_quarter(q):
    files = {}
    for f in FUNDS.itertuples():
        if q < f.first_quarter:
            continue
        ca = CAPITAL[(CAPITAL.fund_id == f.fund_id) & (CAPITAL.quarter == q)].copy()
        nt = NOTICES[(NOTICES.fund_id == f.fund_id) & (NOTICES.quarter == q)].copy()
        if f.administrator == "TPA1":
            out = pd.DataFrame({"FUND_CODE": ca.fund_id, "INVESTOR_ID": ca.investor_id, "PERIOD_END": ca.period_end.map(ymd),
                                "BEG_CAP": ca.beginning_capital, "CONTRIB": ca.contributions, "DISTRIB": ca.distributions,
                                "MGMT_FEE": ca.management_fees, "INCOME": ca.net_income, "REALIZED_GL": ca.realized_gl,
                                "UNREALIZED_GL": ca.unrealized_gl, "END_CAP": ca.ending_capital, "CCY": ca.currency})
            if f.fund_id == "F01" and q == "2026Q2":     # v1 mis-states one LP's valuation; v2 follows next quarter
                bad = out.copy()
                bad.loc[bad.index[0], ["UNREALIZED_GL", "END_CAP"]] += 25_000.0
                files[f"tpa1/TPA1_CAPACCT_{f.fund_id}_{q}_v1.csv"] = (q, to_csv(bad))
                files[f"tpa1/TPA1_CAPACCT_{f.fund_id}_{q}_v2.csv"] = ("2026Q3", to_csv(out))
            else:
                files[f"tpa1/TPA1_CAPACCT_{f.fund_id}_{q}_v1.csv"] = (q, to_csv(out))
            sign = nt.type.str.startswith("DISTRIBUTION").map({True: -1.0, False: 1.0})
            cf = pd.DataFrame({"NOTICE_ID": [f"C-{f.fund_id}-{q}-{s:05d}" for s in nt.seq], "FUND_CODE": nt.fund_id,
                               "INVESTOR_ID": nt.investor_id, "VALUE_DATE": nt.date.map(ymd),
                               "TXN_CODE": nt.type.map(TPA1_TYPES), "AMOUNT": (nt.amount * sign).round(2), "CCY": nt.currency})
            if f.fund_id == "F02" and q == "2024Q2":     # administrator re-sent one notice line
                cf = pd.concat([cf, cf.iloc[[2]]], ignore_index=True)
            files[f"tpa1/TPA1_CASHFLOW_{f.fund_id}_{q}_v1.csv"] = (q, to_csv(cf))
        else:
            out = pd.DataFrame({"fund_id": ca.fund_id, "lp_id": ca.investor_id, "as_of_date": ca.period_end.map(str),
                                "opening_balance": ca.beginning_capital.map(money), "contributions": ca.contributions.map(money),
                                "distributions": ca.distributions.map(money), "fees": ca.management_fees.map(money),
                                "net_income": ca.net_income.map(money), "realised": ca.realized_gl.map(money),
                                "unrealised": ca.unrealized_gl.map(money), "closing_balance": ca.ending_capital.map(money),
                                "currency": ca.currency})
            if f.fund_id == "F05" and q == "2025Q4":     # unknown LP — must be quarantined, not loaded
                junk = out.iloc[[0]].copy()
                junk["lp_id"] = "INV999"
                out = pd.concat([out, junk], ignore_index=True)
            files[f"tpa2/tpa2_capital_account_{f.fund_id.lower()}_{q.lower()}_v1.csv"] = (q, to_csv(out))
            tx = pd.DataFrame({"txn_ref": [f"TPA2-{f.fund_id}-{s:06d}" for s in nt.seq], "fund_id": nt.fund_id,
                               "lp_id": nt.investor_id, "settle_date": nt.date.map(str), "txn_type": nt.type.map(TPA2_TYPES),
                               "amount": nt.amount.map(money), "currency": nt.currency})
            total = pd.DataFrame([{"txn_ref": "TOTAL", "fund_id": "", "lp_id": "", "settle_date": "", "txn_type": "",
                                   "amount": money(nt.amount.sum()), "currency": ""}])
            files[f"tpa2/tpa2_transactions_{f.fund_id.lower()}_{q.lower()}_v1.csv"] = (q, to_csv(pd.concat([tx, total])))
    pos = POSITIONS[POSITIONS.as_of_date == quarter_end(q)]
    files[f"portfolio_monitoring/valuations_{q}.csv"] = (q, to_csv(pos))
    dtx = DEAL_TXNS[DEAL_TXNS.quarter == q].drop(columns=["quarter"]).copy()
    dtx.insert(0, "txn_id", [f"DT-{q}-{i:04d}" for i in range(1, len(dtx) + 1)])
    dtx["currency"] = dtx.fund_id.map(FUNDS.set_index("fund_id").fund_currency)
    files[f"portfolio_monitoring/deal_transactions_{q}.csv"] = (q, to_csv(dtx))
    fxq = FX[(FX.rate_date >= quarter_start(q)) & (FX.rate_date <= quarter_end(q))]
    files[f"fx/fx_rates_{q}.csv"] = (q, to_csv(fxq))
    return files


written = skipped = 0
existing = {f["path"].replace("\\", "/").split("/Files/landing/")[-1] for f in fs_ls(LANDING)}
for q in quarter_list(FIRST_QUARTER, LAST_QUARTER):
    for rel, (deliver_q, text) in render_quarter(q).items():
        if deliver_q > deliver_through:
            continue
        if rel in existing:
            skipped += 1
            continue
        fs_write_text(f"{LANDING}/{rel}", text)
        written += 1

# Reference data is re-published every run (it is small and slowly changing).
fs_write_text(f"{REFERENCE}/funds.csv", to_csv(FUNDS))
fs_write_text(f"{REFERENCE}/investors.csv", to_csv(INVESTORS))
fs_write_text(f"{REFERENCE}/legal_entities.csv", to_csv(LEGAL_ENTITIES))
fs_write_text(f"{REFERENCE}/commitments.csv", to_csv(COMMITMENTS))
fs_write_text(f"{REFERENCE}/investments.csv", to_csv(INVESTMENTS[INVESTMENTS.investment_date <= quarter_end(deliver_through)]))
print(f"landing files written: {written}, already present: {skipped}, delivered through {deliver_through}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
