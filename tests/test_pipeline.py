"""End-to-end: runs the real Fabric notebooks locally through three deliveries.

1. administrators deliver through 2026Q2. TPA 1's F01 Q2 capital account over-states one LP by 25,000,
   so LP capital no longer ties to the portfolio valuation: the gate must FAIL (DQ08) and block release.
2. the 2026Q3 delivery arrives together with F01 Q2 v2 (restatement): the gate must pass.
3. the same run again with nothing new (must be a no-op for bronze/silver)
"""
import glob
import os

import pandas as pd
import pytest

import run_local

pytestmark = pytest.mark.slow
LOAD = ["nb_02_bronze", "nb_03_silver", "nb_04_gold", "nb_05_quality_gate"]


def table(root, name):
    files = glob.glob(os.path.join(root, "Tables", name, "*.parquet"))
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def run(spark, notebooks, run_id, **params):
    for nb in notebooks:
        run_local.run_notebook(nb, spark, params={"RUN_ID": run_id, **params})


@pytest.fixture(scope="module")
def lakehouse(spark, tmp_path_factory):
    root = str(tmp_path_factory.mktemp("lakehouse"))
    os.environ["PORTFOLIO_LOCAL_ROOT"] = root
    snapshots = {}
    with pytest.raises(RuntimeError, match="DQ08") as gate_failure:
        run(spark, ["nb_00_setup", "nb_01_generate_source_files"] + LOAD, "run-1", deliver_through="2026Q2")
    snapshots["run1_error"] = str(gate_failure.value)
    snapshots["q2_capital"] = table(root, "silver_capital_account")
    run(spark, ["nb_01_generate_source_files"] + LOAD, "run-2", deliver_through="2026Q3")
    snapshots["files_after_q3"] = len(table(root, "ops_processed_files"))
    run(spark, LOAD, "run-3")
    return root, snapshots


def test_gate_blocks_misstated_quarter(lakehouse):
    root, snaps = lakehouse
    dq = table(root, "ops_dq_results")
    run1_failed = dq[(dq.run_id == "run-1") & (~dq.passed)]
    assert list(run1_failed.check_id) == ["DQ08"], run1_failed[["check_id", "detail"]].to_string()
    assert "F01" in run1_failed.detail.iloc[0] and "2026-06-30" in run1_failed.detail.iloc[0]


def test_quality_gate_passes_after_restatement(lakehouse):
    root, _ = lakehouse
    dq = table(root, "ops_dq_results")
    assert set(dq.run_id) == {"run-1", "run-2", "run-3"}
    failed = dq[(dq.run_id != "run-1") & (dq.severity == "ERROR") & (~dq.passed)]
    assert failed.empty, failed[["run_id", "check_id", "detail"]].to_string()
    assert dq.groupby("run_id").check_id.nunique().eq(18).all()


def test_restatement_v2_replaces_v1(lakehouse):
    root, snaps = lakehouse
    before = snaps["q2_capital"]
    after = table(root, "silver_capital_account")
    key = (after.fund_id == "F01") & (after.period_end.astype(str) == "2026-06-30")
    assert set(after[key]._file_version) == {2}
    b = before[(before.fund_id == "F01") & (before.period_end.astype(str) == "2026-06-30")].set_index("investor_id")
    a = after[key].set_index("investor_id")
    diff = (b.ending_capital - a.ending_capital).abs()
    assert diff.max() == pytest.approx(25_000.0) and (diff > 0).sum() == 1   # exactly the mis-stated LP


def test_rerun_is_idempotent(lakehouse):
    root, snaps = lakehouse
    assert len(table(root, "ops_processed_files")) == snaps["files_after_q3"]
    audit = table(root, "ops_load_audit")
    assert audit[(audit.run_id == "run-3") & (audit.layer.isin(["bronze"]))].empty
    ca = table(root, "silver_capital_account")
    assert not ca.duplicated(["fund_id", "investor_id", "period_end"]).any()


def test_bad_rows_quarantined_not_loaded(lakehouse):
    root, _ = lakehouse
    q = table(root, "silver_quarantine")
    assert q.reason.str.contains("unknown investor").sum() == 1
    assert q.reason.str.contains("footer/total row").sum() == 44
    assert "INV999" not in set(table(root, "silver_capital_account").investor_id)
    cf = table(root, "silver_cashflow")
    assert not cf.cashflow_id.duplicated().any()                       # re-sent TPA 1 notice deduplicated


def test_gold_irr_recomputes_from_fact_flows(lakehouse, common_ns):
    root, _ = lakehouse
    flows = table(root, "fact_irr_flow")
    perf = table(root, "gold_performance")
    asof = perf.as_of_date.max()
    sel = flows[(flows.level == "INVESTOR") & (flows.fund_id == "F02") & (flows.investor_id == "INV004")
                & (flows.as_of_date == asof)]
    expected = common_ns["xirr"](list(sel.flow_date), list(sel.amount_cad))
    got = perf[(perf.grain == "FUND_INVESTOR") & (perf.fund_id == "F02") & (perf.investor_id == "INV004")
               & (perf.as_of_date == asof) & (perf.currency_code == "CAD")].irr.iloc[0]
    assert got == pytest.approx(expected, abs=1e-9)
    assert 0 < got < 0.5


def test_nav_bridge_closes_in_every_currency(lakehouse):
    root, _ = lakehouse
    br = table(root, "gold_nav_bridge")
    s = br.groupby(["fund_id", "investor_id", "period_end", "currency_code", "is_closing"]).amount.sum().unstack()
    assert (s[False] - s[True]).abs().max() < 1.0


def test_mixed_currency_grains_never_report_local(lakehouse):
    root, _ = lakehouse
    perf = table(root, "gold_performance")
    assert perf[(perf.grain == "PORTFOLIO_NET") & (perf.currency_code == "LOCAL")].empty
    # an investor whose funds span currencies gets no LOCAL figure; single-currency investors do
    funds = table(root, "dim_fund").set_index("fund_id").fund_currency
    ccys = table(root, "fact_commitment").assign(c=lambda x: x.fund_id.map(funds)).groupby("investor_id").c.nunique()
    last = perf[perf.as_of_date == perf.as_of_date.max()]                # by then every fund has called capital
    local_inv = set(last[(last.grain == "INVESTOR") & (last.currency_code == "LOCAL")].investor_id)
    assert local_inv == set(ccys[ccys == 1].index)
    assert perf[(perf.grain == "PORTFOLIO_NET") & (perf.currency_code == "LE")].empty
    assert not perf[(perf.grain == "INVESTOR") & (perf.currency_code == "LE")].empty


def test_net_returns_below_gross(lakehouse):
    root, _ = lakehouse
    perf = table(root, "gold_performance")
    last = perf[(perf.as_of_date == perf.as_of_date.max()) & (perf.currency_code == "LOCAL")]
    net = last[last.grain == "FUND_NET"].set_index("fund_id")
    gross = last[last.grain == "FUND_GROSS"].set_index("fund_id")
    gross = gross.reindex(net.index)
    assert (net.tvpi < gross.tvpi).all()                               # fees make net < gross
    assert (net.irr < gross.irr).all()
