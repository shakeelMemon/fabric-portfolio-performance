# Talk track — presenting this project

Frame it honestly: *"a portfolio build on Microsoft Fabric with synthetic administrator data, using the
patterns I use at work for private-markets reporting."* All fund, LP and company names are fictional.

## 60-second version

> "It's an end-to-end Fabric build for PE and private-credit reporting.
>
> Two fund administrators, Citco for PE and Alter Domus for credit, send quarterly capital accounts and cashflow notices in different formats. They land in OneLake. A metadata-driven bronze layer ingests them as-is. Silver conforms them to one canonical contract, quarantines bad rows with a reason, and MERGEs on business keys, so a `_v2` restatement replaces `_v1`.
>
> Gold is a star schema: cashflows, capital-account snapshots, commitments, positions and credit accruals, with every amount in fund, legal-entity, CAD and USD currency.
>
> IRR, TVPI, DPI and NAV are defined once. They come from a signed flow table, and Python and DAX both compute from that one table.
>
> An 18-check reconciliation gate runs before the Direct Lake model is reframed. In the demo, an administrator's mis-stated Q2 file is blocked by the gate until the restatement arrives."

## Walkthrough order (screen share, about 10 minutes)

1. **Workspace:** Git-connected to `/fabric`. One lakehouse and nine notebooks. `nb_99_run_all` runs the DAG.
2. **Landing files:** show a Citco file (UPPERCASE headers, `YYYYMMDD` dates, negative distributions) next to an Alter Domus file (snake_case headers, `"1,234.56"` amounts, a TOTAL footer row).
3. **`ops_source_mapping`:** onboarding another administrator is a configuration row, not a new notebook.
4. **Silver quarantine:** 44 footer rows and 1 unknown LP, each with its reason. Bronze rows = silver + quarantined + superseded (DQ01).
5. **Gold star schema:** open `fact_cashflow` and explain the contract. Amounts are always positive and `flow_category` gives the direction. Flows use the flow-date FX rate and balances the period-end rate.
6. **`fact_irr_flow`:** each as-of quarter carries its full history plus terminal NAV. That is why one DAX `XIRR` works for any slice.
7. **Quality gate:** walk through DQ04 (roll-forward), DQ05 (opening = prior closing), DQ06/07 (notices = capital account) and DQ08 (LP capital = portfolio valuation). Then run the `--deliver-through 2026Q2` scenario and show it failing on DQ08.
8. **Report:**
   * Overview: switch the currency USD → CAD → fund currency. Mixed-currency slices go blank instead of wrong.
   * Investor matrix.
   * NAV bridge waterfall, including the FX translation bar.
   * Investments: gross vs net.
   * Data Quality page.
9. **RLS:** use *View as* `LP Investor` to show that an LP sees only their own funds and no DQ page.

## Numbers from the demo data (as of 2026-09-30, fund currency)

| Fund | Net TVPI | Gross TVPI | Net IRR | Gross IRR |
|---|---|---|---|---|
| Global Buyout III (PE, USD) | 1.45x | 1.60x | 12.9% | 16.1% |
| Canadian Growth II (PE, CAD) | 1.63x | 1.81x | 17.9% | 21.6% |
| Secondaries I (PE, USD) | 1.37x | 1.45x | 17.3% | 20.3% |
| Senior Credit I (PC, USD) | 1.21x | 1.27x | 7.3% | 9.3% |
| Canadian Credit II (PC, CAD) | 1.15x | 1.21x | 7.2% | 9.9% |
| European Credit I (PC, EUR) | 1.11x | 1.14x | 7.1% | 9.3% |

At the portfolio level (all LPs, USD), net TVPI is 1.32x and net IRR 11.9%. The gross-to-net spread is fees: management fees are called on top of invested capital.

## Likely questions — short answers

* **Why compute IRR flows in Spark rather than DAX?**
  * IRR is sensitive to sign, dates, terminal value and FX. Fixing those in one governed table means the LP statement, the report and the gate can't disagree.
  * DAX only filters and calls XIRR. DQ18 proves the parity.
* **Why rebuild gold every run instead of doing it incrementally?**
  * At quarterly cadence and this volume, a full rebuild is deterministic and trivially reconcilable.
  * At scale I'd partition by `as_of_date` and rebuild only the affected quarters, which the restatement keys tell you.
* **Why does LOCAL go blank?** Summing USD and EUR is meaningless. A blank is defensible; a wrong number in an LP report is not.
* **Why a quarter-average rate in the NAV bridge?** It is standard translation practice for P&L. The FX translation bar makes the remaining difference explicit, so the bridge always closes.
* **Restatements?**
  * The file version is in the name. Bronze keeps both versions, silver MERGE keeps the latest, and `fact_snapshot.file_version` shows which is live.
  * DQ05 catches a restatement that doesn't flow into the next quarter's opening balance.
* **Why Direct Lake, and what are the gotchas?**
  * It gives import-like performance without refresh copies.
  * Gotchas:
    * no calculated tables, so the measure table is a real Delta table;
    * automatic framing must be off if you want a quality gate;
    * V-Order and OPTIMIZE keep framing fast;
    * SQL-endpoint RLS would force DirectQuery fallback, so RLS is defined in the model.
* **Security?** Model RLS by Entra UPN mapping, an app audience instead of workspace access, and no tenant details in Git (the endpoint is injected at deploy time).
* **How is it tested?**
  * CI runs the actual Git-synced notebook files on local Spark 3.5, with parquet standing in for Delta.
  * 15 tests: XIRR against Excel's reference example, plus a three-delivery scenario covering gate failure, restatement and idempotent rerun.
  * They also cover the NAV bridge closing in every currency and Python IRR matching the flow table.
