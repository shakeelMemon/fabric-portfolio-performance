# Runbook

## 1. First deployment

| Step | Action | Check |
|---|---|---|
| 1 | Create a workspace on F2+ (or Trial) capacity | Workspace shows the capacity |
| 2 | **Workspace settings → Git integration**: connect the repo, branch `main`, **Git folder `/fabric`**. Then **Update all** | `lh_portfolio` and 9 notebooks appear |
| 3 | Open `nb_99_run_all` → **Run all** (about 5–10 min on F2) | The last cell prints the run_id. `ops_dq_results` has 18 rows with `passed = true` |
| 4 | Open `nb_00_setup`, run the last cell, and copy the **SQL endpoint server** and **id** | Two values printed |
| 5 | Locally: `python tools/configure_powerbi.py --server <server> --endpoint-id <id> --into-fabric`. Commit, push, then **Update all** in the workspace | Semantic model + report items appear |
| 6 | Semantic model → **Settings**: (a) **Refresh → turn off** *Keep your Direct Lake data up to date*; (b) **Security** → add users or groups to `LP Investor` / `Portfolio Team` | Report opens; visuals render |
| 7 | Replace the demo UPNs in `nb_04_gold` (`sec_user_investor`) with real mappings, or load them from a governed list | Use *View as* `LP Investor` to see a single investor |
| 8 | Schedule `nb_99_run_all` (for example, daily during the delivery window) and `nb_06_maintenance` (weekly) | Monitor hub shows the schedules |

> If step 5's report fails to open, the model or layout format probably differs from your Desktop or Fabric build. Follow `POWERBI_BUILD_GUIDE.md`, which builds the same model in the service from the lakehouse.

## 2. Switching from demo data to real files

1. Set `generate_demo_files = False` in `nb_99_run_all`. You can also remove `nb_01` from the workspace.
2. Land administrator files in `Files/landing/citco/`, `Files/landing/alterdomus/` and so on. Delivery can be by Data Factory copy activity from SFTP, a OneLake shortcut, or the OneLake file explorer.
3. File names must match the `file_regex` in `ops_source_mapping`, and restatements carry `_v2`, `_v3`. To accept a new naming pattern, change the mapping row and re-run `nb_00_setup`.
4. Maintain `Files/reference/*.csv` from the fund-accounting master. In production, replace them with a mirrored or shortcut source table.

## 3. Quarterly operation

| When | What |
|---|---|
| Quarter end + 30–45 days | Administrators deliver. The scheduled `nb_99_run_all` ingests whatever has landed |
| After each run | Check the **Data Quality** page or `ops_dq_results` for the run_id |
| Gate passed | The model is reframed automatically, and reports show the new quarter |
| Gate failed | The previous quarter stays visible. Follow section 4 |
| LP statements | Extract from `gold_performance` (grain `FUND_INVESTOR`, currency = the LP's legal-entity currency) |

## 4. When the quality gate fails

```sql
-- SQL analytics endpoint
SELECT check_id, check_name, failed_rows, detail
FROM dbo.ops_dq_results
WHERE run_id = (SELECT MAX(run_id) FROM dbo.ops_dq_results) AND passed = 0;
```

| Check | Usual cause | Fix |
|---|---|---|
| DQ01/02 | Header change or new value format from an administrator | Look at `silver_quarantine.reason` and update `ops_source_mapping` (column_map / type_map) |
| DQ04 | Administrator file doesn't roll forward | Return it to the administrator and wait for `_v2` |
| DQ05 | A prior quarter was restated in a way that doesn't flow forward, or a quarter is missing | Ask for the corrected prior quarter, or the missing file |
| DQ06/07 | Notices are missing, or a notice was sent in the wrong quarter | Ask for the notice file and confirm the value dates |
| DQ08 | LP capital doesn't tie to the portfolio valuation — **the most common real issue** (valuation updated after the capital account was struck) | Agree which side is right with fund accounting, then get the `_v2` capital account or the corrected valuation file |
| DQ09 | FX rates missing for new dates or currencies | Land the FX file and re-run |
| DQ10 | New fund, investor or investment missing from reference data | Update `Files/reference/*.csv` and re-run |
| DQ17 | An administrator hasn't delivered yet | Wait. Expected during the delivery window |

Re-running is safe at any time. Files already processed are skipped, silver uses MERGE, and gold is rebuilt.

## 5. Restatements

Land the file with the next version suffix (for example `CITCO_CAPACCT_F01_2026Q2_v2.csv`). Bronze ingests it as a new file, and silver MERGE replaces the earlier version for the same business keys. `fact_snapshot.file_version` shows which version is live.

## 6. Onboarding a new administrator or source

1. Add a dict to `SOURCE_MAPPING` in `nb_00_setup` with:
   * `landing_subfolder`, `file_regex` and `bronze_table`;
   * the canonical `entity` (`capital_account` or `cashflow`);
   * `column_map`, `date_format`, `amount_format`, `sign_convention`;
   * `type_map` for cashflows.
2. Run `nb_00_setup`, land a sample file, and run `nb_99_run_all`. Check the quarantine table and DQ01.
3. Add the funds to `funds.csv` with the new `administrator` value.

## 7. Local development

```bash
python local/run_local.py --fresh                       # full pipeline, ~2 min
python local/run_local.py --fresh --deliver-through 2026Q2   # simulate an earlier delivery (gate fails on DQ08 by design)
python -m pytest -v
python tools/build_fabric_items.py                      # after editing notebooks_src/
python tools/build_powerbi.py                           # after changing measures / tables (needs a local run first)
```

Edit notebooks in `notebooks_src/` (VS Code `# %%` cells) and rebuild `fabric/`. If you edit in the Fabric UI instead, commit from the workspace and then copy the change back to `notebooks_src/`. CI fails if the two drift.
