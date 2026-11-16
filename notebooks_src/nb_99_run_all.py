# %% [markdown]
# # nb_99_run_all — orchestrates the quarterly load
# Runs setup → generate (demo only) → bronze → silver → gold → quality gate as a DAG with
# `notebookutils.notebook.runMultiple`, passing one `RUN_ID` so every audit and DQ row of the run ties
# together. Only if the gate passes is the Direct Lake semantic model reframed, so the report never
# shows a quarter that has not reconciled.
# Schedule this notebook (or call it from a Data Factory pipeline) after the administrators' delivery
# window. Set `generate_demo_files = False` once real files land in `Files/landing/`.

# %%
# Parameters
generate_demo_files = True
deliver_through = "2026Q3"
refresh_semantic_model = True

# %%
# MAGIC %run nb_common

# %%
args = {"RUN_ID": RUN_ID}
activities = [{"name": "nb_00_setup", "path": "nb_00_setup", "args": args, "timeoutPerCellInSeconds": 1800}]
previous = "nb_00_setup"
if generate_demo_files:
    activities.append({"name": "nb_01_generate_source_files", "path": "nb_01_generate_source_files",
                       "args": {**args, "deliver_through": deliver_through}, "dependencies": [previous]})
    previous = "nb_01_generate_source_files"
for nb in ["nb_02_bronze", "nb_03_silver", "nb_04_gold", "nb_05_quality_gate"]:
    activities.append({"name": nb, "path": nb, "args": args, "dependencies": [previous],
                       "timeoutPerCellInSeconds": 3600})
    previous = nb

dag = {"activities": activities, "timeoutInSeconds": 4 * 3600, "concurrency": 1}
print(f"run_id {RUN_ID}: {' → '.join(a['name'] for a in activities)}")
result = notebookutils.notebook.runMultiple(dag, {"displayDAGViaGraphviz": False})  # noqa: F821
print(json.dumps({k: v.get("exitVal") if isinstance(v, dict) else str(v) for k, v in result.items()}, indent=2, default=str))

# %% [markdown]
# Reaching this cell means every activity, including the quality gate, succeeded
# (`runMultiple` raises if any notebook fails).

# %%
if refresh_semantic_model:
    try:
        import sempy.fabric as fabric
        fabric.refresh_dataset(dataset=SEMANTIC_MODEL_NAME, refresh_type="full")
        print(f"Reframed semantic model '{SEMANTIC_MODEL_NAME}'")
    except Exception as exc:  # model not deployed yet — the data load itself succeeded
        print(f"Semantic model refresh skipped: {exc}")
log_audit("nb_99_run_all", "ops", SEMANTIC_MODEL_NAME, len(activities), len(activities), detail="run complete")
