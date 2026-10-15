# Gold coverage artifacts

dbt `on-run-end` writes invocation-specific reports here when all five Gold models
were successfully rebuilt in the same command. Generated CSV/JSON files are ignored
by Git. Keep this directory present in local checkouts and the Airflow data mount.

- `<invocation_id>-by_period.csv`: coverage and input diagnostics by rebalance date.
- `<invocation_id>-by_sector.csv`: the same metrics by date and sector.
- `<invocation_id>-input_diagnostics.csv`: ticker/date details, scores, missing-input flags.
- `<invocation_id>-run.json`: research mode, definition, selected-build validation and latest coverage.

Counts of missing inputs overlap; they are diagnostics, not exclusive causal attribution.
A missing normalized score may also result from insufficient sector observations or
zero dispersion. Quality is a quarterly net-income YoY growth proxy, not analyst surprise.
An eligible score does not upgrade observation-based universe history to effective history.

Reports record the selected dbt command's validation, including warnings. The final
Airflow quality-gates task must also succeed before considering the pipeline successful.
Partial Gold selections, standalone `dbt test`, or failures before all Gold models are
built do not publish a fresh complete coverage report; inspect dbt run results/task logs.
See [runtime guide](../../../gold-runtime-guide.md) for commands and policies.
