# Dev cloud test run

The executable AWS platform and its full low-cost runbook are maintained on
the `main` branch. This local-demo branch keeps this pointer so cross-branch
documentation links remain valid without copying cloud-only procedures.

```powershell
git checkout main
Get-Content docs/runbooks/dev-cloud-test-run.md
```

The cloud run must include POS Parquet, the Spark GraphFrames identity step,
Redshift/Spectrum SQL execution, dbt, Great Expectations, evidence capture,
and teardown. `run_cloud_stack.ps1 -Task all` is only an infrastructure and
streaming smoke sequence; it is not the Gold/C360 acceptance run.
