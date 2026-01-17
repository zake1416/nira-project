# iso-onboarding-toolkit

ISO onboarding utilities plus the production-ready prototype:
**ISO Load Integrity & Trust Platform – CAISO Edition**.

## New ISO onboarding steps

1. Create `configs/<ISO>_<feed>.yaml`
2. Run toolkit on 1 week sample data
3. Resolve HIGH severity issues
4. Add CI check to run on every daily file drop
5. Store report artifacts for auditability

## Usage

ISO Trust Platform (CAISO edition):

```bash
python -m iso_trust.cli validate --input data/raw/caiso/*.csv --config configs/caiso.yaml --outdir out
```

Outputs:
- `out/partner_health_report.md`
- `out/partner_health.json`

Pipeline integrations:

```bash
# Dagster UI (all jobs in one workspace)
dagster dev -f src/iso_trust/pipelines/dagster_pipeline.py
# If using Poetry:
# poetry run dagster dev -f src/iso_trust/pipelines/dagster_pipeline.py

# Prefect
python -m iso_trust.pipelines.prefect_flow
```

Dagster jobs available:
- `caiso_daily_trust_job`
- `caiso_historical_backfill_job`
- `caiso_monthly_updates_job`
- `caiso_full_sweep_job` (runs daily + backfill + monthly in one graph)

Dagster schedules (enabled in UI):
- `caiso_daily_schedule`
- `caiso_monthly_schedule`
- `caiso_full_sweep_weekly`

ISO onboarding toolkit (legacy v1):

```bash
python -m iso_toolkit.cli --input examples/sample_iso_load.csv --config examples/config_example.yaml --outdir out
```

Outputs:
- `out/report.md`
- `out/report.json`

## Repo layout

```
iso-onboarding-toolkit/
  README.md
  pyproject.toml
  src/iso_toolkit/
    __init__.py
    cli.py
    config.py
    ingest.py
    checks/
      __init__.py
      schema.py
      timestamp.py
      intervals.py
      units.py
    report.py
  src/iso_trust/
    __init__.py
    cli.py
    runner.py
    config.py
    ingest.py
    normalize.py
    dst.py
    checks/
      __init__.py
      completeness.py
      consistency.py
      sanity.py
    scoring.py
    report.py
    pipelines/
      __init__.py
      dagster_pipeline.py
      prefect_flow.py
  configs/
    caiso.yaml
  examples/
    sample_iso_load.csv
    config_example.yaml
    partner_health_report.md
```
