from dagster import Definitions, Out, ScheduleDefinition, job, op

from iso_trust.config import load_config, validate_config
from iso_trust.ingest import read_inputs, resolve_inputs
from iso_trust.normalize import normalize_caiso
from iso_trust.checks.completeness import check_completeness
from iso_trust.checks.consistency import check_consistency
from iso_trust.checks.sanity import check_value_sanity
from iso_trust.scoring import compute_trust_score
from iso_trust.report import build_report, write_report_files


def _dst_metrics(day_info):
    total_days = len(day_info)
    normal_days = sum(1 for d in day_info.values() if d["type"] == "NORMAL")
    spring_days = sum(1 for d in day_info.values() if d["type"] == "SPRING_FORWARD")
    fall_days = sum(1 for d in day_info.values() if d["type"] == "FALL_BACK")

    return {
        "total_days": total_days,
        "normal_days": normal_days,
        "spring_forward_days": spring_days,
        "fall_back_days": fall_days,
        "dst_days": spring_days + fall_days,
    }


@op(config_schema={"config_path": str})
def load_config_op(context):
    cfg = load_config(context.op_config["config_path"])
    validate_config(cfg)
    return cfg


@op(config_schema={"inputs": [str]})
def resolve_inputs_op(context):
    return resolve_inputs(context.op_config["inputs"])


@op
def ingest_op(input_files):
    return read_inputs(input_files)


@op(out={"normalized": Out(), "issues": Out(), "day_info": Out()})
def normalize_op(df_raw, cfg):
    normalized, issues, day_info = normalize_caiso(df_raw, cfg)
    return normalized, issues, day_info


@op(out={"issues": Out(), "metrics": Out()})
def completeness_op(normalized, cfg, day_info):
    return check_completeness(normalized, cfg, day_info)


@op(out={"issues": Out(), "metrics": Out()})
def consistency_op(normalized, cfg):
    return check_consistency(normalized, cfg)


@op(out={"issues": Out(), "metrics": Out()})
def sanity_op(normalized, cfg):
    return check_value_sanity(normalized, cfg)


@op
def aggregate_issues_op(norm_issues, completeness_issues, consistency_issues, sanity_issues):
    issues = []
    issues.extend(norm_issues)
    issues.extend(completeness_issues)
    issues.extend(consistency_issues)
    issues.extend(sanity_issues)
    return issues


@op
def build_metrics_op(normalized, day_info, completeness_metrics, consistency_metrics, sanity_metrics):
    return {
        "rows": int(len(normalized)),
        "completeness": completeness_metrics,
        "consistency": consistency_metrics,
        "sanity": sanity_metrics,
        "dst": _dst_metrics(day_info),
    }


@op
def trust_op(metrics, cfg):
    return compute_trust_score(metrics, cfg)


@op(config_schema={"outdir": str})
def report_op(context, cfg, issues, metrics, trust, input_files):
    report = build_report(cfg, issues, metrics, trust, input_files)
    write_report_files(report, outdir=context.op_config["outdir"])
    return report


@job(
    name="caiso_daily_trust_job",
    config={
        "ops": {
            "load_config_op": {"config": {"config_path": "configs/caiso.yaml"}},
            "resolve_inputs_op": {"config": {"inputs": ["data/*.xlsx"]}},
            "report_op": {"config": {"outdir": "out"}},
        }
    },
)
def caiso_daily_trust_job():
    cfg = load_config_op()
    input_files = resolve_inputs_op()
    df_raw = ingest_op(input_files)
    normalized, norm_issues, day_info = normalize_op(df_raw, cfg)
    completeness_issues, completeness_metrics = completeness_op(normalized, cfg, day_info)
    consistency_issues, consistency_metrics = consistency_op(normalized, cfg)
    sanity_issues, sanity_metrics = sanity_op(normalized, cfg)
    issues = aggregate_issues_op(
        norm_issues,
        completeness_issues,
        consistency_issues,
        sanity_issues,
    )
    metrics = build_metrics_op(
        normalized,
        day_info,
        completeness_metrics,
        consistency_metrics,
        sanity_metrics,
    )
    trust = trust_op(metrics, cfg)
    report_op(cfg, issues, metrics, trust, input_files)


@job(
    name="caiso_historical_backfill_job",
    config={
        "ops": {
            "load_config_op": {"config": {"config_path": "configs/caiso.yaml"}},
            "resolve_inputs_op": {
                "config": {"inputs": ["data/historicalemshourlyload-*.xlsx"]}
            },
            "report_op": {"config": {"outdir": "out/backfill"}},
        }
    },
)
def caiso_historical_backfill_job():
    cfg = load_config_op()
    input_files = resolve_inputs_op()
    df_raw = ingest_op(input_files)
    normalized, norm_issues, day_info = normalize_op(df_raw, cfg)
    completeness_issues, completeness_metrics = completeness_op(normalized, cfg, day_info)
    consistency_issues, consistency_metrics = consistency_op(normalized, cfg)
    sanity_issues, sanity_metrics = sanity_op(normalized, cfg)
    issues = aggregate_issues_op(
        norm_issues,
        completeness_issues,
        consistency_issues,
        sanity_issues,
    )
    metrics = build_metrics_op(
        normalized,
        day_info,
        completeness_metrics,
        consistency_metrics,
        sanity_metrics,
    )
    trust = trust_op(metrics, cfg)
    report_op(cfg, issues, metrics, trust, input_files)


@job(
    name="caiso_monthly_updates_job",
    config={
        "ops": {
            "load_config_op": {"config": {"config_path": "configs/caiso.yaml"}},
            "resolve_inputs_op": {
                "config": {"inputs": ["data/historical-ems-hourly-load-for-*.xlsx"]}
            },
            "report_op": {"config": {"outdir": "out/monthly"}},
        }
    },
)
def caiso_monthly_updates_job():
    cfg = load_config_op()
    input_files = resolve_inputs_op()
    df_raw = ingest_op(input_files)
    normalized, norm_issues, day_info = normalize_op(df_raw, cfg)
    completeness_issues, completeness_metrics = completeness_op(normalized, cfg, day_info)
    consistency_issues, consistency_metrics = consistency_op(normalized, cfg)
    sanity_issues, sanity_metrics = sanity_op(normalized, cfg)
    issues = aggregate_issues_op(
        norm_issues,
        completeness_issues,
        consistency_issues,
        sanity_issues,
    )
    metrics = build_metrics_op(
        normalized,
        day_info,
        completeness_metrics,
        consistency_metrics,
        sanity_metrics,
    )
    trust = trust_op(metrics, cfg)
    report_op(cfg, issues, metrics, trust, input_files)


@job(
    name="caiso_full_sweep_job",
    config={
        "ops": {
            "load_config_daily": {"config": {"config_path": "configs/caiso.yaml"}},
            "resolve_inputs_daily": {"config": {"inputs": ["data/*.xlsx"]}},
            "report_daily": {"config": {"outdir": "out"}},
            "load_config_backfill": {"config": {"config_path": "configs/caiso.yaml"}},
            "resolve_inputs_backfill": {
                "config": {"inputs": ["data/historicalemshourlyload-*.xlsx"]}
            },
            "report_backfill": {"config": {"outdir": "out/backfill"}},
            "load_config_monthly": {"config": {"config_path": "configs/caiso.yaml"}},
            "resolve_inputs_monthly": {
                "config": {"inputs": ["data/historical-ems-hourly-load-for-*.xlsx"]}
            },
            "report_monthly": {"config": {"outdir": "out/monthly"}},
        }
    },
)
def caiso_full_sweep_job():
    cfg_daily = load_config_op.alias("load_config_daily")()
    inputs_daily = resolve_inputs_op.alias("resolve_inputs_daily")()
    df_daily = ingest_op.alias("ingest_daily")(inputs_daily)
    normalized_daily, norm_issues_daily, day_info_daily = normalize_op.alias("normalize_daily")(df_daily, cfg_daily)
    completeness_issues_daily, completeness_metrics_daily = completeness_op.alias("completeness_daily")(
        normalized_daily, cfg_daily, day_info_daily
    )
    consistency_issues_daily, consistency_metrics_daily = consistency_op.alias("consistency_daily")(
        normalized_daily, cfg_daily
    )
    sanity_issues_daily, sanity_metrics_daily = sanity_op.alias("sanity_daily")(
        normalized_daily, cfg_daily
    )
    issues_daily = aggregate_issues_op.alias("aggregate_issues_daily")(
        norm_issues_daily,
        completeness_issues_daily,
        consistency_issues_daily,
        sanity_issues_daily,
    )
    metrics_daily = build_metrics_op.alias("build_metrics_daily")(
        normalized_daily,
        day_info_daily,
        completeness_metrics_daily,
        consistency_metrics_daily,
        sanity_metrics_daily,
    )
    trust_daily = trust_op.alias("trust_daily")(metrics_daily, cfg_daily)
    report_op.alias("report_daily")(cfg_daily, issues_daily, metrics_daily, trust_daily, inputs_daily)

    cfg_backfill = load_config_op.alias("load_config_backfill")()
    inputs_backfill = resolve_inputs_op.alias("resolve_inputs_backfill")()
    df_backfill = ingest_op.alias("ingest_backfill")(inputs_backfill)
    normalized_backfill, norm_issues_backfill, day_info_backfill = normalize_op.alias("normalize_backfill")(
        df_backfill, cfg_backfill
    )
    completeness_issues_backfill, completeness_metrics_backfill = completeness_op.alias("completeness_backfill")(
        normalized_backfill, cfg_backfill, day_info_backfill
    )
    consistency_issues_backfill, consistency_metrics_backfill = consistency_op.alias("consistency_backfill")(
        normalized_backfill, cfg_backfill
    )
    sanity_issues_backfill, sanity_metrics_backfill = sanity_op.alias("sanity_backfill")(
        normalized_backfill, cfg_backfill
    )
    issues_backfill = aggregate_issues_op.alias("aggregate_issues_backfill")(
        norm_issues_backfill,
        completeness_issues_backfill,
        consistency_issues_backfill,
        sanity_issues_backfill,
    )
    metrics_backfill = build_metrics_op.alias("build_metrics_backfill")(
        normalized_backfill,
        day_info_backfill,
        completeness_metrics_backfill,
        consistency_metrics_backfill,
        sanity_metrics_backfill,
    )
    trust_backfill = trust_op.alias("trust_backfill")(metrics_backfill, cfg_backfill)
    report_op.alias("report_backfill")(cfg_backfill, issues_backfill, metrics_backfill, trust_backfill, inputs_backfill)

    cfg_monthly = load_config_op.alias("load_config_monthly")()
    inputs_monthly = resolve_inputs_op.alias("resolve_inputs_monthly")()
    df_monthly = ingest_op.alias("ingest_monthly")(inputs_monthly)
    normalized_monthly, norm_issues_monthly, day_info_monthly = normalize_op.alias("normalize_monthly")(
        df_monthly, cfg_monthly
    )
    completeness_issues_monthly, completeness_metrics_monthly = completeness_op.alias("completeness_monthly")(
        normalized_monthly, cfg_monthly, day_info_monthly
    )
    consistency_issues_monthly, consistency_metrics_monthly = consistency_op.alias("consistency_monthly")(
        normalized_monthly, cfg_monthly
    )
    sanity_issues_monthly, sanity_metrics_monthly = sanity_op.alias("sanity_monthly")(
        normalized_monthly, cfg_monthly
    )
    issues_monthly = aggregate_issues_op.alias("aggregate_issues_monthly")(
        norm_issues_monthly,
        completeness_issues_monthly,
        consistency_issues_monthly,
        sanity_issues_monthly,
    )
    metrics_monthly = build_metrics_op.alias("build_metrics_monthly")(
        normalized_monthly,
        day_info_monthly,
        completeness_metrics_monthly,
        consistency_metrics_monthly,
        sanity_metrics_monthly,
    )
    trust_monthly = trust_op.alias("trust_monthly")(metrics_monthly, cfg_monthly)
    report_op.alias("report_monthly")(cfg_monthly, issues_monthly, metrics_monthly, trust_monthly, inputs_monthly)


defs = Definitions(
    jobs=[
        caiso_daily_trust_job,
        caiso_historical_backfill_job,
        caiso_monthly_updates_job,
        caiso_full_sweep_job,
    ],
    schedules=[
        ScheduleDefinition(
            job=caiso_daily_trust_job,
            cron_schedule="0 6 * * *",
            name="caiso_daily_schedule",
        ),
        ScheduleDefinition(
            job=caiso_monthly_updates_job,
            cron_schedule="0 8 1 * *",
            name="caiso_monthly_schedule",
        ),
        ScheduleDefinition(
            job=caiso_full_sweep_job,
            cron_schedule="0 4 * * 0",
            name="caiso_full_sweep_weekly",
        ),
    ]
)
