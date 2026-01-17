from typing import Iterable, Dict

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


def validate(inputs: Iterable[str], config_path: str, outdir: str) -> Dict:
    cfg = load_config(config_path)
    validate_config(cfg)

    input_files = resolve_inputs(inputs)
    df_raw = read_inputs(input_files)

    normalized, norm_issues, day_info = normalize_caiso(df_raw, cfg)

    issues = []
    issues.extend(norm_issues)

    completeness_issues, completeness_metrics = check_completeness(
        normalized, cfg, day_info
    )
    consistency_issues, consistency_metrics = check_consistency(normalized, cfg)
    sanity_issues, sanity_metrics = check_value_sanity(normalized, cfg)

    issues.extend(completeness_issues)
    issues.extend(consistency_issues)
    issues.extend(sanity_issues)

    metrics = {
        "rows": int(len(normalized)),
        "completeness": completeness_metrics,
        "consistency": consistency_metrics,
        "sanity": sanity_metrics,
        "dst": _dst_metrics(day_info),
    }

    trust = compute_trust_score(metrics, cfg)
    report = build_report(cfg, issues, metrics, trust, input_files)
    write_report_files(report, outdir=outdir)
    return report
