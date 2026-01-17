import json
import os
from datetime import datetime
from typing import Dict, List

from iso_trust.config import FeedConfig


def build_report(
    cfg: FeedConfig,
    issues: List[Dict],
    metrics: Dict,
    trust: Dict,
    input_files: List[str],
) -> Dict:
    sev_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    issues_sorted = sorted(issues, key=lambda x: sev_order.get(x["severity"], 9))

    return {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "iso": cfg.iso,
        "input_files": input_files,
        "issue_counts": {
            "HIGH": sum(1 for i in issues_sorted if i["severity"] == "HIGH"),
            "MEDIUM": sum(1 for i in issues_sorted if i["severity"] == "MEDIUM"),
            "LOW": sum(1 for i in issues_sorted if i["severity"] == "LOW"),
        },
        "issues": issues_sorted,
        "metrics": metrics,
        "trust": trust,
    }


def _recommendations(issues: List[Dict]) -> List[str]:
    if not issues:
        return ["No action required. Data meets integrity expectations."]

    recs = []
    for issue in issues:
        if issue["severity"] == "HIGH":
            recs.append(issue["hint"])
    if not recs:
        recs = [issues[0]["hint"]]
    return list(dict.fromkeys(recs))


def _md(report: Dict) -> str:
    trust = report["trust"]
    metrics = report["metrics"]
    issues = report["issues"]

    lines = []
    lines.append(f"# ISO Load Integrity & Trust Report: {report['iso']}")
    lines.append("")
    lines.append("## Executive Summary")
    lines.append("")
    lines.append(f"- Generated (UTC): {report['generated_at_utc']}")
    lines.append(f"- Trust Score: {trust['score']} ({trust['level']})")
    lines.append(
        f"- Issues: HIGH={report['issue_counts']['HIGH']}, "
        f"MEDIUM={report['issue_counts']['MEDIUM']}, LOW={report['issue_counts']['LOW']}"
    )
    lines.append("")

    completeness = metrics.get("completeness", {})
    dst = metrics.get("dst", {})
    consistency = metrics.get("consistency", {})

    lines.append("## Coverage & Completeness")
    lines.append("")
    lines.append(
        f"- Completeness: {completeness.get('completeness_pct', 0):.2%} "
        f"({completeness.get('actual_rows', 0)}/{completeness.get('expected_rows', 0)} rows)"
    )
    lines.append(f"- Missing hours across zones: {completeness.get('missing_hours_total', 0)}")
    lines.append(f"- Missing zones per hour: {completeness.get('missing_zones_per_hour', 0)}")
    lines.append("")

    lines.append("## DST Handling Summary")
    lines.append("")
    lines.append(f"- Total days analyzed: {dst.get('total_days', 0)}")
    lines.append(
        f"- NORMAL: {dst.get('normal_days', 0)}, "
        f"SPRING_FORWARD: {dst.get('spring_forward_days', 0)}, "
        f"FALL_BACK: {dst.get('fall_back_days', 0)}"
    )
    lines.append("")

    lines.append("## Internal Consistency")
    lines.append("")
    lines.append(f"- Consistency violations: {consistency.get('violation_count', 0)}")
    lines.append(
        f"- Max abs error: {consistency.get('max_abs_error', 0):.2f} MW"
    )
    lines.append(
        f"- Max pct error: {consistency.get('max_pct_error', 0):.2%}"
    )
    lines.append("")

    lines.append("## Top Issues")
    lines.append("")
    if not issues:
        lines.append("No issues detected ✅")
    else:
        for idx, issue in enumerate(issues, 1):
            lines.append(f"### {idx}. [{issue['severity']}] {issue['check']}")
            lines.append(f"- {issue['message']}")
            lines.append(f"- Hint: {issue['hint']}")
            lines.append("")

    lines.append("## Recommended Actions")
    lines.append("")
    for rec in _recommendations(issues):
        lines.append(f"- {rec}")

    return "\n".join(lines)


def write_report_files(report: Dict, outdir: str) -> None:
    os.makedirs(outdir, exist_ok=True)

    with open(os.path.join(outdir, "partner_health.json"), "w") as f:
        json.dump(report, f, indent=2)

    with open(os.path.join(outdir, "partner_health_report.md"), "w") as f:
        f.write(_md(report))
