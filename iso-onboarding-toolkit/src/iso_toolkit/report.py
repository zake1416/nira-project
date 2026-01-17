import os
import json
from datetime import datetime
from typing import Dict, List, Any


def build_report(cfg, df, issues: List[Dict], extra_stats: Dict[str, Any] = None) -> Dict:
    extra_stats = extra_stats or {}
    sev_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    issues_sorted = sorted(issues, key=lambda x: sev_order.get(x["severity"], 9))

    return {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "feed_name": cfg.feed_name,
        "row_count": int(len(df)),
        "issue_counts": {
            "HIGH": sum(1 for i in issues_sorted if i["severity"] == "HIGH"),
            "MEDIUM": sum(1 for i in issues_sorted if i["severity"] == "MEDIUM"),
            "LOW": sum(1 for i in issues_sorted if i["severity"] == "LOW"),
        },
        "issues": issues_sorted,
        "stats": extra_stats,
    }


def _md(report: Dict) -> str:
    c = report["issue_counts"]
    lines = []
    lines.append(f"# ISO Onboarding Report: {report['feed_name']}")
    lines.append("")
    lines.append(f"- Generated (UTC): {report['generated_at_utc']}")
    lines.append(f"- Rows: {report['row_count']}")
    lines.append(f"- Issues: HIGH={c['HIGH']}, MEDIUM={c['MEDIUM']}, LOW={c['LOW']}")
    lines.append("")
    lines.append("## Top Issues")
    lines.append("")
    if not report["issues"]:
        lines.append("No issues detected ✅")
        return "\n".join(lines)

    for i, it in enumerate(report["issues"], 1):
        lines.append(f"### {i}. [{it['severity']}] {it['check']}")
        lines.append(f"- {it['message']}")
        lines.append(f"- Hint: {it['hint']}")
        lines.append("")
    return "\n".join(lines)


def write_report_files(report: Dict, outdir: str) -> None:
    os.makedirs(outdir, exist_ok=True)
    with open(os.path.join(outdir, "report.json"), "w") as f:
        json.dump(report, f, indent=2)

    with open(os.path.join(outdir, "report.md"), "w") as f:
        f.write(_md(report))
