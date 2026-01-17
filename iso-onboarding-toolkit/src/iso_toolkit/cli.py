import argparse

from iso_toolkit.config import load_config
from iso_toolkit.ingest import read_input
from iso_toolkit.checks.schema import check_schema
from iso_toolkit.checks.timestamp import normalize_timestamps_and_check_dst
from iso_toolkit.checks.intervals import check_missing_intervals
from iso_toolkit.checks.units import check_units
from iso_toolkit.report import build_report, write_report_files


def main() -> None:
    p = argparse.ArgumentParser(description="ISO Data Onboarding Toolkit")
    p.add_argument("--input", required=True, help="Path to ISO feed file (CSV for v1)")
    p.add_argument("--config", required=True, help="Path to YAML config")
    p.add_argument("--outdir", default="out", help="Output directory for reports")
    args = p.parse_args()

    cfg = load_config(args.config)
    df = read_input(args.input)

    issues = []
    issues += check_schema(df, cfg)

    # Timestamp normalization + DST checks returns a normalized df
    df_norm, ts_issues, ts_stats = normalize_timestamps_and_check_dst(df, cfg)
    issues += ts_issues

    issues += check_missing_intervals(df_norm, cfg)
    issues += check_units(df_norm, cfg)

    report = build_report(cfg, df_norm, issues, extra_stats={"timestamp": ts_stats})
    write_report_files(report, outdir=args.outdir)


if __name__ == "__main__":
    main()
