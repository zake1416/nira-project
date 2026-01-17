import argparse

from iso_trust.runner import validate


def run_validate(args) -> None:
    validate(args.input, args.config, args.outdir)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ISO Load Integrity & Trust Platform - CAISO Edition"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    validate = sub.add_parser("validate", help="Validate and score ISO load data")
    validate.add_argument("--input", nargs="+", required=True, help="Input CSV files")
    validate.add_argument("--config", required=True, help="Path to YAML config")
    validate.add_argument("--outdir", default="out", help="Output directory")
    validate.set_defaults(func=run_validate)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
