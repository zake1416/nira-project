from prefect import flow

from iso_trust.runner import validate


@flow(name="caiso_daily_trust")
def caiso_daily_trust_flow():
    return validate(
        inputs=["data/*.xlsx"],
        config_path="configs/caiso.yaml",
        outdir="out",
    )


if __name__ == "__main__":
    caiso_daily_trust_flow()
