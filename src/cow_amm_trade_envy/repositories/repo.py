from dagster import repository
from cow_amm_trade_envy.jobs.calculate_envy_job import envy_job
from cow_amm_trade_envy.schedules.weekly_schedule import weekly_schedule
from cow_amm_trade_envy.assets.assets import (
    run_download_dune,
)


@repository
def price_improvement_repository():
    return [envy_job, weekly_schedule, run_download_dune]
