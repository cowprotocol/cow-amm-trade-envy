from dagster import schedule
from cow_amm_trade_envy.jobs.calculate_envy_job import envy_job


@schedule(
    cron_schedule="0 * * * *",  # todo adjust to weekly
    job=envy_job,
    name="weekly_schedule",
)
def weekly_schedule():
    return {}
