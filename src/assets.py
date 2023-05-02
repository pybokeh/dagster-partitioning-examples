from dagster import asset, DailyPartitionsDefinition, Definitions, EnvVar, MetadataValue, Output
from dagster_duckdb_pandas import duckdb_pandas_io_manager
import os
import pandas as pd


@asset(
    group_name='prices',
    compute_kind='duckdb',
    # Most recent data in my database was for 2023-04-21, so we want to use the next day as start date
    partitions_def=DailyPartitionsDefinition(start_date='2023-04-22'),
    # Need to tell dagster which table column is our data partitioned on.  In my case, it is the "date" column
    metadata={
        "partition_expr": "date",
    }
)
def daily_stock_prices(context) -> Output[pd.DataFrame]:
    # This is the critical part: the "asset_partition_key" is the date or dates that correspond to the
    # selected partition dates that you have selected in the dagit UI
    partition_date_str = context.asset_partition_key_for_output()
    print(f"partition_date_str: {partition_date_str}")

    symbol = 'VRSK'
    api_key = EnvVar("API_KEY")
    CSV_URL = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={symbol}&interval=60min&slice=year1month1&apikey={api_key}'

    df = pd.read_csv(
        CSV_URL,
        parse_dates=['time']
    )
    
    # The API return's the most recent month's worth of data, so we must limit it to just the particular date
    # that we are attempting to backfill or append with.  Otherwise, we will be inserting duplicate records.
    df_final = (
        df.assign(date=pd.to_datetime(df['time'].dt.date))
        .assign(hour=df['time'].dt.hour)
        .assign(symbol=symbol)
        # Backfill or append to only the corresponding day's data
        .query("hour == 17 and date == @partition_date_str")
        .drop(columns=['time','hour'])
        .reset_index(drop=True)
    )

    return Output(
        value=df_final,
        metadata={
            "preview": MetadataValue.md(df_final.head().to_markdown(index=False))
        }
    )

defs = Definitions(
    assets=[
        daily_stock_prices,
    ],
    resources={
        "io_manager": duckdb_pandas_io_manager.configured(
            {
                "database": {"env": "DUCKDB_DB_PATH"),
            }
        )
    },
)
