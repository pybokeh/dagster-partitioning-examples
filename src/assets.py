from dagster import (
    asset,
    AssetIn,
    DailyPartitionsDefinition,
    Definitions,
    EnvVar,
    MetadataValue,
    Output,
    SourceAsset,
    StaticPartitionsDefinition,
)
from dagster_duckdb_pandas import duckdb_pandas_io_manager
from datetime import datetime
from nhtsa_utils import fetch_model_names
from tqdm import tqdm
import io
import pandas as pd
import requests


# https://docs.dagster.io/concepts/assets/software-defined-assets#defining-external-asset-dependencies
# Table make_id_custom is a table that was not created or not materialized by dagster.
# Therefore, need to make this into a SourceAsset so that it is available to assets that depend on it.
make_id_custom = SourceAsset(group_name="static_partitions", key='make_id_custom', io_manager_key='nhtsa_io_manager')
make_id_custom.description = 'Table containing make IDs for cars, trucks, and motorcycles only'


@asset(
    group_name='time_partitions',
    compute_kind='duckdb',
    # Most recent data in sample duckdb database is 2023-04-21, so we want to use the next day as start date
    partitions_def=DailyPartitionsDefinition(start_date='2023-04-22'),
    # Need to tell dagster which table column is our data partitioned on.  In my case, it is the "date" column
    metadata={
        "partition_expr": "date",
    },
    io_manager_key='av_io_manager',
)
def daily_stock_prices(context) -> Output[pd.DataFrame]:
    # This is the critical part: the "asset_partition_key" is the date or dates that correspond to the
    # selected partition dates that you have selected in the dagit UI
    partition_date_str = context.asset_partition_key_for_output()
    print(f"partition_date_str: {partition_date_str}")

    # I know we can configure this asset to use any symbol or api key, but for sake of simplicity, hard-coded them instead
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


veh_type_partition_def = StaticPartitionsDefinition(
    ["passenger", "truck", "motorcycle"]
)


# To consume only make_id column from the make_id_custom source asset, need to add the "ins=" boilerplate
# https://docs.dagster.io/integrations/snowflake/reference#selecting-specific-columns-in-a-downstream-asset
@asset(
    group_name="static_partitions",
    ins={
        "make_id_custom": AssetIn(
            key="make_id_custom",
            metadata={"columns": ["make_id"]},
        )
    },
    compute_kind='duckdb',
    partitions_def=veh_type_partition_def,
    metadata={
        "partition_expr": "vehicletypename",
    },
    io_manager_key='nhtsa_io_manager',
)
def model_names(context, make_id_custom: pd.DataFrame) -> Output[pd.DataFrame]:
    """
    Vehicle model names from NHTSA API (passenger cars and trucks only, last 15 years)

    Parameters
    ----------
    make_id_custom: this is a custom, smaller set of IDs since if we're to do all make IDs, then the downstream
    process of obtaining model names would take a significant amount of time.

    Returns
    -------
    A pandas dataframe containing model name information
    """

    # A good source for vehicle makes: https://cars.usnews.com/cars-trucks/car-brands-available-in-america

    # Instead of hard-coding that we want last 15 model years, we can programmatically define the years for us
    current_year = datetime.today().year
    start_year = datetime.today().year - 14

    df_list = []

    # Initialize progress bar
    progress_bar = tqdm(total=(current_year - start_year + 1) * len(make_id_custom) * 1)

    veh_type_partition_str = context.asset_partition_key_for_output()

    for year in range(start_year, current_year + 1):
        for make_id in make_id_custom['make_id']:
            try:
                response = fetch_model_names(make_id=make_id, model_year=year, vehicle_type=veh_type_partition_str)
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                print(f"HTTP error occurred: {e}")
                continue
            csv_file = io.StringIO(response.content.decode('utf-8'))
            df = pd.read_csv(csv_file)
            df = df.assign(year=year)
            # Some model_names can "look" like int types and so we want to make sure they are explicitly defined as str
            # when concatenating the dataframes together.  Otherwise, will get a pyarrow error due to int/str confusion.
            # Relevant background: https://github.com/wesm/feather/issues/349
            df['model_name'] = df['model_name'].astype('str')
            df_list.append(df)
            progress_bar.update(1)  # increment progress bar

    df_concat = pd.concat(df_list, ignore_index=True)
    today = datetime.today().strftime('%Y-%m-%d')
    df_concat = df_concat.assign(Created_Date=today)

    df_final = df_concat.drop_duplicates()

    # https://docs.dagster.io/tutorial/building-an-asset-graph#step-3-educating-users-with-metadata
    return Output(
        value=df_final,
        metadata={
            "preview": MetadataValue.md(df_final.head().to_markdown(index=False))
        }
    )


# What's really nice about dagster is we can direct our assets to use different databases with very little code changes
# This is known as assigning per asset IO managers: https://docs.dagster.io/concepts/io-management/io-managers#per-asset-io-manager
# Dagster makes it super easy to swap out different backends (de-coupling business logic code from backend code)
defs = Definitions(
    assets=[
        daily_stock_prices,
        make_id_custom,
        model_names,
    ],
    resources={
        "av_io_manager": duckdb_pandas_io_manager.configured(
            {
                "database": {"env": "DUCKDB_DB_PATH_AV"},
            }
        ),
        "nhtsa_io_manager": duckdb_pandas_io_manager.configured(
            {
                "database": {"env": "DUCKDB_DB_PATH_NHTSA"},
            }
        ),
    },
)
