# PURPOSE
To understand dagster's asset partitioning.  I still have a hard time understanding how partitioning is done by reading their documentation.  So I decided to just dig in and figure it out by making a couple practical examples: one that utilizes time-based partitioning and one that utilizes static partitioning. To come up with data I can use for time-based partitioning, I decided to obtain stock price data from alphavantage.co since they have a free API key that you can obtain and it is time series data.  For data for a static partition example, I decided to obtain vehicle model names from NHTSA's [vPIC](https://vpic.nhtsa.dot.gov/api/) api.  NHTSA's model names data have vehicle type names like "passenger", "truck", "motorcycle", which I think would be a good way to _partition_ the data by vehicle type (static partitioning).

By looking at dagster's partition examples, I still was sort of lost.  Like in this [example](https://docs.dagster.io/concepts/partitions-schedules-sensors/partitions#defining-partitioned-assets), it has start_date parameter, but I did not know what the date should be and based on what logic?  Not after some trial and error did I realize what it should be.  I figured out it depends on what data you currently have.  So if I have a table of data, and the most recent data is 2023-04-21, then I should set the start_date to be 2023-04-22. I also didn't know what exactly context.asset_partition_key_for_output() is or does?  Not after printing it and looking at the UI, did I figured out what it is or what it does.

Furthermore, I was using duckdb IO manager, but I soon learned dagster's documentation for duckdb integration isn't as complete as their Snowflake documenation.  It was not until I looked at their Snowflake's [documenation](https://docs.dagster.io/integrations/snowflake/reference#storing-partitioned-assets) did I learn that I need to add the metadata= parameter to add the partition_expr key and it's value.

After figuring out dagster's way of partitioning and also what partitioning does in general, I come to realize the benefits that it affords.  Previously, I would perform full table refreshes.  Essentially doing a "truncate then load" operation.  For large tables, this is quite an expensive operation.  In contrast, with partitioning, I am only loading new data into the table or limiting how much data I need to actually load.  This is performing what is known as incremental load.  With dagster's asset partitioning, it makes this process much easier and manageable.

## Getting Started
This repo's examples assume that you have set 3 environment variables, alpha vantage sample duckdb database, and nhtsa sample duckdb database:

1. API_KEY which is your alpha vantage API key
2. DUCKDB_DB_PATH_AV which is the path to your alpha vantage duckdb database
3. DUCKDB_DB_PATH_NHTSA which is the path to your nhtsa duckddb databse
4. alphavantage.duckdb which is available in this repo's `db` folder.  It has data thru 2023-04-21.
5. nhtsa.duckdb which is also available in this repo's `db` folder.

You can enter the environment variables in a text file called .env per their recommended ["best practices"](https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets) or issue `export` or `set` commands.

My .env file contains the following which you would modify to suite your specific situation:
```
API_KEY=your_alpha_vantage_api_key
DUCKDB_DB_PATH_AV=\path_to_your\alphavantage.duckdb
DUCKDB_DB_PATH_NHTSA=\path_to_your_\nhtsa.duckdb
```

If you do plan to use the .env file, you need to save it in the same location as the assets.py file in this repo's `src` folder.

After you have successfully executed runs to materialize the new partitions, you can then query the duckdb databases to confirm that the new records have been inserted.  Then if you want to start over or try again, you can reset the sample alpha vantage duckdb database to the original state by issuing the following command in duckdb CLI:

`delete from public.daily_stock_prices where date > '2021-4-21';`

Also in the dagit UI, go to Assets, select "daily_stock_prices" and then over on the right, choose option to "Wipe materializations".

Similarly to reset the nhtsa.duckdb database to its original state, you could issue the following command using the duckdb CLI:

`delete from public.model_names;`

Remember to also "Wipe materializations" from the dagit UI for the `model_names` asset.

## dagster.yaml file
From my experience, Alpha Vantage API is not very receptive to concurrent API calls.  The following is what I have in my dagster.yaml file to ensure you don't have run failures:
```
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 1
telemetry:
  enabled: false
```
