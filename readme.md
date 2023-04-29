# PURPOSE
To understand dagster's asset partitioning.  I still have a hard time understanding how partitioning is done by reading their documentation.  So I decided to just dig in and figure it out by using an example. In order to do so, I needed to obtain some sample data.  I decided to get stock price data from alphavantage.co since they have a free API key that you can obtain and since it is time series data, I thought I could look at dagster's time-based partitioning.  By looking at their partition examples, I still was sort of lost.  Like in this [example](https://docs.dagster.io/concepts/partitions-schedules-sensors/partitions#defining-partitioned-assets), it has start_date parameter, but I did not know what the data should be and based on what logic?  Not after some trial and error did I realize what it should be.  I figured out tt depends on what data you currently have.  So if I have a table of data, and the most recent data is 2023-04-21, then I should set the start_date to be 2023-04-22.

I was using duckdb IO manager, but I soon learned dagster's documentation for duckdb integration isn't as complete as their Snowflake documenation.  It was not until I looked at their Snowflake's [documenation](https://docs.dagster.io/integrations/snowflake/reference#storing-partitioned-assets) did I learn that I need to add the metadata= parameter to add the partition_expr key and it's value. 
