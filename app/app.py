from flask import Flask, request
import dlt
from yfinance import Ticker
from datetime import datetime, timezone
import pandas as pd
import warnings
from google.cloud import bigquery
from google.api_core.exceptions import NotFound  # Import the NotFound exception



warnings.filterwarnings("ignore", category=FutureWarning, module="yfinance.utils")

app = Flask(__name__)

@dlt.resource(
    name="staging_yfinance_data"
)
def fetch_yfinance_data(symbols):
    """
    Fetch stock data for the given symbols and add necessary metadata.
    """
    all_data = []
    for symbol in symbols:
        print(f"Loading {symbol}...")
        ticker = Ticker(symbol)
        data = ticker.history(
            period="1mo", interval="1d"
        ).reset_index()  # Fetch historical data

        # Add ticker symbol and load timestamp
        data["ticker"] = symbol
        data["load_timestamp"] = datetime.now(timezone.utc)
        # Ensure date is in MM-DD-YYYY format
        data["Date"] = pd.to_datetime(data["Date"]).dt.strftime("%m-%d-%Y")

        all_data.append(data)

    # Combine all dataframes and return as a dictionary
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df = combined_df.drop_duplicates(subset=["ticker", "Date"])

    yield combined_df.to_dict(orient="records")

def create_final_table_if_not_exists():
    client = bigquery.Client()
    table_id = "woven-mapper-157313.daily_stock_data.final_yfinance_data"

    schema = [
        bigquery.SchemaField("date", "STRING"),
        bigquery.SchemaField("ticker", "STRING"),
        bigquery.SchemaField("open", "FLOAT"),
        bigquery.SchemaField("close", "FLOAT"),
        bigquery.SchemaField("high", "FLOAT"),
        bigquery.SchemaField("low", "FLOAT"),
        bigquery.SchemaField("volume", "INTEGER"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
    ]

    try:
        client.get_table(table_id)  # Check if table exists
        print(f"Table {table_id} already exists.")
    except NotFound:
        # Create the table if it does not exist
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        print(f"Table {table_id} created.")

def merge_staging_to_target():
    """
    Perform a MERGE operation in BigQuery to deduplicate and update rows.
    """
    client = bigquery.Client()
    staging_table = "woven-mapper-157313.daily_stock_data.staging_yfinance_data"
    target_table = "woven-mapper-157313.daily_stock_data.final_yfinance_data"

    query = f"""
    MERGE `{target_table}` AS target
    USING (
        WITH deduplicated_staging AS (
            SELECT
                date,
                ticker,
                open,
                close,
                high,
                low,
                volume,
                load_timestamp,
                ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY load_timestamp DESC) AS row_num
            FROM `{staging_table}`
            WHERE date IS NOT NULL AND ticker IS NOT NULL
    )
        SELECT * FROM deduplicated_staging WHERE row_num = 1
    ) AS staging
    ON target.ticker = staging.ticker AND target.date = staging.date
    WHEN MATCHED THEN
        UPDATE SET
            target.open = staging.open,
            target.close = staging.close,
            target.high = staging.high,
            target.low = staging.low,
            target.volume = staging.volume,
            target.load_timestamp = staging.load_timestamp
    WHEN NOT MATCHED THEN
        INSERT (date, ticker, open, close, high, low, volume, load_timestamp)
        VALUES (staging.date, staging.ticker, staging.open, staging.close, staging.high, staging.low, staging.volume, staging.load_timestamp);
    """
    query_job = client.query(query)
    query_job.result()  # Wait for the query to complete
    print("Merge operation completed successfully.")


@app.route("/", methods=["POST"])
def run_pipeline():
    """
    Handle POST requests to trigger the dlt pipeline.
    """
    try:
        # Symbols from the POST request or defaults
        symbols = request.get_json().get(
            "symbols", ["AAPL", "GOOGL", "MSFT", "TSLA", "NFLX", "META"]
        )

        # Set up DLT pipeline
        pipeline = dlt.pipeline(destination="bigquery", dataset_name="daily_stock_data")

        # Run the pipeline and load data to a staging table
        pipeline.run(fetch_yfinance_data(symbols))

        # Create the final table (if it doesn't already exist). 
        # DLT doesn't do this automatically, unlike the staging table, which it infers based on the pipeline
        create_final_table_if_not_exists()

        # Call a function to merge the staging table into the final table
        merge_staging_to_target()

        print("Pipeline executed successfully.")
        return "Pipeline executed successfully", 200

    except ValueError as e:
        print(f"Schema validation error: {e}")
        return f"Schema validation error: {e}", 400

    except Exception as e:
        print(f"An error occurred: {e}")
        return f"An error occurred: {e}", 500


if __name__ == "__main__":
    app.run(debug=True, port=8080)
