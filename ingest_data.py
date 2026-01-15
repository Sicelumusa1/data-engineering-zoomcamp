#!/usr/bin/env python
# coding: utf-8

"""
Chunked ingestion script for NYC Taxi data.

This script downloads a CSV file, creates a target table in Postgres,
and ingests the data incrementally using pandas chunking.
"""

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click


# Explicit schema to avoid dtype inference issues
DTYPE = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

PARSE_DATES = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]


def ingest_data(url: str, engine, target_table: str, chunksize: int = 100_000,) -> None:
    """
    Stream a CSV file into Postgres in chunks.

    The table schema is created once using an empty DataFrame,
    then data is appended incrementally to avoid loading the full
    dataset into memory.

    Args:
        url: URL to the CSV (or local path).
        engine: SQLAlchemy engine connected to Postgres.
        target_table: Name of the destination table.
        chunksize: Number of rows per chunk.
    """
    df_iter = pd.read_csv(url,dtype=DTYPE,parse_dates=PARSE_DATES,iterator=True,chunksize=chunksize,)

    # Read first chunk to define schema
    first_chunk = next(df_iter)

    # Create empty table with correct schema
    first_chunk.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace",
    )
    print(f"Table '{target_table}' created")

    # Insert first batch of data
    first_chunk.to_sql(
        name=target_table,
        con=engine,
        if_exists="append",
    )
    print(f"Inserted first chunk: {len(first_chunk)} rows")

    # Stream remaining chunks
    for df_chunk in tqdm(df_iter, desc="Inserting chunks"):
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
        )

    print(f"Done ingesting data into '{target_table}'")


@click.command()
@click.option("--pg-user", default="root", show_default=True, help="Postgres user")
@click.option("--pg-pass", default="root", show_default=True, help="Postgres password")
@click.option("--pg-host", default="localhost", show_default=True, help="Postgres host")
@click.option("--pg-port", default=5432, show_default=True, help="Postgres port")
@click.option("--pg-db", default="ny_taxi", show_default=True, help="Postgres database")
@click.option("--year", default=2021, show_default=True, help="Dataset year")
@click.option("--month", default=1, show_default=True, help="Dataset month")
@click.option("--chunksize", default=100_000, show_default=True, help="Rows per chunk")
@click.option(
    "--target-table",
    default="yellow_taxi_data",
    show_default=True,
    help="Destination table name",
)
def main(
    pg_user: str,
    pg_pass: str,
    pg_host: str,
    pg_port: int,
    pg_db: str,
    year: int,
    month: int,
    chunksize: int,
    target_table: str,
) -> None:
    """
    Entry point for the ingestion job.

    Builds the database connection, constructs the dataset URL,
    and triggers the chunked ingestion process.
    """
    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    url_prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    url = f"{url_prefix}/yellow_tripdata_{year:04d}-{month:02d}.csv.gz"

    ingest_data(
        url=url,
        engine=engine,
        target_table=target_table,
        chunksize=chunksize,
    )


if __name__ == "__main__":
    main()
