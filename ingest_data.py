#!/usr/bin/env python
# coding: utf-8

import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from tqdm.auto import tqdm

load_dotenv()

PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")

YEAR = 2021
MONTH = 1
TARGET_TABLE = "yellow_taxi_data"
CHUNKSIZE = 100_000

dtype = {
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

parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]


def run():
    """Ingest NYC taxi data into PostgreSQL database."""
    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    url = f"{prefix}/yellow_tripdata_{YEAR}-{MONTH:02d}.csv.gz"

    engine = create_engine(
        f"postgresql+psycopg://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=CHUNKSIZE,
    )

    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(name=TARGET_TABLE, con=engine, if_exists="replace")
            first = False

        df_chunk.to_sql(name=TARGET_TABLE, con=engine, if_exists="append")


if __name__ == "__main__":
    run()
