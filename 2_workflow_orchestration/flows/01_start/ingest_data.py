import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from time import time

def ingest_data(user, password, host, port, db, table_name, csv_url):
  # the backup files are gzipped, and it's important to keep the correct extension
  # for pandas to be able to open the file
  if csv_url.endswith('.csv.gz'):
      csv_name = 'output.csv.gz'
  else:
      csv_name = 'output.csv'

  os.system(f"wget {csv_url} -O {csv_name}")

  engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

  df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

  df = next(df_iter)

  df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
  df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

  df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
  df.to_sql(name=table_name, con=engine, if_exists="append")

  while True:
    t_start = time()

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.to_sql(name=table_name, con=engine, if_exists="append")

    t_end = time()

    print('inserted another chunk, took %.3f seconds' % (t_end - t_start))

if __name__ == "__main__":
  user = "root"
  password = "root"
  host = "localhost"
  port = "5432"
  db = "ny_taxi"
  table_name = "yellow_taxi_trips"
  csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

  ingest_data(user, password, host, port, db, table_name, csv_url)