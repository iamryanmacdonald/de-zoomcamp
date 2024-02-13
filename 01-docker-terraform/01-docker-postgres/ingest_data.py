import argparse
import os
import pandas as pd

from sqlalchemy import create_engine
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True: 
        t_start = time()
            
        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk (took %.3f seconds)' % (t_end - t_start))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to PostgreSQL')

    parser.add_argument('--user', required=True, help='User name for PostgreSQL')
    parser.add_argument('--password', required=True, help='Password for PostgreSQL')
    parser.add_argument('--host', required=True, help='Host for PostgreSQL')
    parser.add_argument('--port', required=True, help='Port for PostgreSQL')
    parser.add_argument('--db', required=True, help='Database name for PostgreSQL')
    parser.add_argument('--table_name', required=True, help='Name of the PostgreSQL table')
    parser.add_argument('--url', required=True, help='URL of the CSV file')

    args = parser.parse_args()

    main(args)