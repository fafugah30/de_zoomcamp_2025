import pandas as pd
import requests
import gzip
from time import time
from config import config  # type: ignore
from sqlalchemy import create_engine, text

def download_data(url, filename):
    print(f'Downloading data from {url}...')
    response = requests.get(url)
    with open(filename, 'wb') as file:
        file.write(response.content)
    print('Download complete.')

def extract_gzip(file_path, extracted_file):
    print(f'Extracting {file_path}...')
    with gzip.open(file_path, 'rb') as f_in:
        with open(extracted_file, 'wb') as f_out:
            f_out.write(f_in.read())
    print('Extraction complete.')

def connect_and_process():
    try:
        # Load database configuration
        params = config()
        print('Connecting to the PostgreSQL database...')
        
        # Use SQLAlchemy instead of psycopg2 for pandas compatibility
        engine = create_engine(f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}")
        
        # Check database version using SQLAlchemy
        with engine.connect() as conn:
            result = conn.execute(text('SELECT version()'))
            db_version = result.fetchone()
            print(f"PostgreSQL version: {db_version}")

        # Download dataset
        url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz' 
        compressed_filename = 'green_tripdata_2019-10.csv.gz'
        extracted_filename = 'green_tripdata.csv'
        download_data(url, compressed_filename)
        extract_gzip(compressed_filename, extracted_filename)

        # Data processing
        df_iter = pd.read_csv(extracted_filename, chunksize=10000)  # Adjust filename and chunksize as needed

        while True:
            t_start = time()
            try:
                df = next(df_iter)  # Get next chunk
            except StopIteration:
                print("All data processed.")
                break

            # Convert datetime columns
            df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
            df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

            # Insert column headers
            df.head(n=0).to_sql(name='green_tripdata', con=engine, if_exists='replace')

            # Insert into database
            df.to_sql(name='green_tripdata', con=engine, if_exists='append')

            t_end = time()
            print(f'Inserted another chunk, took {t_end - t_start:.3f} seconds')

        # Query example: Count rows
        with engine.connect() as conn:
            result = conn.execute(text('SELECT COUNT(*) FROM green_tripdata'))
            row_count = result.scalar()
            print(f'Total rows in green_tripdata: {row_count}')

        # Query example: Count rows
        with engine.connect() as conn:
            result = conn.execute(text('SELECT MIN(lpep_pickup_datetime) FROM green_tripdata'))
            earliest_pickup = result.scalar()
        print(f'Earliest pickup datetime: {earliest_pickup}')

        # Download dataset
        url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv' 
        extract_filename = 'taxi_zone.csv'
        download_data(url, extract_filename)

        # Load Data 
        df1 = pd.read_csv(extract_filename)

        # Insert column headers
        df1.head(n=0).to_sql(name='taxi_zone', con=engine, if_exists='replace')

        # Insert into database
        df1.to_sql(name='taxi_zone', con=engine, if_exists='append')

    except Exception as error:
        print("Error:", error)
  

if __name__ == "__main__":
    connect_and_process()