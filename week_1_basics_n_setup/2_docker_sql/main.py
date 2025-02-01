#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import psycopg2
from psycopg2 import sql
from config import config  # type: ignore
from time import time
from sqlalchemy import create_engine


def connect():
    """Connect to the PostgreSQL database and return the connection and cursor."""
    try:
        # Load database parameters
        params = config()
        print("Connecting to the PostgreSQL database...")
        connection = psycopg2.connect(**params)
        print("Connection successful.")
        return connection
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        return None

def process_and_insert_data(df_iter, connection):
    """Process data in chunks and insert into the PostgreSQL table."""
    try:
        cursor = connection.cursor()
        while True:
            t_start = time()
            # Fetch the next chunk
            df = next(df_iter)
            
            # Convert datetime columns
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

            # Insert chunk into the database
            df.to_sql(name='yellow_taxi_data', con=connection, if_exists='append', index=False, method='multi')

            t_end = time()
            print(f"Inserted another chunk, took {t_end - t_start:.3f} seconds")
    except StopIteration:
        print("All chunks processed.")
    except Exception as error:
        print(f"Error during data insertion: {error}")
    finally:
        cursor.close()

def run_queries(connection):
    """Run additional queries on the database."""
    try:
        cursor = connection.cursor()
        
        # Query 1: Count the number of rows in the table
        cursor.execute("SELECT COUNT(*) FROM yellow_taxi_data;")
        count = cursor.fetchone()[0]
        print(f"Total rows in yellow_taxi_data: {count}")
        
        # Query 2: Retrieve the earliest pickup date
        cursor.execute("SELECT MIN(tpep_pickup_datetime) FROM yellow_taxi_data;")
        earliest_pickup = cursor.fetchone()[0]
        print(f"Earliest pickup datetime: {earliest_pickup}")

    except Exception as error:
        print(f"Error during query execution: {error}")
    finally:
        cursor.close()

if __name__ == "__main__":
    # Connect to the database
    connection = connect()
    if connection is not None:
        try:
            # Create a DataFrame iterator for demonstration (replace with your actual source)
            chunk_size = 1000
            df_iter = pd.read_csv('yellow_tripdata.csv', chunksize=chunk_size)

            # Process and insert data
            process_and_insert_data(df_iter, connection)

            # Run additional queries
            run_queries(connection)
        except Exception as error:
            print(f"Error: {error}")
        finally:
            connection.close()
            print("Database connection terminated.")
