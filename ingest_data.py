#!/usr/bin/env python
# coding: utf-8

# In[1]:

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine

# In[2]:



#df=pd.read_csv('yellow_tripdata_2021-01.csv',nrows=100)
def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    
    df_iter = pd.read_csv(f'{csv_name}', iterator=True, chunksize=100000)
    
    
    # In[15]:
    
    
    df = next(df_iter)
    

    


    if "green" in url:
        # Convert datetime columns for green taxi data
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        print("Processed green taxi data datetime columns.")
    elif "yellow" in url:
        # Convert datetime columns for yellow taxi data
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        print("Processed yellow taxi data datetime columns.")
    else:
        # Do nothing if URL doesn't match green or yellow
        print("No processing applied as URL doesn't contain 'green' or 'yellow'.")


    
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')
    
    
    
    
    # In[29]:
    
    
    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)

            # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            if "green" in url:
                # Convert datetime columns for green taxi data
                df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
                df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
                print("Processed green taxi data datetime columns.")
            elif "yellow" in url:
                # Convert datetime columns for yellow taxi data
                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
                print("Processed yellow taxi data datetime columns.")
            else:
                # Do nothing if URL doesn't match green or yellow
                print("No processing applied as URL doesn't contain 'green' or 'yellow'.")

            
            
            
            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)    
    
   

# In[ ]:




