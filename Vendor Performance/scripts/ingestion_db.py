import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time
logging.basicConfig(
    filename = "logs/ingestion_db.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = 'a'
)

engine = create_engine('sqlite:///inventory.db')
#Creating a database from the given CSV files by using SQLAlchemy and pandas.
#The code reads all CSV files in the current directory, loads them into pandas DataFrames,
#and then writes those DataFrames to a SQLite database named 'inventory.db'.
#Each table in the database is named after the corresponding CSV file (without the '.csv' extension).

def ingest_db(df, table_name, engine):
    df.to_sql(table_name, con =  engine, if_exists = 'replace', index = False) #Ingesting the data into the database.

def load_raw_data():
    start = time.time()
    for file in os.listdir(): #Reading all CSV files in the current directory.
        if '.csv' in file:
            df = pd.read_csv(file)
            logging.info(f'ingesting {file} into the database')
            ingest_db(df, file[:-4], engine)
    end = time.time()
    total_time = (end - start)/60
    logging.info(f'\nTotal Time taken: {total_time} minutes')
    logging.info('ingestion completed successfully')

if __name__ == '__main__':
    load_raw_data()