import traceback, sys
import time
import logging
import requests
import psycopg2
import pandas as pd
import configparser

#TODO: assert checks if need be
#TODO: memory profiling if need be

CONFIG = configparser.ConfigParser()
CONFIG.read('CONFIG.ini')

def database_connection():
    try:
        conn = psycopg2.connect(f"""dbname={CONFIG['myPostgresDB']['dbname']} user={CONFIG['myPostgresDB']['user']} host={CONFIG['myPostgresDB']['host']} password={CONFIG['myPostgresDB']['password']} port={CONFIG['myPostgresDB']['port']}""")
        conn.autocommit = True
        cursor = conn.cursor()
        print('-----------------------------------------------')
        print("Connected to database...")
        print()
        return cursor
    except Exception as e:
        get_trace_and_log(e)
        print("Cannot connect to database...")

def get_trace_and_log(error):
    """(str: error) -> str

    Returns a distinct traceback with the offending line and logs the error with the time and date."""
    logf = open("errors.log", "a")
    trace = traceback.extract_tb(sys.exc_info()[-1], limit=1)[-1][1]
    logging.error(error)
    print(f'Offending line: {trace}')
    logf.write(f'Error: {str(error)}\n'
               f'Asc time: {time.asctime()}\n')

def fetch_data(query):
    """(query: str) -> str

    Takes in a query, connects to the database and returns the result."""
    result = pd.read_sql(
        sql=query,
        con=psycopg2.connect(
            f"""dbname={CONFIG['myPostgresDB']['dbname']} user={CONFIG['myPostgresDB']['user']} host={CONFIG['myPostgresDB']['host']} password={CONFIG['myPostgresDB']['password']} port={CONFIG['myPostgresDB']['port']}""")
    )
    return result

def get_data(value):
    """() -> list

    Returns a list of every card in the requested table. Used to quickly check if contents populated."""
    query = (
        f"""
        SELECT * FROM {value}
        """)
    data = fetch_data(query)
    return data
