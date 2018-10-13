import traceback, sys
import time
import logging
import requests
import psycopg2
import pandas as pd
import configparser
import csv
from datetime import datetime, timedelta
import pytz

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

def get_api_key():
    """() -> api_key

    Reads the `CONFIG.ini` file for the [eBayAPI] indicator, reads in the respective api_key, and returns the value."""
    api_key = CONFIG['eBayAPI']['api_key']
    return api_key

def get_search_words():
    """() -> list

    Reads a .txt file for search words and returns the corresponding values in list form."""
    # The txt file format DOES NOT WORK, as it is inserting some sort of characters into the DB that we CANNOT remove. 
    # The hard-coded array method is all that works so far.
    # with open("search_words.txt") as txtFile:
    #     words = []
    #     for line in txtFile:
    #         words.append(line)
    #     return words
    words = ['Alpha Black Lotus', 'Alpha Mox Sapphire', 'Alpha Mox Jet', 'Alpha Mox Pearl', 'Alpha Mox Ruby', 'Alpha Mox Emerald', 'Alpha Timetwister', 'Alpha Ancestral Recall', 'Alpha Time Walk',
                'Beta Black Lotus MTG', 'Beta Mox Sapphire', 'Beta Mox Jet', 'Beta Mox Pearl', 'Beta Mox Ruby', 'Beta Mox Emerald', 'Beta Timetwister', 'Beta Ancestral Recall', 'Beta Time Walk',
                'Unlimited Black Lotus MTG', 'Unlimited Mox Sapphire', 'Unlimited Mox Jet', 'Unlimited Mox Pearl', 'Unlimited Mox Ruby', 'Unlimited Mox Emerald', 'Unlimited Timetwister', 'Unlimited Ancestral Recall', 'Unlimited Time Walk',
                'Alpha Tundra MTG', 'Alpha Underground Sea MTG', 'Alpha Badlands MTG', 'Alpha Taiga MTG', 'Alpha Savannah MTG', 'Alpha Scrubland MTG', 'Alpha Volcanic Island MTG', 'Alpha Bayou MTG', 'Alpha Plateau MTG', 'Alpha Tropical Island MTG',
                'Beta Tundra MTG', 'Beta Underground Sea MTG', 'Beta Badlands MTG', 'Beta Taiga MTG', 'Beta Savannah MTG', 'Beta Scrubland MTG', 'Beta Volcanic Island MTG', 'Beta Bayou MTG', 'Beta Plateau MTG', 'Beta Tropical Island MTG',
                'Unlimited Tundra MTG', 'Unlimited Underground Sea MTG', 'Unlimited Badlands MTG', 'Unlimited Taiga MTG', 'Unlimited Savannah MTG', 'Unlimited Scrubland MTG', 'Unlimited Volcanic Island MTG', 'Unlimited Bayou MTG', 'Unlimited Plateau MTG', 'Unlimited Tropical Island MTG',
                'Revised Tundra MTG', 'Revised Underground Sea MTG', 'Revised Badlands MTG', 'Revised Taiga MTG', 'Revised Savannah MTG', 'Revised Scrubland MTG', 'Revised Volcanic Island MTG', 'Revised Bayou MTG', 'Revised Plateau MTG', 'Revised Tropical Island MTG',
                'Alpha Time Vault MTG', 'Beta Time Vault MTG', 'Unlimited Time Vault MTG']
    return words

def get_test_search_words():
    """() -> list

    Returns a list of selected search words we can test against"""
    words = ['Unlimited Black Lotus MTG', 'Revised Tropical Island MTG', 'Beta Badlands MTG']
    return words

def csv_reader(csv_path):
    """(path: str, list: list) -> list

    Takes in a designated path to the desired .csv and appends the rows to a global list."""
    setList = []
    with open(csv_path, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in reader:
            setList.append(row[0])
        return setList

def get_timezones(period_length):
    CST = pytz.timezone('US/Central')
    OLDEST_DATE = (datetime.now(CST) + timedelta(days=1)) - timedelta(days=period_length)
    return OLDEST_DATE
