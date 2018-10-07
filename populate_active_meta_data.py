import pandas as pd
from db_queries import fetch_data, get_trace_and_log, prune_active
from gen_utils import database_connection

def generate_stat_history(setCheck, boolCheck):
    dataArray = []
    if setCheck == 'Alpha' and boolCheck == 'Power':
        query = (
            f"""
                SELECT active_product_nick, avg(active_product_prices), min(active_product_prices), max(active_product_prices), count(active_product_prices), CAST(sum(current_timestamp::date - active_product_start::date) as double precision)/count(active_product_end) as average_length, sum(active_product_prices)
                FROM active_products
                WHERE active_product_nick IN ('{setCheck} Black Lotus', '{setCheck} Mox Sapphire', '{setCheck} Mox Jet', '{setCheck} Mox Pearl', '{setCheck} Mox Ruby', '{setCheck} Mox Emerald', '{setCheck} Timetwister', '{setCheck} Ancestral Recall', '{setCheck} Time Walk')
                GROUP BY active_product_nick;
            """
        )
        data = fetch_data(query)
        dataArray.append(data.values)
        return dataArray
    elif setCheck != 'Alpha' and boolCheck == 'Power':
        query = (
            f"""
                SELECT active_product_nick, avg(active_product_prices), min(active_product_prices), max(active_product_prices), count(active_product_prices), CAST(sum(current_timestamp::date - active_product_start::date) as double precision)/count(active_product_end) as average_length, sum(active_product_prices)
                FROM active_products
                WHERE active_product_nick IN ('{setCheck} Black Lotus MTG', '{setCheck} Mox Sapphire', '{setCheck} Mox Jet', '{setCheck} Mox Pearl', '{setCheck} Mox Ruby', '{setCheck} Mox Emerald', '{setCheck} Timetwister', '{setCheck} Ancestral Recall', '{setCheck} Time Walk')
                GROUP BY active_product_nick;
            """
        )
        data = fetch_data(query)
        dataArray.append(data.values)
        return dataArray
    elif boolCheck == 'Duals':
        query = (
            f"""
                SELECT active_product_nick, avg(active_product_prices), min(active_product_prices), max(active_product_prices), count(active_product_prices), CAST(sum(current_timestamp::date - active_product_start::date) as double precision)/count(active_product_end) as average_length, sum(active_product_prices)
                FROM active_products
                WHERE active_product_nick IN ('{setCheck} Tundra MTG', '{setCheck} Underground Sea MTG', '{setCheck} Badlands MTG', '{setCheck} Taiga MTG', '{setCheck} Savannah MTG', '{setCheck} Scrubland MTG', '{setCheck} Volcanic Island MTG', '{setCheck} Bayou MTG', '{setCheck} Plateau MTG', '{setCheck} Tropical Island MTG')
                GROUP BY active_product_nick;
            """
        )
        data = fetch_data(query)
        dataArray.append(data.values)
        return dataArray

def generate_index_history(setCheck, setId, boolCheck):
    dataArray = []
    if setCheck == 'Alpha' and boolCheck == 'Power':
        query = (
            f"""
                SELECT '{setCheck}', '{setId}', sum(stats.avger), sum(stats.miner), sum(stats.maxer), avg(stats.lengther), sum(stats.counter) ,sum(stats.sumer) 
                FROM (SELECT active_product_nick, avg(active_product_prices) as avger, min(active_product_prices) as miner, max(active_product_prices) as maxer, count(active_product_prices) as counter, CAST(sum(current_timestamp::date - active_product_start::date) as double precision)/count(active_product_end) as lengther, sum(active_product_prices) as sumer
                FROM active_products
                WHERE active_product_nick IN ('{setCheck} Black Lotus', '{setCheck} Mox Sapphire', '{setCheck} Mox Jet', '{setCheck} Mox Pearl', '{setCheck} Mox Ruby', '{setCheck} Mox Emerald', '{setCheck} Timetwister', '{setCheck} Ancestral Recall', '{setCheck} Time Walk')
                GROUP BY active_product_nick) stats;
            """
        )
        data = fetch_data(query)
        dataArray.append(data.values)
        return dataArray
    elif setCheck != 'Alpha' and boolCheck == 'Power':
        query = (
            f"""
                SELECT '{setCheck}', '{setId}', sum(stats.avger), sum(stats.miner), sum(stats.maxer), avg(stats.lengther), sum(stats.counter) ,sum(stats.sumer) 
                FROM (SELECT active_product_nick, avg(active_product_prices) as avger, min(active_product_prices) as miner, max(active_product_prices) as maxer, count(active_product_prices) as counter, CAST(sum(current_timestamp::date - active_product_start::date) as double precision)/count(active_product_end) as lengther, sum(active_product_prices) as sumer
                FROM active_products
                WHERE active_product_nick IN ('{setCheck} Black Lotus MTG', '{setCheck} Mox Sapphire', '{setCheck} Mox Jet', '{setCheck} Mox Pearl', '{setCheck} Mox Ruby', '{setCheck} Mox Emerald', '{setCheck} Timetwister', '{setCheck} Ancestral Recall', '{setCheck} Time Walk')
                GROUP BY active_product_nick) stats;
            """
        )
        data = fetch_data(query)
        dataArray.append(data.values)
        return dataArray
    elif boolCheck == 'Duals':
        query = (
            f"""
                SELECT '{setCheck}', '{setId}', sum(stats.avger), sum(stats.miner), sum(stats.maxer), avg(stats.lengther), sum(stats.counter) ,sum(stats.sumer) 
                FROM (SELECT active_product_nick, avg(active_product_prices) as avger, min(active_product_prices) as miner, max(active_product_prices) as maxer, count(active_product_prices) as counter, CAST(sum(current_timestamp::date - active_product_start::date) as double precision)/count(active_product_end) as lengther, sum(active_product_prices) as sumer
                FROM active_products
                WHERE active_product_nick IN ('{setCheck} Tundra MTG', '{setCheck} Underground Sea MTG', '{setCheck} Badlands MTG', '{setCheck} Taiga MTG', '{setCheck} Savannah MTG', '{setCheck} Scrubland MTG', '{setCheck} Volcanic Island MTG', '{setCheck} Bayou MTG', '{setCheck} Plateau MTG', '{setCheck} Tropical Island MTG')
                GROUP BY active_product_nick) stats;
            """
        )
        data = fetch_data(query)
        dataArray.append(data.values)
        return dataArray

def insert_stats(cursor, mtgArray):
    for neach in mtgArray:
        for each in neach:
            try:
                cursor.execute("""INSERT INTO production_active_products_stats(active_product_nick, active_product_avg, active_product_min, active_product_max, active_product_depth, active_product_avg_length, active_product_sum)
                VALUES (%s, %s, %s, %s, %s, %s, %s)""", (each[0], each[1], each[2], each[3], each[4], each[5], each[6]))
            except Exception as e:
                get_trace_and_log(e)

def insert_index(cursor, mtgArray):
    for neach in mtgArray:
        for each in neach:
            try:
                cursor.execute("""INSERT INTO production_active_products_index(active_product_set_name, active_product_set_id, active_product_index_avg, active_product_index_min, active_product_index_max, active_product_index_length_avg, active_product_index_count_sum, active_product_index_sum)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", (each[0], each[1], each[2], each[3], each[4], each[5], each[6], each[7]))
            except Exception as e:
                get_trace_and_log(e)

powerConfig = {
    'Alpha': 'Power',
    'Beta': 'Power',
    'Unlimited': 'Power',
}
dualsConfig = {
    'Alpha': 'Duals',
    'Beta': 'Duals',
    'Unlimited': 'Duals',
    'Revised': 'Duals'
}

def pipe_duals_stats():
    # generate `cursor` (used to execute db queries)
    cursor = database_connection()
    # iterate over `dualsConfig` and pipe each nested array.
    for each in dualsConfig:
        print(f"Pulling {dualsConfig[each]} from {each}")
        dualsArray = generate_stat_history(setCheck=each, boolCheck=dualsConfig[each])
        if len(dualsArray) > 0:
            print(f"Piping nested arrays")
            insert_stats(cursor=cursor, mtgArray=dualsArray)

def pipe_power_stats():
    cursor = database_connection()
    for each in powerConfig:
        print(f"Pulling {powerConfig[each]} from {each}")
        powerArray = generate_stat_history(setCheck=each, boolCheck=powerConfig[each])
        if len(powerArray) > 0:
            print(f"Piping nested arrays")
            insert_stats(cursor=cursor, mtgArray=powerArray)

def pipe_duals_index():
    cursor = database_connection()
    for each in dualsConfig:
        if each == 'Alpha':
            print(f"Forming {dualsConfig[each]} index from {each} stats")
            dualsArray = generate_index_history(setCheck=each, setId=4, boolCheck=dualsConfig[each])
            if len(dualsArray) > 0:
                print(f"Piping nested arrays")
                insert_index(cursor=cursor, mtgArray=dualsArray)
        elif each == 'Beta':
            print(f"Forming {dualsConfig[each]} index from {each} stats")
            dualsArray = generate_index_history(setCheck=each, setId=5, boolCheck=dualsConfig[each])
            if len(dualsArray) > 0:
                print(f"Piping nested arrays")
                insert_index(cursor=cursor, mtgArray=dualsArray)
        elif each == 'Unlimited':
            print(f"Forming {dualsConfig[each]} index from {each} stats")
            dualsArray = generate_index_history(setCheck=each, setId=6, boolCheck=dualsConfig[each])
            if len(dualsArray) > 0:
                print(f"Piping nested arrays")
                insert_index(cursor=cursor, mtgArray=dualsArray)
        elif each == 'Revised':
            print(f"Forming {dualsConfig[each]} index from {each} stats")
            dualsArray = generate_index_history(setCheck=each, setId=7, boolCheck=dualsConfig[each])
            if len(dualsArray) > 0:
                print(f"Piping nested arrays")
                insert_index(cursor=cursor, mtgArray=dualsArray)

def pipe_power_index():
    cursor = database_connection()
    for each in powerConfig:
        if each == 'Alpha':
            print(f"Pulling {powerConfig[each]} from {each} stats")
            powerArray = generate_index_history(setCheck=each, setId=1, boolCheck=powerConfig[each])
            if len(powerArray) > 0:
                print(f"Piping nested arrays")
                insert_index(cursor=cursor, mtgArray=powerArray)
        elif each == 'Beta':
            print(f"Pulling {powerConfig[each]} from {each} stats")
            powerArray = generate_index_history(setCheck=each, setId=2, boolCheck=powerConfig[each])
            if len(powerArray) > 0:
                print(f"Piping nested arrays")
                insert_index(cursor=cursor, mtgArray=powerArray)
        elif each == 'Unlimited':
            print(f"Pulling {powerConfig[each]} from {each} stats")
            powerArray = generate_index_history(setCheck=each, setId=3, boolCheck=powerConfig[each])
            if len(powerArray) > 0:
                print(f"Piping nested arrays")
                insert_index(cursor=cursor, mtgArray=powerArray)

def prune_db(cursor):
     # prune active_products before making any further calculations (averages, etc.)
    words = ['Alpha Black Lotus', 'Alpha Mox Sapphire', 'Alpha Mox Jet', 'Alpha Mox Pearl', 'Alpha Mox Ruby', 'Alpha Mox Emerald', 'Alpha Timetwister', 'Alpha Ancestral Recall', 'Alpha Time Walk',
                'Beta Black Lotus MTG', 'Beta Mox Sapphire', 'Beta Mox Jet', 'Beta Mox Pearl', 'Beta Mox Ruby', 'Beta Mox Emerald', 'Beta Timetwister', 'Beta Ancestral Recall', 'Beta Time Walk',
                'Unlimited Black Lotus MTG', 'Unlimited Mox Sapphire', 'Unlimited Mox Jet', 'Unlimited Mox Pearl', 'Unlimited Mox Ruby', 'Unlimited Mox Emerald', 'Unlimited Timetwister', 'Unlimited Ancestral Recall', 'Unlimited Time Walk',
                'Alpha Tundra MTG', 'Alpha Underground Sea MTG', 'Alpha Badlands MTG', 'Alpha Taiga MTG', 'Alpha Savannah MTG', 'Alpha Scrubland MTG', 'Alpha Volcanic Island MTG', 'Alpha Bayou MTG', 'Alpha Plateau MTG', 'Alpha Tropical Island MTG',
                'Beta Tundra MTG', 'Beta Underground Sea MTG', 'Beta Badlands MTG', 'Beta Taiga MTG', 'Beta Savannah MTG', 'Beta Scrubland MTG', 'Beta Volcanic Island MTG', 'Beta Bayou MTG', 'Beta Plateau MTG', 'Beta Tropical Island MTG',
                'Unlimited Tundra MTG', 'Unlimited Underground Sea MTG', 'Unlimited Badlands MTG', 'Unlimited Taiga MTG', 'Unlimited Savannah MTG', 'Unlimited Scrubland MTG', 'Unlimited Volcanic Island MTG', 'Unlimited Bayou MTG', 'Unlimited Plateau MTG', 'Unlimited Tropical Island MTG',
                'Revised Tundra MTG', 'Revised Underground Sea MTG', 'Revised Badlands MTG', 'Revised Taiga MTG', 'Revised Savannah MTG', 'Revised Scrubland MTG', 'Revised Volcanic Island MTG', 'Revised Bayou MTG', 'Revised Plateau MTG', 'Revised Tropical Island MTG',
                'Alpha Time Vault MTG', 'Beta Time Vault MTG', 'Unlimited Time Vault MTG']
    # words = ['Revised Tundra MTG']
    for value in words:
        print(f'Pruning {value}....')
        prune_active(value, cursor)
    print('-------------------------------------')
    print('Succesfully pruned active_products')
    print('-------------------------------------')

if __name__ == '__main__':
    inputCheck = input('Beginning once-a-day batch calc script -- are you sure you want to proceed?: ')
    if inputCheck in ('Y', 'y'):
        print('I understand. Beggining once-a-day batch script.')
        prune_db(cursor=database_connection())
        print()
        # begin piping stats
        pipe_power_stats()
        print()
        pipe_duals_stats()
        print()
        # begin piping index
        pipe_power_index()
        print()
        pipe_duals_index()
        print()
        print('Batch process active. Data has been successfully inserted.')
    elif inputCheck in ('N', 'n'):
        print('Exiting batch process.')