import pandas as pd
from db_queries import fetch_data, get_trace_and_log, prune_completed
from gen_utils import database_connection, get_search_words, POWER_CONFIG, DUALS_CONFIG

def generate_stat_history(setCheck, boolCheck):
    dataArray = []
    if setCheck == 'Alpha' and boolCheck == 'Power':
        query = (
            f"""
                SELECT completed_product_nick, avg(completed_product_prices), min(completed_product_prices), max(completed_product_prices), count(completed_product_prices), CAST(sum(completed_product_end::date - completed_product_start::date) as double precision)/count(completed_product_end) as average_length, sum(completed_product_prices)
                FROM completed_products
                WHERE completed_product_nick IN ('{setCheck} Black Lotus', '{setCheck} Mox Sapphire', '{setCheck} Mox Jet', '{setCheck} Mox Pearl', '{setCheck} Mox Ruby', '{setCheck} Mox Emerald', '{setCheck} Timetwister', '{setCheck} Ancestral Recall', '{setCheck} Time Walk')
                AND completed_product_end::date > current_timestamp - interval '90' day
                GROUP BY completed_product_nick;
            """
        )
        data = fetch_data(query)
        dataArray.append(data.values)
        return dataArray
    elif setCheck != 'Alpha' and boolCheck == 'Power':
        query = (
            f"""
                SELECT completed_product_nick, avg(completed_product_prices), min(completed_product_prices), max(completed_product_prices), count(completed_product_prices), CAST(sum(completed_product_end::date - completed_product_start::date) as double precision)/count(completed_product_end) as average_length, sum(completed_product_prices)
                FROM completed_products
                WHERE completed_product_nick IN ('{setCheck} Black Lotus MTG', '{setCheck} Mox Sapphire', '{setCheck} Mox Jet', '{setCheck} Mox Pearl', '{setCheck} Mox Ruby', '{setCheck} Mox Emerald', '{setCheck} Timetwister', '{setCheck} Ancestral Recall', '{setCheck} Time Walk')
                AND completed_product_end::date > current_timestamp - interval '90' day
                GROUP BY completed_product_nick;
            """
        )
        data = fetch_data(query)
        dataArray.append(data.values)
        return dataArray
    elif boolCheck == 'Duals':
        query = (
            f"""
                SELECT completed_product_nick, avg(completed_product_prices), min(completed_product_prices), max(completed_product_prices), count(completed_product_prices), CAST(sum(completed_product_end::date - completed_product_start::date) as double precision)/count(completed_product_end) as average_length, sum(completed_product_prices)
                FROM completed_products
                WHERE completed_product_nick IN ('{setCheck} Tundra MTG', '{setCheck} Underground Sea MTG', '{setCheck} Badlands MTG', '{setCheck} Taiga MTG', '{setCheck} Savannah MTG', '{setCheck} Scrubland MTG', '{setCheck} Volcanic Island MTG', '{setCheck} Bayou MTG', '{setCheck} Plateau MTG', '{setCheck} Tropical Island MTG')
                AND completed_product_end::date > current_timestamp - interval '90' day
                GROUP BY completed_product_nick;
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
                FROM (SELECT completed_product_nick, avg(completed_product_prices) as avger, min(completed_product_prices) as miner, max(completed_product_prices) as maxer, count(completed_product_prices) as counter, CAST(sum(completed_product_end::date - completed_product_start::date) as double precision)/count(completed_product_end) as lengther, sum(completed_product_prices) as sumer
                FROM completed_products
                WHERE completed_product_nick IN ('{setCheck} Black Lotus', '{setCheck} Mox Sapphire', '{setCheck} Mox Jet', '{setCheck} Mox Pearl', '{setCheck} Mox Ruby', '{setCheck} Mox Emerald', '{setCheck} Timetwister', '{setCheck} Ancestral Recall', '{setCheck} Time Walk')
                AND completed_product_end::date > current_timestamp - interval '90' day
                GROUP BY completed_product_nick) stats;
            """
        )
        data = fetch_data(query)
        dataArray.append(data.values)
        return dataArray
    elif setCheck != 'Alpha' and boolCheck == 'Power':
        query = (
            f"""
                SELECT '{setCheck}', '{setId}', sum(stats.avger), sum(stats.miner), sum(stats.maxer), avg(stats.lengther), sum(stats.counter) ,sum(stats.sumer) 
                FROM (SELECT completed_product_nick, avg(completed_product_prices) as avger, min(completed_product_prices) as miner, max(completed_product_prices) as maxer, count(completed_product_prices) as counter, CAST(sum(completed_product_end::date - completed_product_start::date) as double precision)/count(completed_product_end) as lengther, sum(completed_product_prices) as sumer
                FROM completed_products
                WHERE completed_product_nick IN ('{setCheck} Black Lotus MTG', '{setCheck} Mox Sapphire', '{setCheck} Mox Jet', '{setCheck} Mox Pearl', '{setCheck} Mox Ruby', '{setCheck} Mox Emerald', '{setCheck} Timetwister', '{setCheck} Ancestral Recall', '{setCheck} Time Walk')
                AND completed_product_end::date > current_timestamp - interval '90' day
                GROUP BY completed_product_nick) stats;
            """
        )
        data = fetch_data(query)
        dataArray.append(data.values)
        return dataArray
    elif boolCheck == 'Duals':
        query = (
            f"""
                SELECT '{setCheck}', '{setId}', sum(stats.avger), sum(stats.miner), sum(stats.maxer), avg(stats.lengther), sum(stats.counter) ,sum(stats.sumer) 
                FROM (SELECT completed_product_nick, avg(completed_product_prices) as avger, min(completed_product_prices) as miner, max(completed_product_prices) as maxer, count(completed_product_prices) as counter, CAST(sum(completed_product_end::date - completed_product_start::date) as double precision)/count(completed_product_end) as lengther, sum(completed_product_prices) as sumer
                FROM completed_products
                WHERE completed_product_nick IN ('{setCheck} Tundra MTG', '{setCheck} Underground Sea MTG', '{setCheck} Badlands MTG', '{setCheck} Taiga MTG', '{setCheck} Savannah MTG', '{setCheck} Scrubland MTG', '{setCheck} Volcanic Island MTG', '{setCheck} Bayou MTG', '{setCheck} Plateau MTG', '{setCheck} Tropical Island MTG')
                AND completed_product_end::date > current_timestamp - interval '90' day
                GROUP BY completed_product_nick) stats;
            """
        )
        data = fetch_data(query)
        dataArray.append(data.values)
        return dataArray

def insert_stats(cursor, mtgArray):
    for neach in mtgArray:
        for each in neach:
            try:
                cursor.execute("""INSERT INTO production_completed_products_stats(completed_product_nick, completed_product_avg, completed_product_min, completed_product_max, completed_product_depth, completed_product_avg_length, completed_product_sum)
                VALUES (%s, %s, %s, %s, %s, %s, %s)""", (each[0], each[1], each[2], each[3], each[4], each[5], each[6]))
            except Exception as e:
                get_trace_and_log(e)

def insert_index(cursor, mtgArray):
    for neach in mtgArray:
        for each in neach:
            try:
                cursor.execute("""INSERT INTO production_completed_products_index(completed_product_set_name, completed_product_set_id, completed_product_index_avg, completed_product_index_min, completed_product_index_max, completed_product_index_length_avg, completed_product_index_count_sum, completed_product_index_sum)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", (each[0], each[1], each[2], each[3], each[4], each[5], each[6], each[7]))
            except Exception as e:
                get_trace_and_log(e)

def pipe_duals_stats():
    # generate `cursor` (used to execute db queries)
    cursor = database_connection()
    # iterate over `DUALS_CONFIG` and pipe each nested array.
    for each in DUALS_CONFIG:
        print(f"Pulling {DUALS_CONFIG[each]} from {each}")
        dualsArray = generate_stat_history(setCheck=each, boolCheck=DUALS_CONFIG[each])
        if len(dualsArray) > 0:
            print(f"Piping nested arrays")
            insert_stats(cursor=cursor, mtgArray=dualsArray)

def pipe_power_stats():
    cursor = database_connection()
    for each in POWER_CONFIG:
        print(f"Pulling {POWER_CONFIG[each]} from {each}")
        powerArray = generate_stat_history(setCheck=each, boolCheck=POWER_CONFIG[each])
        if len(powerArray) > 0:
            print(f"Piping nested arrays")
            insert_stats(cursor=cursor, mtgArray=powerArray)

def pipe_duals_index():
    cursor = database_connection()
    for each in DUALS_CONFIG:
        if each == 'Alpha':
            print(f"Forming {DUALS_CONFIG[each]} index from {each} stats")
            dualsArray = generate_index_history(setCheck=each, setId=4, boolCheck=DUALS_CONFIG[each])
            if len(dualsArray) > 0:
                print(f"Piping nested arrays")
                insert_index(cursor=cursor, mtgArray=dualsArray)
        elif each == 'Beta':
            print(f"Forming {DUALS_CONFIG[each]} index from {each} stats")
            dualsArray = generate_index_history(setCheck=each, setId=5, boolCheck=DUALS_CONFIG[each])
            if len(dualsArray) > 0:
                print(f"Piping nested arrays")
                insert_index(cursor=cursor, mtgArray=dualsArray)
        elif each == 'Unlimited':
            print(f"Forming {DUALS_CONFIG[each]} index from {each} stats")
            dualsArray = generate_index_history(setCheck=each, setId=6, boolCheck=DUALS_CONFIG[each])
            if len(dualsArray) > 0:
                print(f"Piping nested arrays")
                insert_index(cursor=cursor, mtgArray=dualsArray)
        elif each == 'Revised':
            print(f"Forming {DUALS_CONFIG[each]} index from {each} stats")
            dualsArray = generate_index_history(setCheck=each, setId=7, boolCheck=DUALS_CONFIG[each])
            if len(dualsArray) > 0:
                print(f"Piping nested arrays")
                insert_index(cursor=cursor, mtgArray=dualsArray)

def pipe_power_index():
    cursor = database_connection()
    for each in POWER_CONFIG:
        if each == 'Alpha':
            print(f"Pulling {POWER_CONFIG[each]} from {each} stats")
            powerArray = generate_index_history(setCheck=each, setId=1, boolCheck=POWER_CONFIG[each])
            if len(powerArray) > 0:
                print(f"Piping nested arrays")
                insert_index(cursor=cursor, mtgArray=powerArray)
        elif each == 'Beta':
            print(f"Pulling {POWER_CONFIG[each]} from {each} stats")
            powerArray = generate_index_history(setCheck=each, setId=2, boolCheck=POWER_CONFIG[each])
            if len(powerArray) > 0:
                print(f"Piping nested arrays")
                insert_index(cursor=cursor, mtgArray=powerArray)
        elif each == 'Unlimited':
            print(f"Pulling {POWER_CONFIG[each]} from {each} stats")
            powerArray = generate_index_history(setCheck=each, setId=3, boolCheck=POWER_CONFIG[each])
            if len(powerArray) > 0:
                print(f"Piping nested arrays")
                insert_index(cursor=cursor, mtgArray=powerArray)

def prune_db(cursor):
    """(cursor) -> ()

    Prunes active_products before making any further calculations (averages, etc.)"""
    words = get_search_words()
    # words = ['Revised Tundra MTG']
    for value in words:
        print(f'Pruning {value}....')
        prune_completed(value, cursor)
    print('-------------------------------------')
    print('Succesfully pruned completed_products')
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
        print('Batch process completed. Data has been successfully inserted.')
    elif inputCheck in ('N', 'n'):
        print('Exiting batch process.')
