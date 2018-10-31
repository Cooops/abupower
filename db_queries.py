import pandas as pd
import psycopg2
import json
import datetime
import configparser
from gen_utils import get_trace_and_log, fetch_data

###################
# general queries #
###################
# begin general queries
def prune_completed(value, cursor):
    query = (
        f'''
        DELETE
        FROM completed_products
        WHERE primary_ids IN
            (SELECT primary_ids
            FROM completed_products
            WHERE completed_product_nick = '{value}'
            GROUP BY primary_ids
            HAVING completed_product_prices < (SELECT avg(completed_product_prices) FROM completed_products WHERE completed_product_nick = '{value}')*.20
            AND (SELECT count(*) FROM completed_products WHERE completed_product_nick = '{value}') >= 5
            OR completed_product_end::date - completed_product_start::date > 500
            );
        '''
    )
    cursor.execute(query)
    # cursor.close()

def prune_active(value, cursor):
    #TODO: add logger here so we can see how many items are getting pruned (and which ones) on average @ 9/22/2018
    query = (
        f'''
        DELETE
        FROM active_products
        WHERE primary_ids IN
            (SELECT primary_ids
            FROM active_products
            WHERE active_product_nick = '{value}'
            GROUP BY primary_ids
            HAVING active_product_prices < (SELECT avg(active_product_prices) FROM active_products WHERE active_product_nick = '{value}')*.20
            AND (SELECT count(*) FROM active_products WHERE active_product_nick = '{value}') >= 5
            );
        '''
    )
    cursor.execute(query)
    # cursor.close()

def get_data_single_product_avg(value):
    """() -> variable

    Returns a computed variable of the avg value for _one_ unique value in the database, based on the past 90 days worth of data points."""
    query = (
        f'''
        SELECT completed_product_avg, timestamp
        FROM production_completed_products_stats
        WHERE completed_product_nick = '{value}'
        ORDER BY primary_ids DESC
        LIMIT 2;
        '''
    )
    data = fetch_data(query)
    data = list(data['completed_product_avg'])
    return data

def get_data_single_product_count_90(value):
    """() -> variable

    Returns a computed variable of the count value for _one_ unique value in the database, based on the past 90 days worth of data points."""
    query = (
        f'''
        SELECT completed_product_depth 
        FROM production_completed_products_stats 
        WHERE completed_product_nick = '{value}' 
        ORDER BY primary_ids DESC 
        LIMIT 2;
        '''
    )
    data = fetch_data(query)
    return data.values

def get_data_single_product_avg_length_90(value):
    """() -> variable

    Returns a computer variable of the average listing length for _one_ unique value in the database, based on the past 90 days."""
    query = (
        f'''
        SELECT completed_product_avg_length
        FROM production_completed_products_stats
        WHERE completed_product_nick = '{value}' 
        ORDER BY primary_ids DESC 
        LIMIT 2;
        '''
    )
    data = fetch_data(query)
    return data.values
    
def get_data_single_product_sum_90(value):
    """() -> variable

    Returns a computer variable of the average listing length for _one_ unique value in the database, based on the past 90 days."""
    query = (
        f'''
        SELECT completed_product_sum
        FROM production_completed_products_stats
        WHERE completed_product_nick = '{value}' 
        ORDER BY primary_ids DESC 
        LIMIT 2;
        '''
    )
    data = fetch_data(query)
    return data.values

def get_data_single_product_depth(value):
    """() -> list

    Returns the sum of the cumulative values for the last 6 products in the power indexes in the database (ABU)."""
    query = (
        f'''
        SELECT COUNT(active_product_prices)
        FROM active_products
        WHERE active_product_nick = '{value}'
        '''
    )
    data = fetch_data(query)
    return data.values[0][0]

def get_cumulative_power():
    """() -> list

    Returns the sum of the cumulative values for the last 6 products in the power indexes in the database (ABUC)."""
    query = (
        f'''
        SELECT completed_product_index_sum FROM production_completed_products_index WHERE primary_ids IN (SELECT primary_ids FROM production_completed_products_index WHERE completed_product_set_id IN ('1', '2', '3', '8') ORDER BY primary_ids DESC LIMIT 6);
        '''
    )
    data = fetch_data(query)
    cumSumToday = sum(data.values[0:3])
    cumSumYesterday = sum(data.values[3:6])
    return float(cumSumToday), float(cumSumYesterday)

def get_cumulative_count_power():
    """() -> list

    Returns the sum of the cumulative count for the last 6 products in the power indexes in the database (ABUC)."""
    # SELECT CAST (CAST (sum(completed_product_index_sum) AS text) AS money)::numeric FROM completed_products_index WHERE primary_ids IN (SELECT primary_ids FROM completed_products_index WHERE completed_product_set_id IN ('4', '5', '6', '7') ORDER BY timestamp DESC LIMIT 4);
    query = (
        f'''
        SELECT completed_product_index_count_sum FROM production_completed_products_index WHERE primary_ids IN (SELECT primary_ids FROM production_completed_products_index WHERE completed_product_set_id IN ('1', '2', '3', '8') ORDER BY timestamp DESC LIMIT 6);
        '''
    )
    data = fetch_data(query)
    cumCountToday = sum(data.values[0:3])
    cumCountYesterday = sum(data.values[3:6])
    return float(cumCountToday), float(cumCountYesterday)

def get_cumulative_duals():
    """() -> list

    Returns the sum of the cumulative values for the last 6 values in the duals indexes in the database (ABURC)."""
    # SELECT CAST (CAST (sum(completed_product_index_sum) AS text) AS money)::numeric FROM completed_products_index WHERE primary_ids IN (SELECT primary_ids FROM completed_products_index WHERE completed_product_set_id IN ('4', '5', '6', '7') ORDER BY timestamp DESC LIMIT 4);
    query = (
        f'''
        SELECT completed_product_index_sum FROM production_completed_products_index WHERE primary_ids IN (SELECT primary_ids FROM production_completed_products_index WHERE completed_product_set_id IN ('4', '5', '6', '7', '10') ORDER BY timestamp DESC LIMIT 8);
        '''
    )
    data = fetch_data(query)
    cumSumToday = sum(data.values[0:4])
    cumSumYesterday = sum(data.values[4:8])
    return float(cumSumToday), float(cumSumYesterday)

def get_cumulative_count_duals():
    """() -> list

    Returns the sum of the cumulative count for the last 6 products in the duals indexes in the database (ABURC)."""
    # SELECT CAST (CAST (sum(completed_product_index_sum) AS text) AS money)::numeric FROM completed_products_index WHERE primary_ids IN (SELECT primary_ids FROM completed_products_index WHERE completed_product_set_id IN ('4', '5', '6', '7') ORDER BY timestamp DESC LIMIT 4);
    query = (
        f'''
        SELECT completed_product_index_count_sum FROM production_completed_products_index WHERE primary_ids IN (SELECT primary_ids FROM production_completed_products_index WHERE completed_product_set_id IN ('4', '5', '6', '7', '10') ORDER BY timestamp DESC LIMIT 8);
        '''
    )
    data = fetch_data(query)
    cumCountToday = sum(data.values[0:4])
    cumCountYesterday = sum(data.values[4:8])
    return float(cumCountToday), float(cumCountYesterday)


########################
# set specific queries #
########################
# begin alpha queries
def get_data_alpha_avg_length():
    """() -> list

    Returns a list of all of the alpha values' avg length inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_length_avg, timestamp
        FROM production_completed_products_index
        WHERE completed_product_set_id = '1'
        AND completed_product_index_length_avg != 0
        ORDER BY primary_ids ASC;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_alpha_total_sold():
    """() -> list

    Returns a list of the total number of alpha sold copies over the past 90 days, in ascending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_count_sum, timestamp::date
        FROM production_completed_products_index
        WHERE completed_product_set_id = '1'
        AND completed_product_index_count_sum != 0
        ORDER BY primary_ids ASC;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_alpha_avg_all():
    """() -> list

    Returns a list of all of the alpha values' avg price inserted into the database, in ascending order by the timestamp."""
    query = (
        f'''
        SELECT completed_product_index_avg, timestamp::date
        FROM production_completed_products_index
        WHERE completed_product_set_id = '1'
		AND completed_product_index_avg IS NOT NULL
        ORDER BY timestamp ASC;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_alpha_breakdown():
    """() -> list

    Returns a list of all of the values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT DISTINCT * 
        FROM production_completed_products_stats
        WHERE completed_product_nick IN ('Alpha Black Lotus', 'Alpha Mox Sapphire', 'Alpha Mox Jet', 'Alpha Mox Pearl', 'Alpha Mox Ruby', 'Alpha Mox Emerald', 'Alpha Timetwister', 'Alpha Ancestral Recall', 'Alpha Time Walk')
        ORDER BY timestamp DESC
        LIMIT 9;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_alpha_all():
    """() -> list

    Returns a list of the unlimited data in the database, in descending order by end date."""
    query = (
        f'''
        SELECT completed_product_nick, completed_product_titles, CAST (CAST (completed_product_prices AS text) AS money), completed_product_end::timestamp::date, completed_product_lst_type, completed_product_img_url, completed_product_loc, completed_product_img_thumb, completed_product_depth
        FROM completed_products
        WHERE completed_product_nick IN ('Alpha Black Lotus', 'Alpha Mox Sapphire', 'Alpha Mox Jet', 'Alpha Mox Pearl', 'Alpha Mox Ruby', 'Alpha Mox Emerald', 'Alpha Timetwister', 'Alpha Ancestral Recall', 'Alpha Time Walk')
        ORDER BY completed_product_end DESC;
        ''')
    data = fetch_data(query)
    return data

def get_data_alpha_avg():
    """() -> variable

    Returns a list of last 2 values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_avg, timestamp
        FROM production_completed_products_index
        WHERE completed_product_set_id = '1'
        ORDER BY primary_ids DESC
        LIMIT 2
        '''
    )
    data = fetch_data(query)
    data = list(data['completed_product_index_avg'])
    calc = ((data[0]-data[1])/data[1])*100
    if calc >= 0:
        return f'${data[0]:,.0f} (±{calc:,.2f}%)'
    else:
        return f'${data[0]:,.0f} (±{calc:,.2f}%)'

def get_data_alpha_active_all():
    """() -> list

    Returns a list of all of the active alpha cards in the database, sorted in ascending order by start date."""
    query = (
        f'''
        SELECT active_product_nick, active_product_titles, CAST (CAST (active_product_prices AS text) AS money), active_product_start::timestamp::date, active_product_lst_type, active_product_img_url, active_product_loc, active_product_img_thumb, active_product_depth, active_product_watch_count
        FROM active_products
        WHERE active_product_nick IN ('Alpha Black Lotus', 'Alpha Mox Sapphire', 'Alpha Mox Jet', 'Alpha Mox Pearl', 'Alpha Mox Ruby', 'Alpha Mox Emerald', 'Alpha Timetwister', 'Alpha Ancestral Recall', 'Alpha Time Walk')
        ORDER BY active_product_start DESC;
        ''')
    data = fetch_data(query)
    return data

def get_data_alpha_cumulative_totals():
    """() -> list

    Returns a list of the cumulative sum of alpha cards in the database, sorted in ascending order by timestamp."""
    query = (
        f'''
        SELECT completed_product_index_sum, timestamp::date FROM production_completed_products_index WHERE completed_product_set_id = '1' AND completed_product_index_sum IS NOT NULL ORDER BY timestamp ASC; 
        ''')
    data = fetch_data(query)
    return data

def get_data_alpha_power_cumulative_totals():
    """() -> list

    Returns a list of all of the data points for alpha power in the database, sorted in ascending order by start date."""
    query = (
        f'''
        SELECT completed_product_index_sum, timestamp::date FROM production_completed_products_index WHERE completed_product_set_id = '1' AND completed_product_index_sum IS NOT NULL ORDER BY timestamp ASC; 
        ''')
    data = fetch_data(query)
    return data

def get_data_alpha_power_stats():
    """() -> list

    Returns the last 10 data points for each distinct (unique) power related value from the alpha set, in descending order by timestamp."""
    query = (
        f'''
        SELECT DISTINCT * 
        FROM production_completed_products_stats
        WHERE completed_product_nick IN ('Alpha Black Lotus', 'Alpha Mox Sapphire', 'Alpha Mox Jet', 'Alpha Mox Pearl', 'Alpha Mox Ruby', 'Alpha Mox Emerald', 'Alpha Timetwister', 'Alpha Ancestral Recall', 'Alpha Time Walk')
        ORDER BY timestamp DESC
        LIMIT 10;
        '''
    )
    data = fetch_data(query)
    return data

# begin beta queries    
def get_data_beta_avg_all():
    """() -> list

    Returns a list of all of the beta index avg values inserted into the database, in ascending order by the timestamp."""
    query = (
        f'''
        SELECT completed_product_index_avg, timestamp::date
        FROM production_completed_products_index
        WHERE completed_product_set_id = '2'
		AND completed_product_index_avg IS NOT NULL
        ORDER BY timestamp ASC
        '''
    )
    data = fetch_data(query)
    return data

def get_data_beta_avg_length():
    """() -> list

    Returns a list of all of the beta index avg length values inserted into the database, in ascending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_length_avg, timestamp
        FROM production_completed_products_index
        WHERE completed_product_set_id = '2'
        AND completed_product_index_length_avg != 0
        ORDER BY primary_ids ASC;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_beta_total_sold():
    """() -> list

    Returns a list of all of the beta index total (sum) count values inserted into the database, in ascending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_count_sum, timestamp::date
        FROM production_completed_products_index
        WHERE completed_product_set_id = '2'
        AND completed_product_index_count_sum != 0
        ORDER BY primary_ids ASC;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_beta_breakdown():
    """() -> list

    Returns a list of all of the distinct beta stat values inserted into the database, in descending order by the timestamp."""
    query = (
        f'''
        SELECT DISTINCT *
        FROM production_completed_products_stats
        WHERE completed_product_nick IN ('Beta Black Lotus MTG', 'Beta Mox Sapphire', 'Beta Mox Jet', 'Beta Mox Pearl', 'Beta Mox Ruby', 'Beta Mox Emerald', 'Beta Timetwister', 'Beta Ancestral Recall', 'Beta Time Walk')
        ORDER BY timestamp DESC
        LIMIT 9;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_beta_all():
    """() -> list

    Returns a list of the unlimited data in the database, in descending order by end date."""
    query = (
        f'''
        SELECT completed_product_nick, completed_product_titles, CAST (CAST (completed_product_prices AS text) AS money), completed_product_end::timestamp::date, completed_product_lst_type, completed_product_img_url, completed_product_loc, completed_product_img_thumb, completed_product_depth
        FROM completed_products
        WHERE completed_product_nick IN ('Beta Black Lotus MTG', 'Beta Mox Sapphire', 'Beta Mox Jet', 'Beta Mox Pearl', 'Beta Mox Ruby', 'Beta Mox Emerald', 'Beta Timetwister', 'Beta Ancestral Recall', 'Beta Time Walk')
        ORDER BY completed_product_end DESC;
        ''')
    data = fetch_data(query)
    return data

def get_data_beta_avg():
    """() -> variable

    Returns a list of last 2 values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_avg
        FROM production_completed_products_index
        WHERE completed_product_set_id = '2'
        ORDER BY primary_ids DESC
        LIMIT 2
        '''
    )
    data = fetch_data(query)
    data = list(data['completed_product_index_avg'])
    calc = ((data[0]-data[1])/data[1])*100
    if calc >= 0:
        return f'${data[0]:,.0f} (±{calc:,.2f}%)'
    else:
        return f'${data[0]:,.0f} (±{calc:,.2f}%)'

def get_data_beta_active_all():
    """() -> list

    Returns a list of all of the active beta cards in the database, sorted in descending order by start date."""
    query = (
        f'''
        SELECT active_product_nick, active_product_titles, CAST (CAST (active_product_prices AS text) AS money), active_product_start::timestamp::date, active_product_lst_type, active_product_img_url, active_product_loc, active_product_img_thumb, active_product_depth, active_product_watch_count
        FROM active_products
        WHERE active_product_nick IN ('Beta Black Lotus MTG', 'Beta Mox Sapphire', 'Beta Mox Jet', 'Beta Mox Pearl', 'Beta Mox Ruby', 'Beta Mox Emerald', 'Beta Timetwister', 'Beta Ancestral Recall', 'Beta Time Walk')
        ORDER BY active_product_start DESC;
        ''')
    data = fetch_data(query)
    return data

def get_data_beta_cumulative_totals():
    """() -> list

    Returns a list of the cumulative sum of beta cards in the database, sorted in ascending order by timestamp."""
    query = (
        f'''
        SELECT completed_product_index_sum, timestamp::date FROM production_completed_products_index WHERE completed_product_set_id = '2' AND completed_product_index_sum IS NOT NULL ORDER BY timestamp ASC; 
        ''')
    data = fetch_data(query)
    return data

def get_data_beta_power_cumulative_totals():
    """() -> list

    Returns a list of all of the data points for beta power in the database, sorted in ascending order by start date."""
    query = (
        f'''
        SELECT completed_product_index_sum, timestamp::date FROM production_completed_products_index WHERE completed_product_set_id = '2' AND completed_product_index_sum IS NOT NULL ORDER BY timestamp ASC; 
        ''')
    data = fetch_data(query)
    return data

def get_data_beta_power_stats():
    """() -> list

    Returns the last 10 data points for each distinct (unique) power related value from the beta set, in descending order by timestamp."""
    query = (
        f'''
        SELECT DISTINCT * 
        FROM production_completed_products_stats
        WHERE completed_product_nick IN ('Beta Black Lotus MTG', 'Beta Mox Sapphire', 'Beta Mox Jet', 'Beta Mox Pearl', 'Beta Mox Ruby', 'Beta Mox Emerald', 'Beta Timetwister', 'Beta Ancestral Recall', 'Beta Time Walk')
        ORDER BY timestamp DESC
        LIMIT 10;
        '''
    )
    data = fetch_data(query)
    return data

# begin unlimited queries
def get_data_unlimited_avg_all():
    """() -> list

    Returns a list of all of the unlimited avg values inserted into the database, in ascending order by timestamp."""
    query = (
        f'''
        SELECT completed_product_index_avg, timestamp
        FROM production_completed_products_index
        WHERE completed_product_set_id = '3'
		AND completed_product_index_avg IS NOT NULL
        ORDER BY timestamp ASC;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_unlimited_avg_length():
    """() -> list

       Returns a list of the averages of each respective product in the database, in ascending order by primary id."""
    query = (
        f'''
        SELECT completed_product_index_length_avg, timestamp
        FROM production_completed_products_index
        WHERE completed_product_set_id = '3'
        AND completed_product_index_length_avg != 0
        ORDER BY primary_ids ASC;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_unlimited_total_sold():
    """() -> list

    Returns a list of all of the values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_count_sum, timestamp
        FROM production_completed_products_index
        WHERE completed_product_set_id = '3'
        AND completed_product_index_count_sum != 0
        ORDER BY primary_ids ASC;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_unlimited_breakdown():
    """() -> list

        Returns a list of all of the values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT DISTINCT * 
        FROM production_completed_products_stats
        WHERE completed_product_nick IN ('Unlimited Black Lotus MTG', 'Unlimited Mox Sapphire', 'Unlimited Mox Jet', 'Unlimited Mox Pearl', 'Unlimited Mox Ruby', 'Unlimited Mox Emerald', 'Unlimited Timetwister', 'Unlimited Ancestral Recall', 'Unlimited Time Walk')
        ORDER BY timestamp DESC
        LIMIT 9;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_unlimited_all():
    """() -> list

    Returns a list of the unlimited data in the database, in descending order by end date."""
    query = (
        f'''
        SELECT completed_product_nick, completed_product_titles, CAST (CAST (completed_product_prices AS text) AS money), completed_product_end::timestamp::date, completed_product_lst_type, completed_product_img_url, completed_product_loc, completed_product_img_thumb, completed_product_depth
        FROM completed_products
        WHERE completed_product_nick IN ('Unlimited Black Lotus MTG', 'Unlimited Mox Sapphire', 'Unlimited Mox Jet', 'Unlimited Mox Pearl', 'Unlimited Mox Ruby', 'Unlimited Mox Emerald', 'Unlimited Timetwister', 'Unlimited Ancestral Recall', 'Unlimited Time Walk')
        ORDER BY completed_product_end DESC;
        ''')
    data = fetch_data(query)
    return data

def get_data_unlimited_avg():
    """() -> list

    Returns a list of last 2 values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_avg
        FROM production_completed_products_index
        WHERE completed_product_set_id = '3'
        ORDER BY primary_ids DESC
        LIMIT 2
        '''
    )
    data = fetch_data(query)
    data = list(data['completed_product_index_avg'])
    calc = ((data[0]-data[1])/data[1])*100
    if calc >= 0:
        return f'${data[0]:,.0f} (±{calc:,.2f}%)'
    else:
        return f'${data[0]:,.0f} (±{calc:,.2f}%)'

def get_data_unlimited_active_all():
    """() -> list

    Returns a list of all of the active unlimited cards in the database, sorted in ascending order by start date."""
    query = (
        f'''
        SELECT active_product_nick, active_product_titles, CAST (CAST (active_product_prices AS text) AS money), active_product_start::timestamp::date, active_product_lst_type, active_product_img_url, active_product_loc, active_product_img_thumb, active_product_depth, active_product_watch_count
        FROM active_products
        WHERE active_product_nick IN ('Unlimited Black Lotus MTG', 'Unlimited Mox Sapphire', 'Unlimited Mox Jet', 'Unlimited Mox Pearl', 'Unlimited Mox Ruby', 'Unlimited Mox Emerald', 'Unlimited Timetwister', 'Unlimited Ancestral Recall', 'Unlimited Time Walk')
        ORDER BY active_product_start DESC;
        ''')
    data = fetch_data(query)
    return data

def get_data_unlimited_cumulative_totals():
    """() -> list

    Returns a list of the cumulative sum of unlimited cards in the database, sorted in ascending order by timestmap."""
    query = (
        f'''
        SELECT completed_product_index_sum, timestamp FROM production_completed_products_index WHERE completed_product_set_id = '3' AND completed_product_index_sum IS NOT NULL ORDER BY timestamp ASC; 
        ''')
    data = fetch_data(query)
    return data

def get_data_unlimited_power_cumulative_totals():
    """() -> list

    Returns a list of all of the data points for unlimited power in the database, sorted in ascending order by start date."""
    query = (
        f'''
        SELECT completed_product_index_sum, timestamp FROM production_completed_products_index WHERE completed_product_set_id = '3' AND completed_product_index_sum IS NOT NULL ORDER BY timestamp ASC; 
        ''')
    data = fetch_data(query)
    return data

def get_data_unlimited_power_stats():
    """() -> list

    Returns the last 10 data points for each distinct (unique) power related value from the unlimited set, in descending order by timestamp."""
    query = (
        f'''
        SELECT DISTINCT * 
        FROM production_completed_products_stats
        WHERE completed_product_nick IN ('Unlimited Black Lotus MTG', 'Unlimited Mox Sapphire', 'Unlimited Mox Jet', 'Unlimited Mox Pearl', 'Unlimited Mox Ruby', 'Unlimited Mox Emerald', 'Unlimited Timetwister', 'Unlimited Ancestral Recall', 'Unlimited Time Walk')
        ORDER BY timestamp DESC
        LIMIT 10;
        '''
    )
    data = fetch_data(query)
    return data

# begin ce & ice queries
def get_data_ce_avg_all():
    """() -> list

    Returns a list of all of the ce avg values inserted into the database, in ascending order by timestamp."""
    query = (
        f'''
        SELECT completed_product_index_avg, timestamp
        FROM production_completed_products_index
        WHERE completed_product_set_id = '8'
		AND completed_product_index_avg IS NOT NULL
        ORDER BY timestamp ASC;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_ce_avg_length():
    """() -> list

       Returns a list of the averages of each respective product in the database, in ascending order by primary id."""
    query = (
        f'''
        SELECT completed_product_index_length_avg, timestamp
        FROM production_completed_products_index
        WHERE completed_product_set_id = '8'
        AND completed_product_index_length_avg != 0
        ORDER BY primary_ids ASC;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_ce_total_sold():
    """() -> list

    Returns a list of all of the values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_count_sum, timestamp
        FROM production_completed_products_index
        WHERE completed_product_set_id = '8'
        AND completed_product_index_count_sum != 0
        ORDER BY primary_ids ASC;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_ce_breakdown():
    """() -> list

        Returns a list of all of the values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT DISTINCT * 
        FROM production_completed_products_stats
        WHERE completed_product_nick IN ('Collectors Black Lotus MTG', 'Collectors Mox Sapphire', 'Collectors Mox Jet', 'Collectors Mox Pearl', 'Collectors Mox Ruby', 'Collectors Mox Emerald', 'Collectors Timetwister', 'Collectors Ancestral Recall', 'Collectors Time Walk')
        ORDER BY timestamp DESC
        LIMIT 9;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_ce_all():
    """() -> list

    Returns a list of the ce data in the database, in descending order by end date."""
    query = (
        f'''
        SELECT completed_product_nick, completed_product_titles, CAST (CAST (completed_product_prices AS text) AS money), completed_product_end::timestamp::date, completed_product_lst_type, completed_product_img_url, completed_product_loc, completed_product_img_thumb, completed_product_depth
        FROM completed_products
        WHERE completed_product_nick IN ('Collectors Black Lotus MTG', 'Collectors Mox Sapphire', 'Collectors Mox Jet', 'Collectors Mox Pearl', 'Collectors Mox Ruby', 'Collectors Mox Emerald', 'Collectors Timetwister', 'Collectors Ancestral Recall', 'Collectors Time Walk')
        ORDER BY completed_product_end DESC;
        ''')
    data = fetch_data(query)
    return data

def get_data_ce_avg():
    """() -> list

    Returns a list of last 2 values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_avg
        FROM production_completed_products_index
        WHERE completed_product_set_id = '8'
        ORDER BY primary_ids DESC
        LIMIT 2
        '''
    )
    data = fetch_data(query)
    data = list(data['completed_product_index_avg'])
    calc = ((data[0]-data[1])/data[1])*100
    if calc >= 0:
        return f'${data[0]:,.0f} (±{calc:,.2f}%)'
    else:
        return f'${data[0]:,.0f} (±{calc:,.2f}%)'

def get_data_ce_active_all():
    """() -> list

    Returns a list of all of the active ce cards in the database, sorted in ascending order by start date."""
    query = (
        f'''
        SELECT active_product_nick, active_product_titles, CAST (CAST (active_product_prices AS text) AS money), active_product_start::timestamp::date, active_product_lst_type, active_product_img_url, active_product_loc, active_product_img_thumb, active_product_depth, active_product_watch_count
        FROM active_products
        WHERE active_product_nick IN ('Collectors Black Lotus MTG', 'Collectors Mox Sapphire', 'Collectors Mox Jet', 'Collectors Mox Pearl', 'Collectors Mox Ruby', 'Collectors Mox Emerald', 'Collectors Timetwister', 'Collectors Ancestral Recall', 'Collectors Time Walk')
        ORDER BY active_product_start DESC;
        ''')
    data = fetch_data(query)
    return data

def get_data_ce_cumulative_totals():
    """() -> list

    Returns a list of the cumulative sum of ce cards in the database, sorted in ascending order by timestmap."""
    query = (
        f'''
        SELECT completed_product_index_sum, timestamp FROM production_completed_products_index WHERE completed_product_set_id = '8' AND completed_product_index_sum IS NOT NULL ORDER BY timestamp ASC; 
        ''')
    data = fetch_data(query)
    return data

def get_data_ce_power_cumulative_totals():
    """() -> list

    Returns a list of all of the data points for ce power in the database, sorted in ascending order by start date."""
    query = (
        f'''
        SELECT completed_product_index_sum, timestamp FROM production_completed_products_index WHERE completed_product_set_id = '8' AND completed_product_index_sum IS NOT NULL ORDER BY timestamp ASC; 
        ''')
    data = fetch_data(query)
    return data

def get_data_ce_power_stats():
    """() -> list

    Returns the last 10 data points for each distinct (unique) power related value from the ce set, in descending order by timestamp."""
    query = (
        f'''
        SELECT DISTINCT * 
        FROM production_completed_products_stats
        WHERE completed_product_nick IN ('Collectors Black Lotus MTG', 'Collectors Mox Sapphire', 'Collectors Mox Jet', 'Collectors Mox Pearl', 'Collectors Mox Ruby', 'Collectors Mox Emerald', 'Collectors Timetwister', 'Collectors Ancestral Recall', 'Collectors Time Walk')
        ORDER BY timestamp DESC
        LIMIT 10;
        '''
    )
    data = fetch_data(query)
    return data

#######################
# begin duals queries #
#######################
# begin alpha queries
def get_data_alpha_duals_avg_all():
    """() -> list

    Returns a list of all of the values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_avg, timestamp::date
        FROM production_completed_products_index
        WHERE completed_product_set_id = '4'
		AND completed_product_index_avg IS NOT NULL
        ORDER BY timestamp ASC;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_alpha_duals_avg_length():
    """() -> list

       Returns a list of the averages of each respective product in the database, in ascending order by primary id."""
    query = (
        f'''
        SELECT completed_product_index_length_avg, timestamp 
        FROM (SELECT completed_product_index_length_avg, timestamp
            FROM production_completed_products_index
            WHERE completed_product_set_id = '4'
            AND completed_product_index_count_sum != 0
            ORDER BY timestamp ASC) sub;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_alpha_duals_total_sold():
    """() -> list

    Returns a list of all of the values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_count_sum, timestamp::date
        FROM production_completed_products_index
        WHERE completed_product_set_id = '4'
        AND completed_product_index_count_sum != 0
        ORDER BY primary_ids ASC;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_alpha_duals_breakdown():
    """() -> list

    Returns a list of all of the values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT DISTINCT *
        FROM production_completed_products_stats
        WHERE completed_product_nick IN ('Alpha Tundra MTG', 'Alpha Underground Sea MTG', 'Alpha Badlands MTG', 'Alpha Taiga MTG', 'Alpha Savannah MTG', 'Alpha Scrubland MTG', 'Alpha Volcanic Island MTG', 'Alpha Bayou MTG', 'Alpha Plateau MTG', 'Alpha Tropical Island MTG')
        ORDER BY timestamp DESC
        LIMIT 10;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_alpha_duals_all():
    """() -> list

    Returns a list of the alpha data in the database, in descending order by end date."""
    query = (
        f'''
        SELECT completed_product_nick, completed_product_titles, CAST (CAST (completed_product_prices AS text) AS money), completed_product_end::timestamp::date, completed_product_lst_type, completed_product_img_url, completed_product_loc, completed_product_img_thumb, completed_product_depth
        FROM completed_products
        WHERE completed_product_nick IN ('Alpha Tundra MTG', 'Alpha Underground Sea MTG', 'Alpha Badlands MTG', 'Alpha Taiga MTG', 'Alpha Savannah MTG', 'Alpha Scrubland MTG', 'Alpha Volcanic Island MTG', 'Alpha Bayou MTG', 'Alpha Plateau MTG', 'Alpha Tropical Island MTG')
        ORDER BY completed_product_end DESC;
        ''')
    data = fetch_data(query)
    return data

def get_data_alpha_duals_avg():
    """() -> list

    Returns a list of last 2 alpha index average values inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_avg
        FROM production_completed_products_index
        WHERE completed_product_set_id = '4'
        ORDER BY primary_ids DESC
        LIMIT 2
        '''
    )
    data = fetch_data(query)
    data = list(data['completed_product_index_avg'])
    calc = ((data[0]-data[1])/data[1])*100
    if calc >= 0:
        return f'${data[0]:,.0f} (±{calc:,.2f}%)'
    else:
        return f'${data[0]:,.0f} (±{calc:,.2f}%)'

def get_data_alpha_duals_active_all():
    """() -> list

    Returns a list of all of the active alpha cards in the database, sorted in ascending order by start date."""
    query = (
        f'''
        SELECT active_product_nick, active_product_titles, CAST (CAST (active_product_prices AS text) AS money), active_product_start::timestamp::date, active_product_lst_type, active_product_img_url, active_product_loc, active_product_img_thumb, active_product_depth, active_product_watch_count
        FROM active_products
        WHERE active_product_nick IN ('Alpha Tundra MTG', 'Alpha Underground Sea MTG', 'Alpha Badlands MTG', 'Alpha Taiga MTG', 'Alpha Savannah MTG', 'Alpha Scrubland MTG', 'Alpha Volcanic Island MTG', 'Alpha Bayou MTG', 'Alpha Plateau MTG', 'Alpha Tropical Island MTG')
        ORDER BY active_product_start DESC;
        ''')
    data = fetch_data(query)
    return data

def get_data_alpha_duals_cumulative_totals():
    """() -> list

    Returns a list of all of the active alpha cards in the database, sorted in ascending order by start date."""
    query = (
        f'''
        SELECT completed_product_index_sum, timestamp::date FROM production_completed_products_index WHERE completed_product_set_id = '4' AND completed_product_index_sum IS NOT NULL ORDER BY timestamp ASC; 
        ''')
    data = fetch_data(query)
    return data

def get_data_alpha_duals_stats():
    """() -> list

    Returns the last 10 data points for each distinct (unique) value from the alpha set, in descending order by timestamp."""
    query = (
        f'''
        SELECT DISTINCT * 
        FROM production_completed_products_stats
        WHERE completed_product_nick IN ('Alpha Tundra MTG', 'Alpha Underground Sea MTG', 'Alpha Badlands MTG', 'Alpha Taiga MTG', 'Alpha Savannah MTG', 'Alpha Scrubland MTG', 'Alpha Volcanic Island MTG', 'Alpha Bayou MTG', 'Alpha Plateau MTG', 'Alpha Tropical Island MTG')
        ORDER BY timestamp DESC
        LIMIT 10;
        '''
    )
    data = fetch_data(query)
    return data
    
# begin beta queries
def get_data_beta_duals_avg_all():
    """() -> list

    Returns a list of all of the values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_avg, timestamp::date
        FROM production_completed_products_index
        WHERE completed_product_set_id = '5'
		AND completed_product_index_avg IS NOT NULL
        ORDER BY timestamp ASC;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_beta_duals_avg_length():
    """() -> list

       Returns a list of the averages of each respective product in the database, in ascending order by primary id."""
    query = (
        f'''
        SELECT completed_product_index_length_avg, timestamp
        FROM (SELECT completed_product_index_length_avg, timestamp
            FROM production_completed_products_index
            WHERE completed_product_set_id = '5'
            AND completed_product_index_count_sum != 0
            ORDER BY timestamp ASC) sub;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_beta_duals_total_sold():
    """() -> list

    Returns a list of all of the values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_count_sum, timestamp::date
        FROM production_completed_products_index
        WHERE completed_product_set_id = '5'
        AND completed_product_index_count_sum != 0
        ORDER BY primary_ids ASC;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_beta_duals_breakdown():
    """() -> list

    Returns a list of all of the values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT DISTINCT *
        FROM production_completed_products_stats
        WHERE completed_product_nick IN ('Beta Tundra MTG', 'Beta Underground Sea MTG', 'Beta Badlands MTG', 'Beta Taiga MTG', 'Beta Savannah MTG', 'Beta Scrubland MTG', 'Beta Volcanic Island MTG', 'Beta Bayou MTG', 'Beta Plateau MTG', 'Beta Tropical Island MTG')
        ORDER BY timestamp DESC
        LIMIT 10;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_beta_duals_all():
    """() -> list

    Returns a list of the beta data in the database, in descending order by end date."""
    query = (
        f'''
        SELECT completed_product_nick, completed_product_titles, CAST (CAST (completed_product_prices AS text) AS money), completed_product_end::timestamp::date, completed_product_lst_type, completed_product_img_url, completed_product_loc, completed_product_img_thumb, completed_product_depth
        FROM completed_products
        WHERE completed_product_nick IN ('Beta Tundra MTG', 'Beta Underground Sea MTG', 'Beta Badlands MTG', 'Beta Taiga MTG', 'Beta Savannah MTG', 'Beta Scrubland MTG', 'Beta Volcanic Island MTG', 'Beta Bayou MTG', 'Beta Plateau MTG', 'Beta Tropical Island MTG')
        ORDER BY completed_product_end DESC;
        ''')
    data = fetch_data(query)
    return data

def get_data_beta_duals_avg():
    """() -> list

    Returns a list of last 2 beta index average values inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_avg
        FROM production_completed_products_index
        WHERE completed_product_set_id = '5'
        ORDER BY primary_ids DESC
        LIMIT 2
        '''
    )
    data = fetch_data(query)
    data = list(data['completed_product_index_avg'])
    calc = ((data[0]-data[1])/data[1])*100
    if calc >= 0:
        return f'${data[0]:,.0f} (±{calc:,.2f}%)'
    else:
        return f'${data[0]:,.0f} (±{calc:,.2f}%)'

def get_data_beta_duals_active_all():
    """() -> list

    Returns a list of all of the active beta cards in the database, sorted in ascending order by start date."""
    query = (
        f'''
        SELECT active_product_nick, active_product_titles, CAST (CAST (active_product_prices AS text) AS money), active_product_start::timestamp::date, active_product_lst_type, active_product_img_url, active_product_loc, active_product_img_thumb, active_product_depth, active_product_watch_count
        FROM active_products
        WHERE active_product_nick IN ('Beta Tundra MTG', 'Beta Underground Sea MTG', 'Beta Badlands MTG', 'Beta Taiga MTG', 'Beta Savannah MTG', 'Beta Scrubland MTG', 'Beta Volcanic Island MTG', 'Beta Bayou MTG', 'Beta Plateau MTG', 'Beta Tropical Island MTG')
        ORDER BY active_product_start DESC;
        ''')
    data = fetch_data(query)
    return data

def get_data_beta_duals_cumulative_totals():
    """() -> list

    Returns a list of all of the active beta cards in the database, sorted in ascending order by start date."""
    query = (
        f'''
        SELECT completed_product_index_sum, timestamp FROM production_completed_products_index WHERE completed_product_set_id = '5' AND completed_product_index_sum IS NOT NULL ORDER BY timestamp ASC; 
        ''')
    data = fetch_data(query)
    return data

def get_data_beta_duals_stats():
    """() -> list

    Returns the last 10 data points for each distinct (unique) value from the beta set, in descending order by timestamp."""
    query = (
        f'''
        SELECT DISTINCT * 
        FROM production_completed_products_stats
        WHERE completed_product_nick IN ('Beta Tundra MTG', 'Beta Underground Sea MTG', 'Beta Badlands MTG', 'Beta Taiga MTG', 'Beta Savannah MTG', 'Beta Scrubland MTG', 'Beta Volcanic Island MTG', 'Beta Bayou MTG', 'Beta Plateau MTG', 'Beta Tropical Island MTG')
        ORDER BY timestamp DESC
        LIMIT 10;
        '''
    )
    data = fetch_data(query)
    return data

# begin unlimited queries
def get_data_unlimited_duals_avg_all():
    """() -> list

    Returns a list of all of the values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_avg, timestamp
        FROM production_completed_products_index
        WHERE completed_product_set_id = '6'
		AND completed_product_index_avg IS NOT NULL
        ORDER BY timestamp ASC;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_unlimited_duals_avg_length():
    """() -> list

       Returns a list of the averages of each respective product in the database, in ascending order by primary id."""
    query = (
        f'''
        SELECT completed_product_index_length_avg, timestamp
        FROM (SELECT completed_product_index_length_avg, timestamp
            FROM production_completed_products_index
            WHERE completed_product_set_id = '6'
            AND completed_product_index_count_sum != 0
            ORDER BY timestamp ASC) sub;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_unlimited_duals_total_sold():
    """() -> list

    Returns a list of all of the values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_count_sum, timestamp::date
        FROM production_completed_products_index
        WHERE completed_product_set_id = '6'
        AND completed_product_index_count_sum != 0
        ORDER BY primary_ids ASC;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_unlimited_duals_breakdown():
    """() -> list

    Returns a list of all of the values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT DISTINCT *
        FROM production_completed_products_stats
        WHERE completed_product_nick IN ('Unlimited Tundra MTG', 'Unlimited Underground Sea MTG', 'Unlimited Badlands MTG', 'Unlimited Taiga MTG', 'Unlimited Savannah MTG', 'Unlimited Scrubland MTG', 'Unlimited Volcanic Island MTG', 'Unlimited Bayou MTG', 'Unlimited Plateau MTG', 'Unlimited Tropical Island MTG')
        ORDER BY timestamp DESC
        LIMIT 10;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_unlimited_duals_all():
    """() -> list

    Returns a list of the unlimited data in the database, in descending order by end date."""
    query = (
        f'''
        SELECT completed_product_nick, completed_product_titles, CAST (CAST (completed_product_prices AS text) AS money), completed_product_end::timestamp::date, completed_product_lst_type, completed_product_img_url, completed_product_loc, completed_product_img_thumb, completed_product_depth
        FROM completed_products
        WHERE completed_product_nick IN ('Unlimited Tundra MTG', 'Unlimited Underground Sea MTG', 'Unlimited Badlands MTG', 'Unlimited Taiga MTG', 'Unlimited Savannah MTG', 'Unlimited Scrubland MTG', 'Unlimited Volcanic Island MTG', 'Unlimited Bayou MTG', 'Unlimited Plateau MTG', 'Unlimited Tropical Island MTG')
        ORDER BY completed_product_end DESC;
        ''')
    data = fetch_data(query)
    return data

def get_data_unlimited_duals_avg():
    """() -> list

    Returns a list of last 2 unlimited index average values inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_avg
        FROM production_completed_products_index
        WHERE completed_product_set_id = '6'
        ORDER BY primary_ids DESC
        LIMIT 2
        '''
    )
    data = fetch_data(query)
    data = list(data['completed_product_index_avg'])
    calc = ((data[0]-data[1])/data[1])*100
    if calc >= 0:
        return f'${data[0]:,.0f} (±{calc:,.2f}%)'
    else:
        return f'${data[0]:,.0f} (±{calc:,.2f}%)'

def get_data_unlimited_duals_active_all():
    """() -> list

    Returns a list of all of the active unlimited cards in the database, sorted in ascending order by start date."""
    query = (
        f'''
        SELECT active_product_nick, active_product_titles, CAST (CAST (active_product_prices AS text) AS money), active_product_start::timestamp::date, active_product_lst_type, active_product_img_url, active_product_loc, active_product_img_thumb, active_product_depth, active_product_watch_count
        FROM active_products
        WHERE active_product_nick IN ('Unlimited Tundra MTG', 'Unlimited Underground Sea MTG', 'Unlimited Badlands MTG', 'Unlimited Taiga MTG', 'Unlimited Savannah MTG', 'Unlimited Scrubland MTG', 'Unlimited Volcanic Island MTG', 'Unlimited Bayou MTG', 'Unlimited Plateau MTG', 'Unlimited Tropical Island MTG')
        ORDER BY active_product_start DESC;
        ''')
    data = fetch_data(query)
    return data

def get_data_unlimited_duals_cumulative_totals():
    """() -> list

    Returns a list of all of the active unlimited cards in the database, sorted in ascending order by start date."""
    query = (
        f'''
        SELECT completed_product_index_sum, timestamp FROM production_completed_products_index WHERE completed_product_set_id = '6' AND completed_product_index_sum IS NOT NULL ORDER BY timestamp ASC; 
        ''')
    data = fetch_data(query)
    return data

def get_data_unlimited_duals_stats():
    """() -> list

    Returns the last 10 data points for each distinct (unique) value from the unlimited set, in descending order by timestamp."""
    query = (
        f'''
        SELECT DISTINCT * 
        FROM production_completed_products_stats
        WHERE completed_product_nick IN ('Unlimited Tundra MTG', 'Unlimited Underground Sea MTG', 'Unlimited Badlands MTG', 'Unlimited Taiga MTG', 'Unlimited Savannah MTG', 'Unlimited Scrubland MTG', 'Unlimited Volcanic Island MTG', 'Unlimited Bayou MTG', 'Unlimited Plateau MTG', 'Unlimited Tropical Island MTG')
        ORDER BY timestamp DESC
        LIMIT 10;
        '''
    )
    data = fetch_data(query)
    return data

# begin ce queries
def get_data_ce_duals_avg_all():
    """() -> list

    Returns a list of all of the values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_avg, timestamp
        FROM production_completed_products_index
        WHERE completed_product_set_id = '6'
		AND completed_product_index_avg IS NOT NULL
        ORDER BY timestamp ASC;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_ce_duals_avg_length():
    """() -> list

       Returns a list of the averages of each respective product in the database, in ascending order by primary id."""
    query = (
        f'''
        SELECT completed_product_index_length_avg, timestamp
        FROM (SELECT completed_product_index_length_avg, timestamp
            FROM production_completed_products_index
            WHERE completed_product_set_id = '6'
            AND completed_product_index_count_sum != 0
            ORDER BY timestamp ASC) sub;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_ce_duals_total_sold():
    """() -> list

    Returns a list of all of the values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_count_sum, timestamp::date
        FROM production_completed_products_index
        WHERE completed_product_set_id = '6'
        AND completed_product_index_count_sum != 0
        ORDER BY primary_ids ASC;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_ce_duals_breakdown():
    """() -> list

    Returns a list of all of the values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT DISTINCT *
        FROM production_completed_products_stats
        WHERE completed_product_nick IN ('Collectors Tundra MTG', 'Collectors Underground Sea MTG', 'Collectors Badlands MTG', 'Collectors Taiga MTG', 'Collectors Savannah MTG', 'Collectors Scrubland MTG', 'Collectors Volcanic Island MTG', 'Collectors Bayou MTG', 'Collectors Plateau MTG', 'Collectors Tropical Island MTG')
        ORDER BY timestamp DESC
        LIMIT 10;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_ce_duals_all():
    """() -> list

    Returns a list of the ce data in the database, in descending order by end date."""
    query = (
        f'''
        SELECT completed_product_nick, completed_product_titles, CAST (CAST (completed_product_prices AS text) AS money), completed_product_end::timestamp::date, completed_product_lst_type, completed_product_img_url, completed_product_loc, completed_product_img_thumb, completed_product_depth
        FROM completed_products
        WHERE completed_product_nick IN ('Collectors Tundra MTG', 'Collectors Underground Sea MTG', 'Collectors Badlands MTG', 'Collectors Taiga MTG', 'Collectors Savannah MTG', 'Collectors Scrubland MTG', 'Collectors Volcanic Island MTG', 'Collectors Bayou MTG', 'Collectors Plateau MTG', 'Collectors Tropical Island MTG')
        ORDER BY completed_product_end DESC;
        ''')
    data = fetch_data(query)
    return data

def get_data_ce_duals_avg():
    """() -> list

    Returns a list of last 2 ce index average values inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_avg
        FROM production_completed_products_index
        WHERE completed_product_set_id = '6'
        ORDER BY primary_ids DESC
        LIMIT 2
        '''
    )
    data = fetch_data(query)
    data = list(data['completed_product_index_avg'])
    calc = ((data[0]-data[1])/data[1])*100
    if calc >= 0:
        return f'${data[0]:,.0f} (±{calc:,.2f}%)'
    else:
        return f'${data[0]:,.0f} (±{calc:,.2f}%)'

def get_data_ce_duals_active_all():
    """() -> list

    Returns a list of all of the active ce cards in the database, sorted in ascending order by start date."""
    query = (
        f'''
        SELECT active_product_nick, active_product_titles, CAST (CAST (active_product_prices AS text) AS money), active_product_start::timestamp::date, active_product_lst_type, active_product_img_url, active_product_loc, active_product_img_thumb, active_product_depth, active_product_watch_count
        FROM active_products
        WHERE active_product_nick IN ('Collectors Tundra MTG', 'Collectors Underground Sea MTG', 'Collectors Badlands MTG', 'Collectors Taiga MTG', 'Collectors Savannah MTG', 'Collectors Scrubland MTG', 'Collectors Volcanic Island MTG', 'Collectors Bayou MTG', 'Collectors Plateau MTG', 'Collectors Tropical Island MTG')
        ORDER BY active_product_start DESC;
        ''')
    data = fetch_data(query)
    return data

def get_data_ce_duals_cumulative_totals():
    """() -> list

    Returns a list of all of the active ce cards in the database, sorted in ascending order by start date."""
    query = (
        f'''
        SELECT completed_product_index_sum, timestamp FROM production_completed_products_index WHERE completed_product_set_id = '6' AND completed_product_index_sum IS NOT NULL ORDER BY timestamp ASC; 
        ''')
    data = fetch_data(query)
    return data

def get_data_ce_duals_stats():
    """() -> list

    Returns the last 10 data points for each distinct (unique) value from the ce set, in descending order by timestamp."""
    query = (
        f'''
        SELECT DISTINCT * 
        FROM production_completed_products_stats
        WHERE completed_product_nick IN ('Collectors Tundra MTG', 'Collectors Underground Sea MTG', 'Collectors Badlands MTG', 'Collectors Taiga MTG', 'Collectors Savannah MTG', 'Collectors Scrubland MTG', 'Collectors Volcanic Island MTG', 'Collectors Bayou MTG', 'Collectors Plateau MTG', 'Collectors Tropical Island MTG')
        ORDER BY timestamp DESC
        LIMIT 10;
        '''
    )
    data = fetch_data(query)
    return data

#######################
# begin revised queries
def get_data_revised_avg_all():
    """() -> list

    Returns a list of all of the values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_avg, timestamp
        FROM production_completed_products_index
        WHERE completed_product_set_id = '7'
		AND completed_product_index_avg IS NOT NULL
        ORDER BY timestamp ASC;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_revised_avg_length():
    """() -> list

       Returns a list of the averages of each respective product in the database, in ascending order by primary id."""
    query = (
        f'''
        SELECT completed_product_index_length_avg, timestamp::date 
        FROM (SELECT completed_product_index_length_avg, timestamp
            FROM production_completed_products_index
            WHERE completed_product_set_id = '7'
            AND completed_product_index_count_sum != 0
            ORDER BY timestamp ASC) sub;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_revised_total_sold():
    """() -> list

    Returns a list of all of the values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_count_sum, timestamp::date
        FROM production_completed_products_index
        WHERE completed_product_set_id = '7'
        AND completed_product_index_count_sum != 0
        ORDER BY primary_ids ASC;;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_revised_breakdown():
    """() -> list

    Returns a list of all of the values avg inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT DISTINCT *
        FROM production_completed_products_stats
        WHERE completed_product_nick IN ('Revised Tundra MTG', 'Revised Underground Sea MTG', 'Revised Badlands MTG', 'Revised Taiga MTG', 'Revised Savannah MTG', 'Revised Scrubland MTG', 'Revised Volcanic Island MTG', 'Revised Bayou MTG', 'Revised Plateau MTG', 'Revised Tropical Island MTG')
        ORDER BY timestamp DESC
        LIMIT 10;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_revised_all():
    """() -> list
    
    Returns a list of the revised data in the database, in descending order by end date."""
    query = (
        f'''
        SELECT completed_product_nick, completed_product_titles, CAST (CAST (completed_product_prices AS text) AS money), completed_product_end::timestamp::date, completed_product_lst_type, completed_product_img_url, completed_product_loc, completed_product_img_thumb, completed_product_depth
        FROM completed_products
        WHERE completed_product_nick IN ('Revised Tundra MTG', 'Revised Underground Sea MTG', 'Revised Badlands MTG', 'Revised Taiga MTG', 'Revised Savannah MTG', 'Revised Scrubland MTG', 'Revised Volcanic Island MTG', 'Revised Bayou MTG', 'Revised Plateau MTG', 'Revised Tropical Island MTG')
        ORDER BY completed_product_end DESC;
        ''')
    data = fetch_data(query)
    return data

def get_data_revised_stats():
    """() -> list

    Returns the last 10 data points for each distinct (unique) value from the revised set, in descending order by timestamp."""
    query = (
        f'''
        SELECT DISTINCT * 
        FROM production_completed_products_stats
        WHERE completed_product_nick IN ('Revised Tundra MTG', 'Revised Underground Sea MTG', 'Revised Badlands MTG', 'Revised Taiga MTG', 'Revised Savannah MTG', 'Revised Scrubland MTG', 'Revised Volcanic Island MTG', 'Revised Bayou MTG', 'Revised Plateau MTG', 'Revised Tropical Island MTG')
        ORDER BY timestamp DESC
        LIMIT 10;
        '''
    )
    data = fetch_data(query)
    return data

def get_data_revised_avg():
    """() -> list

    Returns a list of last 2 revised index average values inserted into the database, in descending order by the primary id."""
    query = (
        f'''
        SELECT completed_product_index_avg
        FROM production_completed_products_index
        WHERE completed_product_set_id = '7'
        ORDER BY primary_ids DESC
        LIMIT 2
        '''
    )
    data = fetch_data(query)
    data = list(data['completed_product_index_avg'])
    calc = ((data[0]-data[1])/data[1])*100
    if calc >= 0:
        return f'${data[0]:,.0f} (±{calc:,.2f}%)'
    else:
        return f'${data[0]:,.0f} (±{calc:,.2f}%)'

def get_data_revised_active_all():
    """() -> list

    Returns a list of all of the active revised cards in the database, sorted in ascending order by start date."""
    query = (
        f'''
        SELECT active_product_nick, active_product_titles, CAST (CAST (active_product_prices AS text) AS money), active_product_start::timestamp::date, active_product_lst_type, active_product_img_url, active_product_loc, active_product_img_thumb, active_product_depth, active_product_watch_count
        FROM active_products
        WHERE active_product_nick IN ('Revised Tundra MTG', 'Revised Underground Sea MTG', 'Revised Badlands MTG', 'Revised Taiga MTG', 'Revised Savannah MTG', 'Revised Scrubland MTG', 'Revised Volcanic Island MTG', 'Revised Bayou MTG', 'Revised Plateau MTG', 'Revised Tropical Island MTG')
        ORDER BY active_product_start DESC;
        ''')
    data = fetch_data(query)
    return data

def get_data_revised_cumulative_totals():
    """() -> list

    Returns a list of all of the active revised cards in the database, sorted in ascending order by start date."""
    query = (
        f'''
        SELECT completed_product_index_sum, timestamp FROM production_completed_products_index WHERE completed_product_set_id = '7' AND completed_product_index_sum IS NOT NULL ORDER BY timestamp ASC; 
        ''')
    data = fetch_data(query)
    return data

# begin individual card queries
def get_all_data_individual_general(value):
    """() -> list

    Returns a list of all of the data points for the specific value, sorted in ascending order by start date."""
    query = (
        f'''
        SELECT *, timestamp::date, completed_product_end::date as endDate,  to_char(to_timestamp (date_part('month', completed_product_end::date)::text, 'MM'), 'Month') as month, DATE_PART('day', completed_product_end::date) as day, completed_product_end::date - completed_product_start::date as length
        FROM completed_products 
        WHERE completed_product_nick = '{value}'
        ORDER BY endDate ASC;
        ''')
    data = fetch_data(query)
    return data

def get_all_data_individual_stats(value):
    """() -> list

    Returns a list of all of the data points for the specific value, sorted in ascending order by start date."""
    # SELECT completed_product_avg, timestamp::date, completed_product_avg_length, completed_product_depth, completed_product_sum
    #TODO: changed timestamp::date @ 10/29/2018
    query = (
        f'''
        SELECT completed_product_avg, timestamp, completed_product_avg_length, completed_product_depth, completed_product_sum
        FROM production_completed_products_stats 
        WHERE completed_product_nick = '{value}'
        ORDER BY timestamp ASC;
        ''')
    data = fetch_data(query)
    return data

def get_all_data_individual_stats_active(value):
    """() -> list

    Returns a list of all of the active data points for the specific value, sorted in ascending order by start date."""
    query = (
        f'''
        SELECT active_product_avg, timestamp::date, active_product_avg_length, active_product_depth, active_product_sum
        FROM production_active_products_stats 
        WHERE active_product_nick = '{value}'
        ORDER BY timestamp::date ASC;
        ''')
    data = fetch_data(query)
    return data

def get_data_single_product_depth_last_2(value):
    """() -> list

    Returns the depth for the specific value within the active products table."""
    query = (
        f'''
        SELECT active_product_depth
        FROM production_active_products_stats
        WHERE active_product_nick = '{value}'
		ORDER BY timestamp DESC;
        '''
    )
    data = fetch_data(query)
    return data.values

def get_data_active_index_count_sum(value):
    """() -> list

    Returns the active index sum (total count) for the specific value within the production_active_products_index."""
    query = (
        f'''
        SELECT active_product_index_count_sum
        FROM production_active_products_index
        WHERE active_product_set_id = '{value}'
        ORDER BY timestamp DESC LIMIT 1;
        '''
    )
    data = fetch_data(query)
    return data.values
