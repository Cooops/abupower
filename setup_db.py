import psycopg2
from gen_utils import get_trace_and_log, database_connection

def create_tables():
    """() -> executes function

    Create series of tables in respective PostgreSQL database."""
    commands = (
        """
        CREATE TABLE active_products(
            primary_ids SERIAL PRIMARY KEY UNIQUE,
            active_product_nick VARCHAR(255) NOT NULL,
            active_product_titles VARCHAR(255) NOT NULL,
            active_product_ids BIGSERIAL NOT NULL UNIQUE,
            active_product_prices FLOAT NOT NULL,
            active_product_cat_names VARCHAR(255) NOT NULL,
            active_product_cat_ids INT NOT NULL,
            active_product_img_thumb VARCHAR(255) NOT NULL,
            active_product_img_url VARCHAR(255) NOT NULL,
            active_product_lst_type VARCHAR(255) NOT NULL,
            active_product_watch_count INT NOT NULL,
            active_product_con VARCHAR(255) NOT NULL,
            active_product_loc VARCHAR(255) NOT NULL,
            active_product_start VARCHAR(255) NOT NULL,
            active_product_end VARCHAR(255) NOT NULL,
            active_product_depth INT NOT NULL,
            timestamp timestamp default current_timestamp)
        """,
        """
        CREATE TABLE completed_products(
            primary_ids SERIAL PRIMARY KEY UNIQUE,
            completed_product_nick VARCHAR(255) NOT NULL,
            completed_product_titles VARCHAR(255) NOT NULL,
            completed_product_ids BIGSERIAL NOT NULL UNIQUE,
            completed_product_prices FLOAT NOT NULL,
            completed_product_cat_names VARCHAR(255) NOT NULL,
            completed_product_cat_ids INT NOT NULL,
            completed_product_img_thumb VARCHAR(255) NOT NULL,
            completed_product_img_url VARCHAR(255) NOT NULL,
            completed_product_lst_type VARCHAR(255) NOT NULL,
            completed_product_con VARCHAR(255) NOT NULL,
            completed_product_loc VARCHAR(255) NOT NULL,
            completed_product_start VARCHAR(255) NOT NULL,
            completed_product_end VARCHAR(255) NOT NULL,
            timestamp timestamp default current_timestamp,
            completed_product_depth INT NOT NULL)
        """,
        """
           CREATE TABLE production_completed_products_stats(
           primary_ids SERIAL PRIMARY KEY UNIQUE,
           completed_product_nick VARCHAR(255),
           completed_product_avg FLOAT,
           completed_product_min FLOAT,
           completed_product_max FLOAT,
           completed_product_depth INT,
           completed_product_avg_length FLOAT,
           completed_product_sum FLOAT,
           timestamp timestamp default current_timestamp)
        """,
        """
           CREATE TABLE production_completed_products_index(
           primary_ids SERIAL PRIMARY KEY UNIQUE,
           completed_product_set_name VARCHAR(255),
           completed_product_set_id INT,
           completed_product_index_avg FLOAT,
           completed_product_index_min FLOAT ,
           completed_product_index_max FLOAT,
           completed_product_index_length_avg FLOAT ,
           completed_product_index_count_sum INT,
           completed_product_index_sum FLOAT,
           timestamp timestamp default current_timestamp)
        """,
        """
           CREATE TABLE production_active_products_stats(
           primary_ids SERIAL PRIMARY KEY UNIQUE,
           active_product_nick VARCHAR(255),
           active_product_avg FLOAT,
           active_product_min FLOAT,
           active_product_max FLOAT,
           active_product_depth INT,
           active_product_avg_length FLOAT,
           active_product_sum FLOAT,
           timestamp timestamp default current_timestamp)
        """,
        """
           CREATE TABLE production_active_products_index(
           primary_ids SERIAL PRIMARY KEY UNIQUE,
           active_product_set_name VARCHAR(255),
           active_product_set_id INT,
           active_product_index_avg FLOAT,
           active_product_index_min FLOAT ,
           active_product_index_max FLOAT,
           active_product_index_length_avg FLOAT ,
           active_product_index_count_sum INT,
           active_product_index_sum FLOAT,
           timestamp timestamp default current_timestamp)
        """,
        )
    conn = None
    try:
        cur = database_connection()
        try:
            cur.execute(commands)
        except:
            for command in commands:
                    cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        get_trace_and_log(e)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    create_tables()
