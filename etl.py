import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """This function used to load data from S3 buckets to the staging tables in RedShift"""
    for query in copy_table_queries:
        print(query)
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)


def insert_tables(cur, conn):
    """This function used to insert data from staging tables in Redshift to final analytical tables"""
    for query in insert_table_queries:
        print(query)
        try:
            cur.execute(query)
        except psycopg2.IntegrityError as e:
            conn.rollback()
            print('insert failure:',e)
        else:
            conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()