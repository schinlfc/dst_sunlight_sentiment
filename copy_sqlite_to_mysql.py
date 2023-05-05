import util
import pandas as pd
import sqlite3


def copy_table(table, sqlite_con, mysql_con):
    print(f'reading {table}')
    df = pd.read_sql_query(f'select * from {table}', con=sqlite_con)
    print('writing')
    df.to_sql(table, con=mysql_con, if_exists='append', index=False, chunksize=10000)


def main():
    sqlite_con = sqlite3.connect('tweets_cleaned.db')
    engine = util.create_engine()
    tables = [
        'tweet',
        'user',
        'place',
        'score',
        'tweet_legal_tz',
    ]
    for table in tables:
        with engine.connect() as mysql_con:
            copy_table(table, sqlite_con, mysql_con)

    # copy_table('place', sqlite_con, None)

    sqlite_con.close()


if __name__ == '__main__':
    main()
