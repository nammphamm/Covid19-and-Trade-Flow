import psycopg2

from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Create database and return new connection string
    :return:
    """
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=postgres")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS comtrade")
    cur.execute("CREATE DATABASE comtrade WITH ENCODING 'utf8'")
    print("Database created")

    # close connection
    conn.close()

    # connect to database just created
    conn = psycopg2.connect("host=127.0.0.1 dbname=comtrade")
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    # drop tables in case we need to start fresh
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create tables as defined
    :return:
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()

    # drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
