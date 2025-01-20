import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

os.environ["DB_NAME"] = "dwh"  # костыль для того чтобы psycopg2 подключался к dwh а не к banking-etl-db


def connect_db():
    conn = psycopg2.connect(  # создаем подключение к бд
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )

    cur = conn.cursor()  # создаем курсор
    return conn, cur


def close_db(conn, cur):
    cur.close()
    conn.close()
