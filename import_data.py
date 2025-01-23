import os

import pandas as pd
from dotenv import load_dotenv

from utils.connection import close_db, connect_db

load_dotenv()


def create_schema_and_tables(conn, cur):
    # создадим таблицы для загрузки данных из data/ чтобы сравнить с имеющимися данными
    cur.execute(
        """
        DROP TABLE IF EXISTS data.deal_info;
        CREATE TABLE data.deal_info (
            deal_rk INT8,
            deal_num TEXT,
            deal_name TEXT,
            deal_sum NUMERIC,
            client_rk INT8,
            account_rk INT8,
            agreement_rk INT8,
            deal_start_date DATE,
            department_rk INT8,
            product_rk INT8,
            deal_type_cd TEXT,
            effective_from_date DATE,
            effective_to_date DATE
        );

        DROP TABLE IF EXISTS data.dict_currency;
        CREATE TABLE data.dict_currency (
            currency_cd TEXT,
            currency_name TEXT,
            effective_from_date DATE,
            effective_to_date DATE
        );

        DROP TABLE IF EXISTS data.product_info;
        CREATE TABLE data.product_info (
            product_rk INT8,
            product_name TEXT,
            effective_from_date DATE,
            effective_to_date DATE
        );
        """
    )
    conn.commit()


def import_table_data(conn, cur, input_path, table_name, columns):
    cur.execute(f"TRUNCATE TABLE data.{table_name};")

    df = pd.read_csv(
        input_path, sep=",", encoding="cp1251"
    )  # поменял кодировку чтобы csv прочиталось

    values = ", ".join(
        ["%s"] * len(columns)
    )  # можно вставить кол-во аргументов равное длине значения из словаря tables
    columns_str = ", ".join(columns)

    insert_query = f"""
        INSERT INTO data.{table_name} ({columns_str})
        VALUES ({values})
        """

    for index, row in df.iterrows():
        values = [row[col] for col in columns]
        cur.execute(insert_query, values)

    conn.commit()
    records_count = len(df)
    print(
        f"Загружено {records_count} записей в таблицу {table_name} из {input_path}"
    )


def import_tables():
    try:
        conn, cur = connect_db()

        create_schema_and_tables(conn, cur)

        tables = {
            "deal_info": [
                "deal_rk",
                "deal_num",
                "deal_name",
                "deal_sum",
                "client_rk",
                "account_rk",
                "agreement_rk",
                "deal_start_date",
                "department_rk",
                "product_rk",
                "deal_type_cd",
                "effective_from_date",
                "effective_to_date",
            ],
            "dict_currency": [
                "currency_cd",
                "currency_name",
                "effective_from_date",
                "effective_to_date",
            ],
            "product_info": [
                "product_rk",
                "product_name",
                "effective_from_date",
                "effective_to_date",
            ],
        }

        for table, columns in tables.items():
            file_name = f"{table}.csv"
            input_path = os.path.join(os.getenv("IMPORT_PATH"), file_name)

            import_table_data(conn, cur, input_path, table, columns)

    except Exception as e:
        print(f"Ошибка выполнения скрипта: {str(e)}")
        raise e
    finally:
        close_db(conn, cur)


if __name__ == "__main__":
    import_tables()
