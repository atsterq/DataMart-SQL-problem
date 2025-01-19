import os

import pandas as pd
from dotenv import load_dotenv

from utils.connection import close_db, connect_db

load_dotenv()


def import_table_data(conn, cur, input_path, table_name, columns):
    try:
        df = pd.read_csv(input_path, sep=",")
        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join(columns)

        insert_query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
        """

        for index, row in df.iterrows():
            values = [row[col] for col in columns]
            cur.execute(insert_query, values)

        conn.commit()
        records_count = len(df)
        print(
            f"Loaded {records_count} records into {table_name} from {input_path}"
        )

        return records_count

    except Exception as e:
        conn.rollback()
        print(f"Error loading data: {str(e)}")
        raise e


def reload_loan_holiday_info(conn, cur):
    table_name = "dm.loan_holiday_info"

    try:
        cur.execute(f"TRUNCATE TABLE {table_name};")

        with open("loan_holiday_info_prototype.sql", "r") as file:
            reload_sql = file.read()

        cur.execute(reload_sql)

        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        records_count = cur.fetchone()[0]

        conn.commit()
        print(f"Reloaded {records_count} records into {table_name}")

    except Exception as e:
        conn.rollback()
        print(f"Error reloading data: {str(e)}")
        raise e


def import_loan_holiday_data():
    try:
        conn, cur = connect_db()

        tables = {
            "rd.deal": [
                "deal_id",
                "client_id",
                "effective_from",
                "effective_to",
            ],
            "rd.loan_holiday": [
                "holiday_id",
                "deal_id",
                "start_dt",
                "end_dt",
                "holiday_type",
            ],
            "rd.product": [
                "product_id",
                "product_name",
                "product_type",
            ],
        }

        for table, columns in tables.items():
            file_name = f"{table.split('.')[1]}.csv"
            input_path = os.path.join(
                os.getenv("IMPORT_PATH", "data"), file_name
            )

            if os.path.exists(input_path):
                import_table_data(conn, cur, input_path, table, columns)
            else:
                print(f"Warning: {input_path} not found")

        reload_loan_holiday_info(conn, cur)

    except Exception as e:
        print(f"Script execution error: {str(e)}")
        raise e
    finally:
        close_db(conn, cur)


if __name__ == "__main__":
    import_loan_holiday_data()
