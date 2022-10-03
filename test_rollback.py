import sqlite3
import pandas as pd


# create connection to sqlite
conn = sqlite3.connect(r"D:\NMT\OneDrive\Viettravel Airline\Database\VTA_RM.db")
c = conn.cursor()

def delete_rows_SQL(table, file_name):
    query = f"""
        DELETE FROM {table}
            WHERE file_name = '{file_name}'
        """
    c.execute(query)
    conn.commit()







