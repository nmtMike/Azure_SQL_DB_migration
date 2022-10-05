import sqlite3
import pandas as pd


# create connection to sqlite
conn = sqlite3.connect(r"C:\Users\admin\Desktop\Temp\VTA_RM - testing.db")
c = conn.cursor()

def delete_rows_SQL(table, file_name):
    query = f"""
        DELETE FROM {table}
            WHERE file_name = '{file_name}'
        """
    c.execute(query)
    conn.commit()

