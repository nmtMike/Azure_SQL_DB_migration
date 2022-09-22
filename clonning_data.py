# import and create connection to Azure SQL Database
import urllib
import pandas as pd
from sqlalchemy import create_engine, inspect, MetaData, Table, Column, Integer, String, Date, Float, DateTime

az_db_driver = "{ODBC Driver 17 for SQL Server}"
az_db_server = "vta-server.database.windows.net"
az_db_database = "vta-database"
az_db_user = "vtaadmin"
az_db_password = "PRMvta2022mike"

az_db_string = f"""Driver={az_db_driver};Server=tcp:{az_db_server},1433;Database={az_db_database};
Uid={az_db_user};Pwd={az_db_password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"""

az_db_params = urllib.parse.quote_plus(az_db_string)
az_db_conn_str = 'mssql+pyodbc:///?autocommit=true&odbc_connect={}'.format(az_db_params)
az_db_engine = create_engine(az_db_conn_str)
az_db_conn = az_db_engine.connect()

meta = MetaData()



# _____________ read from SQLite and write to Azure
# connect to current source SQLite
import sqlite3
conn = sqlite3.connect(r"D:\NMT\OneDrive\Viettravel Airline\Database\VTA_RM.db")
c = conn.cursor()

# cargo
cargo = pd.read_sql_query('SELECT * FROM cargo', conn)
cargo['flight_date'] = pd.to_datetime(cargo['flight_date'])
cargo['modified_time'] = pd.to_datetime(cargo['modified_time'])
cargo.to_sql('cargo', az_db_conn, if_exists='replace', index=False)

# dim_agent
dim_agent = pd.read_sql_query('SELECT * FROM dim_agent', conn)
dim_agent['modified_time'] = pd.to_datetime(dim_agent['modified_time'])
dim_agent.to_sql('dim_agent', az_db_conn, if_exists='replace', index=False)

# dim_calendar
dim_calendar = pd.read_sql_query('SELECT * FROM dim_calendar', conn)
dim_calendar['Date'] = pd.to_datetime(dim_calendar['Date'])
dim_calendar['modified_time'] = pd.to_datetime(dim_calendar['modified_time'])
dim_calendar.to_sql('dim_calendar', az_db_conn, if_exists='replace', index=False)

# dim_fare_code
dim_fare_code = pd.read_sql_query('SELECT * FROM dim_fare_code', conn)
dim_fare_code['valid_date'] = pd.to_datetime(dim_fare_code['valid_date'])
dim_fare_code['modified_time'] = pd.to_datetime(dim_fare_code['modified_time'])
dim_fare_code.to_sql('dim_fare_code', az_db_conn, if_exists='replace', index=False)

# dim_routes
dim_routes = pd.read_sql_query('SELECT * FROM dim_routes', conn)
dim_routes['modified_time'] = pd.to_datetime(dim_routes['modified_time'])
dim_routes.to_sql('dim_routes', az_db_conn, if_exists='replace', index=False)

# dim_slot_time
dim_slot_time = pd.read_sql_query('SELECT * FROM dim_slot_time', conn)
dim_slot_time['modified_time'] = pd.to_datetime(dim_slot_time['modified_time'])
dim_slot_time.to_sql('dim_slot_time', az_db_conn, if_exists='replace', index=False)

# flight_type
flight_type = pd.read_sql_query('SELECT * FROM flight_type', conn)
flight_type['modified_time'] = pd.to_datetime(flight_type['modified_time'])
flight_type.to_sql('flight_type', az_db_conn, if_exists='replace', index=False)

# flown_aircraft_leg
flown_aircraft_leg = pd.read_sql_query('SELECT * FROM flown_aircraft_leg', conn)
flown_aircraft_leg['modified_time'] = pd.to_datetime(flown_aircraft_leg['modified_time'])
flown_aircraft_leg['std_lt'] = pd.to_datetime(flown_aircraft_leg['std_lt'])
flown_aircraft_leg['sta_lt'] = pd.to_datetime(flown_aircraft_leg['sta_lt'])
flown_aircraft_leg.to_sql('flown_aircraft_leg', az_db_conn, if_exists='replace', index=False)

# inflow_cash
inflow_cash = pd.read_sql_query('SELECT * FROM inflow_cash', conn)
inflow_cash['modified_time'] = pd.to_datetime(inflow_cash['modified_time'])
inflow_cash['Date'] = pd.to_datetime(inflow_cash['Date'])
inflow_cash.to_sql('inflow_cash', az_db_conn, if_exists='replace', index=False)

# log_table
log_table = pd.read_sql_query('SELECT * FROM log_table', conn)
log_table['modified_time'] = pd.to_datetime(log_table['modified_time'])
log_table.to_sql('log_table', az_db_conn, if_exists='replace', index=False)

# market_pricing
market_pricing = pd.read_sql_query('SELECT * FROM market_pricing', conn)
market_pricing['modified_time'] = pd.to_datetime(market_pricing['modified_time'])
market_pricing['departure_datetime'] = pd.to_datetime(market_pricing['departure_datetime'])
market_pricing['pricing_date'] = pd.to_datetime(market_pricing['pricing_date'])
market_pricing.to_sql('market_pricing', az_db_conn, if_exists='replace', index=False)

# pax_revenue
pax_revenue = pd.read_sql_query('SELECT * FROM pax_revenue', conn)
pax_revenue['modified_time'] = pd.to_datetime(pax_revenue['modified_time'])
pax_revenue['DEPARTURE_DATE'] = pd.to_datetime(pax_revenue['DEPARTURE_DATE'])
pax_revenue['BOOK_DATE'] = pd.to_datetime(pax_revenue['BOOK_DATE'])
pax_revenue.to_sql('pax_revenue', az_db_conn, if_exists='replace', index=False)

# payment_detail
payment_detail = pd.read_sql_query('SELECT * FROM payment_detail', conn)
payment_detail['modified_time'] = pd.to_datetime(payment_detail['modified_time'])
payment_detail['BOOK_DATE_GMT'] = pd.to_datetime(payment_detail['BOOK_DATE_GMT'])
payment_detail['BOOK_DATE_LCL'] = pd.to_datetime(payment_detail['BOOK_DATE_LCL'])
payment_detail['LAST_MODIFIED_GMT'] = pd.to_datetime(payment_detail['LAST_MODIFIED_GMT'])
payment_detail['LAST_MODIFIED_LCL'] = pd.to_datetime(payment_detail['LAST_MODIFIED_LCL'])
payment_detail['DATE_PAID_GMT'] = pd.to_datetime(payment_detail['DATE_PAID_GMT'])
payment_detail['DATE_PAID_LCL'] = pd.to_datetime(payment_detail['DATE_PAID_LCL'])
payment_detail.to_sql('payment_detail', az_db_conn, if_exists='replace', index=False)

