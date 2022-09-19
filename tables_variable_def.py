# import and create connection to Azure SQL Database

import urllib
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


# create tables for SQLAlchemy
meta.clear()
cargo_tb = Table(
    'cargo', meta,
    Column('flight_date', Date),
    Column('flight_no', String),
    Column('routing', String),
    Column('cargo_cap', String),
    Column('cargo_load_factor', String),
    Column('awbs', String),
    Column('gross_wt', String),
    Column('charge_wt', String),
    Column('cbm', String),
    Column('revenue_before_tax', Float),
    Column('revenue_after_tax', Float),
    Column('file_name', String),
    Column('modified_time', String)
)

dim_agent_tb = Table(
    'dim_agent', meta,
    Column('no', Integer),
    Column('AG_Code', String),
    Column('Channel', String),
    Column('Agent', String),
    Column('Full_Agent_Name', String),
    Column('Stattus', String),
    Column('In_Charge', String),
    Column('City', String),
    Column('District', String),
    Column('Region', String),
    Column('Incentive', String),
    Column('use_for_report', Integer),
)

dim_calendar_tb = Table(
    'dim_calendar', meta,
    Column('Date', Date),
    Column('Year', Integer),
    Column('Quarter', Integer),
    Column('Month', Integer),
    Column('Month_code', String),
    Column('Weeknum', Integer),
    Column('Weekday', String),
    Column('Weekday_code', Integer),
    Column('Weeknum_code', String),
    Column('day_in_month', Integer),
    Column('date_ddmmmyy', String),
    Column('date_ddmmm', String),
    Column('dd_ddd', String),
    Column('lunar_date', String),
    Column('file_name', String),
    Column('modified_time', String),
)

dim_fare_code_tb = Table(
    'dim_fare_code', meta,
    Column('valid_date', Date),
    Column('class_code', String),
    Column('base_fare', Integer),
    Column('sector', String),
    Column('all_in', Float),
    Column('file_name', String),
    Column('modified_time', String),
)

dim_routes_tb = Table(
    'dim_routes', meta,
    Column('Sector', String),
    Column('Sector2', String),
    Column('Route', String),
    Column('Sector_Distance', Integer),
    Column('Order_route', Integer),
    Column('Order_sector', Integer),
    Column('file_name', String),
    Column('modified_time', String),
)

flight_type_tb = Table(
    'flight_type', meta,
    Column('flight_num', String),
    Column('flight_type', String),
    Column('order', String),
    Column('file_name', String),
    Column('modified_time', String),
)

flown_aircraft_leg_tb = Table(
    'flown_aircraft_leg', meta,
    Column('carrier', String),
    Column('flt_num', String),
    Column('suffix', String),
    Column('Flight_Origin_Date_LT', DateTime),
    Column('dep_station', String),
    Column('arr_station', String),
    Column('std_lt', DateTime),
    Column('sta_lt', DateTime),
    Column('aircraft_type', String),
    Column('config', String),
    Column('flight_status', String),
    Column('traffic_restriction_code', String),
    Column('schedule_group', String),
    Column('file_name', String),
    Column('modified_time', String),
)

inflow_cash_tb = Table(
    'inflow_cash', meta,
    Column('Date', Date),
    Column('Actual_inflow_cash', Integer),
    Column('file_name', String),
    Column('modified_time', String),
)

log_table_tb = Table(
    'log_table', meta,
    Column('file_name', String),
    Column('modified_time', String),
    Column('table_name', String),
    Column('dir_file', String),
)

market_pricing_tb = Table(
    'market_pricing', meta,
    Column('VU_compare_unique_flight_code', String),
    Column('name', String),
    Column('bag', String),
    Column('meal', String),
    Column('adjusted_price', Integer),
    Column('departure_datetime', DateTime),
    Column('pricing_date', DateTime),
    Column('file_name', String),
    Column('modified_time', String)
)

pax_revenue_tb = Table(
    'pax_revenue', meta,
    Column('CONFIRMATION_NUM', String),
    Column('RES_SEG_STATUS_DESCRIPTION', String),
    Column('BOOKING_AGENT', String),
    Column('IATA_NUM', String),
    Column('FARE_CLASS_CODE', String),
    Column('SAVED_FB_CODE', String),
    Column('PTC_DESCRIPTION', String),
    Column('TITLE', String),
    Column('LAST_NAME', String),
    Column('FIRST_NAME', String),
    Column('INFANT', Integer),
    Column('FLIGHT_NUM', Integer),
    Column('CARRIER_CODE', String),
    Column('FROM_AIRPORT', String),
    Column('TO_AIRPORT', String),
    Column('DEPARTURE_DATE', String),
    Column('DEPARTURE_TIME', String),
    Column('FLIGHT_STATUS', String),
    Column('BOOK_DATE', String),
    Column('CNT_DATE', Integer),
    Column('BF', Float),
    Column('PNLT', Float),
    Column('AX', Float),
    Column('C4', Float),
    Column('YR01', Float),
    Column('YR02', Float),
    Column('YQ', Float),
    Column('TOTAL_TAX', Float),
    Column('SERVICES_FEE', Float),
    Column('file_name', String),
    Column('modified_time', String)
)

payment_detail_tb = Table(
    'payment_detail', meta,
    Column('CONFIRMATION_NUM', String),
    Column('BOOKING_TYPE', String),
    Column('RES_STATUS', Integer),
    Column('RES_PAYMENT_ID', Integer),
    Column('BOOK_DATE_GMT', String),
    Column('BOOK_DATE_LCL', String),
    Column('LAST_MODIFIED_GMT', String),
    Column('LAST_MODIFIED_LCL', String),
    Column('BOOKING_AGENT', String),
    Column('CRS_CODE', Float),
    Column('USER_ID', String),
    Column('DEPT_NAME', String),
    Column('DESCRIPTION', String),
    Column('IATA_NUM', String),
    Column('PARENT_IATA_NUM', Integer),
    Column('PERSON_ORG_ID', Integer),
    Column('FIRST_NAME', String),
    Column('LAST_NAME', String),
    Column('CARDHOLDER_NAME', String),
    Column('DATE_PAID_GMT', String),
    Column('DATE_PAID_LCL', String),
    Column('ACCOUNT_NUM', String),
    Column('PAYMENT_TYPE', String),
    Column('PAYMENT_METHOD', String),
    Column('PAYMENT_METHOD_DESC', String),
    Column('TRANS_STATUS', String),
    Column('REFERENCE', String),
    Column('AUTHORIZATION_CODE', String),
    Column('VOUCHER_NUM', String),
    Column('VOUCHER_NUM_FULL', String),
    Column('VCHR_CREATED_GMT', String),
    Column('VCHR_CREATED_LCL', String),
    Column('RES_CURRENCY', String),
    Column('CURRENCY_PAID', String),
    Column('AMOUNT_PAID', Float),
    Column('BASE_CURRENCY', String),
    Column('BASE_AMOUNT', Float),
    Column('RPT_CURRENCY', String),
    Column('RPT_AMOUNT', Integer),
    Column('file_name', String),
    Column('modified_time', String)
)

replicated_flight_code_days_tb = Table(
    'replicated_flight_code_days', meta,
    Column('index', Integer),
    Column('unique_flight_code', String),
    Column('days_before_departure', Integer),
    Column('vu_revenue', Integer)
)

reservation_tb = Table(
   'reservation', meta,
    Column('last_name', String),
    Column('first_name', String),
    Column('confirm', String),
    Column('book_date', DateTime),
    Column('departure_date', Date),
    Column('flight_number', String),
    Column('class_code', String),
    Column('ori_station', String),
    Column('des_station', String),
    Column('status', String),
    Column('iata', String),
    Column('copr_id', String),
    Column('record_locator', String),
    Column('eticket_num', String),
    Column('file_name', String),
    Column('modified_time', String),
)

target_cost_tb = Table(
   'target_cost', meta,
    Column('Valid_date', DateTime),
    Column('Sector', String),
    Column('Revenue', Float),
    Column('Frequency', Float),
    Column('cargo_revenue', Float),
    Column('Variable_cost_per_flight', Float),
    Column('Fixed_cost_per_flight', Float),
    Column('Total_cost_per_flight', Float),
)

total_market_price_tb = Table(
   'total_market_price', meta,
    Column('name', String),
    Column('bag', Integer),
    Column('meal', Integer),
    Column('adjusted_price', Integer),
    Column('sector', String),
    Column('departure_datetime', DateTime),
    Column('pricing_date', DateTime),
    Column('file_name', String),
    Column('modified_time', DateTime),
)