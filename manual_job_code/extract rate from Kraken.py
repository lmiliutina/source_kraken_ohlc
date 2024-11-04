import requests
import time
from datetime import datetime
import psycopg2

# Database connection settings
db_config = {
    'host': '',
    'port': '',
    'dbname': '',
    'user': '',
    'password': ''
}

# Connecting to the database
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    print("Database connection established successfully.")
except Exception as e:
    print("Database connection error:", e)
    exit()

# Creating a table with additional fields for base and quote currencies
create_table_query = '''
CREATE TABLE IF NOT EXISTS ods.kraken_currency_rate (
    id serial,
    pair VARCHAR(20),
    base_currency VARCHAR(10),
    quote_currency VARCHAR(10),
    time TIMESTAMP,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    vwap NUMERIC,
    volume NUMERIC,
    count INTEGER,
    insert_dt timestamp without time zone DEFAULT now(),
    PRIMARY KEY (pair, time)
)
'''

try:
    cursor.execute(create_table_query)
    conn.commit()
    print("Table created successfully or already exists.")
except Exception as e:
    print("Table creation error:", e)
    conn.close()
    exit()

# Fetching the list of assets (currencies)
assets_url = 'https://api.kraken.com/0/public/Assets'
response_assets = requests.get(assets_url)
assets_data = response_assets.json()

if assets_data['error']:
    print("Error fetching assets:", assets_data['error'])
    conn.close()
    exit()
else:
    assets = assets_data['result']

# Creating a dictionary to map asset names to their alternative names
asset_names = {}
for asset_code, asset_info in assets.items():
    asset_names[asset_code] = asset_info['altname']

# List of base currencies for filtering
base_currencies = [
    "XDG", #"DOGE",
    "XBT", #"BTC",
    "ETH",
    "XTZ",
    "LTC",
    "ALGO",
    "BCH",
    "SOL",
    "ADA",
    "GRT",
