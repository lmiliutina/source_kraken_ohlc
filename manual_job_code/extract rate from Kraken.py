import requests
import time
from datetime import datetime
import psycopg2

# Настройки подключения к базе данных
db_config = {
    'host': '',
    'port': '',
    'dbname': '',
    'user': '',
    'password': ''
}

# Подключение к базе данных
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    print("Подключение к базе данных успешно установлено.")
except Exception as e:
    print("Ошибка подключения к базе данных:", e)
    exit()

# Создание таблицы с дополнительными полями для базовой и котируемой валют
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
    print("Таблица успешно создана или уже существует.")
except Exception as e:
    print("Ошибка при создании таблицы:", e)
    conn.close()
    exit()

# Получение списка активов (валют)
assets_url = 'https://api.kraken.com/0/public/Assets'
response_assets = requests.get(assets_url)
assets_data = response_assets.json()

if assets_data['error']:
    print("Ошибка при получении активов:", assets_data['error'])
    conn.close()
    exit()
else:
    assets = assets_data['result']

# Создаем словарь для соответствия названий активов и их альтернативных имен
asset_names = {}
for asset_code, asset_info in assets.items():
    asset_names[asset_code] = asset_info['altname']

# Список базовых валют для фильтрации
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
    "USDT",
    "ATOM",
    "TRX",
    "DOT",
    "USDC",
    "MATIC"
]

# Преобразуем список базовых валют в множество для быстрого поиска
base_currencies_set = set(base_currencies)

# Получение списка валютных пар
asset_pairs_url = 'https://api.kraken.com/0/public/AssetPairs'
response = requests.get(asset_pairs_url)
data = response.json()

if data['error']:
    print("Ошибка при получении валютных пар:", data['error'])
    conn.close()
    exit()
else:
    asset_pairs = data['result']

# Фильтрация валютных пар по котируемой валюте (EUR или USD) и базовой валюте (из списка)
filtered_pairs = {}
for pair, pair_info in asset_pairs.items():
    base_asset = pair_info['base']
    quote_asset = pair_info['quote']

    # Преобразуем активы в их альтернативные имена
    base_currency = asset_names.get(base_asset, base_asset)
    quote_currency = asset_names.get(quote_asset, quote_asset)

    if quote_currency in ['EUR', 'USD'] and base_currency in base_currencies_set:
        filtered_pairs[pair] = {
            'base_currency': base_currency,
            'quote_currency': quote_currency
        }

if not filtered_pairs:
    print("Нет валютных пар с заданными условиями.")
    conn.close()
    exit()

# Вывод отфильтрованных валютных пар
print("Отфильтрованные валютные пары:")
for pair in filtered_pairs:
    print(f"{pair}: {filtered_pairs[pair]['base_currency']}/{filtered_pairs[pair]['quote_currency']}")

# Функция для преобразования даты в UNIX Timestamp
def date_to_timestamp(date_str):
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    return int(dt.timestamp())

# Начальная дата (1 августа 2024 года)
start_date = '2024-08-01'
since = date_to_timestamp(start_date)

# Интервал в минутах
interval = 1440  # 1 день

# URL для получения OHLC данных
ohlc_url = 'https://api.kraken.com/0/public/OHLC'

# Получаем текущую метку времени
current_time = int(time.time())

# Цикл по отфильтрованным валютным парам
for pair, currencies in filtered_pairs.items():
    print(f"Получаем данные для валютной пары: {pair}")

    base_currency = currencies['base_currency']
    quote_currency = currencies['quote_currency']

    last = since

    while last < current_time:
        params = {
            'pair': pair,
            'interval': interval,
            'since': last
        }

        response = requests.get(ohlc_url, params=params)
        ohlc_data = response.json()

        if ohlc_data['error']:
            print(f"Ошибка при получении данных для {pair}:", ohlc_data['error'])
            break
        else:
            # Проверяем, есть ли данные для валютной пары
            if pair in ohlc_data['result']:
                data_list = ohlc_data['result'][pair]
                new_last = ohlc_data['result']['last']

                # Проверяем, изменилось ли значение last
                if last == new_last:
                    print(f"Достигнут конец данных для валютной пары {pair}.")
                    break
                else:
                    last = new_last

                if not data_list:
                    print(f"Нет новых данных для валютной пары {pair}.")
                    break

                print(f"Получено {len(data_list)} записей для {pair}.")

                # Преобразуем и сохраняем данные
                for data_point in data_list:
                    # OHLC формат: [time, open, high, low, close, vwap, volume, count]
                    timestamp = int(data_point[0])
                    open_price = float(data_point[1])
                    high_price = float(data_point[2])
                    low_price = float(data_point[3])
                    close_price = float(data_point[4])
                    vwap = float(data_point[5])
                    volume = float(data_point[6])
                    count = int(data_point[7])

                    # Преобразуем метку времени в формат datetime
                    dt_time = datetime.utcfromtimestamp(timestamp)

                    print(pair,base_currency, quote_currency, dt_time, open_price,close_price)

                    # Вставляем данные в базу данных
                    insert_query = '''
                    INSERT INTO ods.kraken_currency_rate (pair, base_currency, quote_currency, time, open, high, low, close, vwap, volume, count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (pair, time) DO NOTHING
                    '''
                    record_to_insert = (
                        pair,
                        base_currency,
                        quote_currency,
                        dt_time,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        vwap,
                        volume,
                        count
                    )

                    try:
                        cursor.execute(insert_query, record_to_insert)
                        conn.commit()
                    except Exception as e:
                        print("Ошибка при вставке данных в базу данных:", e)
                        conn.rollback()

                # Задержка для соблюдения лимитов API
                time.sleep(1)
            else:
                print(f"Нет данных для валютной пары {pair}")
                break

    print(f"Завершена обработка валютной пары: {pair}")


# Закрытие соединения с базой данных
cursor.close()
conn.close()
print("Соединение с базой данных закрыто.")
