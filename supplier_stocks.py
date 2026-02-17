#!/usr/bin/python3
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import os
from dotenv import load_dotenv
import json
import datetime
import sqlite3

# Загружаем токен из .env
# load_dotenv()
# wb_api_token = os.getenv('wb_api_token')

wb_api_token = 'ваш_токен_api'

# Текущая дата для параметра dateFrom
data_ymd = datetime.date.today().isoformat()

# URL API остатков
url = 'https://statistics-api.wildberries.ru/api/v1/supplier/stocks'

headers = {
    'Authorization': wb_api_token
}

params = {
    "dateFrom": data_ymd
}

# Выполняем запрос
res = requests.get(url, headers=headers, params=params)
res.raise_for_status()  # выбросит исключение при HTTP-ошибке

data = res.content.decode()
js_obj = json.loads(data)

# Создаём DataFrame из ответа API
df = pd.DataFrame(js_obj)

# Подключаемся к базе (файл my_database1.db будет создан, если его нет)
conn = sqlite3.connect("my_database1.db")

# Создаём таблицу с нужной схемой, если её ещё нет
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS supplier_stocks (
    id_db INTEGER PRIMARY KEY AUTOINCREMENT,
    "lastChangeDate" TEXT,
    "warehouseName" TEXT,
    "supplierArticle" TEXT,
    "nmId" TEXT,
    "barcode" TEXT,
    "quantity" INTEGER,
    "inWayToClient" INTEGER,
    "inWayFromClient" INTEGER,
    "quantityFull" INTEGER,
    "category" TEXT,
    "subject" TEXT,
    "brand" TEXT,
    "techSize" TEXT,
    "Price" INTEGER,
    "Discount" INTEGER,
    "isSupply" TEXT,
    "isRealization" TEXT,
    "SCCode" TEXT
)
""")
conn.commit()

# Вставляем данные из DataFrame в таблицу.
# Параметр if_exists='append' добавляет строки, не удаляя старые.
# index=False предотвращает запись индекса DataFrame как отдельного столбца.
df.to_sql("supplier_stocks", conn, if_exists="append", index=False)

# Сохраняем изменения и закрываем соединение
conn.commit()
conn.close()
