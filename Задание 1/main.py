import sys
from dataclasses import dataclass
from get_curs import *
from ResponceBD import *
import logging
import re
import sqlite3

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO, filename="main.log",filemode="w", encoding='utf-8',
                        format="%(asctime)s %(levelname)s %(message)s")
                    

    name_db = "currency.db"
    date = sys.argv[1]
    if not re.match(r"\d\d\d\d-\d\d-\d\d", date):
        logging.info("Invalid date format")
        print("Invalid date format")
        raise SystemExit
        
    vcodes = sys.argv[2:]
    logging.info(f"codes: {vcodes}")
    print(f"codes: {vcodes}")


    currency_rates_response = '''CREATE TABLE IF NOT EXISTS CURRENCY_RATES
                            (order_id INTEGER,
                            name TEXT PRIMARY KEY, numeric_code TEXT NOT NULL,
                            alphabetic_code TEXT NOT NULL, scale INTEGER NOT NULL,
                            rate TEXT NOT NULL,
                            FOREIGN KEY (order_id) REFERENCES CURRENCY_ORDER(id))'''

    curency_order_response = '''CREATE TABLE IF NOT EXISTS CURRENCY_ORDER
                            (id INTEGER PRIMARY KEY AUTOINCREMENT, ondate TEXT UNIQUE NOT NULL)'''

    SenderResponce.apply(name_db, [Response(curency_order_response), Response(currency_rates_response)])

    add_in_db(name_db, vcodes, date)

   # Подключение к базе данных
    conn = sqlite3.connect(name_db)
    c = conn.cursor()

    # Формирование SQL-запроса с использованием параметров
    query = '''SELECT CURRENCY_RATES.*, CURRENCY_ORDER.*
            FROM CURRENCY_RATES
            JOIN CURRENCY_ORDER ON CURRENCY_RATES.order_id = CURRENCY_ORDER.id
            WHERE CURRENCY_RATES.numeric_code IN ({})'''.format(','.join(['?']*len(vcodes)))
    
    # Выполнение SQL-запроса и извлечение результатов
    c.execute(query, tuple(vcodes))
    rows = c.fetchall()

    # Вывод результатов
    for row in rows:
        print(row)

    # Закрытие соединения с базой данных
    conn.close()

    










        



