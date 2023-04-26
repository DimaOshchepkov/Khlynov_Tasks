import sys
from dataclasses import dataclass
from get_curs import *
from ResponceBD import *
import logging

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO, filename="main.log",filemode="w", encoding='utf-8',
                        format="%(asctime)s %(levelname)s %(message)s")
                    

    name_db = "currency.db"
    date = sys.argv[1]
    vcodes = sys.argv[2:]

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










        



