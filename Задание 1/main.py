from zeep import Client
import sqlite3
from lxml import etree
import sys
from dataclasses import dataclass
                   

@dataclass
class Response:
    __insert_responce : str = None
    __data : list[tuple] = None

    def __init__(self, insert_responce: str, data : list[tuple] = []) -> None:
        self.__insert_responce = insert_responce
        self.__data = data
    
    @property
    def insert_responce(self) -> str:
        return self.__insert_responce
    
    @property
    def data(self) -> list[tuple]:
        return self.__data
    
class SenderResponce:

    @staticmethod
    def apply(name_bd: str, inserts_responce : list[Response]) -> None:
        
        conn = sqlite3.connect(name_bd)
        cursor = conn.cursor()

        for ins_res in inserts_responce:
            # Execute SQL query to insert data into CURRENCY_RATES table
            cursor.execute(ins_res.insert_responce, ins_res.data)
            conn.commit()

        # Commit the transaction and close connection
        
        conn.close()


def GetCursOnDateXML(date: str) -> None:

    client = Client('http://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?wsdl')
    # Construct the SOAP request payload
    request_payload = {
        'On_date': date  # Replace with the actual value for the 'On_date' parameter
    }

    return client.service.GetCursOnDateXML(**request_payload)

def add_in_db(name_db : str, Vcodes : list, date : str) -> None:

    try:
        xml = GetCursOnDateXML(date)
    except Exception as e:
        print("Ошибка при получении данных c сервиса\n", e)
        print(e)
        return

    # Установка соединения с базой данных
    conn = sqlite3.connect(name_db)
    cursor = conn.cursor()
    for code in Vcodes:
        vcode = xml.xpath(f".//Vcode[normalize-space(text())={code}][1]")[0]
        valute_curs_on_date = vcode.getparent()
        vname = valute_curs_on_date.find("Vname")
        vnom = valute_curs_on_date.find("Vnom")
        vcurs = valute_curs_on_date.find("Vcurs")
        vch_code = valute_curs_on_date.find("VchCode")

        try:
            # Проверка наличия order_date в таблице CURRENCY_ORDER
            cursor.execute("SELECT id FROM CURRENCY_ORDER WHERE ondate = ?", (date,))
            existing_order = cursor.fetchone()

            if existing_order is None:
                # Вставка нового значения в таблицу CURRENCY_ORDER
                insert_currency_order = Response("INSERT INTO CURRENCY_ORDER (ondate) VALUES (?)",
                                                    (date,))
                SenderResponce.apply(name_db, [insert_currency_order])
        
            # Получение order_id для связи с таблицей CURRENCY_ORDER
            cursor.execute("SELECT id FROM CURRENCY_ORDER WHERE ondate = ?", (date,))
            order_id = cursor.fetchone()[0]

            cursor.execute("SELECT name FROM CURRENCY_RATES WHERE name = ?", (vname.text,))
            existing_name = cursor.fetchone()

            if existing_name is None:
                data_for_currency_rates = (order_id, vname.text, vcode.text,
                                        vch_code.text, vnom.text, vcurs.text)
            
                insert_currency_rates = Response("""INSERT INTO CURRENCY_RATES
                        (order_id, name, numeric_code, alphabetic_code, scale, rate)
                        VALUES (?, ?, ?, ?, ?, ?)""", data_for_currency_rates)
            
                SenderResponce.apply(name_db, [insert_currency_rates])
        except Exception as e:
            print("Ошибка при работе с бд\n", e)

    # Закрытие соединения с базой данных
    cursor.close()
    conn.close()

name_db = "currency.db"
vcodes = ['826', '944']

currency_rates_response = '''CREATE TABLE IF NOT EXISTS CURRENCY_RATES
                        (order_id INTEGER,
                        name TEXT PRIMARY KEY, numeric_code TEXT NOT NULL,
                        alphabetic_code TEXT NOT NULL, scale INTEGER NOT NULL,
                        rate TEXT NOT NULL,
                        FOREIGN KEY (order_id) REFERENCES CURRENCY_ORDER(id))'''

curency_order_response = '''CREATE TABLE IF NOT EXISTS CURRENCY_ORDER
                        (id INTEGER PRIMARY KEY AUTOINCREMENT, ondate TEXT UNIQUE NOT NULL)'''

SenderResponce.apply(name_db, [Response(curency_order_response), Response(currency_rates_response)])

add_in_db(name_db, vcodes, '2020-01-01')










        



