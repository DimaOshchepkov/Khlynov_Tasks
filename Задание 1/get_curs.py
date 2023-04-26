from zeep import Client
import sqlite3
from lxml import etree
import logging
from ResponceBD import *

__all__ = ['add_in_db']

def get_curs_on_date_XML(date: str) -> etree._Element:
    """
    Request for http://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?wsdl
    Method: GetCursOnDateXML

    The data is structured like this:
    <ValuteData>
        <ValuteCursOnDate>
            <Vname>Японская иена                                                                                                                                                                                                                                                 </Vname>
            <Vnom>100</Vnom>
            <Vcurs>60.6164</Vcurs>
            <Vcode>392</Vcode>
            <VchCode>JPY</VchCode>
        </ValuteCursOnDate>
    </ValuteData>

    Args:
        date (str): date for which you need to get information about the currency

    Returns:
        etree._Element: xml tree
    """
    client = Client('http://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?wsdl')
    # Construct the SOAP request payload
    request_payload = {
        'On_date': date  # Replace with the actual value for the 'On_date' parameter
    }

    xml = client.service.GetCursOnDateXML(**request_payload)
    if xml is not None:
        logging.info("Success request")
        print("Success request")

    return xml

def find_info_currency_from_xml(xml : etree._Element, code : str) -> tuple:
    """Finds the values needed for the database
    return None if code not found

    Args:
        xml (etree._Element): xml tree, which is obtained by the get_curs_on_date_XML request 
        code (str): currency code

    Returns:
        tuple: (vname.text, vnom.text, vch_code.text, vcurs.text) or None
    """
    sup = xml.xpath(f".//Vcode[normalize-space(text())={code}][1]")
    if not sup:
        logging.info(f"Code {code} not found")
        print(f"Code {code} not found")
        return None
    
    vcode = sup[0]
    valute_curs_on_date = vcode.getparent()
    vname = valute_curs_on_date.find("Vname")
    vnom = valute_curs_on_date.find("Vnom")
    vcurs = valute_curs_on_date.find("Vcurs")
    vch_code = valute_curs_on_date.find("VchCode")

    logging.info(f"For {code} found {vname.text}, {vnom.text}, {vch_code.text}, {vcurs.text}")

    return (vname.text, vnom.text, vch_code.text, vcurs.text)



def add_in_db(name_db : str, Vcodes : list, date : str) -> None:
    """
    Adds information about currencies by Vcodes to the database if this information is not contained there

    Args:
        name_db (str): name of the database
        Vcodes (list): Currency codes
        date (str):  date for which you need to get information about the currency
    """    

    try:
        xml = get_curs_on_date_XML(date)
        logging.info('xml recieved')
    except Exception as e:
        print("Ошибка при получении данных c сервиса\n", e)
        logging.error(e)
        return

    # Установка соединения с базой данных
    conn = sqlite3.connect(name_db)
    cursor = conn.cursor()
    for code in Vcodes:
        sup = find_info_currency_from_xml(xml, code)
        if sup is None:
            continue

        name, nom, ch_code, curs = find_info_currency_from_xml(xml, code)
        try:
            # Проверка наличия order_date в таблице CURRENCY_ORDER
            cursor.execute("SELECT id FROM CURRENCY_ORDER WHERE ondate = ?", (date,))
            existing_order = cursor.fetchone()

            if existing_order is None:
                logging.info(f"write to database CURRENCY_ORDER")
                print("write to database CURRENCY_ORDER")
                # Вставка нового значения в таблицу CURRENCY_ORDER
                insert_currency_order = Response("INSERT INTO CURRENCY_ORDER (ondate) VALUES (?)",
                                                    (date,))
                SenderResponce.apply(name_db, [insert_currency_order])
        
            # Получение order_id для связи с таблицей CURRENCY_ORDER
            cursor.execute("SELECT id FROM CURRENCY_ORDER WHERE ondate = ?", (date,))
            order_id = cursor.fetchone()[0]

            cursor.execute("SELECT name FROM CURRENCY_RATES WHERE name = ?", (name,))
            existing_name = cursor.fetchone()

            if existing_name is None:
                logging.info("write to database CURRENCY_RATES")
                print("write to database CURRENCY_RATES")
                data_for_currency_rates = (order_id, name, code,
                                        ch_code, nom, curs)
            
                insert_currency_rates = Response("""INSERT INTO CURRENCY_RATES
                        (order_id, name, numeric_code, alphabetic_code, scale, rate)
                        VALUES (?, ?, ?, ?, ?, ?)""", data_for_currency_rates)
            
                SenderResponce.apply(name_db, [insert_currency_rates])
        except Exception as e:
            print("Ошибка при работе с бд\n", e)
            logging.error(e)

    # Закрытие соединения с базой данных
    cursor.close()
    conn.close()