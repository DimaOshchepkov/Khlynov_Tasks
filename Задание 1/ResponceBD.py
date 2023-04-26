import sqlite3
from dataclasses import dataclass
import logging

__all__ = ['Response', 'SenderResponce']

logging.basicConfig(level=logging.INFO, filename="main.log",filemode="w", encoding='utf-8',
                    format="%(asctime)s %(levelname)s %(message)s")
                   

@dataclass
class Response:
    """ DB request class. Stores a string and a parameter"""    
    __responce : str = None
    __data : list[tuple] = None

    def __init__(self, responce: str, data : list[tuple] = []) -> None:
        self.__responce = responce
        self.__data = data
    
    @property
    def responce(self) -> str:
        return self.__responce
    
    @property
    def data(self) -> list[tuple]:
        return self.__data
    
class SenderResponce:
    """Static class that sends Responses to BD"""

    @staticmethod
    def apply(name_bd: str, responces : list[Response]) -> None:
        
        conn = sqlite3.connect(name_bd)
        cursor = conn.cursor()

        for res in responces:
            cursor.execute(res.responce, res.data)
            conn.commit()
            logging.info(f"Response: {res.responce}, {res.data} are successfully sent")
            print("Response: " + res.responce +''
                  + str(res.data) + " are successfully sent")

        
        conn.close()