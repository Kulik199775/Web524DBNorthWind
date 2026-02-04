import pyodbc
import os

class Database:
    """Класс для подключения к SQL Server и создания БД NorthWind"""

    def __init__(self):
        """Инициализация подключения к MSSQL Server"""

        self.SERVER = os.getenv('MS_SQL_SERVER')
        self.DATABASE = os.getenv('MS_SQL_DATABASE')
        self.USER = os.getenv('MS_SQL_USER')
        self.PASSWORD = os.getenv('MS_SQL_KEY')
        self.DRIVER = os.getenv('MS_SQL_DRIVER')

        self.conn = None
        self.connection_string = None

    def connect(self, password):
        """Установка подключения к БД"""

        try:
            pwd = password if password is not None else self.PASSWORD

            self.connection_string = (
                f"DRIVER={{{self.DRIVER}}};"
                f"SERVER={self.SERVER};"
                f"DATABASE={self.DATABASE};"
                f"UID={self.USER};"
                f"PWD={pwd};"
                "TrustServerCertificate=yes"
            )
            self.conn = pyodbc.connect(self.connection_string)
            self.conn.autocommit = True
            print(f'Подключение к БД {self.DATABASE} на сервере {self.SERVER} установлено')
            return True

        except pyodbc.InterfaceError as IFEr:
            print(f'Ошибка драйвера ODBC: {IFEr}')
            return False
        except pyodbc.OperationalError as OEr:
            print(f'Ошибка подключения: {OEr}')
            return False
        except Exception as ex:
            print(f'Ошибка подключения: {ex}')


    def create_database(self):
        """Создание базы данных"""
        try:
            pad_database_conn_str = (
                f"DRIVER={{{self.DRIVER}}};"
                f"SERVER={self.SERVER};"
                "DATABASE=pad_database;"
            )
            if self.PASSWORD:
                pad_database_conn_str += f'UID={self.USER};PWD={self.PASSWORD};'
            else:
                pad_database_conn_str += 'Trusted_Connection=yes'

            pad_database_conn_str += 'TrustServerCertificate=yes'
            pad_database = pyodbc.connect(pad_database_conn_str)
            pad_database.autocommit = True
            cursor = pad_database.cursor()
            cursor.execute(f'CREATE DATABASE [{self.DATABASE}]')
            print(f'База данных {self.DATABASE} успешно создана')
            cursor.close()
            pad_database.close()
            return True

        except Exception as ex:
            print(f'Ошибка при создании БД: {ex}')