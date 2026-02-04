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



