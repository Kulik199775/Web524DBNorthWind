import csv

import pyodbc
import os
from dotenv import load_dotenv

class Database:
    """Класс для подключения к SQL Server и создания БД NorthWind"""

    def __init__(self):
        """Инициализация подключения к MSSQL Server"""

        load_dotenv()
        self.SERVER = os.getenv('MS_SQL_SERVER')
        self.DATABASE = os.getenv('MS_SQL_DATABASE')
        self.USER = os.getenv('MS_SQL_USER')
        self.PASSWORD = os.getenv('MS_SQL_KEY')
        self.DRIVER = os.getenv('MS_SQL_DRIVER')
        self.PAD_DATABASE = os.getenv('MS_SQL_PAD_DATABASE')

        self.conn = None
        self.connection_string = None

    def connect(self, password=None):
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
            pad_conn_str = (
                f"DRIVER={{{self.DRIVER}}};"
                f"SERVER={self.SERVER};"
                f"DATABASE={self.PAD_DATABASE};"
                f"UID={self.USER};"
                f"PWD={self.PASSWORD};"
                "TrustServerCertificate=yes;"
                "Encrypt=no"
            )
            pad_database = pyodbc.connect(pad_conn_str)
            print(f"✓ Подключились к {self.PAD_DATABASE}")

            pad_database.autocommit = True
            cursor = pad_database.cursor()

            # Проверяем, существует ли база данных
            cursor.execute(f"""
                            IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{self.DATABASE}')
                            BEGIN
                                CREATE DATABASE [{self.DATABASE}]
                                PRINT 'База данных {self.DATABASE} создана'
                            END
                            ELSE
                            BEGIN
                                PRINT 'База данных {self.DATABASE} уже существует'
                            END
                        """)

            # Выполняем дополнительный запрос для проверки
            cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{self.DATABASE}'")
            result = cursor.fetchone()

            if result:
                print(f"✓ База данных '{self.DATABASE}' создана/существует")
            else:
                print(f"✗ Не удалось создать базу данных '{self.DATABASE}'")

            cursor.close()
            pad_database.close()
            return True

        except Exception as ex:
            print(f'✗ Ошибка при создании БД: {ex}')
            return False

    def create_table(self):
        """Создание таблиц в БД NorthWind"""

        try:
            # убеждаемся, что подключение установлено
            if self.conn is None:
                if not self.connect():
                    return False

            cursor = self.conn.cursor()

            # таблица customers_data
            cursor.execute("""IF NOT EXISTS 
                            (SELECT * FROM sysobjects WHERE name='customers_data' AND xtype='U')
                            CREATE TABLE customers_data (
                                customer_id NVARCHAR(10) PRIMARY KEY NOT NULL,
                                company_name NVARCHAR(100) NOT NULL,
                                contact_name NVARCHAR(50) NOT NULL
                            )""")

            # таблица employees_data
            cursor.execute("""IF NOT EXISTS 
                        (SELECT * FROM sysobjects WHERE name='employees_data' AND xtype='U')
                        CREATE TABLE employees_data (
                            employee_id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
                            first_name NVARCHAR(100) NOT NULL,
                            last_name NVARCHAR(100) NOT NULL,
                            title NVARCHAR(100) NOT NULL,
                            birth_date DATE NOT NULL,
                            notes NVARCHAR(1000) NOT NULL
                        )""")

            # таблица orders_data
            cursor.execute("""IF NOT EXISTS 
                        (SELECT * FROM sysobjects WHERE name='orders_data' AND xtype='U')
                        CREATE TABLE orders_data (
                            order_id INT IDENTITY(1,1) PRIMARY KEY,
                            customer_id NVARCHAR(10) NOT NULL,
                            employee_id INT NOT NULL,
                            order_date DATE NOT NULL,
                            ship_city NVARCHAR(100) NOT NULL,
                            FOREIGN KEY (customer_id) REFERENCES customers_data(customer_id),
                            FOREIGN KEY (employee_id) REFERENCES employees_data(employee_id)
                        )""")
            cursor.close()
            print(f'Таблицы успешно созданы')
            return True

        except Exception as ex:
            print(f'Ошибка при создании таблиц: {ex}')
            return False

    def execute_query(self, query, params=None):
        """Выполнение SQL запросов и возврат результата"""
        try:
            if self.conn is None:
                if not self.connect(password=None):
                    return None

            cursor = self.conn.cursor()

            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            if query.strip().upper().startswith('SELECT'):
                columns = [column[0] for column in cursor.description]

                result = []
                for row in cursor.fetchall():
                    row_dict = {}
                    for i, col in enumerate(columns):
                        row_dict[col] = row[i]
                    result.append(row_dict)

                cursor.close()
                return result
            else:
                affected_rows = cursor.rowcount
                cursor.close()
                return affected_rows
        except Exception as ex:
            print(f'Ошибка выполнения запроса: {ex}')
            return None

    def read_csv_data(self, filename):
        """Чтение данных из CSV файла"""
        data = []
        try:
            with open(filename, 'r', encoding='utf-8') as file:
                sample = file.read(1024)
                file.seek(0)

                if '\t' in sample:
                    delimiter = '\t'
                else:
                    delimiter = ','

                csv_reader = csv.DictReader(file, delimiter=delimiter, quotechar='"')
                for row in csv_reader:
                    cleaned_row = {}
                    for key, value in row.items():
                        if value is not None:
                            cleaned_value = value.strip().strip('"').strip("'")
                            cleaned_row[key.strip()] = cleaned_value
                        else:
                            cleaned_row[key.strip()] = value
                    data.append(cleaned_row)

            print(f'Прочитано {len(data)} записей из файла {filename}')
            return data
        except FileNotFoundError:
            print(f'Файл {filename} не найден')
            return []
        except Exception as ex:
            print(f'Ошибка при чтении файла {filename}: {ex}')
            return []


    def insert_customers_from_csv(self, filename='customers_data.csv'):
        """Вставляет данные о клиентах из CSV в БД"""

        customers_data = self.read_csv_data(filename)
        inserted_count = 0

        for customer in customers_data:
            customer_id = customer.get('customer_id') or customer.get('CustomerID')
            company_name = customer.get('company_name') or customer.get('CompanyName')
            contact_name = customer.get('contact_name') or customer.get('ContactName')

            if not all([customer_id, company_name, contact_name]):
                print(f'Пропущена запись с неполными данными: {customer}')
                continue

            query = """
                IF NOT EXISTS (SELECT 1 FROM customers_data WHERE customer_id = ?)
                INSERT INTO customers_data (customer_id, company_name, contact_name)
                VALUES (?, ?, ?)
            """
            result = self.execute_query(query, (
                customer_id,
                customer_id,
                company_name,
                contact_name
            ))

            if result is not None and result > 0:
                inserted_count += 1
        print(f"Вставлено {inserted_count} клиентов")
        return inserted_count

    def insert_employees_from_csv(self, filename='employees_data.csv'):
        """Вставляет данные о сотрудниках из CSV в БД"""

        employees_data = self.read_csv_data(filename)
        inserted_count = 0

        for employee in employees_data:
            first_name = employee.get('first_name') or employee.get('FirstName')
            last_name = employee.get('last_name') or employee.get('LastName')
            title = employee.get('title') or employee.get('Title')
            birth_date = employee.get('birth_date') or employee.get('BirthDate')
            notes = employee.get('notes') or employee.get('Notes')

            if not all([first_name, last_name, title, birth_date, notes]):
                print(f'Пропущена запись с неполными данными: {employee}')
                continue

            query = """
                INSERT INTO employees_data (first_name, last_name, title, birth_date, notes)
                VALUES (?, ?, ?, ?, ?)
            """
            result = self.execute_query(query, (
                first_name,
                last_name,
                title,
                birth_date,
                notes
            ))

            if result is not None and result > 0:
                inserted_count += 1

        print(f"Вставлено {inserted_count} сотрудников")
        return inserted_count

    def insert_orders_from_csv(self, filename="orders_data.csv"):
        """Вставляет данные о заказах из CSV в БД"""

        orders_data = self.read_csv_data(filename)
        inserted_count = 0

        employees_query = "SELECT employee_id FROM employees_data"
        existing_employees = self.execute_query(employees_query)
        if existing_employees is None:
            existing_employees = []

        employee_ids = [str(row['employee_id']) for row in existing_employees]
        if not employee_ids:
            print("ОШИБКА: В таблице employees_data нет записей. Нельзя вставить заказы.")
            return 0

        for order in orders_data:
            # Обрабатываем названия колонок
            customer_id = order.get('customer_id') or order.get('CustomerID')
            employee_id = order.get('employee_id') or order.get('EmployeeID')
            order_date = order.get('order_date') or order.get('OrderDate')
            ship_city = order.get('ship_city') or order.get('ShipCity')

            if not all([customer_id, employee_id, order_date, ship_city]):
                print(f"Пропущена запись с неполными данными: {order}")
                continue

            # Проверяем, существует ли сотрудник
            if str(employee_id) not in employee_ids:
                print(f"Пропущен заказ: сотрудник с ID {employee_id} не существует")
                continue

            query = """
                INSERT INTO orders_data (customer_id, employee_id, order_date, ship_city)
                VALUES (?, ?, ?, ?)
                """
            result = self.execute_query(query, (
                customer_id,
                employee_id,
                order_date,
                ship_city
            ))

            if result is not None and result > 0:
                inserted_count += 1
            else:
                print(f"Не удалось вставить заказ: customer_id={customer_id}, employee_id={employee_id}")

        print(f"Вставлено {inserted_count} заказов")
        return inserted_count

    def select_all_data(self, table_name):
        """Получение всех данных из таблицы"""
        query = f"SELECT * FROM {table_name}"
        result = self.execute_query(query)
        if result is not None:
            print(f"Найдено {len(result)} записей в таблице {table_name}")
            for row in result:
                print(row)
        return result

    def select_data_with_condition(self, table_name, condition=None):
        """Получение данных из таблицы с заданным условием"""
        if condition:
            query = f"SELECT * FROM {table_name} WHERE {condition}"
        else:
            query = f"SELECT * FROM {table_name}"

        result = self.execute_query(query)
        if result is not None:
            print(f"Найдено {len(result)} записей в таблице {table_name}")
        return result

    def update_record(self, table_name, set_values, condition):
        """Изменение записи в таблице"""

        set_parts = []
        params = []
        for column, value in set_values.items():
            set_parts.append(f"{column} = ?")
            params.append(value)

        set_clause = ", ".join(set_parts)
        query = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"

        result = self.execute_query(query, params)
        if result is not None:
            print(f"Обновлено {result} записей в таблице {table_name}")
        return result

    def delete_record(self, table_name, condition):
        """Удаление записи из таблицы"""
        query = f"DELETE FROM {table_name} WHERE {condition}"

        result = self.execute_query(query)
        if result is not None:
            print(f"Удалено {result} записей из таблицы {table_name}")
        return result

    def show_table_info(self, table_name):
        """Показать информацию о таблице"""
        query = f"""
            SELECT 
                COUNT(*) as total_records,
                MIN(order_date) as min_date,
                MAX(order_date) as max_date
            FROM {table_name}
        """
        result = self.execute_query(query, None)
        if result:
            print(f"\nИнформация о таблице {table_name}:")
            for row in result:
                for key, value in row.items():
                    print(f"  {key}: {value}")

    def close_connection(self):
        """Закрытие соединения с базой данных"""
        if self.conn:
            self.conn.close()
            print("\nСоединение с базой данных закрыто")
            self.conn = None


if __name__ == "__main__":

    try:
        drivers = pyodbc.drivers()
        print(f"Найдено драйверов ODBC: {len(drivers)}")
        for driver in drivers:
            print(f"  - {driver}")
    except:
        print("Не удалось получить список драйверов")

    # Инициализация базы данных
    db = Database()

    # 1. Создание базы данных
    db.create_database()

    # 2. Подключение к базе данных
    db.connect(password=None)

    # 3. Создание таблиц
    db.create_table()

    # 4. Импорт данных из CSV файлов
    print("\n" + "=" * 50)
    print("ИМПОРТ ДАННЫХ ИЗ CSV")
    print("=" * 50)

    db.insert_customers_from_csv("customers_data.csv")
    db.insert_employees_from_csv("employees_data.csv")
    db.insert_orders_from_csv("orders_data.csv")

    # 5. Просмотр данных
    print("\n" + "=" * 50)
    print("ПРОСМОТР ДАННЫХ")
    print("=" * 50)

    db.select_all_data("customers_data")
    db.select_all_data("employees_data")
    db.select_all_data("orders_data")

    # 6. Пример запроса с условием
    print("\n" + "=" * 50)
    print("ПОИСК ДАННЫХ")
    print("=" * 50)

    # Поиск заказов с определенным клиентом
    db.select_data_with_condition(
        "orders_data",
        "customer_id = 'ALFKI'"
    )

    # 7. Пример обновления данных
    print("\n" + "=" * 50)
    print("ОБНОВЛЕНИЕ ДАННЫХ")
    print("=" * 50)

    db.update_record(
        "customers_data",
        {"contact_name": "Новый Контакт"},
        "customer_id = 'ALFKI'"
    )

    # 8. Закрытие соединения
    db.close_connection()