"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import os
import psycopg2

# объявление переменных с путями до файлов csv
employees = os.path.join(os.path.dirname(__file__), 'north_data/employees_data.csv')
customers = os.path.join(os.path.dirname(__file__), 'north_data/customers_data.csv')
orders = os.path.join(os.path.dirname(__file__), 'north_data/orders_data.csv')

def read_csv(data_file):
    """
    функция читает файл csv и возвращает список кортежей
    """
    with open(data_file, 'r') as file:
        data_table = [tuple(row) for row in csv.reader(file)]
    return data_table

def write_to_table(table):
    """
    функиця создает подключение к БД и курсор через контекстные менеджеры и записывает данные в таблицы БД.
    принимает на вход переменную со ссылкой на файл csv, запускает функцию чтения файла read_csv и в зависимости от
    файла записывает данные в соответствующую таблицы БД
    """
    conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='12345')
    try:
        with conn:
            with conn.cursor() as cur:
                data = read_csv(table)
                for row in data[1:]:
                    if 'employees_data' in table:
                        cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", row)
                    elif 'customers_data' in table:
                        cur.execute("INSERT INTO customers VALUES (%s, %s, %s)", row)
                    elif 'orders_data' in table:
                        cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", row)
    finally:
        conn.close()


if __name__ == "__main__":
    write_to_table(employees)
    write_to_table(customers)
    write_to_table(orders)
