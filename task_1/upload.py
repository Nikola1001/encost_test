import psycopg2
from psycopg2 import Error
import openpyxl


try:
    # Подключение
    con = psycopg2.connect(user="postgres",
                                  password="1001",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="encost_test")
    cursor = con.cursor()

    create_table_query = '''CREATE TABLE IF NOT EXISTS endpoint_names
                              (endpoint_id INT PRIMARY KEY NOT NULL,
                              endpoint_name TEXT); '''
    cursor.execute(create_table_query)
    con.commit()


    file = openpyxl.open('названия точек.xlsm', read_only=True)
    sheet = file.active

    for row in range(2, sheet.max_row + 1):
        endpoint_id = sheet[row][0].value
        endpoint_name = sheet[row][1].value

        if endpoint_id:
            if not(endpoint_name):
                endpoint_name =''
            cursor.execute( "INSERT INTO endpoint_names (endpoint_id, endpoint_name) VALUES (%s, %s) ON CONFLICT (endpoint_id) DO UPDATE SET endpoint_id = excluded.endpoint_id, endpoint_name = excluded.endpoint_name;",(endpoint_id, endpoint_name))
            con.commit()

    # SQL = "SELECT * FROM endpoint_names;"
    # cursor.execute(SQL)
    # rows = cursor.fetchall()
    # print(rows)

except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", error)
finally:
    if con:
        cursor.close()
        con.close()





