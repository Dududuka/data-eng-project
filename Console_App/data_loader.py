from mysql.connector import Error
import pandas as pd


def load_table(connection, table_name):
    try:

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            query = 'SELECT * FROM {}'
            cursor.execute(query.format(table_name))
            df_transactions = pd.DataFrame(cursor.fetchall())
            return df_transactions
    except Error as e:
        print("Error while connecting to Database", e)
    finally:
        if connection.is_connected():
            cursor.close()
        connection.close()
        print("Database connection is closed")
