import mysql.connector
from mysql.connector import Error
import data_loader as dl


def transactions_by_zip(zip_value, year_value, month_value):
    from mysql.connector import Error

    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='creditcard_capstone',
                                             user='root',
                                             password='a')
        if connection.is_connected():
            db_Info = connection.get_server_info()
            cursor = connection.cursor()
            SQLQuery = '''SELECT cc.TIMEID, cc.TRANSACTION_ID, cc.TRANSACTION_VALUE, cc.TRANSACTION_TYPE
                    FROM cdw_sapp_customer c, cdw_sapp_credit_card cc
                    where c.SSN = cc.CUST_SSN and c.CUST_ZIP = '{}' and
                    cc.TIMEID LIKE '{}'
                    ORDER BY cc.TIMEID DESC
                '''
            cursor.execute(SQLQuery.format(zip_value, year_value + month_value + '%'))

            # get all records
            records = cursor.fetchall()
            print("Total number of rows in table: ", cursor.rowcount)
            print("\nPrinting each row")
            for row in records:
                print("TIMEID = ", row[0], )
                print("TRANSACTION_ID = ", row[1])
                print("TRANSACTION_VALUE = ", row[2])
                print("TRANSACTION_TYPE = ", row[3], "\n")
    except Error as e:
        print("Error while connecting to Database", e)
    finally:
        if connection.is_connected():
            cursor.close()
        connection.close()

def transactions_by_type(type_value):
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='creditcard_capstone',
                                             user='root',
                                             password='a')
        if connection.is_connected():
            db_Info = connection.get_server_info()
            cursor = connection.cursor()
            SQLQuery = '''select TRANSACTION_TYPE, count(TRANSACTION_ID) as number_of_transactions, 
            sum(TRANSACTION_VALUE) as total_amount from cdw_sapp_credit_card
            where TRANSACTION_TYPE = '{}'
                '''
            cursor.execute(SQLQuery.format(type_value))

            # get all records
            records = cursor.fetchall()
            print("Printing the result: ")
            for row in records:
                print("TRANSACTION_TYPE = ", row[0], )
                print("NUMBER_OF_TRANSACTIONS = ", row[1])
                print("TOTAL_AMOUNT = ", row[2], "\n")
    except Error as e:
        print("Error while connecting to Database", e)
    finally:
        if connection.is_connected():
            cursor.close()
        connection.close()

def transactions_by_state(state_value):
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='creditcard_capstone',
                                             user='root',
                                             password='a')
        if connection.is_connected():
            cursor = connection.cursor()
            SQLQuery = '''select b.BRANCH_CODE, count(cc.TRANSACTION_ID) as NUMBER_OF_TRANSACTIONS, 
sum(cc.TRANSACTION_VALUE) as TOTAL_VALUE
from cdw_sapp_credit_card cc, cdw_sapp_branch b
where cc.BRANCH_CODE = b.BRANCH_CODE and b.BRANCH_STATE='{}'
group by b.BRANCH_CODE;
                '''
            cursor.execute(SQLQuery.format(state_value))

            records = cursor.fetchall()
            print("Printing the result for '{}': ".format(state_value))
            for row in records:
                print("BRANCH_CODE = ", row[0], )
                print("NUMBER_OF_TRANSACTIONS = ", row[1])
                print("TOTAL_VALUE = ", row[2], "\n")
    except Error as e:
        print("Error while connecting to Database", e)
    finally:
        if connection.is_connected():
            cursor.close()
        connection.close()

def view_customer_details(ssn_value):
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='creditcard_capstone',
                                             user='root',
                                             password='a')
        if connection.is_connected():
            cursor = connection.cursor()
            SQLQuery = '''select * from cdw_sapp_customer where SSN = '{}';'''
            cursor.execute(SQLQuery.format(ssn_value))

            records = cursor.fetchall()
            print("Printing the result for '{}': ".format(ssn_value))
            for row in records:
                print(row)
    except Error as e:
        print("Error while connecting to Database", e)
    finally:
        if connection.is_connected():
            cursor.close()
        connection.close()


def modify_customer_details(ssn_value, field_name, field_value):
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='creditcard_capstone',
                                             user='root',
                                             password='a')
        if connection.is_connected():
            cursor = connection.cursor()
            SQLQuery = '''update cdw_sapp_customer  set {} = '{}'
where SSN = '{}';'''
            cursor.execute(SQLQuery.format(field_name, field_value, ssn_value))
            connection.commit()
            #print(SQLQuery.format(field_name, field_value, ssn_value))
            if cursor.rowcount == 0:
                print('Customer details update failed. Returning to previous menu.')
            else:
                print("Customer details successfully updated.")
    except Error as e:
        print("Error while connecting to Database", e)
    finally:
        if connection.is_connected():
            cursor.close()
        connection.close()

def generate_monthly_bill(cc_number, year_value, month_value):
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='creditcard_capstone',
                                             user='root',
                                             password='a')
        if connection.is_connected():
            cursor = connection.cursor()
            SQLQuery = '''select CREDIT_CARD_NO, sum(TRANSACTION_VALUE) as MONTHLY_BILL
from cdw_sapp_credit_card 
where CREDIT_CARD_NO='{}'
and TIMEID like '{}{}%';'''
            cursor.execute(SQLQuery.format(cc_number, year_value, month_value))

            records = cursor.fetchall()
            print("Printing the monthly bill: ")
            for row in records:
                print(row)
    except Error as e:
        print("Error while connecting to Database", e)
    finally:
        if connection.is_connected():
            cursor.close()
        connection.close()

def transactions_between_dates(ssn_value, start_date, end_date):
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='creditcard_capstone',
                                             user='root',
                                             password='a')
        if connection.is_connected():
            cursor = connection.cursor()
            SQLQuery = '''select * from cdw_sapp_credit_card
where CUST_SSN='{}'
and TIMEID between '{}' and '{}'
order by TIMEID desc;'''
            cursor.execute(SQLQuery.format(ssn_value, start_date, end_date))

            records = cursor.fetchall()
            print("Printing the result: ")
            for row in records:
                print(row)
    except Error as e:
        print("Error while connecting to Database", e)
    finally:
        if connection.is_connected():
            cursor.close()
        connection.close()

def get_field_name():
    choice = ''
    field = None
    while choice != 'b':
        choice = input('''
Press 1 to modify city.
Press 2 to modify zip code.
Press 3 to modify state.
Press 4 to modify address.
Press 5 to modify phone number. 
Press b to go back. ''')
        if choice == '1':
            field = 'CUST_CITY'
            break
        elif choice == '2':
            field = 'CUST_ZIP'
            break
        elif choice == '3':
            field = 'CUST_STATE'
            break
        elif choice == '4':
            field = 'FULL_STREET_ADDRESS'
            break
        elif choice == 'b':
            print('Going back. ')
            break
        else:
            print('Wrong input. ')
    return field


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    connection = mysql.connector.connect(host='localhost',
                                         database='creditcard_capstone',
                                         user='root',
                                         password='a')

    dl.load_table(connection, 'cdw_sapp_credit_card')

    option = ''

    while option != 'q':

        option = input(
'''Press 1 to view transactions. 
Press 2 to view or update customer details. 
Press q to quit. ''')

        if option == '1':
            transaction_option = ''
            while transaction_option != 'b':
                transaction_option = input('''
Press 1 to view transactions by zip code for a particular month and year.
Press 2 to view transactions of a given type. 
Press 3 to view transactions in a given state. 
Press b to go back. ''')
                if transaction_option == '1':
                    zipCode = input('Enter zip code: ')
                    year = input('Enter year: ')
                    month = input('Enter month: ')
                    transactions_by_zip(zipCode, year, month)
                elif transaction_option == '2':
                    transaction_type = input('Enter the transaction type: ')
                    transactions_by_type(transaction_type)
                elif transaction_option == '3':
                    state = input('Enter state in two-letter format: ')
                    transactions_by_state(state)
                elif transaction_option == 'b':
                    print('Going back. ')
                else:
                    print('Wrong input. ')
        elif option == '2':
            customer_details_option = ''
            while customer_details_option != 'b':
                customer_details_option = input('''
Press 1 to view existing details of a customer by SSN.
Press 2 to update customer details. 
Press 3 to generate a monthly bill.
Press 4 to view transaction by period. 
Pres b to go back. ''')
                if customer_details_option == '1':
                    ssn = input('Please enter SSN to view customer details: ')
                    view_customer_details(ssn)
                elif customer_details_option == '2':
                    ssn = input('Please enter SSN to modify customer details: ')
                    field_n = get_field_name()
                    if field_n:
                        field_v = input('Enter new value for the field you selected: ')
                        modify_customer_details(ssn, field_n, field_v)
                elif customer_details_option == '3':
                    cc = input('Please enter credit card number (16 digits): ')
                    year = input('Please enter year: ')
                    month = input('Please enter month: ')
                    generate_monthly_bill(cc, year, month)
                elif customer_details_option == '4':
                    ssn = input('Please enter customer SSN: ')
                    start = input('Please enter start date as YYYYMMDD: ')
                    end = input('Please enter end date as YYYYMMDD: ')
                    transactions_between_dates(ssn, start, end)
                elif customer_details_option == 'b':
                    print('Going back. ')
                else:
                    print('Wrong input. ')
        elif option == 'q':
            print('Bye')
        else:
            print('This input is not supported. ')


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
