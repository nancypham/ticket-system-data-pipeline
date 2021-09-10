from mysql.connector import connect
import pandas as pd
from secrets import user, password

def get_db_connection():
    connection = None
    try:
        connection = connect(user=user, # move to secrets
            password=password, # move to secrets
            host='localhost',
            port='3306',
            database='ticket_system')
    except Exception as error:
        print("Error while connecting to database for job tracker", error)
    
    return connection

def load_third_party(connection, file_path_csv):
    cursor = connection.cursor()
    
    dataFile = pd.read_csv(file_path_csv, header=None, delimiter=',')

    for i, row in dataFile.iterrows():
        sql = "INSERT INTO sales (ticket_id, trans_date, event_id, event_name, event_date, event_type, event_city, customer_id, price, num_tickets) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(sql, tuple(row)) #question
    
    connection.commit()
    cursor.close()
    return 

def query_popular_tickets(connection):
    # Get the most popular ticket in the past month
    sql_stmt = "SELECT event_name FROM (SELECT event_name, SUM(num_tickets) AS num_tickets FROM sales GROUP BY event_name ORDER BY num_tickets DESC) AS tot_tickets LIMIT 3"
    cursor = connection.cursor()
    cursor.execute(sql_stmt)
    records = cursor.fetchall()
    cursor.close()
    return records

def display_readable_results(records):
    print('Here are the most popular tickets in the past month:')
    for item in records:
        for i in range(0,len(item)):
            print('- ', item[i])

if __name__ == '__main__':
    from secrets import csv_path
   
    conn = get_db_connection()

    csv_file = csv_path
    load_third_party(conn, csv_file)
    
    records = query_popular_tickets(conn)
    display_readable_results(records)

'''
Command line execution log:

source /Users/<USER>/opt/anaconda3/bin/activate
(base) ➜  source /Users/<USER>/opt/anaconda3/bin/activate
conda activate base
(base) ➜  conda activate base
(base) ➜  /Users/<USER>/opt/anaconda3/bin/python <PATH TO PYTHON FILE>
Here are the most popular tickets in the past month:
-  Washington Spirits vs Sky Blue FC
-  Christmas Spectacular
-  The North American International Auto Show
'''