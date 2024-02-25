####
#Created for CSD310 by Ryan Norrbom
#Created Date: 2/22/2024
##Displays the Table Content
#
#Password and username are not sanitized
####

#Start

#Import the connector
import mysql.connector

def connect_to_database():
    return mysql.connector.connect(
        host="localhost",  # Replace with your host, usually 'localhost'
        user="outdoor_user",  # Replace with your database username
        password="adventure",  # Replace with your database password
        database="outland_adventures_db"  # Replace with your database name
    )

def fetch_data_from_table(table_name):
    db_connection = connect_to_database()
    cursor = db_connection.cursor()
    query = f"SELECT * FROM {table_name};"
    cursor.execute(query)
    records = cursor.fetchall()
    print(f"\nData from {table_name}:")
    for record in records:
        print(record)
    cursor.close()
    db_connection.close()

def main():
    tables = ['Customers', 'Trips', 'Equipment', 'Bookings', 'Sales', 'Inventory']
    for table in tables:
        fetch_data_from_table(table)

if __name__ == "__main__":
    main()


#End