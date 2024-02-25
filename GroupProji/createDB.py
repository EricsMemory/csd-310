####
#Created for CSD310 by Ryan Norrbom
#Created Date: 2/22/2024
##Creates and connects to a mysql table a default user, Creates/Updates/Deletes DB content 
#
#Password and username are not sanitized
####

#start

import mysql.connector
from mysql.connector import Error
import displayOutlandAdv 
import os

def create_database(mysql_user,mysql_password,osPath):
    try:
        connection = mysql.connector.connect(
            host='localhost',  # usually 'localhost'
            user=mysql_user,
            password=mysql_password
        )

        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("DROP DATABASE IF EXISTS outland_adventures_db")
            cursor.execute("CREATE DATABASE IF NOT EXISTS outland_adventures_db")
            print("Database 'outland_adventures_db' created successfully")
            run_db_init_script(mysql_user,mysql_password,osPath)
            displayOutlandAdv.main()
    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

def run_db_init_script(mysql_user,mysql_password,osPath):
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user=mysql_user,
            password=mysql_password,
            database='outland_adventures_db'
        )
        cursor = connection.cursor()
        script_file = os.path.join(osPath,'db_init_2024.sql')

        print("Running db_init_2024.sql script...")
        with open(script_file, 'r') as file:
            sql_script = file.read()
        for statement in sql_script.split(';'):
            if statement.strip():
                cursor.execute(statement)
            connection.commit()
        #Add Customer Data
        

        print("Database initialization script executed successfully")
    except Error as e:
        print(f"Error while executing the script: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed after executing the script")

osPath = os.getcwd()
mysqlUser = input("Enter yourMySQL username (root or something else): ")
mysqlPassword = input("Please enter the MysqlPassword: ")

create_database(mysqlUser,mysqlPassword,osPath)

#end