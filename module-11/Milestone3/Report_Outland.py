####
#Created for CSD310 by Ryan Norrbom
#Created Date: 2/24/2024
##Creates and connects to a mysql table a default user, Creates several reports 
#
#Password and username are not sanitized
####

# You will need to pip install pandas and pip install matplotlib into your python environment and pymysql
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from datetime import datetime
from datetime import datetime
from pytz import timezone
import pytz



# Connect to the MySQL database
db = mysql.connector.connect(
    host="localhost",
    user="outdoor_user",  
    password="adventure",  
    database="outland_adventures_db"  
)

# Create the SQLAlchemy engine
engine = create_engine('mysql+pymysql://outdoor_user:adventure@localhost/outland_adventures_db')

#Get current time in Pacific Timezone
def get_current_time():
    #Define the date format to use the specified format
    date_format = "%b-%d-%Y %I:%M %p"

    #Get the current date and time in Pacific Time
    date = datetime.now(timezone("US/Pacific"))

    #Print the current date and time in the specified format
    date =  date.strftime(date_format)

    return date

#Displays number of days equipment has been in use. 
def equipment_aging_report():
    cursor = db.cursor()
    query = """
    SELECT EquipmentID, Type, DATEDIFF(CURDATE(), PurchaseDate) AS DaysSincePurchase
    FROM Equipment;
    """
    cursor.execute(query)
    result = cursor.fetchall()
    df_equipment_aging = pd.read_sql(query, engine)
    date = get_current_time()
    # Plotting the Equipment Aging Report
    plt.figure(figsize=(14, 7))
    plt.barh(df_equipment_aging['Type'], df_equipment_aging['DaysSincePurchase'], color='salmon')
    plt.title(f'Equipment Aging Report - {date}')
    plt.xlabel('Days Since Purchase')
    plt.ylabel('Equipment Type')
    plt.tight_layout()  # Adjusts plot to ensure everything fits without overlapping
    print(f"\n\nDisplaying Eauipment Aging Report - {date}:\n ")
    plt.show(block=False) #Remove block=False to see the popup windows in non-interactive modes
    plt.pause(2)
    for row in result:
        print(f"Equipment ID: {row[0]}, Type: {row[1]}, Days Since Purchase: {row[2]}")
    cursor.close()

# Displays number of bookings per trip
def trip_utilization_report():
    cursor = db.cursor()
    query = """
    SELECT Trips.TripID, Destination, COUNT(Bookings.TripID) AS NumberOfBookings
    FROM Trips
    LEFT JOIN Bookings ON Trips.TripID = Bookings.TripID
    GROUP BY Trips.TripID;
    """
    cursor.execute(query)
    result = cursor.fetchall()
    df_trip_utilization = pd.read_sql(query, engine)
    date = get_current_time()
    # Plotting the Trip Utilization Report
    plt.figure(figsize=(14, 7))
    plt.bar(df_trip_utilization['Destination'], df_trip_utilization['NumberOfBookings'], color='skyblue')
    plt.title(f'Trip Utilization Report - {date}')
    plt.xlabel('Destination')
    plt.ylabel('Number of Bookings')
    plt.xticks(rotation=45)
    plt.tight_layout()  # Adjusts plot to ensure everything fits without overlapping
    print(f"\n\nDisplaying Trip Utilization Report - {date}:\n ")
    plt.show(block=False) #Remove block=False to see the popup windows in non-interactive modes
    plt.pause(2)
    for row in result:
        print(f"Trip ID: {row[0]}, Destination: {row[1]}, Number of Bookings: {row[2]}")
    cursor.close()

#Shows customers who have booked more than one trip
def repeat_customer_report():
    cursor = db.cursor()
    query = """
    SELECT CustomerID, COUNT(*) as TripsBooked
    FROM Bookings
    GROUP BY CustomerID
    HAVING COUNT(*) > 1;
    """
    cursor.execute(query)
    result = cursor.fetchall()
    date = get_current_time()
    repeat_customers_df = pd.read_sql(query, engine)
    repeat_customers_df.plot(kind='bar', x='CustomerID', y='TripsBooked', title='Repeat Customer Report')
    print(f"\n\nDisplaying Repeat Customer Report - {date}:\n ")
    plt.title(f'Repeat Customer Report - {date}')
    plt.show(block=False) #Remove block=False to see the popup windows in non-interactive modes
    plt.pause(2)
    for row in result:
        print(f"Customer ID: {row[0]}, Trips Booked: {row[1]}")
    cursor.close()

# Displays equipment sales by type
def equipment_sales_report():
    cursor = db.cursor()
    query = """
    SELECT Equipment.Type, SUM(Sales.Quantity) AS TotalSold
    FROM Sales
    JOIN Equipment ON Sales.EquipmentID = Equipment.EquipmentID
    GROUP BY Equipment.Type
    ORDER BY TotalSold DESC;
    """
    cursor.execute(query)
    result = cursor.fetchall()
    date = get_current_time()
    equipment_sales_df = pd.read_sql(query, engine)
    equipment_sales_df.plot(kind='bar', x='Type', y='TotalSold', title='Equipment Sales Report')
    print(f"\n\nDisplaying Equipment Sales Report - {date}:\n ")
    plt.title(f'Equipment Report - {date}')
    plt.show(block=False) #Remove block=False to see the popup windows in non-interactive modes
    plt.pause(2)
    for row in result:
        print(f"Equipment Type: {row[0]}, Total Sold: {row[1]}")
    cursor.close()



# Call the functions
equipment_aging_report()
trip_utilization_report()
equipment_sales_report()
repeat_customer_report()

# Make sure to close the database connection when done
db.close()
