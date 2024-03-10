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
from datetime import datetime, timedelta, date
from pytz import timezone
import random



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

def equipment_aging_report():
    cursor = db.cursor()
    
    # Updated SQL query to calculate the difference in months
    query = """
    SELECT EquipmentID, Type, TIMESTAMPDIFF(YEAR, PurchaseDate, CURDATE()) AS YearsSincePurchase
    FROM Equipment;
    """
    
    cursor.execute(query)
    result = cursor.fetchall()
    
    # Fetching the data into a DataFrame
    df_equipment_aging = pd.read_sql(query, engine)
    
    date = get_current_time()
    
    # Plotting the Equipment Aging Report
    plt.figure(figsize=(14, 7))
    plt.barh(df_equipment_aging['Type'], df_equipment_aging['YearsSincePurchase'], color='salmon')
    plt.title(f'Equipment Aging Report - {date}')
    plt.xlabel('Years Since Purchase')
    plt.ylabel('Equipment Type')
    plt.tight_layout()
    
    print(f"\n\nDisplaying Equipment Aging Report - {date}:\n")
    plt.show(block=False)
    plt.pause(2)
    
    # Adjusted print statement to reflect months
    for row in result:
        print(f"Equipment ID: {row[0]}, Type: {row[1]}, Years Since Purchase: {row[2]}")
    
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

# Eqiupment Sales by Quarter report creation
def equipment_sales_report_by_quarter():
    cursor = db.cursor()
    query = """
    SELECT Equipment.Type, YEAR(Sales.SaleDate) AS Year, QUARTER(Sales.SaleDate) AS Quarter, SUM(Sales.Quantity) AS TotalSold
    FROM Sales
    JOIN Equipment ON Sales.EquipmentID = Equipment.EquipmentID
    GROUP BY Equipment.Type, Year, Quarter
    ORDER BY Year DESC, Quarter DESC, TotalSold DESC;
    """
    cursor.execute(query)
    result = cursor.fetchall()
    date = get_current_time()
    equipment_sales_df = pd.read_sql(query, engine)

        # Pivot the data to prepare for plotting
    equipment_sales_pivot = equipment_sales_df.pivot_table(index=['Year', 'Quarter'], columns='Type', values='TotalSold', fill_value=0)

    # Plotting the data
    equipment_sales_pivot.plot(kind='bar', title='Equipment Sales Report by Quarter', hatch='/', figsize=(10, 6))
    plt.ylabel('Total Sold')
    plt.xlabel('Year, Quarter')
    plt.title(f'Equipment Sales Report by Quarter')
    plt.tight_layout()
    plt.show()
    
    # For simplicity, let's print the data in a tabular format.
    print(f"\n\nDisplaying Equipment Sales Report by Quarter - {date}:\n")
    for row in result:
        print(f"Equipment Type: {row[0]}, Year: {row[1]}, Quarter: {row[2]}, Total Sold: {row[3]}")

    cursor.close()

# New functions to add additional records to the DB
def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def insert_sales_records():
    cursor = db.cursor()

    start_date = date(2022, 1, 1)
    end_date = date(2024, 3, 4)
    dates = random.sample(list(daterange(start_date, end_date)), 10)

    equipment_ids = [1, 2, 3, 4, 5, 6, 2, 3, 4,2, 3, 4]  # Example IDs, adjust as needed
    customer_ids = [1, 1, 2, 2, 3, 4, 5, 6, 2, 2, 3, 4]  # Example IDs, adjust as needed
    quantities = [1, 2, 3, 4,1,1,1,1,2,3]    # Example quantities, adjust as needed

    for i in range(10):
        query = f"INSERT INTO Sales (EquipmentID, CustomerID, SaleDate, Quantity) VALUES ({equipment_ids[i]}, {customer_ids[i]}, '{dates[i]}', {quantities[i]});"
        cursor.execute(query)

    db.commit()  # Make sure to commit the changes to the database
    cursor.close()
    print("Inserted 10 new sales records.")

# Call the function to insert 10 new records records
#insert_sales_records() # Remove tag to add records

# Call the functions
equipment_aging_report()
trip_utilization_report()
equipment_sales_report_by_quarter()
repeat_customer_report()

# Make sure to close the database connection when done
db.close()
