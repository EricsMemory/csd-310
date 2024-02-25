/*
    Title: db_init_2024.sql
    Author: Ryan Norrbom
    Created Date: February 22nd 2024
    Created For: Group Project - CSD 310
    Description: Outdoor Adventures database initialization script.
*/

-- drop database user if exists 
DROP USER IF EXISTS 'outdoor_user'@'localhost';


-- create outdoor_user and grant them all privileges to the movies database 
CREATE USER 'outdoor_user'@'localhost' IDENTIFIED WITH mysql_native_password BY 'adventure';

-- grant all privileges to the movies database to user movies_user on localhost 
GRANT ALL PRIVILEGES ON outland_adventures_db.* TO 'outdoor_user'@'localhost';


-- drop tables if they are present
DROP TABLE IF EXISTS Bookings;
DROP TABLE IF EXISTS Sales;
DROP TABLE IF EXISTS Inventory;
DROP TABLE IF EXISTS Customers;
DROP TABLE IF EXISTS Trips;
DROP TABLE IF EXISTS Equipment;


-- create the customers table 
CREATE TABLE Customers (
    CustomerID INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(255),
    Email VARCHAR(255),
    PhoneNumber VARCHAR(20)
);



-- create the trips table 
CREATE TABLE Trips (
    TripID INT AUTO_INCREMENT PRIMARY KEY,
    Destination VARCHAR(255),
    DifficultyLevel VARCHAR(50),
    DurationDays INT
);

-- create the equipment table 
CREATE TABLE Equipment (
    EquipmentID INT AUTO_INCREMENT PRIMARY KEY,
    Type VARCHAR(50),
    PurchaseDate DATE,
    Price DECIMAL(10,2)
);

-- create the bookings table 
CREATE TABLE Bookings (
    BookingID INT AUTO_INCREMENT PRIMARY KEY,
    CustomerID INT,
    TripID INT,
    BookingDate DATE,
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID),
    FOREIGN KEY (TripID) REFERENCES Trips(TripID)
);

-- create the sales table 
CREATE TABLE Sales (
    SaleID INT AUTO_INCREMENT PRIMARY KEY,
    EquipmentID INT,
    CustomerID INT,
    SaleDate DATE,
    Quantity INT,
    FOREIGN KEY (EquipmentID) REFERENCES Equipment(EquipmentID),
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);


-- create the inventory table 
CREATE TABLE Inventory (
    EquipmentID INT,
    Quantity INT,
    FOREIGN KEY (EquipmentID) REFERENCES Equipment(EquipmentID)
);

-- Populate Customers
INSERT INTO Customers (Name, Email, PhoneNumber) VALUES
    ('John Doe', 'john.doe@example.com', '555-1234'),
    ('Jane Smith', 'jane.smith@example.com', '555-5678'),
    ('Emily Johnson', 'emily.johnson@example.com', '555-9101'),
    ('Michael Scott', 'michael.scott@example.com', '555-3421'),
    ('Linda White', 'linda.white@example.com', '555-8765'),
    ('David Wilson', 'david.wilson@example.com', '555-4321')
;

-- Populate Trips
INSERT INTO Trips (Destination, DifficultyLevel, DurationDays) VALUES
    ('Grand Canyon', 'Moderate', 5),
    ('Yellowstone', 'Easy', 3),
    ('Yosemite', 'Hard', 7),
    ('Zion', 'Moderate', 4),
    ('Arches', 'Easy', 2),
    ('Glacier National Park', 'Hard', 6)
;

-- Populate Equipment
INSERT INTO Equipment (Type, PurchaseDate, Price) VALUES
    ('Tent', '2022-01-15', 199.99),
    ('Backpack', '2022-02-20', 89.99),
    ('Sleeping Bag', '2022-03-10', 59.99),
    ('Hiking Boots', '2022-04-05', 129.99),
    ('Climbing Gear', '2022-05-25', 250.00),
    ('Water Filter', '2022-06-15', 29.99);

-- Populate Bookings with repeat customers
INSERT INTO Bookings (CustomerID, TripID, BookingDate) VALUES
    (1, 1, '2023-04-01'),
    (1, 2, '2023-04-08'),
    (2, 3, '2023-04-15'),
    (2, 4, '2023-04-22'),
    (3, 5, '2023-04-29'),
    (4, 6, '2023-05-06'),
    (5, 1, '2023-05-13'),
    (6, 2, '2023-05-20');

-- Populate Sales with repeat customers
INSERT INTO Sales (EquipmentID, CustomerID, SaleDate, Quantity) VALUES
    (1, 1, '2023-04-02', 1),
    (2, 1, '2023-04-09', 1),
    (3, 2, '2023-04-16', 1),
    (4, 2, '2023-04-23', 1),
    (5, 3, '2023-04-30', 1),
    (6, 4, '2023-05-07', 1),
    (1, 5, '2023-05-14', 2),
    (2, 6, '2023-05-21', 2);

-- Populate Inventory
INSERT INTO Inventory (EquipmentID, Quantity) VALUES
    (1, 10),
    (2, 15),
    (3, 20),
    (4, 5),
    (5, 8),
    (6, 12);
 

