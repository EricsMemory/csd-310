import mysql.connector
from mysql.connector import errorcode

config = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "Sq46b591218!",
    "database": "movies",
    "raise_on_warnings": True
}


try:
    db = mysql.connector.connect(**config)
    
    print("\n Database user {} connected to MySQL on host {} with database {}".format(config["user"], config["host"], config["database"]))
 
    input("\n\n Press any key to continue...")
    
    cursor = db.cursor()
    
    cursor.execute("SELECT * FROM studio")
    
    studios = cursor.fetchall()
    
    print("-- DISPLAYING Studio RECORDS --")
    for studio_id in studios:
        print("Studio ID: {}\nStudio Name: {}\n".format(studio_id[0], studio_id[1]))
    
    cursor.execute("SELECT * FROM genre")
    
    genres = cursor.fetchall()
    
    print("-- DISPLAYING Genre RECORDS --")
    for genre_id in studios:
        print("Genre ID: {}\nGenre Name: {}\n".format(genre_id[0], genre_id[1]))
    
    cursor.execute("SELECT film_name, film_runtime FROM film WHERE film_runtime < 120")
    
    films = cursor.fetchall()
    
    print("-- DISPLAYING Short Film RECORDS --")
    for film_name in films:
        print("Film Name: {}\nFilm Runtime: {}\n".format(film_name[0], film_name[1]))
        
    cursor.execute("SELECT film_name, film_director FROM film ORDER BY film_director, film_name")
    
    directors = cursor.fetchall()
    
    print("-- DISPLAYING Director RECORDS in Order --")
    for director in directors:
        print("Film Name: {}\nDirector: {}\n".format(director[0], director[1]))
    
    
    
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print(" The supplied username or password are invalid")
    
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print(" The specified database does not exist")
        
    else:
        print(err)
        
finally:
 db.close()
 
 


