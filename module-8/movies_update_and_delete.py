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
    
    def show_films(cursor, title):
		
        cursor.execute("SELECT film_name AS Name, film_director AS Director, genre_name AS Genre, studio_name AS 'Studio Name' FROM film INNER JOIN genre ON film.genre_id = genre.genre_id INNER JOIN studio ON film.studio_id = studio.studio_id")
		
        films = cursor.fetchall()
		
        print("\n -- {} --".format(title))
	
        for film in films:
            print("Film Name: {}\nDirector: {}\nGenre Name ID: {}\nStudio Name: {}\n".format(film[0], film[1], film[2], film[3]))
		
    
    show_films(cursor, "DISPLAYING FILMS AFTER DELETE")
    
    
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print(" The supplied username or password are invalid")
    
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print(" The specified database does not exist")
        
    else:
        print(err)
        
finally:
 db.close()
 
 


