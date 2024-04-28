import mysql.connector

# Configuration for MySQL connection
config = {
    "user": "root",
    "password": "Erenlevi90M",
    "host": "127.0.0.1",
    "database": "movies",
    "raise_on_warnings": True
}

try:
    # Connect to MySQL database
    db = mysql.connector.connect(**config)

    cursor = db.cursor()

    # Query 1: Select all fields for the studio table
    query1 = "SELECT * FROM studio"
    cursor.execute(query1)
    studios = cursor.fetchall()
    print("-- DISPLAYING Studio RECORDS --")
    for studio in studios:
        print("Studio ID:", studio[0])
        print("Studio Name:", studio[1])

    # Query 2: Select all fields for the genre table
    query2 = "SELECT * FROM genre"
    cursor.execute(query2)
    genres = cursor.fetchall()
    print("-- DISPLAYING Genre RECORDS --")
    for genre in genres:
        print("Genre ID:", genre[0])
        print("Genre Name:", genre[1])

    # Query 3: Select movie names for movies with a runtime of less than two hours
    query3 = "SELECT film_name FROM film WHERE film_runtime < 120"
    cursor.execute(query3)
    short_films = cursor.fetchall()
    print("-- DISPLAYING Short Film RECORDS --")
    for film in short_films:
        print("Film Name:", film[0])

    # Query 4: Get a list of film names and directors grouped by director
    query4 = "SELECT film_name, film_director FROM film GROUP BY film_name, film_director"
    cursor.execute(query4)
    director_films = cursor.fetchall()
    print("-- DISPLAYING Director RECORDS in Order --")
    for film in director_films:
        print("Film Name:", film[0])
        print("Director:", film[1])

except mysql.connector.Error as err:
    # Handle MySQL errors
    print("MySQL Error:", err)

finally:
    # Close cursor and database connection
    if 'cursor' in locals():
        cursor.close()
    if 'db' in locals():
        db.close()