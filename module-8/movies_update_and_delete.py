import mysql.connector
from mysql.connector import errorcode

# Configuration for MySQL connection
config = {
    "user": "root",
    "password": "Erenlevi90M",
    "host": "localhost",
    "database": "movies",
    "raise_on_warnings": True
}

def display_films(cursor, title):
    # Execute an inner join query to retrieve film information
    cursor.execute("SELECT film_name, film_director, genre_name, studio_name "
                   "FROM film "
                   "INNER JOIN genre ON film.genre_id = genre.genre_id "
                   "INNER JOIN studio ON film.studio_id = studio.studio_id")

    # Fetch the results from the cursor object
    films = cursor.fetchall()

    print("\n -- {} --".format(title))

    # Iterate over the film dataset and display the results
    for film in films:
        print("Film Name: {}\nDirector: {}\nGenre: {}\nStudio: {}\n".format(film[0], film[1], film[2], film[3]))

def main():
    try:
        # Connect to MySQL database
        db = mysql.connector.connect(**config)
        print("\nDatabase user {} connected to MySQL on host {} with database {}".format(config["user"],
                                                                                          config["host"],
                                                                                          config["database"]))
        cursor = db.cursor()

        # Display existing films
        display_films(cursor, "DISPLAYING FILMS")

        # Insert a new film
        cursor.execute("INSERT INTO film (film_name, film_releaseDate, film_runtime, film_director, studio_id, genre_id) "
                       "VALUES ('Nope', 2022, 130, 'Jordan Peele', 3, 1)")
        db.commit()  # Commit the transaction

        # Display films after insertion
        display_films(cursor, "DISPLAYING FILMS AFTER INSERT")

        # Update the genre of the film "Alien"
        cursor.execute("UPDATE film SET genre_id = 1 WHERE film_name = 'Alien'")
        db.commit()  # Commit the transaction

        # Display films after update
        display_films(cursor, "DISPLAYING FILMS AFTER UPDATE - Changed Alien to Horror")

        # Delete the film "Gladiator"
        cursor.execute("DELETE FROM film WHERE film_name = 'Gladiator'")
        db.commit()  # Commit the transaction

        # Display films after deletion
        display_films(cursor, "DISPLAYING FILMS AFTER DELETE")

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print("MySQL Error:", err)

    finally:
        # Close cursor and database connection
        if 'cursor' in locals():
            cursor.close()
        if 'db' in locals():
            db.close()
            print("\nDatabase user {} disconnected from MySQL".format(config["user"]))

# Call the main function
if __name__ == "__main__":
    main()