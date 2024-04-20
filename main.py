from database import connect_to_database, create_tables

if __name__ == "__main__":
    # Connect to the database
    conn = connect_to_database()

    if conn:
        try:
            # Print a success message if connection is successful
            print("Successfully connected to the PostgreSQL database!")
            
            # Create a cursor object
            curr = conn.cursor()
            
            # Call the function to create tables
            create_tables(conn, curr)
            
            # Commit the transaction
            conn.commit()
            
        except Exception as e:
            # Print a generic error message for any exceptions
            print(f"An error occurred: {e}")
        finally:
            # Close the database connection
            conn.close()
