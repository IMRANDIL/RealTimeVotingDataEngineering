from database import connect_to_database




if __name__ == "__main__":
    conn = connect_to_database()
    
    if conn:
        try:
            cursor = conn.cursor()
            
        except Exception as e:
            # Print a generic error message for any exceptions
            print(f"An error occurred: {e}")