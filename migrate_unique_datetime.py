
import data_manager
import mysql.connector
from mysql.connector import Error

def migrate():
    connection = data_manager.get_connection()
    if not connection:
        print("Failed to connect to DB.")
        return

    try:
        cursor = connection.cursor()
        
        # 1. Add is_unique column
        try:
            cursor.execute("ALTER TABLE column_definitions ADD COLUMN is_unique BOOLEAN DEFAULT FALSE")
            print("Added 'is_unique' column.")
        except Error as e:
            if e.errno == 1060: # Key column 'is_unique' doesn't exist in table
                 print("'is_unique' column already exists.")
            else:
                 print(f"Error adding 'is_unique': {e}")

        # 2. Modify data_type enum
        try:
            # Note: Modifying ENUM can be tricky. We simply redefine it with the new value included.
            cursor.execute("ALTER TABLE column_definitions MODIFY COLUMN data_type ENUM('str', 'int', 'date', 'datetime') DEFAULT 'str'")
            print("Modified 'data_type' ENUM to include 'datetime'.")
        except Error as e:
            print(f"Error modifying 'data_type': {e}")

        connection.commit()
        print("Migration completed.")

    except Error as e:
        print(f"Migration failed: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    migrate()
