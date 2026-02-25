
import data_manager
import mysql.connector
from mysql.connector import Error

def migrate_import_date():
    connection = data_manager.get_connection()
    if not connection:
        print("Failed to connect to DB.")
        return

    try:
        cursor = connection.cursor()
        
        # 1. Get all tables from import_tables
        cursor.execute("SELECT table_name FROM import_tables")
        tables = [row[0] for row in cursor.fetchall()]
        
        # Add stocks if not in list (legacy)
        if 'stocks' not in tables:
            tables.append('stocks')
            
        print(f"Found tables to migrate: {tables}")

        for table in tables:
            try:
                print(f"Migrating {table}...")
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN ImportDate DATETIME DEFAULT CURRENT_TIMESTAMP")
                print(f"Added ImportDate to {table}.")
            except Error as e:
                if e.errno == 1060: # Key column 'ImportDate' doesn't exist in table (Duplicate column)
                     print(f"ImportDate already exists in {table}.")
                elif e.errno == 1146: # Table doesn't exist
                     print(f"Table {table} does not exist, skipping.")
                else:
                     print(f"Error migrating {table}: {e}")

        connection.commit()
        print("Migration completed.")

    except Error as e:
        print(f"Migration failed: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    migrate_import_date()
