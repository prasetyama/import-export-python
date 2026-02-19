
import data_manager

def check_import_date():
    connection = data_manager.get_connection()
    if not connection:
        print("Failed to connect.")
        return

    try:
        cursor = connection.cursor(dictionary=True)
        # Check 'stocks' table (or whatever table was used in test_import_modes)
        # In test_import_modes.py, table_name is 'stocks'.
        # But migrate script said stocks doesn't exist? 
        # Ah, 'stocks' might be the display name or the table name passed in API, 
        # but create_new_import_table might not have been called for it if it's dynamic?
        # Let's check 'inventory' or 'events_...' from previous tests.
        
        # Actually, test_import_modes.py uses 'stocks'. If 'stocks' table doesn't exist in DB, 
        # the import would fail unless it's auto-created? 
        # The app logic: api_import_file -> loop files -> validates headers against config -> process_import_async -> import_file_process
        # It relies on 'stocks' table existing in 'import_tables' and physical DB.
        
        # Let's check what tables have data and ImportDate.
        cursor.execute("SHOW TABLES")
        tables = [row['Tables_in_inventory_db'] for row in cursor.fetchall()] # Adjust DB name key if needed
        print(f"Tables: {tables}")
        
        for table in tables:
            if table in ['column_definitions', 'column_aliases', 'import_jobs', 'import_job_details', 'import_tables']:
                continue
                
            print(f"\nChecking {table}...")
            try:
                cursor.execute(f"SELECT * FROM {table} LIMIT 5")
                rows = cursor.fetchall()
                if rows:
                    if 'ImportDate' in rows[0]:
                        print(f"  ImportDate exists. Sample: {rows[0]['ImportDate']}")
                    else:
                        print("  ImportDate MISSING!")
                else:
                    print("  Table empty.")
            except Exception as e:
                print(f"  Error checking table: {e}")

    except Exception as e:
        print(f"Check failed: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    check_import_date()
