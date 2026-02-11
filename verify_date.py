import data_manager
import pandas as pd

filename = 'date_test.csv'
print(f"Testing date import with {filename}...")

success, result = data_manager.import_stock_data(filename)

print(f"Success: {success}")
if success:
    print(f"Success Count: {result['success_count']}")
    
    # Verify the inserted dates in DB
    conn = data_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT sku, date FROM stocks WHERE sku LIKE 'DATE-%'")
    rows = cursor.fetchall()
    print("\nInserted Data:")
    for row in rows:
        print(f"SKU: {row[0]}, Date: {row[1]}")
    cursor.close()
    conn.close()

    if result['errors']:
        print("\nErrors:")
        for err in result['errors']:
            print(f" - {err}")
else:
    print(f"Result: {result}")
