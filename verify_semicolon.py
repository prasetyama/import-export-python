import data_manager
import pandas as pd

filename = 'semicolon.csv'
print(f"Testing semicolon import with {filename}...")

success, result = data_manager.import_stock_data(filename)

print(f"Success: {success}")
if success:
    print(f"Success Count: {result['success_count']}")
    if result['errors']:
        print("\nErrors:")
        for err in result['errors']:
            print(f" - {err}")
else:
    print(f"Result: {result}")
