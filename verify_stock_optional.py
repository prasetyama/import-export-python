import data_manager
import os

filename = 'messy_stock_optional.csv'
print(f"Testing import with {filename}...")

success, result = data_manager.import_stock_data(filename)

print(f"Success: {success}")
if success:
    print(f"Success Count: {result['success_count']}")
    if result['errors']:
        print("Errors:")
        for err in result['errors']:
            print(f" - {err}")
else:
    print(f"Result: {result}")
