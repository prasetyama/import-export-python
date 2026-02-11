import data_manager
import pandas as pd

filename = 'validation_test.csv'
print(f"Testing validation with {filename}...")

success, result = data_manager.import_stock_data(filename)

print(f"Success: {success}")
if success:
    print(f"Success Count: {result['success_count']}")
    
    if result['errors']:
        print("\nErrors (Expected):")
        for err in result['errors']:
            print(f" - {err}")
else:
    print(f"Result: {result}")
