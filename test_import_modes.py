
import requests
import io

def test_imports():
    url = "http://localhost:5000/api/import"
    
    # Create a dummy CSV file
    csv_content = """sku,warehouse_code,stock_pcs,date
    ITEM001,WH001,100,2023-10-27
    ITEM002,WH002,200,2023-10-28"""
    
    # Create file object separately to be able to seek it
    file_obj = io.BytesIO(csv_content.encode('utf-8'))
    
    # Test 1: Quick Mode
    print("\n--- Testing Quick Mode ---")
    file_obj.seek(0)
    files_quick = {'files': ('test_stock.csv', file_obj, 'text/csv')}
    data_quick = {'mode': 'quick', 'table_name': 'stocks'}
    
    try:
        response_quick = requests.post(url, files=files_quick, data=data_quick)
        print(f"Status Code: {response_quick.status_code}")
        print("Response JSON:", response_quick.json())
    except Exception as e:
        print(f"Quick Mode Test Failed: {e}")

    # Test 2: Full Mode
    print("\n--- Testing Full Mode ---")
    file_obj.seek(0)
    files_full = {'files': ('test_stock.csv', file_obj, 'text/csv')}
    data_full = {'mode': 'full', 'table_name': 'stocks'}
    
    try:
        response_full = requests.post(url, files=files_full, data=data_full)
        print(f"Status Code: {response_full.status_code}")
        print("Response JSON:", response_full.json())
    except Exception as e:
        print(f"Full Mode Test Failed: {e}")

if __name__ == "__main__":
    test_imports()
