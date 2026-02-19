
import requests
import json
import uuid

BASE_URL = "http://localhost:5000/api"

def test_create_table_unique_datetime():
    print("\n--- Testing Create Table with Unique and Datetime ---")
    table_name = f"events_{str(uuid.uuid4())[:8]}"
    payload = {
        "table_name": table_name,
        "display_name": f"Events {table_name}",
        "allowed_filename": "events",
        "columns": [
            {"name": "event_id", "type": "str", "is_unique": True},
            {"name": "event_date", "type": "datetime"}
        ]
    }
    
    try:
        response = requests.post(f"{BASE_URL}/tables", json=payload)
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}")
        
        if response.status_code == 201:
            return table_name
        else:
            return None
    except Exception as e:
        print(f"Failed: {e}")
        return None

def test_add_column_unique_datetime(table_name):
    print(f"\n--- Testing Add Column to {table_name} ---")
    if not table_name:
        print("Skipping: No table created.")
        return

    # Add Unique Int Column
    try:
        payload = {"column_name": "seq_num", "data_type": "int", "is_unique": True}
        response = requests.post(f"{BASE_URL}/tables/{table_name}/columns", json=payload)
        print(f"Add Unique Int Status: {response.status_code}")
        print(f"Response: {response.json()}")
    except Exception as e:
        print(f"Failed Add Unique Int: {e}")

    # Add Datetime Column
    try:
        payload = {"column_name": "updated_at", "data_type": "datetime"}
        response = requests.post(f"{BASE_URL}/tables/{table_name}/columns", json=payload)
        print(f"Add Datetime Status: {response.status_code}")
        print(f"Response: {response.json()}")
    except Exception as e:
        print(f"Failed Add Datetime: {e}")

if __name__ == "__main__":
    table = test_create_table_unique_datetime()
    test_add_column_unique_datetime(table)
