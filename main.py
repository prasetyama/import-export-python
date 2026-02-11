import data_manager
import config
import os
import sys

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def print_menu():
    print("\n=== Python Import/Export App (MySQL) ===")
    print("1. View Data")
    print("2. Export Data")
    print("3. Import Data")
    print("4. Exit")
    print("========================================")

def view_data():
    conn = data_manager.get_connection()
    if conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {config.TABLE_NAME}")
        rows = cursor.fetchall()
        if not rows:
            print(f"No data found in '{config.TABLE_NAME}'.")
        else:
            print(f"\nData in '{config.TABLE_NAME}':")
            print(f"{'ID':<5} {'Name':<30} {'Price':<15} {'Quantity':<10}")
            print("-" * 65)
            for row in rows:
                print(f"{row[0]:<5} {row[1]:<30} {row[2]:<15} {row[3]:<10}")
        cursor.close()
        conn.close()
    else:
        print("Failed to connect to database. Check config.py.")

def export_menu():
    filename = input("Enter filename to export (e.g., export.csv or export.xlsx): ")
    if filename.endswith('.csv'):
        data_manager.export_data(filename, 'csv')
    elif filename.endswith('.xlsx'):
        data_manager.export_data(filename, 'excel')
    else:
        print("Invalid format. Please use .csv or .xlsx extension.")

def import_menu():
    filename = input("Enter filename to import (e.g., sample_import.csv): ")
    data_manager.import_data(filename)

def main():
    while True:
        print_menu()
        choice = input("Enter choice: ")
        
        if choice == '1':
            view_data()
        elif choice == '2':
            export_menu()
        elif choice == '3':
            import_menu()
        elif choice == '4':
            print("Exiting...")
            sys.exit()
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
