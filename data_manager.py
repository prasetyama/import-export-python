import pandas as pd
import mysql.connector
from mysql.connector import Error
import config
import os

def get_connection():
    """Establishes a connection to the database."""
    try:
        connection = mysql.connector.connect(
            host=config.DB_HOST,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            database=config.DB_NAME
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error connecting to database: {e}")
        return None

def export_data(filename, format_type='csv'):
    """Exports data from the database to a CSV or Excel file."""
    connection = get_connection()
    if not connection:
        return False

    try:
        query = f"SELECT * FROM {config.TABLE_NAME}"
        df = pd.read_sql(query, connection)
        
        if format_type == 'csv':
            df.to_csv(filename, index=False)
        elif format_type == 'excel':
            df.to_excel(filename, index=False)
        
        print(f"Data successfully exported to {filename}")
        return True
    except Exception as e:
        print(f"Error during export: {e}")
        return False
    finally:
        if connection.is_connected():
            connection.close()

def import_data(filename):
    """Imports data from a CSV or Excel file into the database."""
    if not os.path.exists(filename):
        print(f"File not found: {filename}")
        return False

    connection = get_connection()
    if not connection:
        return False

    try:
        if filename.endswith('.csv'):
            df = pd.read_csv(filename)
            # Reload dataframe without header
            df_no_header = pd.read_csv(filename, header=None, sep=None, engine='python')
        elif filename.endswith('.xlsx') or filename.endswith('.xls'):
            df = pd.read_excel(filename)
            df_no_header = pd.read_excel(filename, header=None)
        else:
            print("Unsupported file format.")
            return False

        cursor = connection.cursor()
        
        # Determine columns from dataframe
        # Assuming table columns: name, price, quantity (id is auto-increment)
        # We start by checking if expected columns exist in the file
        required_columns = ['name', 'price', 'quantity']
        if not all(col in df.columns for col in required_columns):
            print(f"File must contain columns: {required_columns}")
            return False

        # Construct INSERT query
        insert_query = f"INSERT INTO {config.TABLE_NAME} (name, price, quantity) VALUES (%s, %s, %s)"
        
        # Prepare data
        data_to_insert = []
        for index, row in df.iterrows():
            data_to_insert.append((row['name'], row['price'], row['quantity']))

        cursor.executemany(insert_query, data_to_insert)
        connection.commit()
        
        print(f"Successfully imported {cursor.rowcount} records.")
        return True

    except Exception as e:
        print(f"Error during import: {e}")
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def get_column_configs():
    """Fetches column definitions and aliases from the database."""
    connection = get_connection()
    if not connection:
        return {}
    
    try:
        cursor = connection.cursor(dictionary=True)
        
        # Get Definitions
        cursor.execute("SELECT * FROM column_definitions")
        definitions = cursor.fetchall()
        
        # Get Aliases
        cursor.execute("""
            SELECT ca.id, ca.alias_name, cd.column_name 
            FROM column_aliases ca
            JOIN column_definitions cd ON ca.column_id = cd.id
        """)
        aliases = cursor.fetchall()
        
        config = {}
        for row in definitions:
            config[row['column_name']] = {
                'id': row['id'],
                'is_mandatory': bool(row['is_mandatory']),
                'data_type': row['data_type'],
                'aliases': [row['column_name']], # Simple list for backend logic
                'aliases_list': [] # Object list for UI (id, name)
            }
            
        for row in aliases:
            if row['column_name'] in config:
                config[row['column_name']]['aliases'].append(row['alias_name'])
                config[row['column_name']]['aliases_list'].append({'id': row['id'], 'name': row['alias_name']})
                        
        return config
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def update_column_config(column_id, is_mandatory, data_type):
    """Updates configuration for a column."""
    connection = get_connection()
    if not connection: return False
    try:
        cursor = connection.cursor()
        cursor.execute(
            "UPDATE column_definitions SET is_mandatory=%s, data_type=%s WHERE id=%s",
            (is_mandatory, data_type, column_id)
        )
        connection.commit()
        return True
    except Error as e:
        print(f"Error updating config: {e}")
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def add_alias(column_id, alias_name):
    """Adds a new alias."""
    connection = get_connection()
    if not connection: return False
    try:
        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO column_aliases (column_id, alias_name) VALUES (%s, %s)",
            (column_id, alias_name.lower())
        )
        connection.commit()
        return True
    except Error as e:
        print(f"Error adding alias: {e}")
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def delete_alias(alias_id):
    """Deletes an alias."""
    connection = get_connection()
    if not connection: return False
    try:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM column_aliases WHERE id=%s", (alias_id,))
        connection.commit()
        return True
    except Error as e:
        print(f"Error deleting alias: {e}")
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def import_stock_data(filename):
    """Imports stock data using dynamic configuration from database."""
    if not os.path.exists(filename):
        return False, ["File not found."]

    # Load Config from DB
    configs = get_column_configs()
    if not configs:
        return False, ["Failed to load column configurations from database."]

    connection = get_connection()
    if not connection:
        return False, ["Database connection failed."]

    cursor = None
    try:
        # 1. Read File
        if filename.endswith('.csv') or filename.endswith('.txt'):
             # Use sep=None and engine='python' to auto-detect ',' or ';'
            df = pd.read_csv(filename, sep=None, engine='python')
        elif filename.endswith('.xlsx') or filename.endswith('.xls'):
            df = pd.read_excel(filename)
        else:
            return False, ["Unsupported file format."]

        # 2. Normalize Headers
        df.columns = [str(col).strip().lower() for col in df.columns]

        # 3. Map Columns using Dynamic Config
        final_columns = {}
        missing_columns = []
        
        # Build mapping dict from config
        # Structure: key=column_name, value=[aliases...]
        column_mapping = { name: conf['aliases'] for name, conf in configs.items() }

        for key, possible_names in column_mapping.items():
            found = False
            for name in possible_names:
                if name in df.columns:
                    final_columns[key] = name
                    found = True
                    break
            if not found:
                missing_columns.append(key)
        
        # Check Mandatory Columns
        missing_required = []
        for col_key in missing_columns:
            if configs[col_key]['is_mandatory']:
                missing_required.append(col_key)

        if missing_required:
            return False, [f"Missing mandatory columns: {', '.join(missing_required)}"]

        # 4. Validation & Insertion
        cursor = connection.cursor()
        success_count = 0
        errors = []

        insert_query = """
            INSERT INTO stocks (sku, warehouse_code, stock_pcs, stock_box, stock_cs, date, dist_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            warehouse_code=VALUES(warehouse_code),
            stock_pcs=VALUES(stock_pcs),
            stock_box=VALUES(stock_box),
            stock_cs=VALUES(stock_cs),
            date=VALUES(date),
            dist_id=VALUES(dist_id)
        """

        data_to_insert = []
        
        for index, row in df.iterrows():
            row_errors = []
            
            # Helper to safely get value (whether by name or index)
            def get_val(key):
                col_ref = final_columns.get(key)
                if col_ref is not None:
                    # if col_ref is integer (index) or string (name)
                    if isinstance(col_ref, int):
                         return row.iloc[col_ref]
                    return row[col_ref]
                return None

            sku = get_val('sku')
            warehouse = get_val('warehouse_code')
            stock_pcs = get_val('stock_pcs')
            stock_box = get_val('stock_box')
            stock_cs = get_val('stock_cs')
            date_val = get_val('date')
            dist_id = get_val('dist_id')

            # Dynamic Row-Level Validation
            for key in configs:
                conf = configs[key]
                val = locals().get(key.split('_')[-1] if '_' in key else key) # Quick hack mapping? No, need direct mapping.
                # Actually, I have the variables above. Let's make a validation loop.
                # To map 'sku' (key) to sku (variable), we can use a dict.
                val_map = {
                    'sku': sku,
                    'warehouse_code': warehouse,
                    'stock_pcs': stock_pcs,
                    'stock_box': stock_box,
                    'stock_cs': stock_cs,
                    'date': date_val,
                    'dist_id': dist_id
                }
                
                val = val_map.get(key)
                
                # Check Mandatory
                if conf['is_mandatory']:
                    if pd.isna(val) or str(val).strip() == '':
                         row_errors.append(f"Row {index+1}: {key} is missing.")
                         continue

                # Check Data Type
                if pd.notna(val) and str(val).strip() != '':
                    if conf['data_type'] == 'int':
                        try:
                            int(val)
                        except ValueError:
                            row_errors.append(f"Row {index+1}: {key} must be an integer.")
                    elif conf['data_type'] == 'date':
                        try:
                             # Just validation, parsing happens later for all dates
                             pd.to_datetime(val, dayfirst=False, errors='raise')
                        except ValueError:
                             try:
                                 pd.to_datetime(val, dayfirst=True, errors='raise')
                             except:
                                 row_errors.append(f"Row {index+1}: {key} invalid date format.")

            if row_errors:
                errors.extend(row_errors)
                continue

            # Final Data Clean-up for Insertion
            # Values need to be cleaned (e.g., date formatted, ints converted)
            try:
                stock_pcs = int(stock_pcs) if pd.notna(stock_pcs) and str(stock_pcs).strip() != '' else 0
                stock_box = int(stock_box) if pd.notna(stock_box) and str(stock_box).strip() != '' else 0
                stock_cs = int(stock_cs) if pd.notna(stock_cs) and str(stock_cs).strip() != '' else 0
                dist_id = int(dist_id) if pd.notna(dist_id) and str(dist_id).strip() != '' else 0
                
                # Date
                if pd.isna(date_val) or str(date_val).strip() == '':
                    date_val = None
                else:
                    try:
                        parsed = pd.to_datetime(date_val, dayfirst=False)
                        date_val = parsed.strftime('%Y-%m-%d')
                    except:
                         # Retry dayfirst
                         parsed = pd.to_datetime(date_val, dayfirst=True)
                         date_val = parsed.strftime('%Y-%m-%d')

                data_to_insert.append((str(sku), str(warehouse), stock_pcs, stock_box, stock_cs, date_val, dist_id))
            except Exception as e:
                # Should be caught by type check, but just in case
                row_errors.append(f"Row {index+1}: Data conversion error {str(e)}")
                errors.extend(row_errors)
                continue

        if data_to_insert:
            cursor.executemany(insert_query, data_to_insert)
            connection.commit()
            success_count = cursor.rowcount

        return True, {
            "success_count": len(data_to_insert),
            "errors": errors
        }

    except Exception as e:
        return False, [f"System Error: {str(e)}"]
    finally:
        if connection and connection.is_connected():
            if cursor:
                cursor.close()
            connection.close()
