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

def get_column_configs(table_name='stocks'):
    """Fetches column definitions and aliases from the database."""
    connection = get_connection()
    if not connection:
        return {}
    
    try:
        cursor = connection.cursor(dictionary=True)
        
        # Get Definitions
        query = "SELECT * FROM column_definitions WHERE table_name = %s"
        cursor.execute(query, (table_name,))
        definitions = cursor.fetchall()
        
        # Get Aliases
        cursor.execute("""
            SELECT ca.id, ca.alias_name, cd.column_name 
            FROM column_aliases ca
            JOIN column_definitions cd ON ca.column_id = cd.id
            WHERE cd.table_name = %s
        """, (table_name,))
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

def get_import_tables():
    """Fetches list of available import tables."""
    connection = get_connection()
    if not connection: return []
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM import_tables")
        tables = cursor.fetchall()
        return tables
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

def create_new_import_table(table_name, display_name, initial_columns):
    """
    Creates a new import table dynamically.
    initial_columns: list of dicts {'name': 'col_name', 'type': 'str'|'int'|'date'}
    """
    connection = get_connection()
    if not connection: return False
    
    try:
        cursor = connection.cursor()
        
        # 1. Register in import_tables
        cursor.execute(
            "INSERT INTO import_tables (table_name, display_name) VALUES (%s, %s)",
            (table_name, display_name)
        )
        
        # 2. Create physical table
        col_defs = ["id INT AUTO_INCREMENT PRIMARY KEY"]
        for col in initial_columns:
            dtype = "VARCHAR(255)"
            default = "DEFAULT ''"
            if col['type'] == 'int':
                dtype = "INT"
                default = "DEFAULT 0"
            elif col['type'] == 'date':
                dtype = "DATE"
                default = "NULL"
            
            col_defs.append(f"{col['name']} {dtype} {default}")
            
        create_sql = f"CREATE TABLE {table_name} ({', '.join(col_defs)})"
        cursor.execute(create_sql)
        
        # 3. Register columns in column_definitions
        for col in initial_columns:
            cursor.execute(
                "INSERT INTO column_definitions (table_name, column_name, is_mandatory, data_type) VALUES (%s, %s, %s, %s)",
                (table_name, col['name'], True, col['type'])
            )
            
        connection.commit()
        return True
    except Error as e:
        print(f"Error creating table: {e}")
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def add_column_to_table(table_name, column_name, data_type):
    """Adds a new column to an existing import table."""
    connection = get_connection()
    if not connection: return False
    
    try:
        cursor = connection.cursor()
        
        # 1. Alter physical table
        dtype_sql = "VARCHAR(255) DEFAULT ''"
        if data_type == 'int':
            dtype_sql = "INT DEFAULT 0"
        elif data_type == 'date':
            dtype_sql = "DATE NULL"
            
        alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {dtype_sql}"
        cursor.execute(alter_sql)
        
        # 2. Register in column_definitions
        cursor.execute(
            "INSERT INTO column_definitions (table_name, column_name, is_mandatory, data_type) VALUES (%s, %s, %s, %s)",
            (table_name, column_name, False, data_type)
        )
        
        connection.commit()
        return True
    except Error as e:
        print(f"Error adding column: {e}")
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

def import_sales_data(filename):
    """Imports sales data using dynamic configuration from database."""
    if not os.path.exists(filename):
        return False, ["File not found."]

    # Load Config from DB
    configs = get_column_configs(table_name='sales')
    if not configs:
        return False, ["Failed to load sales column configurations from database."]

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

        # Correct columns for sales table
        insert_query = """
            INSERT INTO sales (
                dist_id, date, salesman, sku, re_pcs, do_pcs, rj_pcs, 
                amount_jual, amount_std, amount_trd, 
                disc_add, disc_prod, disc_po, disc_bonus
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

            # Get values for all sales columns
            vals = {}
            for col in ['dist_id', 'date', 'salesman', 'sku', 're_pcs', 'do_pcs', 'rj_pcs', 
                        'amount_jual', 'amount_std', 'amount_trd', 
                        'disc_add', 'disc_prod', 'disc_po', 'disc_bonus']:
                vals[col] = get_val(col)

            # Dynamic Row-Level Validation
            for key in configs:
                conf = configs[key]
                val = vals.get(key)
                
                # Check Mandatory
                if conf['is_mandatory']:
                    if pd.isna(val) or str(val).strip() == '':
                         row_errors.append(f"Row {index+1}: {key} is missing.")
                         continue

                # Check Data Type (Basic)
                if pd.notna(val) and str(val).strip() != '':
                    if conf['data_type'] == 'int':
                        try:
                            int(float(val)) # float first to handle 10.0
                        except ValueError:
                            row_errors.append(f"Row {index+1}: {key} must be an integer.")
                    elif conf['data_type'] == 'date':
                        try:
                             pd.to_datetime(val, dayfirst=False, errors='raise')
                        except ValueError:
                             try:
                                 pd.to_datetime(val, dayfirst=True, errors='raise')
                             except:
                                 row_errors.append(f"Row {index+1}: {key} invalid date format.")

            if row_errors:
                errors.extend(row_errors)
                continue

            # Final Data Clean-up
            try:
                # Helper for clean int
                def clean_int(v):
                    return int(float(v)) if pd.notna(v) and str(v).strip() != '' else 0
                
                # Helper for clean decimal/float
                def clean_float(v):
                     return float(v) if pd.notna(v) and str(v).strip() != '' else 0.0

                dist_id = clean_int(vals['dist_id'])
                re_pcs = clean_int(vals['re_pcs'])
                do_pcs = clean_int(vals['do_pcs'])
                rj_pcs = clean_int(vals['rj_pcs'])
                
                amount_jual = clean_int(vals['amount_jual'])
                amount_std = clean_int(vals['amount_std'])
                amount_trd = clean_int(vals['amount_trd'])
                disc_add = clean_int(vals['disc_add'])
                disc_prod = clean_int(vals['disc_prod'])
                disc_po = clean_int(vals['disc_po'])
                disc_bonus = clean_int(vals['disc_bonus'])

                sku = str(vals['sku']) if pd.notna(vals['sku']) else ''
                salesman = str(vals['salesman']) if pd.notna(vals['salesman']) else ''
                
                date_val = vals['date']
                if pd.isna(date_val) or str(date_val).strip() == '':
                    date_val = None
                else:
                    try:
                        parsed = pd.to_datetime(date_val, dayfirst=False)
                        date_val = parsed.strftime('%Y-%m-%d')
                    except:
                         parsed = pd.to_datetime(date_val, dayfirst=True)
                         date_val = parsed.strftime('%Y-%m-%d')

                data_to_insert.append((
                    dist_id, date_val, salesman, sku, re_pcs, do_pcs, rj_pcs,
                    amount_jual, amount_std, amount_trd,
                    disc_add, disc_prod, disc_po, disc_bonus
                ))
            except Exception as e:
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

def import_dynamic_data(filename, table_name):
    """
    Generic import function for any configured table.
    """
    if not os.path.exists(filename):
        return False, ["File not found."]

    # Load Config from DB
    configs = get_column_configs(table_name=table_name)
    if not configs:
        return False, [f"Failed to load configuration for table {table_name}."]

    connection = get_connection()
    if not connection:
        return False, ["Database connection failed."]

    cursor = None
    try:
        # 1. Read File
        if filename.endswith('.csv') or filename.endswith('.txt'):
            df = pd.read_csv(filename, sep=None, engine='python')
        elif filename.endswith('.xlsx') or filename.endswith('.xls'):
            df = pd.read_excel(filename)
        else:
            return False, ["Unsupported file format."]

        # 2. Normalize Headers
        df.columns = [str(col).strip().lower() for col in df.columns]

        # 3. Map Columns
        final_columns = {}
        missing_columns = []
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
        
        # Check Mandatory
        missing_required = []
        for col_key in missing_columns:
            if configs[col_key]['is_mandatory']:
                missing_required.append(col_key)

        if missing_required:
            return False, [f"Missing mandatory columns: {', '.join(missing_required)}"]

        # 4. Prepare SQL
        cursor = connection.cursor()
        
        # Get actual columns to insert (configured keys)
        insert_keys = list(configs.keys())
        placeholders = ', '.join(['%s'] * len(insert_keys))
        columns_sql = ', '.join(insert_keys)
        
        insert_query = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})"

        data_to_insert = []
        errors = []
        
        for index, row in df.iterrows():
            row_vals = []
            row_valid = True
            row_errors = []
            
            for key in insert_keys:
                conf = configs[key]
                col_ref = final_columns.get(key)
                
                val = None
                if col_ref is not None:
                    if isinstance(col_ref, int):
                         val = row.iloc[col_ref]
                    else:
                         val = row[col_ref]

                # Validation & Cleaning
                clean_val = None
                
                # Check Mandatory
                if conf['is_mandatory']:
                     if pd.isna(val) or str(val).strip() == '':
                         row_errors.append(f"Row {index+1}: {key} is missing.")
                         row_valid = False
                         
                if not row_valid: break

                if pd.notna(val) and str(val).strip() != '':
                    try:
                        if conf['data_type'] == 'int':
                            clean_val = int(float(val))
                        elif conf['data_type'] == 'date':
                            try:
                                parsed = pd.to_datetime(val, dayfirst=False)
                                clean_val = parsed.strftime('%Y-%m-%d')
                            except:
                                parsed = pd.to_datetime(val, dayfirst=True)
                                clean_val = parsed.strftime('%Y-%m-%d')
                        else:
                            clean_val = str(val)
                    except ValueError:
                         row_errors.append(f"Row {index+1}: Invalid value for {key} ({val})")
                         row_valid = False
                else:
                    # Default values for missing/empty
                    if conf['data_type'] == 'int':
                        clean_val = 0
                    elif conf['data_type'] == 'date':
                        clean_val = None
                    else:
                        clean_val = ''
                
                row_vals.append(clean_val)
            
            if row_errors:
                errors.extend(row_errors)
                continue
                
            if row_valid:
                data_to_insert.append(tuple(row_vals))

        if data_to_insert:
            cursor.executemany(insert_query, data_to_insert)
            connection.commit()
            success_count = cursor.rowcount
            return True, {"success_count": success_count, "errors": errors}
        else:
            return False, errors + ["No valid rows to insert."]

    except Exception as e:
        return False, [f"System Error: {str(e)}"]
    finally:
        if connection and connection.is_connected():
            if cursor:
                cursor.close()
            connection.close()

def import_file_process(filename):
    """Detects file type based on name and calls appropriate import function."""
    base_name = os.path.basename(filename).lower()
    
    if base_name == 'pv_inventory.csv':
        return import_stock_data(filename)
    elif base_name == 'pv_salesunion.csv':
        return import_sales_data(filename)
    else:
        return False, ["Filename does not match expected import types."]
