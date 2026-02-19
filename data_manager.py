import pandas as pd
import mysql.connector
from mysql.connector import Error
import config
import os
import zipfile
import tempfile
import shutil
import uuid
import json
import threading

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
                'is_unique': bool(row.get('is_unique', False)),
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

def update_column_config(column_id, is_mandatory, is_unique, data_type):
    """Updates configuration for a column."""
    connection = get_connection()
    if not connection: return False
    try:
        cursor = connection.cursor()
        cursor.execute(
            "UPDATE column_definitions SET is_mandatory=%s, is_unique=%s, data_type=%s WHERE id=%s",
            (is_mandatory, is_unique, data_type, column_id)
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

def update_allowed_filename(table_id, allowed_filename):
    """Updates the allowed_filename for an import table."""
    connection = get_connection()
    if not connection: return False
    try:
        cursor = connection.cursor()
        cursor.execute(
            "UPDATE import_tables SET allowed_filename = %s WHERE id = %s",
            (allowed_filename.strip().lower(), table_id)
        )
        connection.commit()
        return True
    except Error as e:
        print(f"Error updating allowed filename: {e}")
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def create_new_import_table(table_name, display_name, initial_columns, allowed_filename=''):
    """
    Creates a new import table dynamically.
    allowed_filename: expected filename for auto-detect import
    """
    connection = get_connection()
    if not connection: return False
    
    try:
        cursor = connection.cursor()
        
        # 1. Register in import_tables
        cursor.execute(
            "INSERT INTO import_tables (table_name, display_name, allowed_filename) VALUES (%s, %s, %s)",
            (table_name, display_name, allowed_filename.strip().lower())
        )
        
        # 2. Create physical table
        col_defs = ["id INT AUTO_INCREMENT PRIMARY KEY"]
        unique_constraints = []
        
        for col in initial_columns:
            dtype = "VARCHAR(255)"
            default = "DEFAULT ''"
            
            if col['type'] == 'int':
                dtype = "INT"
                default = "DEFAULT 0"
            elif col['type'] == 'date':
                dtype = "DATE"
                default = "NULL"
            elif col['type'] == 'datetime':
                dtype = "DATETIME"
                default = "NULL"
            
            col_defs.append(f"{col['name']} {dtype} {default}")
            
            if col.get('is_unique'):
                unique_constraints.append(f"UNIQUE ({col['name']})")
            
        # Add system column ImportDate
        col_defs.append("ImportDate DATETIME DEFAULT CURRENT_TIMESTAMP")
            
        create_sql = f"CREATE TABLE {table_name} ({', '.join(col_defs + unique_constraints)})"
        cursor.execute(create_sql)
        
        # 3. Register columns in column_definitions
        for col in initial_columns:
            is_unique = col.get('is_unique', False)
            cursor.execute(
                "INSERT INTO column_definitions (table_name, column_name, is_mandatory, is_unique, data_type) VALUES (%s, %s, %s, %s, %s)",
                (table_name, col['name'], True, is_unique, col['type'])
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

def add_column_to_table(table_name, column_name, data_type, is_unique=False):
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
        elif data_type == 'datetime':
            dtype_sql = "DATETIME NULL"
            
        alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {dtype_sql}"
        if is_unique:
             alter_sql += f", ADD UNIQUE ({column_name})"
             
        cursor.execute(alter_sql)
        
        # 2. Register in column_definitions
        cursor.execute(
            "INSERT INTO column_definitions (table_name, column_name, is_mandatory, is_unique, data_type) VALUES (%s, %s, %s, %s, %s)",
            (table_name, column_name, False, is_unique, data_type)
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
            INSERT INTO stocks (sku, warehouse_code, stock_pcs, stock_box, stock_cs, date, dist_id, ImportDate)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
            warehouse_code=VALUES(warehouse_code),
            stock_pcs=VALUES(stock_pcs),
            stock_box=VALUES(stock_box),
            stock_cs=VALUES(stock_cs),
            date=VALUES(date),
            dist_id=VALUES(dist_id),
            ImportDate=NOW()
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

def _check_import_file_basic(filename):
    """Basic validation for file existence and extension."""
    if not os.path.exists(filename):
        return False, ["File not found."]
    _, extension = os.path.splitext(filename)
    if extension.lower() not in ['.csv', '.txt']:
        return False, [f"Unsupported file extension '{extension}'. Only .csv and .txt are allowed."]
    return True, None


def import_file_process(filename, table_name):
    """
    Generic import function for a specific table.
    """
    valid, errs = _check_import_file_basic(filename)
    if not valid: return False, errs

    base_name = os.path.basename(filename)
    name_only, _ = os.path.splitext(base_name)
    name_only_lower = name_only.lower()

    # Load Config from DB
    configs = get_column_configs(table_name=table_name)
    if not configs:
        return False, [f"Failed to load configuration for table {table_name}."]

    connection = get_connection()
    if not connection:
        return False, ["Database connection failed."]

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        # Check Filename Validation against Table Config
        cursor.execute("SELECT allowed_filename FROM import_tables WHERE table_name = %s", (table_name,))
        table_info = cursor.fetchone()
        
        if table_info and table_info['allowed_filename']:
            allowed_list = [a.strip().lower() for a in table_info['allowed_filename'].split(',') if a.strip()]
            if allowed_list and name_only_lower not in allowed_list:
                return False, [f"Filename '{name_only}' does not match any of the configured allowed filenames for table '{table_name}'. Expected one of: {', '.join(allowed_list)}"]

        # Read File
        df = pd.read_csv(filename, sep=None, engine='python')

        # Normalize Headers
        df.columns = [str(col).strip().lower() for col in df.columns]

        # Map Columns
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
        missing_required = [col_key for col_key in missing_columns if configs[col_key]['is_mandatory']]
        if missing_required:
            return False, [f"Missing mandatory columns: {', '.join(missing_required)}"]

        # Prepare SQL
        cursor = connection.cursor()
        insert_keys = [k for k in configs.keys() if k != 'ImportDate']
        placeholders = ', '.join(['%s'] * len(insert_keys))
        columns_sql = ', '.join(insert_keys)
        update_clause = ", ".join([f"{col}=VALUES({col})" for col in insert_keys])
        update_clause += ", ImportDate=NOW()"
        insert_query = f"INSERT INTO {table_name} ({columns_sql}, ImportDate) VALUES ({placeholders}, NOW()) ON DUPLICATE KEY UPDATE {update_clause}"

        data_to_insert = []
        errors = []
        
        for index, row in df.iterrows():
            row_vals = []
            row_valid = True
            row_errors = []
            
            for key in insert_keys:
                conf = configs[key]
                col_ref = final_columns.get(key)
                val = row[col_ref] if col_ref is not None else None

                # Validation & Cleaning
                if conf['is_mandatory'] and (pd.isna(val) or str(val).strip() == ''):
                    row_errors.append(f"Row {index+1}: {key} is missing.")
                    row_valid = False
                    break
                         
                if pd.notna(val) and str(val).strip() != '':
                    try:
                        if conf['data_type'] == 'int':
                            clean_val = int(float(val))
                        elif conf['data_type'] == 'date':
                            try:
                                clean_val = pd.to_datetime(val, dayfirst=False).strftime('%Y-%m-%d')
                            except:
                                clean_val = pd.to_datetime(val, dayfirst=True).strftime('%Y-%m-%d')
                        elif conf['data_type'] == 'datetime':
                            try:
                                clean_val = pd.to_datetime(val, dayfirst=False).strftime('%Y-%m-%d %H:%M:%S')
                            except:
                                clean_val = pd.to_datetime(val, dayfirst=True).strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            clean_val = str(val)
                    except Exception:
                        row_errors.append(f"Row {index+1}: Invalid value for {key} ({val})")
                        row_valid = False
                        break
                else:
                    clean_val = 0 if conf['data_type'] == 'int' else (None if conf['data_type'] in ['date', 'datetime'] else '')
                
                row_vals.append(clean_val)
            
            if not row_valid:
                errors.extend(row_errors)
                continue
            data_to_insert.append(tuple(row_vals))

        if data_to_insert:
            cursor.executemany(insert_query, data_to_insert)
            connection.commit()
            return True, {"success_count": cursor.rowcount, "errors": errors}
        else:
            return False, errors + ["No valid rows to insert."]

    except Exception as e:
        return False, [f"System Error: {str(e)}"]
    finally:
        if connection and connection.is_connected():
            if cursor: cursor.close()
            connection.close()


def import_dynamic_data(filename):
    """Auto-detects table based on filename and processes the import."""
    valid, errs = _check_import_file_basic(filename)
    if not valid: return False, errs

    base_name = os.path.basename(filename)
    name_only_lower = os.path.splitext(base_name)[0].lower()

    connection = get_connection()
    if not connection: return False, ["Database connection failed."]
    
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT table_name, allowed_filename FROM import_tables WHERE allowed_filename != ''")
        tables = cursor.fetchall()
        
        for table in tables:
            allowed_list = [a.strip().lower() for a in table['allowed_filename'].split(',') if a.strip()]
            if name_only_lower in allowed_list:
                return import_file_process(filename, table['table_name'])
        
        return False, [f"Filename '{os.path.splitext(base_name)[0]}' does not match any configured table. Update Master Config or select table manually."]
    except Error as e:
        return False, [f"Auto-detect Error: {str(e)}"]
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def extract_zip(zip_path, extract_to):
    valid_extensions = ['.csv', '.txt']
    extracted_files = []

    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(extract_to)

        # Walk extracted directory and collect valid files
        for root, dirs, files in os.walk(extract_to):
            # Skip macOS resource fork directories
            dirs[:] = [d for d in dirs if d != '__MACOSX']
            for fname in files:
                # Skip hidden/resource fork files
                if fname.startswith('._') or fname.startswith('.'):
                    continue
                _, ext = os.path.splitext(fname)
                if ext.lower() in valid_extensions:
                    extracted_files.append(os.path.join(root, fname))

        return extracted_files
    except zipfile.BadZipFile:
        return []

def import_multiple_files(file_paths, table_name='auto'):
    results = []

    for filepath in file_paths:
        base_name = os.path.basename(filepath)

        try:
            if table_name and table_name != 'auto':
                result, messages = import_file_process(filepath, table_name)
            else:
                result, messages = import_dynamic_data(filepath)

            if result:
                success_count = messages.get('success_count', 0) if isinstance(messages, dict) else 0
                errors = messages.get('errors', []) if isinstance(messages, dict) else []
                results.append({
                    'filename': base_name,
                    'success': True,
                    'message': f"Processed {success_count} records.",
                    'errors': errors
                })
            else:
                error_msg = messages[0] if isinstance(messages, list) and messages else str(messages)
                results.append({
                    'filename': base_name,
                    'success': False,
                    'message': error_msg,
                    'errors': messages if isinstance(messages, list) else [str(messages)]
                })
        except Exception as e:
            results.append({
                'filename': base_name,
                'success': False,
                'message': f"Unexpected error: {str(e)}",
                'errors': [str(e)]
            })

    return results


# ==================== IMPORT JOB TRACKING ====================

def create_import_job(batch_id, filename, table_name):
    """Creates a new import job record in the database."""
    connection = get_connection()
    if not connection: return None
    try:
        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO import_jobs (batch_id, filename, table_name, status) VALUES (%s, %s, %s, 'pending')",
            (batch_id, filename, table_name)
        )
        connection.commit()
        return batch_id
    except Error as e:
        print(f"Error creating job: {e}")
        return None
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


def update_job_status(batch_id, status=None, total_rows=None, processed_rows=None,
                      success_count=None, error_count=None, error_details=None):
    """Updates import job progress fields."""
    connection = get_connection()
    if not connection: return False
    try:
        cursor = connection.cursor()
        updates = []
        params = []

        if status is not None:
            updates.append("status = %s")
            params.append(status)
        if total_rows is not None:
            updates.append("total_rows = %s")
            params.append(total_rows)
        if processed_rows is not None:
            updates.append("processed_rows = %s")
            params.append(processed_rows)
        if success_count is not None:
            updates.append("success_count = %s")
            params.append(success_count)
        if error_count is not None:
            updates.append("error_count = %s")
            params.append(error_count)
        if error_details is not None:
            updates.append("error_details = %s")
            params.append(json.dumps(error_details))
        if status in ('completed', 'failed'):
            updates.append("completed_at = NOW()")

        if not updates:
            return True

        params.append(batch_id)
        query = f"UPDATE import_jobs SET {', '.join(updates)} WHERE batch_id = %s"
        cursor.execute(query, tuple(params))
        connection.commit()
        return True
    except Error as e:
        print(f"Error updating job: {e}")
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


def get_job(batch_id):
    """Gets a single import job by batch_id."""
    connection = get_connection()
    if not connection: return None
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM import_jobs WHERE batch_id = %s", (batch_id,))
        job = cursor.fetchone()
        if job and job.get('error_details'):
            if isinstance(job['error_details'], str):
                job['error_details'] = json.loads(job['error_details'])
        # Convert datetime to string for JSON serialization
        if job:
            for key in ('created_at', 'completed_at'):
                if job.get(key):
                    job[key] = job[key].isoformat()
        return job
    except Error as e:
        print(f"Error getting job: {e}")
        return None
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


def get_all_jobs(limit=50):
    """Gets all import jobs, most recent first."""
    connection = get_connection()
    if not connection: return []
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM import_jobs ORDER BY created_at DESC LIMIT %s", (limit,))
        jobs = cursor.fetchall()
        for job in jobs:
            if job.get('error_details') and isinstance(job['error_details'], str):
                job['error_details'] = json.loads(job['error_details'])
            for key in ('created_at', 'completed_at'):
                if job.get(key):
                    job[key] = job[key].isoformat()
        return jobs
    except Error as e:
        print(f"Error getting jobs: {e}")
        return []
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


def create_job_detail(batch_id, filename):
    """Initializes a tracking record for an individual file in a batch."""
    connection = get_connection()
    if not connection: return False
    try:
        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO import_job_details (batch_id, filename, status) VALUES (%s, %s, 'pending')",
            (batch_id, filename)
        )
        connection.commit()
        return True
    except Error as e:
        print(f"Error creating job detail: {e}")
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


def update_job_detail(batch_id, filename, status=None, success_count=None, error_count=None, error_details=None):
    """Updates tracking results for an individual file."""
    connection = get_connection()
    if not connection: return False
    try:
        cursor = connection.cursor()
        updates = []
        params = []
        if status:
            updates.append("status = %s")
            params.append(status)
        if success_count is not None:
            updates.append("success_count = %s")
            params.append(success_count)
        if error_count is not None:
            updates.append("error_count = %s")
            params.append(error_count)
        if error_details:
            updates.append("error_details = %s")
            params.append(json.dumps(error_details))
        
        if not updates: return True
        
        params.extend([batch_id, filename])
        query = f"UPDATE import_job_details SET {', '.join(updates)} WHERE batch_id = %s AND filename = %s"
        cursor.execute(query, tuple(params))
        connection.commit()
        return True
    except Error as e:
        print(f"Error updating job detail: {e}")
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


def get_job_file_details(batch_id):
    """Retrieves results for all files in a specific batch."""
    connection = get_connection()
    if not connection: return []
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM import_job_details WHERE batch_id = %s", (batch_id,))
        details = cursor.fetchall()
        for d in details:
            if d.get('error_details') and isinstance(d['error_details'], str):
                d['error_details'] = json.loads(d['error_details'])
            if d.get('created_at'):
                d['created_at'] = d['created_at'].isoformat()
        return details
    except Error as e:
        print(f"Error getting job file details: {e}")
        return []
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


def quick_validate_file(filepath, table_name):
    """
    Quick validation: checks file extension, headers, and basic rules.
    """
    valid, errs = _check_import_file_basic(filepath)
    if not valid: return False, errs[0], 0

    try:
        df = pd.read_csv(filepath, sep=None, engine='python')
        if df.empty: return False, "File is empty.", 0
        total_rows = len(df)

        # Normalize headers
        df.columns = [str(col).strip().lower() for col in df.columns]

        # Auto-detect if needed
        if not table_name or table_name == 'auto':
            base_name = os.path.basename(filepath)
            name_only = os.path.splitext(base_name)[0].lower()
            
            connection = get_connection()
            if not connection: return False, "DB connection failed", 0
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT table_name, allowed_filename FROM import_tables WHERE allowed_filename != ''")
            tables = cursor.fetchall()
            
            target_table = None
            for t in tables:
                allowed_list = [a.strip().lower() for a in t['allowed_filename'].split(',') if a.strip()]
                if name_only in allowed_list:
                    target_table = t['table_name']
                    break
            
            if not target_table:
                return False, f"Filename '{name_only}' not recognized.", 0
            table_name = target_table

        configs = get_column_configs(table_name=table_name)
        if not configs:
            return False, f"No column configuration found for table '{table_name}'.", 0

        # Check mandatory column headers exist
        column_mapping = {name: conf['aliases'] for name, conf in configs.items()}
        missing_required = []

        for key, possible_names in column_mapping.items():
            if configs[key]['is_mandatory']:
                found = any(name in df.columns for name in possible_names)
                if not found:
                    missing_required.append(key)

        if missing_required:
            return False, f"Missing mandatory columns: {', '.join(missing_required)}", total_rows

        # Validate first 2 data rows (basic type check)
        sample = df.head(2)
        row_errors = []

        for idx, row in sample.iterrows():
            for key, conf in configs.items():
                aliases = conf['aliases']
                val = None
                for alias in aliases:
                    if alias in sample.columns:
                        val = row[alias]
                        break

                if val is None or (pd.isna(val) and conf['is_mandatory']):
                    if conf['is_mandatory']:
                        row_errors.append(f"Row {idx+1}: {key} is missing.")
                    continue

                if pd.notna(val) and str(val).strip() != '':
                    if conf['data_type'] == 'int':
                        try:
                            int(float(val))
                        except (ValueError, TypeError):
                            row_errors.append(f"Row {idx+1}: {key} must be integer, got '{val}'.")
                    elif conf['data_type'] == 'date':
                        try:
                            pd.to_datetime(val, errors='raise')
                        except:
                            row_errors.append(f"Row {idx+1}: {key} invalid date format '{val}'.")

        if row_errors:
            return False, f"Validation errors in sample rows: {'; '.join(row_errors)}", total_rows

        return True, None, total_rows

    except Exception as e:
        return False, f"Error reading file: {str(e)}", 0


def process_import_async(file_paths, table_name, batch_id, temp_dirs=None):
    """
    Background worker: processes all files for a batch job.
    Updates job status in DB as it progresses.
    Cleans up files when done.
    """
    try:
        update_job_status(batch_id, status='processing')

        all_errors = []
        total_success = 0
        total_errors = 0
        total_processed = 0

        for filepath in file_paths:
            fname = os.path.basename(filepath)
            create_job_detail(batch_id, fname)
            try:
                update_job_detail(batch_id, fname, status='processing')
                if table_name and table_name != 'auto':
                    result, messages = import_file_process(filepath, table_name)
                else:
                    result, messages = import_dynamic_data(filepath)

                file_success = 0
                file_errors = []
                if result:
                    file_success = messages.get('success_count', 0) if isinstance(messages, dict) else 0
                    file_errors = messages.get('errors', []) if isinstance(messages, dict) else []
                    total_success += file_success
                    total_errors += len(file_errors)
                    total_processed += file_success + len(file_errors)
                    if file_errors:
                        all_errors.extend([f"{fname}: {e}" for e in file_errors])
                else:
                    error_msgs = messages if isinstance(messages, list) else [str(messages)]
                    total_errors += 1
                    total_processed += 1
                    all_errors.extend([f"{fname}: {e}" for e in error_msgs])
                    file_errors = error_msgs

                # Update per-file status
                file_status = 'completed' if result and not file_errors else ('completed' if result else 'failed')
                update_job_detail(
                    batch_id, fname, 
                    status=file_status, 
                    success_count=file_success, 
                    error_count=len(file_errors) if isinstance(file_errors, list) else 1,
                    error_details=file_errors
                )

                # Update progress after each file
                update_job_status(
                    batch_id,
                    processed_rows=total_processed,
                    success_count=total_success,
                    error_count=total_errors,
                    error_details=all_errors if all_errors else None
                )

            except Exception as e:
                fname = os.path.basename(filepath)
                all_errors.append(f"{fname}: Unexpected error: {str(e)}")
                total_errors += 1

        # Final status
        final_status = 'completed' if total_success > 0 else 'failed'
        update_job_status(
            batch_id,
            status=final_status,
            processed_rows=total_processed,
            success_count=total_success,
            error_count=total_errors,
            error_details=all_errors if all_errors else None
        )

    except Exception as e:
        update_job_status(
            batch_id,
            status='failed',
            error_details=[f"Fatal error: {str(e)}"]
        )
    finally:
        # Cleanup files
        for fp in file_paths:
            try:
                os.remove(fp)
            except:
                pass
        if temp_dirs:
            for td in temp_dirs:
                try:
                    shutil.rmtree(td, ignore_errors=True)
                except:
                    pass
