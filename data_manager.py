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

from gdrive_utils import upload_file_to_gdrive
from dotenv import load_dotenv
load_dotenv()

SERVICE_ACCOUNT_FILE = os.getenv("GDRIVE_CREDENTIALS")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")

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
# "Skipped": 0,
# "Uploaded": 1,
# "Failed": 2,
# "Waiting Process": 3,
# "Validation Process": 4,
# "Validasi Sukses": 5,
# "Validasi Failed": 6,
# "Processing Failed": 8,
# "Processing Complete": 9,

def create_import_job(batch_id, filename, dist_id=None, file_size=None, user_id=None):
    """Creates a new import job record in the database."""
    print(f"Creating job for batch_id: {batch_id}, filename: {filename}")
    connection = get_connection()
    if not connection: return None
    try:
        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO upload_logs (file_type, file_name, file_size, status, file_name_zip, distid, user_id, created_at) VALUES (%s, %s, %s, '3', %s, %s, %s, NOW())",
            (filename, filename, file_size, batch_id, dist_id, user_id)
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


def update_job_status(batch_id, filename=None, status=None, total_rows=None, processed_rows=None,
                      success_count=None, error_count=None, error_details=None, message=None, notes=None, link_file=None):
    """Updates the status of an import job (or specific file in a batch)."""
    connection = get_connection()
    if not connection: return False
    try:
        cursor = connection.cursor()
        
        updates = []
        params = []
        
        if status:
            updates.append("status = %s")
            params.append(status)
        # if total_rows is not None:
        #     updates.append("total_rows = %s")
        #     params.append(total_rows)
        # if processed_rows is not None:
        #     updates.append("processed_rows = %s")
        #     params.append(processed_rows)
        # if success_count is not None:
        #     updates.append("success_count = %s")
        #     params.append(success_count)
        # if error_count is not None:
        #     updates.append("error_count = %s")
        #     params.append(error_count)
        if message:
            updates.append("message = %s")
            params.append(message)
        if notes:
            updates.append("notes = %s")
            params.append(notes)
        if error_details:
            if isinstance(error_details, list):
                error_details = json.dumps(error_details)
            updates.append("message = %s")
            params.append(error_details)
            
        if status in ('2','3', '5', '9'):
            updates.append("update_process = NOW()")

        if link_file:
            updates.append("link_file = %s")
            params.append(link_file)

        if not updates:
            return True

        if filename:
            params.append(batch_id)
            params.append(filename)
            query = f"UPDATE upload_logs SET {', '.join(updates)} WHERE file_name_zip = %s AND file_name = %s"
        else:
            params.append(batch_id)
            query = f"UPDATE upload_logs SET {', '.join(updates)} WHERE file_name_zip = %s"

        cursor.execute(query, tuple(params))
        connection.commit()
        return True
    except Error as e:
        print(f"Error updating job status: {e}")
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


def _aggregate_job_rows(rows):
    """Helper to aggregate multiple rows for the same batch_id."""
    if not rows:
        return None
        
    first = rows[0]
    batch_id = first['batch_id']
    
    total_rows = 0
    processed_rows = 0
    success_count = 0
    error_count = 0
    all_error_details = []
    
    statuses = set()
    files_list = []
    
    for r in rows:
        total_rows += r.get('total_rows') or 0
        processed_rows += r.get('processed_rows') or 0
        success_count += r.get('success_count') or 0
        error_count += r.get('error_count') or 0
        
        errs = r.get('error_details')
        if errs:
            if isinstance(errs, str):
                try:
                    errs = json.loads(errs)
                except:
                    pass
            if isinstance(errs, list):
                formatted_err = {"filename": r['filename'], "errors": errs}
                all_error_details.append(formatted_err)
        
        statuses.add(r['status'])
        
        files_list.append({
            "filename": r['filename'],
            "status": r['status'],
            "total_rows": r.get('total_rows'),
            "processed_rows": r.get('processed_rows'),
            "error_count": r.get('error_count'),
        })

    # Determine aggregate status
    if 'processing' in statuses:
        agg_status = 'processing'
    elif 'pending' in statuses:
        if len(statuses) > 1: agg_status = 'processing'
        else: agg_status = 'pending'
    elif 'failed' in statuses and 'completed' not in statuses:
        agg_status = 'failed'
    elif 'failed' in statuses and 'completed' in statuses:
         agg_status = 'completed_with_errors'
    else:
        agg_status = 'completed'

    job = {
        "batch_id": batch_id,
        "table_name": first['table_name'],
        "status": agg_status,
        "total_rows": total_rows,
        "processed_rows": processed_rows,
        "success_count": success_count,
        "error_count": error_count,
        "error_details": all_error_details,
        "files": files_list,
        "created_at": first['created_at'],
        "completed_at": first.get('completed_at')
    }

    # Date formatting
    for key in ('created_at', 'completed_at'):
        if job.get(key) and hasattr(job[key], 'isoformat'):
            job[key] = job[key].isoformat()
            
    return job

def get_job(batch_id):
    """Gets a single import job by batch_id, aggregating multiple file rows."""
    connection = get_connection()
    if not connection: return None
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM import_jobs WHERE batch_id = %s", (batch_id,))
        rows = cursor.fetchall()
        return _aggregate_job_rows(rows)
    except Error as e:
        print(f"Error getting job: {e}")
        return None
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


def get_all_jobs(limit=100):
    """Gets all import jobs, grouped by batch_id."""
    connection = get_connection()
    if not connection: return []
    try:
        cursor = connection.cursor(dictionary=True)
        # Fetch individual rows, but we need enough to find unique batches
        cursor.execute("SELECT * FROM import_jobs ORDER BY created_at DESC LIMIT %s", (limit * 2,))
        all_rows = cursor.fetchall()
        
        # Group by batch_id preserving order of appearance (most recent created_at first)
        batches = {}
        batch_order = []
        for r in all_rows:
            bid = r['batch_id']
            if bid not in batches:
                batches[bid] = []
                batch_order.append(bid)
            batches[bid].append(r)
            
        aggregated_jobs = []
        for bid in batch_order:
            if len(aggregated_jobs) >= limit: break
            aggregated_jobs.append(_aggregate_job_rows(batches[bid]))
            
        return aggregated_jobs
    except Error as e:
        print(f"Error getting jobs: {e}")
        return []
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


def quick_validate_file(filepath, table_name, dist_id=None):
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

        # dist_id prefix validation (first 2 digits)
        if dist_id and str(dist_id).strip():
            target_prefix = str(dist_id).strip()[:2]
            
            # Find the actual column in DF that maps to 'distid' (database column name)
            distid_col_in_df = None
            # We look for 'distid' key in configs or any case-insensitive variation if 'distid' is common
            search_key = 'DISTID'
            if search_key in configs:
                aliases = configs[search_key]['aliases']
                for alias in aliases:
                    if alias in df.columns:
                        distid_col_in_df = alias
                        break
            
            if distid_col_in_df:
                # Check first 5 rows for prefix match
                sample_dist = df.head(5)[distid_col_in_df].dropna().astype(str)
                for val in sample_dist:
                    val_clean = val.strip()
                    if val_clean and val_clean[:2] != target_prefix:
                        return False, f"DistID value does not match expected prefix '{target_prefix}' for dist_id '{dist_id}'. Found value: '{val_clean}'", total_rows

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

def _check_missing_table_files(all_file_paths, batch_id, dist_id):
    """
    Checks which import_tables have no matching uploaded file in all_file_paths.
    For each missing table, creates a job entry via create_import_job with status '0'.
    Returns a list of missing table dicts.
    """
    connection = get_connection()
    if not connection:
        return []

    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT table_name, display_name, allowed_filename FROM import_tables WHERE allowed_filename != '' AND allowed_filename IS NOT NULL")
        tables = cursor.fetchall()

        if not tables:
            return []

        # Get uploaded filenames (without extension, lowercased)
        uploaded_names = []
        for fp in all_file_paths:
            base_name = os.path.basename(fp)
            name_only_lower = os.path.splitext(base_name)[0].lower()
            uploaded_names.append(name_only_lower)

        missing_tables = []
        for table in tables:
            allowed_list = [a.strip().lower() for a in table['allowed_filename'].split(',') if a.strip()]
            # Check if any uploaded file matches this table's allowed filenames
            has_match = any(name in allowed_list for name in uploaded_names)
            if not has_match:
                missing_filename = table['allowed_filename']
                # Create job entry for the missing file
                create_import_job(batch_id, table['display_name'], dist_id, 0)
                update_job_status(
                    batch_id,
                    filename=table['display_name'],
                    status='0',
                    message="User skipped this step"
                )
                missing_tables.append({
                    'table_name': table['table_name'],
                    'display_name': table['display_name'],
                    'allowed_filename': missing_filename
                })

        return missing_tables
    except Error as e:
        print(f"Error checking missing table files: {e}")
        return []
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def process_import_async(file_paths, table_name, batch_id, temp_dirs=None):
    """
    Background worker: processes all files for a batch job.
    Updates job status in DB as it progresses.
    Cleans up files when done.
    """
    try:
        for filepath in file_paths:
            fname = os.path.basename(filepath)
            
            try:
                # Mark file as processing
                update_job_status(batch_id, filename=fname, status='3')
                
                if table_name and table_name != 'auto':
                    result, messages = import_file_process(filepath, table_name)
                else:
                    result, messages = import_dynamic_data(filepath)

                file_success = 0
                file_errors = []
                file_status = '2'
                message = ''
                notes = ''
                
                if result:
                    file_success = messages.get('success_count', 0) if isinstance(messages, dict) else 0
                    file_errors = messages.get('errors', []) if isinstance(messages, dict) else []
                    notes = f"Berhasil memproses {(file_success + len(file_errors))} data"
                    message = f"File uploaded successfully"

                    # Extract date range summary from DOTANGGAL column if present
                    try:
                        df_summary = pd.read_csv(filepath, sep=None, engine='python')
                        df_summary.columns = [str(col).strip().lower() for col in df_summary.columns]
                        if 'dotanggal' in df_summary.columns:
                            dates = pd.to_datetime(df_summary['dotanggal'], errors='coerce').dropna()
                            if not dates.empty:
                                min_date = dates.min().strftime('%d/%m/%Y')
                                max_date = dates.max().strftime('%d/%m/%Y')
                                notes += f"; Periode DO: {min_date} s/d {max_date}"
                        if 'amount_jual' in df_summary.columns:
                            total_jual = pd.to_numeric(df_summary['amount_jual'], errors='coerce').sum()
                            notes += f"; Total Penjualan: Rp {total_jual:,.0f}"
                        if 'exportdate' or 'export_date' in df_summary.columns:
                            dates = pd.to_datetime(df_summary['exportdate'], errors='coerce').dropna()
                            if not dates.empty:
                                min_date = dates.min().strftime('%d/%m/%Y')
                                max_date = dates.max().strftime('%d/%m/%Y')
                                notes += f"; dengan Export Date: {min_date} s/d {max_date}"
                    except Exception as e_date:
                        print(f"Warning: Could not extract date range from DOTANGGAL: {e_date}")
                    drive_link = None

                    upload_to_gdrive_result = upload_to_gdrive([filepath])
                    if isinstance(upload_to_gdrive_result, list):
                        for res in upload_to_gdrive_result:
                            if 'error' in res:
                                notes += f"; GDrive upload failed: {res['error']}"
                            else:
                                drive_link = f"https://drive.google.com/file/d/{res['gdrive_file_id']}/view?usp=drive_link"
                                notes += f"; Uploaded to GDrive with link : {drive_link}"
                                
                    else:
                        notes += f"; GDrive upload skipped: {upload_to_gdrive_result}"
                    
                    if not file_errors: 
                        file_status = '9'
                    else:
                        file_status = '9' # Logic: completed processing the file. Errors are details.
                else:
                    error_msgs = messages if isinstance(messages, list) else [str(messages)]
                    file_errors = error_msgs
                    file_status = '2'
                    message = error_msgs[0] if error_msgs else "File processing failed."

                # Update final status for this file
                update_job_status(
                    batch_id, 
                    filename=fname, 
                    status=file_status, 
                    success_count=file_success, 
                    error_count=len(file_errors),
                    error_details=file_errors if file_errors else None,
                    processed_rows=(file_success + len(file_errors)),
                    total_rows=(file_success + len(file_errors)),
                    message=message,
                    notes=notes,
                    link_file=drive_link
                )

            except Exception as e:
                # File level exception
                update_job_status(
                    batch_id, 
                    filename=fname, 
                    status='failed',
                    error_count=1,
                    error_details=[f"Unexpected error processing file: {str(e)}"]
                )

    except Exception as e:
        print(f"Batch level error in process_import_async: {e}")
        pass
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

def upload_to_gdrive(filepath):
    """Uploads a file to Google Drive and returns the file ID."""
    gdrive_results = []
    if GDRIVE_FOLDER_ID and SERVICE_ACCOUNT_FILE:
        for fp in filepath:
            try:
                file_id = upload_file_to_gdrive(fp, GDRIVE_FOLDER_ID, SERVICE_ACCOUNT_FILE)
                gdrive_results.append({'filename': os.path.basename(fp), 'gdrive_file_id': file_id})
            except Exception as e:
                gdrive_results.append({'filename': os.path.basename(fp), 'error': str(e)})
    else:
        gdrive_results = 'GDrive config missing'
    return gdrive_results
