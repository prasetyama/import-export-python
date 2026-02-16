import mysql.connector
from mysql.connector import Error
import config

def create_database():
    try:
        # Connect to MySQL Server (no database selected yet)
        connection = mysql.connector.connect(
            host=config.DB_HOST,
            user=config.DB_USER,
            password=config.DB_PASSWORD
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Create Database
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config.DB_NAME}")
            print(f"Database '{config.DB_NAME}' created or already exists.")
            
            # Use Database
            cursor.execute(f"USE {config.DB_NAME}")
            
            # Create Table
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {config.TABLE_NAME} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                price DECIMAL(10, 2) NOT NULL,
                quantity INT NOT NULL
            )
            """
            cursor.execute(create_table_query)
            print(f"Table '{config.TABLE_NAME}' created or already exists.")
            
            # Insert Sample Data (only if empty)
            cursor.execute(f"SELECT COUNT(*) FROM {config.TABLE_NAME}")
            result = cursor.fetchone()
            if result[0] == 0:
                sample_data = [
                    ('Laptop Gaming', 15000000, 10),
                    ('Mouse Wireless', 150000, 50),
                    ('Keyboard Mechanical', 750000, 30),
                    ('Monitor 24 Inch', 2000000, 20),
                    ('Headset Gaming', 500000, 25)
                ]
                insert_query = f"INSERT INTO {config.TABLE_NAME} (name, price, quantity) VALUES (%s, %s, %s)"
                cursor.executemany(insert_query, sample_data)
                connection.commit()
                print(f"Inserted {cursor.rowcount} sample records.")
            else:
                print("Table already has data. Skipping sample data insertion.")

            # Create 'stocks' table
            create_stock_table_query = """
            CREATE TABLE IF NOT EXISTS stocks (
                id INT AUTO_INCREMENT PRIMARY KEY,
                sku VARCHAR(50) NOT NULL UNIQUE,
                warehouse_code VARCHAR(50),
                stock_pcs INT DEFAULT 0,
                stock_box INT DEFAULT 0,
                stock_cs INT DEFAULT 0,
                date DATE 
            )
            """
            cursor.execute(create_stock_table_query)
            print("Table 'stocks' created or already exists.")

            # Alter Table to add date column
            try:
                alter_table_query = """
                ALTER TABLE stocks
                ADD COLUMN date DATE
                """
                cursor.execute(alter_table_query)
                print("Table 'stocks' altered to add date column.")
            except Error as err:
                 if err.errno == 1060: # Duplicate column name
                     print("Column 'date' already exists in 'stocks'.")
                 else:
                     raise err

            # --- Master Config Tables ---
            # 1. Column Definitions
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS column_definitions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                column_name VARCHAR(50) NOT NULL UNIQUE,
                is_mandatory BOOLEAN DEFAULT FALSE,
                data_type ENUM('str', 'int', 'date') DEFAULT 'str'
            )
            """)
            print("Table 'column_definitions' created or already exists.")

            # 2. Column Aliases
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS column_aliases (
                id INT AUTO_INCREMENT PRIMARY KEY,
                column_id INT NOT NULL,
                alias_name VARCHAR(100) NOT NULL,
                FOREIGN KEY (column_id) REFERENCES column_definitions(id) ON DELETE CASCADE
            )
            """)
            print("Table 'column_aliases' created or already exists.")

            # Seed Default Data
            # Check if empty to avoid duplicates
            cursor.execute("SELECT COUNT(*) FROM column_definitions")
            if cursor.fetchone()[0] == 0:
                # Default columns matching current logic
                defaults = [
                    ('sku', 1, 'str'),
                    ('warehouse_code', 1, 'str'),
                    ('stock_pcs', 1, 'int'),
                    ('stock_box', 0, 'int'),
                    ('stock_cs', 0, 'int'),
                    ('date', 0, 'date')
                ]
                cursor.executemany(
                    "INSERT INTO column_definitions (column_name, is_mandatory, data_type) VALUES (%s, %s, %s)",
                    defaults
                )
                print("Seeded default column_definitions.")

                # Seed Aliases
                # Get IDs first
                cursor.execute("SELECT id, column_name FROM column_definitions")
                col_map = {row[1]: row[0] for row in cursor.fetchall()}
                
                aliases = []
                # Helper to add aliases
                def add_aliases(col_key, names):
                    if col_key in col_map:
                        for name in names:
                            aliases.append((col_map[col_key], name))

                add_aliases('sku', ['sku', 'kode barang', 'item code'])
                add_aliases('warehouse_code', ['kode gudang', 'warehouse', 'gudang'])
                add_aliases('stock_pcs', ['stock pcs', 'pcs', 'stok pcs'])
                add_aliases('stock_box', ['stock box', 'box', 'stok box'])
                add_aliases('stock_cs', ['stock cs', 'cs', 'stok cs'])
                add_aliases('date', ['date', 'tanggal'])

                if aliases:
                    cursor.executemany(
                        "INSERT INTO column_aliases (column_id, alias_name) VALUES (%s, %s)",
                        aliases
                    )
                    print("Seeded default column_aliases.")

            # --- Import Jobs Table ---
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS import_jobs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                batch_id VARCHAR(36) UNIQUE NOT NULL,
                filename VARCHAR(255),
                table_name VARCHAR(100),
                status ENUM('pending','validating','processing','completed','failed') DEFAULT 'pending',
                total_rows INT DEFAULT 0,
                processed_rows INT DEFAULT 0,
                success_count INT DEFAULT 0,
                error_count INT DEFAULT 0,
                error_details JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP NULL
            )
            """)
            print("Table 'import_jobs' created or already exists.")

            # --- Import Job Details Table ---
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS import_job_details (
                id INT AUTO_INCREMENT PRIMARY KEY,
                batch_id VARCHAR(36),
                filename VARCHAR(255),
                status ENUM('pending','processing','completed','failed') DEFAULT 'pending',
                success_count INT DEFAULT 0,
                error_count INT DEFAULT 0,
                error_details JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (batch_id) REFERENCES import_jobs(batch_id) ON DELETE CASCADE
            )
            """)
            print("Table 'import_job_details' created or already exists.")

            connection.commit()

    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")

if __name__ == "__main__":
    create_database()
