import os

# Database Configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'root123456%qaz!') # SENSITIVE: Change this or use env var
DB_NAME = os.getenv('DB_NAME', 'inventory_db')

# Table Name
TABLE_NAME = 'stocks'
