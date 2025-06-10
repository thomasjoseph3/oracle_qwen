import oracledb
import json
import re
import os
import logging
from dotenv import load_dotenv
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load environment variables from .env file
load_dotenv()

# Fetch database connection details from environment variables
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_SERVICE_NAME = os.getenv("DB_SERVICE_NAME")
DB_PRIVILEGE = os.getenv("DB_PRIVILEGE", "").upper()  # Default to no privilege if not set

# Ensure environment variables are set
required_vars = ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_SERVICE_NAME"]
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Create DSN (Data Source Name) - simplified for oracledb
dsn = f"{DB_HOST}:{DB_PORT}/{DB_SERVICE_NAME}"

try:
    # Establish the database connection
    logging.info(f"Connecting to Oracle database at {dsn}")
    
    if DB_PRIVILEGE == "SYSDBA":
        connection = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn, mode=oracledb.AUTH_MODE_SYSDBA)
    elif DB_PRIVILEGE == "SYSOPER":
        connection = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn, mode=oracledb.AUTH_MODE_SYSOPER)
    else:
        connection = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn)
    
    cursor = connection.cursor()
    logging.info("Database connection established successfully.")
    
except oracledb.DatabaseError as e:
    logging.error(f"Database connection failed: {str(e)}")
    raise SystemExit(1)

# Load data from JSON file
try:
    with open("new_data.json", "r") as file:
        data = json.load(file)
    logging.info("JSON data loaded successfully.")
    logging.info(f"Tables found in JSON: {list(data.keys())}")
except FileNotFoundError:
    logging.error("Error: JSON file 'new_data.json' not found.")
    raise
except json.JSONDecodeError as e:
    logging.error(f"Error decoding JSON file: {e}")
    raise

# Function to handle NULL values, escape strings, and format dates for Oracle
def format_value(value):
    if value is None or value == "":
        return "NULL"
    elif isinstance(value, str):
        # Check if the value is a datetime in ISO 8601 format
        if re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?", value):
            # Convert to Oracle TO_TIMESTAMP format
            value = value.split("+")[0].split("Z")[0]  # Remove timezone info
            # Handle different timestamp formats
            if "." in value:
                return f"TO_TIMESTAMP('{value}', 'YYYY-MM-DD\"T\"HH24:MI:SS.FF')"
            else:
                return f"TO_TIMESTAMP('{value}', 'YYYY-MM-DD\"T\"HH24:MI:SS')"
        else:
            # Escape single quotes for Oracle
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
    elif isinstance(value, bool):
        return "1" if value else "0"  # Convert boolean to number for Oracle
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        logging.warning(f"Unsupported data type {type(value)} for value: {value}")
        return f"'{str(value)}'"

# Function to get table insertion order based on foreign key dependencies
def get_table_order():
    """Define the order of table insertion to respect foreign key constraints"""
    return [
        "assets",
        "cradles", 
        "rails",
        "vessels",
        "trolleys",
        "lifts",
        "inventory",
        "assets_maintenance",
        "work_orders",
        "wheels_load",
        "wheels_temperature"
    ]

# Get the correct insertion order
table_order = get_table_order()
total_records = 0
successful_inserts = 0
failed_inserts = 0

# Process tables in the correct order
for table_name in table_order:
    if table_name not in data:
        logging.info(f"No data found for table: {table_name}, skipping...")
        continue
    
    rows = data[table_name]
    logging.info(f"Processing table: {table_name} ({len(rows)} records)")
    
    table_success = 0
    table_failures = 0
    
    for i, row in enumerate(rows):
        try:
            # Filter out empty values and None values from row
            filtered_row = {k: v for k, v in row.items() if v is not None and v != ""}
            
            if not filtered_row:
                logging.warning(f"Skipping empty row {i+1} in table {table_name}")
                continue
            
            columns = ", ".join(filtered_row.keys())
            values = ", ".join(format_value(value) for value in filtered_row.values())
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            
            cursor.execute(insert_query)
            table_success += 1
            total_records += 1
            
        except oracledb.DatabaseError as e:
            table_failures += 1
            failed_inserts += 1
            logging.error(f"Database error inserting row {i+1} into table {table_name}: {str(e)}")
            logging.debug(f"Failed query: {insert_query}")
        except Exception as e:
            table_failures += 1
            failed_inserts += 1
            logging.error(f"Unexpected error inserting row {i+1} into table {table_name}: {str(e)}")
    
    successful_inserts += table_success
    logging.info(f"Table {table_name} completed: {table_success} successful, {table_failures} failed")

# Process any remaining tables not in the predefined order
remaining_tables = set(data.keys()) - set(table_order)
if remaining_tables:
    logging.info(f"Processing remaining tables: {list(remaining_tables)}")
    for table_name in remaining_tables:
        rows = data[table_name]
        logging.info(f"Processing table: {table_name} ({len(rows)} records)")
        
        table_success = 0
        table_failures = 0
        
        for i, row in enumerate(rows):
            try:
                # Filter out empty values and None values from row
                filtered_row = {k: v for k, v in row.items() if v is not None and v != ""}
                
                if not filtered_row:
                    logging.warning(f"Skipping empty row {i+1} in table {table_name}")
                    continue
                
                columns = ", ".join(filtered_row.keys())
                values = ", ".join(format_value(value) for value in filtered_row.values())
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
                
                cursor.execute(insert_query)
                table_success += 1
                total_records += 1
                
            except oracledb.DatabaseError as e:
                table_failures += 1
                failed_inserts += 1
                logging.error(f"Database error inserting row {i+1} into table {table_name}: {str(e)}")
            except Exception as e:
                table_failures += 1
                failed_inserts += 1
                logging.error(f"Unexpected error inserting row {i+1} into table {table_name}: {str(e)}")
        
        successful_inserts += table_success
        logging.info(f"Table {table_name} completed: {table_success} successful, {table_failures} failed")

# Commit the transaction
try:
    connection.commit()
    logging.info("Transaction committed successfully.")
    logging.info(f"Summary: {successful_inserts} records inserted successfully, {failed_inserts} failed")
except oracledb.DatabaseError as e:
    logging.error(f"Error committing transaction: {str(e)}")
    logging.info("Rolling back transaction...")
    connection.rollback()

# Verify data insertion by counting records in each table
logging.info("Verifying data insertion...")
for table_name in data.keys():
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        logging.info(f"Table {table_name}: {count} records")
    except oracledb.DatabaseError as e:
        logging.warning(f"Could not count records in table {table_name}: {str(e)}")

# Close the connection
try:
    cursor.close()
    logging.info("Cursor closed successfully.")
except oracledb.DatabaseError as e:
    logging.error(f"Error closing cursor: {str(e)}")

try:
    connection.close()
    logging.info("Database connection closed successfully.")
except oracledb.DatabaseError as e:
    logging.error(f"Error closing database connection: {str(e)}")

logging.info("Data insertion process completed.")