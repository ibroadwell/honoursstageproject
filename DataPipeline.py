import os
import shutil
import mysql.connector
import json
from mysql.connector import Error

def _connect_to_mysql(config_data):
    """
    Establishes a connection to the MySQL database using provided configuration data.
    Returns the connection object and a dictionary cursor.
    """
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(
            host=config_data["host"],
            user=config_data["user"],
            password=config_data["password"],
            database=config_data["database"]
        )
        cursor = conn.cursor(dictionary=True)
        print(f"Successfully connected to MySQL database: {config_data['database']}")
        return conn, cursor
    except Error as err:
        print(f"Error connecting to MySQL database: {err}")
        if conn and conn.is_connected():
            conn.close()
        return None, None

def return_secure_priv(config_path="config.json"):
    """
    Connects to MySQL using config.json, queries for 'secure_file_priv',
    and returns its value.
    """
    try:
        with open(config_path) as json_file:
            config_data = json.load(json_file)
    except FileNotFoundError:
        print(f"Error: Config file '{config_path}' not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{config_path}'. Check file format.")
        return None

    conn, cursor = _connect_to_mysql(config_data)
    if not conn or not cursor:
        return None

    try:
        query = "SHOW VARIABLES LIKE 'secure_file_priv';"
        cursor.execute(query)
        result = cursor.fetchone()

        if result and 'Value' in result:
            return result['Value']
        else:
            print("Variable 'secure_file_priv' not found or is NULL.")
            return None

    except Error as err:
        print("Database error:", err)
        return None
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def move_csv_files(source_folder, destination_folder):
    """
    Finds all CSV files in source_folder and copies them to destination_folder.
    The destination_folder should be the MySQL secure_file_priv path.
    """
    print(f"\n--- Copying CSV Files from '{source_folder}' to '{destination_folder}' ---")
    moved_files = []
    try:
        if not os.path.exists(source_folder):
            print(f"Error: Source folder '{source_folder}' does not exist. Please check the path.")
            return []

        if not os.path.exists(destination_folder):
            print(f"Warning: Destination folder '{destination_folder}' does not exist. Please create it or verify your secure_file_priv setting.")
            return []

        source_folder = os.path.abspath(source_folder)
        destination_folder = os.path.abspath(destination_folder)

        for filename in os.listdir(source_folder):
            if filename.endswith(".csv"):
                source_path = os.path.join(source_folder, filename)
                destination_path = os.path.join(destination_folder, filename)
                try:
                    shutil.copy2(source_path, destination_path)
                    print(f"Copied: '{filename}' to '{destination_path}'")
                    moved_files.append(filename)
                except shutil.Error as se:
                    print(f"Shutil error moving '{filename}': {se}")
                except OSError as e:
                    print(f"OS error moving '{filename}': {e}")
    except OSError as e:
        print(f"Error accessing source folder '{source_folder}': {e}")
    return moved_files

def run_sql_script(sql_script_path, config_data, secure_priv_path_for_sql=None):
    """
    Reads and executes SQL statements from a file.
    Assumes SQL statements are separated by semicolons.
    If secure_priv_path_for_sql is provided, it replaces '{SECURE_PRIV_PATH}' placeholder.
    """
    print(f"\n--- Running SQL Script: '{os.path.basename(sql_script_path)}' ---")
    conn, cursor = _connect_to_mysql(config_data)
    if not conn or not cursor:
        print(f"Skipping script '{os.path.basename(sql_script_path)}' due to database connection error.")
        return False

    try:
        with open(sql_script_path, 'r') as file:
            sql_script_content = file.read()

        if secure_priv_path_for_sql:
            sql_safe_secure_priv_path = secure_priv_path_for_sql.replace('\\', '/')
            sql_script_content = sql_script_content.replace('{SECURE_PRIV_PATH}', sql_safe_secure_priv_path)
            print(f"  Placeholder '{{SECURE_PRIV_PATH}}' replaced with '{sql_safe_secure_priv_path}'")

        # Split the script into individual statements using ';' as a delimiter.
        # Filter out empty strings that might result from splitting.
        statements = [s.strip() for s in sql_script_content.split(';') if s.strip()]

        if not statements:
            print(f"No SQL statements found in '{os.path.basename(sql_script_path)}'.")
            return True

        for statement in statements:
            try:
                cursor.execute(statement)
            except Error as err:
                print(f"  Error executing statement: {statement[:100]}{'...' if len(statement) > 100 else ''}")
                print(f"  MySQL Error: {err}")
                conn.rollback() # Rollback changes for this script on error
                return False
        conn.commit() # Commit changes for this script if all statements succeed
        print(f"Finished SQL script: '{os.path.basename(sql_script_path)}' (Committed)")
        return True
    except FileNotFoundError:
        print(f"Error: SQL script not found: '{sql_script_path}'")
        return False
    except Exception as e:
        print(f"An unexpected error occurred while reading or executing SQL script '{sql_script_path}': {e}")
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def load_data_pipeline(source_csv_folder, sql_scripts_folder, sql_script_files, config_path="config.json"):
    """
    Orchestrates the entire data loading pipeline:
    1. Gets secure_file_priv path from MySQL.
    2. Moves CSV files to the secure_file_priv directory.
    3. Runs specified SQL scripts (table creation, data loading),
       replacing '{SECURE_PRIV_PATH}' placeholder in SQL if present.
    """
    print("\n--- Starting Data Loading Pipeline ---")

    try:
        with open(config_path) as json_file:
            config_data = json.load(json_file)
    except FileNotFoundError:
        print(f"Error: Config file '{config_path}' not found. Exiting pipeline.")
        return False
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{config_path}'. Check file format. Exiting pipeline.")
        return False

    # 1. Get secure_file_priv path
    secure_priv_path = return_secure_priv(config_path)
    if not secure_priv_path:
        print("Failed to retrieve secure_file_priv path. Exiting pipeline.")
        return False
    print(f"\nMySQL secure_file_priv path: '{secure_priv_path}'")

    # 2. Move CSV files
    moved_csv_filenames = move_csv_files(source_csv_folder, secure_priv_path)
    if not moved_csv_filenames:
        print("No CSV files were moved. Please check source folder and file names. Exiting pipeline.")
        return False

    # 3. Run SQL scripts in order
    all_scripts_succeeded = True
    for script_name in sql_script_files:
        script_path = os.path.join(sql_scripts_folder, script_name)
        success = run_sql_script(script_path, config_data, secure_priv_path)
        if not success:
            all_scripts_succeeded = False
            print(f"Pipeline stopped due to failure in script: '{script_name}'")
            break

    if all_scripts_succeeded:
        print("\n--- Data Loading Pipeline Completed Successfully! ---")
        return True
    else:
        print("\n--- Data Loading Pipeline Failed. ---")
        return False


SOURCE_CSV_FOLDER = 'data'

SQL_SCRIPTS_FOLDER = 'sql_scripts'

SQL_SCRIPT_FILES = ['build_census_tables.sql',
                    'build_oa_lookup.sql',
                    'build_load_postcode_estimates.sql',
                    'build_tables.sql',
                    'load_census_data.sql',
                    'load_data.sql',
                    'load_oa_lookup.sql']

success = load_data_pipeline(SOURCE_CSV_FOLDER, SQL_SCRIPTS_FOLDER, SQL_SCRIPT_FILES)
if success:
    print("Full data load process finished successfully.")
else:
    print("Full data load process encountered errors.")