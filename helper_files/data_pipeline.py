# data_pipeline.py

import os
import shutil
import mysql.connector
import json
from mysql.connector import Error
from tqdm import tqdm
import helper_files.logger as logger
import helper_files.helper as helper

def connect_to_mysql(config_data):
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
        logger.log(f"Successfully connected to MySQL database: {config_data['database']}")
        return conn, cursor
    except Error as err:
        logger.log(f"Error connecting to MySQL database: {err}")
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
        logger.log(f"Error: Config file '{config_path}' not found.")
        return None
    except json.JSONDecodeError:
        logger.log(f"Error: Could not decode JSON from '{config_path}'. Check file format.")
        return None

    conn, cursor = connect_to_mysql(config_data)
    if not conn or not cursor:
        return None

    try:
        query = "SHOW VARIABLES LIKE 'secure_file_priv';"
        cursor.execute(query)
        result = cursor.fetchone()

        if result and 'Value' in result:
            return result['Value']
        else:
            logger.log("Variable 'secure_file_priv' not found or is NULL.")
            return None

    except Error as err:
        logger.log("Database error:", err)
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
    logger.log(f"\n--- Copying CSV Files from '{source_folder}' to '{destination_folder}' ---")
    moved_files = []

    project_root = helper.get_project_root()
    source_folder = os.path.join(project_root, source_folder)

    try:
        if not os.path.exists(source_folder):
            logger.log(f"Error: Source folder '{source_folder}' does not exist. Please check the path.")
            return False, []

        if not os.path.exists(destination_folder):
            logger.log(f"Warning: Destination folder '{destination_folder}' does not exist. Please create it or verify your secure_file_priv setting.")
            return False, []

        source_folder = os.path.abspath(source_folder)
        destination_folder = os.path.abspath(destination_folder)

        all_files_exist = True
        
        for filename in tqdm(os.listdir(source_folder), desc="Checking files"):
            if filename.endswith(".csv"):
                source_path = os.path.join(source_folder, filename)
                destination_path = os.path.join(destination_folder, filename)
                
                if os.path.exists(destination_path):
                    logger.log(f"Skipped: '{filename}' already exists in '{destination_folder}'")
                    continue
                else:
                    all_files_exist = False
                
                try:
                    shutil.copy2(source_path, destination_path)
                    logger.log(f"Copied: '{filename}' to '{destination_path}'")
                    moved_files.append(filename)
                except shutil.Error as se:
                    logger.log(f"Shutil error moving '{filename}': {se}")
                    return False, []
                except OSError as e:
                    logger.log(f"OS error moving '{filename}': {e}")
                    return False, []
    except OSError as e:
        logger.log(f"Error accessing source folder '{source_folder}': {e}")
        return False, []
    
    return True, moved_files

def run_sql_script(sql_script_path, config_data, secure_priv_path_for_sql=None):
    """
    Reads and executes SQL statements from a file.
    Assumes SQL statements are separated by semicolons.
    If secure_priv_path_for_sql is provided, it replaces '{SECURE_PRIV_PATH}' placeholder.
    """
    logger.log(f"\n--- Running SQL Script: '{os.path.basename(sql_script_path)}' ---")
    conn, cursor = connect_to_mysql(config_data)
    if not conn or not cursor:
        logger.log(f"Skipping script '{os.path.basename(sql_script_path)}' due to database connection error.")
        return False

    try:
        with open(sql_script_path, 'r') as file:
            sql_script_content = file.read()

        if secure_priv_path_for_sql:
            sql_safe_secure_priv_path = secure_priv_path_for_sql.replace('\\', '/')
            sql_script_content = sql_script_content.replace('{SECURE_PRIV_PATH}', sql_safe_secure_priv_path)
            logger.log(f"  Placeholder '{{SECURE_PRIV_PATH}}' replaced with '{sql_safe_secure_priv_path}'")

        statements = [s.strip() for s in sql_script_content.split(';') if s.strip()]

        if not statements:
            logger.log(f"No SQL statements found in '{os.path.basename(sql_script_path)}'.")
            return True

        for statement in tqdm(statements, desc="Running sql"):
            try:
                cursor.execute(statement)
            except Error as err:
                logger.log(f"  Error executing statement: {statement[:100]}{'...' if len(statement) > 100 else ''}")
                logger.log(f"  MySQL Error: {err}")
                conn.rollback()
                return False
        conn.commit()
        logger.log(f"Finished SQL script: '{os.path.basename(sql_script_path)}' (Committed)")
        return True
    except FileNotFoundError:
        logger.log(f"Error: SQL script not found: '{sql_script_path}'")
        return False
    except Exception as e:
        logger.log(f"An unexpected error occurred while reading or executing SQL script '{sql_script_path}': {e}")
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def load_data_pipeline(source_csv_folder, sql_scripts_folder, sql_script_files, config_path=helper.affix_root_path("config.json")):
    """
    Orchestrates the entire data loading pipeline:
    1. Gets secure_file_priv path from MySQL.
    2. Moves CSV files to the secure_file_priv directory.
    3. Runs specified SQL scripts (table creation, data loading),
       replacing '{SECURE_PRIV_PATH}' placeholder in SQL if present.
    """
    logger.log("\n--- Starting Data Loading Pipeline ---")

    try:
        with open(config_path) as json_file:
            config_data = json.load(json_file)
    except FileNotFoundError:
        logger.log(f"Error: Config file '{config_path}' not found. Exiting pipeline.")
        return False
    except json.JSONDecodeError:
        logger.log(f"Error: Could not decode JSON from '{config_path}'. Check file format. Exiting pipeline.")
        return False

    secure_priv_path = return_secure_priv(config_path)
    if not secure_priv_path:
        logger.log("Failed to retrieve secure_file_priv path. Exiting pipeline.")
        return False
    logger.log(f"\nMySQL secure_file_priv path: '{secure_priv_path}'")

    success_copy, moved_csv_filenames = move_csv_files(source_csv_folder, secure_priv_path)
    if not success_copy:
        logger.log("Failed to access folders or copy files. Exiting pipeline.")
        return False
    if not moved_csv_filenames:
        logger.log("Note: No new CSV files were moved as all were already present in the secure directory. Proceeding with pipeline.")

    all_scripts_succeeded = True
    for script_name in tqdm(sql_script_files, desc="SQL Files run"):
        script_path = os.path.join(sql_scripts_folder, script_name)
        success = run_sql_script(script_path, config_data, secure_priv_path)
        if not success:
            all_scripts_succeeded = False
            logger.log(f"Pipeline stopped due to failure in script: '{script_name}'")
            break

    if all_scripts_succeeded:
        logger.log("\n--- Data Loading Pipeline Completed Successfully! ---")
        return True
    else:
        logger.log("\n--- Data Loading Pipeline Failed. ---")
        return False



def run_initial_build():
    """
    Runs the data pipeline for the inital build of all the database tables.
    """
    SOURCE_CSV_FOLDER = helper.affix_root_path('data')

    SQL_SCRIPTS_FOLDER = helper.affix_root_path('sql_scripts')

    SQL_SCRIPT_FILES = ['build_census_tables.sql',
                        'build_oa_lookup.sql',
                        'build_load_postcode_estimates.sql',
                        'build_tables.sql',
                        'load_census_data.sql',
                        'load_data.sql',
                        'load_oa_lookup.sql']

    success = load_data_pipeline(SOURCE_CSV_FOLDER, SQL_SCRIPTS_FOLDER, SQL_SCRIPT_FILES)
    if success:
        logger.log("Full data load process finished successfully.")
    else:
        logger.log("Full data load process encountered errors.")