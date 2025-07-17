import pyodbc
import yaml
import os
from tqdm import tqdm
import pandas as pd

def load_private_config(path="C:/Users/gowth/Documents/rag_datasets/Openalex_dataprocessing/ETL_pipeline/config/config_private.yml"):
    with open(path, "r") as file:
        return yaml.safe_load(file)["database"]

def load_config(path="C:/Users/gowth/Documents/rag_datasets/Openalex_dataprocessing/ETL_pipeline/config/development.yml"):
    with open(path, "r") as file:
        return yaml.safe_load(file)

def get_connection():
    config = load_private_config()

    if config.get("trusted_connection", False):
        conn_str = (
            f"DRIVER={{{config['driver']}}};"
            f"SERVER={config['server']};"
            f"DATABASE={config.get('database', 'master')};"
            f"Trusted_Connection=yes;"
        )
    else:
        raise ValueError("Windows authentication requires 'trusted_connection: true'")

    return pyodbc.connect(conn_str, autocommit=False)

def execute_sql_file(connection, filepath):
    cursor = connection.cursor()

    with open(filepath, 'r', encoding='utf-8') as file:
        sql_script = file.read()

    for command in sql_script.split('\nGO'):
        if command.strip():
            try:
                cursor.execute(command)
                print("Command Excetution completed..")
            except Exception as e:
                print(f"❌ Error running command:\n{command}\n\nError: {e}\n")
            
    
    connection.commit()
    cursor.close()
    print("✅ SQL script executed successfully.")

def extract_table_name(csv_path):
    """
    Extracts the table name from a CSV file path.
    Example: 'C:/data/papers_main.csv' -> 'papers_main'
    """
    return os.path.splitext(os.path.basename(csv_path))[0]


def insert_csv_with_pyodbc(connection, csv_path_on_disk):
    table_name = extract_table_name(csv_path_on_disk)

    df = pd.read_csv(csv_path_on_disk)
    df = df.where(pd.notnull(df), None)  # Convert NaNs to None
    columns = df.columns.tolist()
    placeholders = ', '.join(['?'] * len(columns))
    column_list = ', '.join(f"[{col}]" for col in columns)
    insert_sql = f"INSERT INTO [{table_name}] ({column_list}) VALUES ({placeholders})"

    cursor = connection.cursor()
    tqdm_bar = tqdm(total=len(df), desc=f"Inserting into {table_name}", unit="rows")

    for idx, row in df.iterrows():
        try:
            cursor.execute(insert_sql, tuple(row))
        except Exception as e:
            print(f"\n❌ Insertion stopped at row {idx}:")
            print(f"Row content: {row.to_dict()}")
            print(f"Error: {e}")
            break  # Stop immediately on first error
        finally:
            tqdm_bar.update(1)

    tqdm_bar.close()
    connection.commit()
    cursor.close()


# Example usage
if __name__ == "__main__":
    conn = get_connection()
    config = load_config()
    file_path = config['database_paths']['sql_file']
    # execute_sql_file(conn, file_path)
    root_dir = config['database_paths']['csv_root']
    for filename in os.listdir(root_dir):
        if filename.lower().endswith(".csv"):
            csv_path = os.path.join(root_dir, filename)
            insert_csv_with_pyodbc(conn, csv_path)
        
    conn.close()
