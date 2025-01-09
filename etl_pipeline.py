import pandas as pd
import mysql.connector
from mysql.connector import Error

# Database Configuration
db_config_source = {
    'user': 'user',
    'password': 'password',
    'host': 'address',
    'port': 3306,
    'database': 'source_db'
}

db_config_destination = {
    'user': 'user',
    'password': 'password',
    'host': 'address',
    'port': 3306,
    'database': 'destination_db'
}

# Extract Function
def extract_data(query, db_config):
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print("Connected to source database.")
            data = pd.read_sql(query, connection)
            return data
    except Error as e:
        print(f"Error while connecting to database: {e}")
    finally:
        if connection.is_connected():
            connection.close()
            print("Source database connection closed.")

# Transform Function
def transform_data(data):
    print("Transforming data...")
    # Example transformation: Rename columns and filter data
    data.columns = [col.lower() for col in data.columns]
    transformed_data = data.dropna() 
    print("Data transformation complete.")
    return transformed_data

# Load Function
def load_data(data, table_name, db_config):
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print("Connected to destination database.")
            cursor = connection.cursor()
            for i, row in data.iterrows():
                placeholders = ", ".join(["%s"] * len(row))
                columns = ", ".join(row.index)
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                cursor.execute(sql, tuple(row))
            connection.commit()
            print(f"Data successfully loaded into {table_name}.")
    except Error as e:
        print(f"Error while connecting to database: {e}")
    finally:
        if connection.is_connected():
            connection.close()
            print("Destination database connection closed.")

# ETL Process
def etl_process():
    # Define the query for extraction
    query = "SELECT * FROM source_table"

    # Extract
    data = extract_data(query, db_config_source)
    if data is None or data.empty:
        print("No data extracted. Exiting ETL process.")
        return

    # Transform
    transformed_data = transform_data(data)

    # Load
    load_data(transformed_data, "destination_table", db_config_destination)

# Run the ETL process
if __name__ == "__main__":
    etl_process()
