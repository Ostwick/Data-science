import pandas as pd
import logging
import mysql.connector

# Configure logging
logging.basicConfig(filename="data_validation.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Database configuration
db_config = {
    'user': 'appsheet',
    'password': 'Sup@2023!',
    'host': '192.168.0.251',
    'port': 3306,
    'database': 'bi'
}

def fetch_data(query):
    """Fetches data from the MySQL database."""
    connection = mysql.connector.connect(**db_config)
    df = pd.read_sql(query, connection)
    connection.close()
    return df

def check_missing_values(df, required_columns):
    """Checks for missing values in required columns."""
    missing = df[required_columns].isnull().sum()
    for col, count in missing.items():
        if count > 0:
            logging.warning(f"Column '{col}' has {count} missing values.")

def check_unique_values(df, unique_columns):
    """Checks for duplicate values in columns that should be unique."""
    for col in unique_columns:
        duplicates = df[col].duplicated().sum()
        if duplicates > 0:
            logging.warning(f"Column '{col}' has {duplicates} duplicate values.")

def check_value_ranges(df, column_ranges):
    """Checks if numerical columns fall within predefined ranges."""
    for col, (min_val, max_val) in column_ranges.items():
        if not df[col].between(min_val, max_val).all():
            logging.warning(f"Column '{col}' has values out of range ({min_val}, {max_val}).")

def check_duplicates(df):
    """Checks for duplicate rows."""
    duplicate_rows = df.duplicated().sum()
    if duplicate_rows > 0:
        logging.warning(f"Dataset has {duplicate_rows} duplicate rows.")

def validate_data(df):
    """Runs all validation checks on the dataset."""
    required_columns = ['id', 'name', 'price']  
    unique_columns = ['id']  
    column_ranges = {'price': (0, 10000)}  

    check_missing_values(df, required_columns)
    check_unique_values(df, unique_columns)
    check_value_ranges(df, column_ranges)
    check_duplicates(df)
    logging.info("Data validation completed.")

# Example usage
if __name__ == "__main__":
    query = "SELECT * FROM historico"
    data = fetch_data(query)
    validate_data(data)
    print("Data validation completed. Check logs for details.")
