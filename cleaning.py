import pandas as pd
import numpy as np
import logging
import mysql.connector

# Logger configuration
logging.basicConfig(
    filename='data_cleaning.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Database configuration
db_config = {
    'user': 'usuario',
    'password': 'senha',
    'host': 'endereco',
    'port': 9999,
    'database': 'base'
}

def fetch_data_from_db(query):
    """
    Fetches data from the MySQL database based on an SQL query.

    Args:
        query (str): Query to fetch the data.

    Returns:
        pd.DataFrame: Data returned in DataFrame format.

.
    """
    try:
        conn = mysql.connector.connect(**db_config)
        df = pd.read_sql(query, conn)
        conn.close()
        logging.info("Data successfully fetched from database")
        return df
    except Exception as e:
        logging.error(f"Error fetching data from database: {e}")
        raise

def save_data_to_db(df, table_name):
    """
    Saves the processed data back to the database.

    Args:
        df (pd.DataFrame): DataFrame to be saved.
        table_name (str): Name of the table in the database.

    Returns:
        None
    """
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Create SQL statements to insert data
        for _, row in df.iterrows():
            placeholders = ', '.join(['%s'] * len(row))
            columns = ', '.join(row.index)
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(row))

        conn.commit()
        conn.close()
        logging.info(f"Data successfully saved in table {table_name}.")
    except Exception as e:
        logging.error(f"Error saving data to database: {e}")
        raise

def handle_missing_values(df, strategy='mean', columns=None):
    """
    Handles missing values ​​in a DataFrame.

    Args:
        df (pd.DataFrame): DataFrame to be processed.
        strategy (str): Strategy for dealing with missing values ('mean', 'median', 'mode', 'drop').
        columns (list): List of columns to apply the treatment  (se None, aplica a todas as colunas).

    Returns:
        pd.DataFrame: DataFrame with missing values ​​handled.
    """
    try:
        if columns is None:
            columns = df.columns

        if strategy == 'mean':
            for col in columns:
                if df[col].dtype in [np.float64, np.int64]:
                    df[col].fillna(df[col].mean(), inplace=True)
        elif strategy == 'median':
            for col in columns:
                if df[col].dtype in [np.float64, np.int64]:
                    df[col].fillna(df[col].median(), inplace=True)
        elif strategy == 'mode':
            for col in columns:
                df[col].fillna(df[col].mode()[0], inplace=True)
        elif strategy == 'drop':
            df.dropna(subset=columns, inplace=True)
        else:
            logging.error(f"Unknown strategy: {strategy}")

        logging.info(f"Missing values ​​handled using the strategy: {strategy}")
        return df
    except Exception as e:
        logging.error(f"Error handling missing values: {e}")
        raise

def format_data(df, columns_format):
    """
    Formats data in specific columns according to the provided formats.

    Args:
        df (pd.DataFrame): DataFrame to be processed.
        columns_format (dict): Dictionary where the key is the column name and the value is the desired type ('datetime', 'str', etc.).

    Returns:
        pd.DataFrame: DataFrame with formatted data.
    """
    try:
        for column, fmt in columns_format.items():
            if fmt == 'datetime':
                df[column] = pd.to_datetime(df[column], errors='coerce')
            elif fmt == 'str':
                df[column] = df[column].astype(str)
            elif fmt == 'float':
                df[column] = pd.to_numeric(df[column], errors='coerce')
            elif fmt == 'int':
                df[column] = pd.to_numeric(df[column], errors='coerce', downcast='integer')
            else:
                logging.warning(f"Unknown format: {fmt} for column: {column}")

        logging.info("Data formatted successfully.")
        return df
    except Exception as e:
        logging.error(f"Error formatting data: {e}")
        raise

def detect_outliers(df, columns, method='zscore', threshold=3):
    """
    Detects and removes outliers from numeric columns.

    Args:
        df (pd.DataFrame): DataFrame to be processed.
        columns (list): List of columns to check for outliers.
        method (str): MMethod to detect outliers ('zscore' ou 'iqr').
        threshold (float): Limit to identify outliers.

    Returns:
        pd.DataFrame: DataFrame with outliers removed.
    """
    try:
        if method == 'zscore':
            for col in columns:
                z_scores = (df[col] - df[col].mean()) / df[col].std()
                df = df[abs(z_scores) < threshold]
        elif method == 'iqr':
            for col in columns:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                df = df[(df[col] >= Q1 - 1.5 * IQR) & (df[col] <= Q3 + 1.5 * IQR)]
        else:
            logging.warning(f"Unknown method to detect outliers: {method}")

        logging.info("Outliers successfully detected and removed.")
        return df
    except Exception as e:
        logging.error(f"Error detecting/removing outliers: {e}")
        raise

def clean_data_pipeline(df, missing_strategy='mean', columns_format=None, outlier_columns=None, outlier_method='zscore'):
    """
    Main pipeline for data cleaning.

    Args:
        df (pd.DataFrame): DataFrame to be cleaned.
        missing_strategy (str): Strategy for dealing with missing values.
        columns_format (dict): Dictionary of column formatting.
        outlier_columns (list): List of columns to check for outliers.
        outlier_method (str): Method to detect outliers  ('zscore' ou 'iqr').

    Returns:
        pd.DataFrame: Clean DataFrame.
    """
    try:
        # Handling missing values
        df = handle_missing_values(df, strategy=missing_strategy)

        # Data formatting
        if columns_format:
            df = format_data(df, columns_format)

        # Outlier detection and removal
        if outlier_columns:
            df = detect_outliers(df, outlier_columns, method=outlier_method)

        logging.info("Data cleansing pipeline completed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error in data cleaning pipeline: {e}")
        raise

# Example
if __name__ == "__main__":
    query = "SELECT * FROM historico" #Tabela que uso no trabalho
    
    # Fetch data from database
    df = fetch_data_from_db(query)

    # Process the data
    cleaned_df = clean_data_pipeline(
        df,
        missing_strategy='mean',
        columns_format={'data_coluna': 'datetime', 'valor': 'float'},
        outlier_columns=['valor'],
        outlier_method='zscore'
    )

    # Save the cleaned data back to the database
    save_data_to_db(cleaned_df, 'historico_limpo')
