import pandas as pd
import numpy as np
import logging
import mysql.connector

# Configurar o logger para registrar ações e erros
logging.basicConfig(
    filename='data_cleaning.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuração do banco de dados
db_config = {
    'user': 'usuario',
    'password': 'senha',
    'host': 'endereco',
    'port': 9999,
    'database': 'base'
}

def fetch_data_from_db(query):
    """
    Busca dados do banco de dados MySQL com base em uma consulta SQL.

    Args:
        query (str): Consulta SQL para buscar os dados.

    Returns:
        pd.DataFrame: Dados retornados em formato de DataFrame.
    """
    try:
        conn = mysql.connector.connect(**db_config)
        df = pd.read_sql(query, conn)
        conn.close()
        logging.info("Dados buscados com sucesso do banco de dados.")
        return df
    except Exception as e:
        logging.error(f"Erro ao buscar dados do banco de dados: {e}")
        raise

def save_data_to_db(df, table_name):
    """
    Salva os dados processados de volta no banco de dados.

    Args:
        df (pd.DataFrame): DataFrame a ser salvo.
        table_name (str): Nome da tabela no banco de dados.

    Returns:
        None
    """
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Criar instruções SQL para inserir os dados
        for _, row in df.iterrows():
            placeholders = ', '.join(['%s'] * len(row))
            columns = ', '.join(row.index)
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(row))

        conn.commit()
        conn.close()
        logging.info(f"Dados salvos com sucesso na tabela {table_name}.")
    except Exception as e:
        logging.error(f"Erro ao salvar dados no banco de dados: {e}")
        raise

def handle_missing_values(df, strategy='mean', columns=None):
    """
    Trata valores ausentes em um DataFrame.

    Args:
        df (pd.DataFrame): DataFrame a ser processado.
        strategy (str): Estratégia para lidar com valores ausentes ('mean', 'median', 'mode', 'drop').
        columns (list): Lista de colunas para aplicar o tratamento (se None, aplica a todas as colunas).

    Returns:
        pd.DataFrame: DataFrame com valores ausentes tratados.
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
            logging.error(f"Estratégia desconhecida: {strategy}")

        logging.info(f"Valores ausentes tratados usando a estratégia: {strategy}")
        return df
    except Exception as e:
        logging.error(f"Erro ao lidar com valores ausentes: {e}")
        raise

def format_data(df, columns_format):
    """
    Formata dados em colunas específicas de acordo com os formatos fornecidos.

    Args:
        df (pd.DataFrame): DataFrame a ser processado.
        columns_format (dict): Dicionário onde a chave é o nome da coluna e o valor é o tipo desejado ('datetime', 'str', etc.).

    Returns:
        pd.DataFrame: DataFrame com dados formatados.
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
                logging.warning(f"Formato desconhecido: {fmt} para coluna: {column}")

        logging.info("Dados formatados com sucesso.")
        return df
    except Exception as e:
        logging.error(f"Erro ao formatar dados: {e}")
        raise

def detect_outliers(df, columns, method='zscore', threshold=3):
    """
    Detecta e remove outliers de colunas numéricas.

    Args:
        df (pd.DataFrame): DataFrame a ser processado.
        columns (list): Lista de colunas para verificar outliers.
        method (str): Método para detectar outliers ('zscore' ou 'iqr').
        threshold (float): Limite para identificar outliers.

    Returns:
        pd.DataFrame: DataFrame com outliers removidos.
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
            logging.warning(f"Método desconhecido para detectar outliers: {method}")

        logging.info("Outliers detectados e removidos com sucesso.")
        return df
    except Exception as e:
        logging.error(f"Erro ao detectar/remover outliers: {e}")
        raise

def clean_data_pipeline(df, missing_strategy='mean', columns_format=None, outlier_columns=None, outlier_method='zscore'):
    """
    Pipeline principal para limpeza de dados.

    Args:
        df (pd.DataFrame): DataFrame a ser limpo.
        missing_strategy (str): Estratégia para lidar com valores ausentes.
        columns_format (dict): Dicionário de formatações para colunas.
        outlier_columns (list): Lista de colunas para verificar outliers.
        outlier_method (str): Método para detectar outliers ('zscore' ou 'iqr').

    Returns:
        pd.DataFrame: DataFrame limpo.
    """
    try:
        # Tratamento de valores ausentes
        df = handle_missing_values(df, strategy=missing_strategy)

        # Formatação de dados
        if columns_format:
            df = format_data(df, columns_format)

        # Detecção e remoção de outliers
        if outlier_columns:
            df = detect_outliers(df, outlier_columns, method=outlier_method)

        logging.info("Pipeline de limpeza de dados concluído com sucesso.")
        return df
    except Exception as e:
        logging.error(f"Erro na pipeline de limpeza de dados: {e}")
        raise

# Exemplo de uso
if __name__ == "__main__":
    query = "SELECT * FROM historico" #Tabela que uso no trabalho
    
    # Buscar dados do banco de dados
    df = fetch_data_from_db(query)

    # Processar os dados
    cleaned_df = clean_data_pipeline(
        df,
        missing_strategy='mean',
        columns_format={'data_coluna': 'datetime', 'valor': 'float'},
        outlier_columns=['valor'],
        outlier_method='zscore'
    )

    # Salvar os dados limpos de volta no banco de dados
    save_data_to_db(cleaned_df, 'historico_limpo')
