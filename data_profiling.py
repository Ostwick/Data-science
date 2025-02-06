import pandas as pd
import numpy as np
import mysql.connector

# Database configuration
db_config_source = {
    'user': 'user',
    'password': 'password',
    'host': 'address',
    'port': 3306,
    'database': 'source_db'
}

class DataProfiler:
    def __init__(self, dataframe: pd.DataFrame):
        self.df = dataframe

    @staticmethod
    def fetch_data(query: str):
        """Fetches data from the database."""
        connection = mysql.connector.connect(**db_config)
        df = pd.read_sql(query, connection)
        connection.close()
        return df

    def summary_statistics(self):
        """Returns summary statistics of numerical columns."""
        return self.df.describe()

    def missing_values(self):
        """Returns the count of missing values in each column."""
        return self.df.isnull().sum()

    def detect_outliers(self, method='zscore', threshold=3):
        """Detects outliers using Z-score or IQR method."""
        if method == 'zscore':
            z_scores = np.abs((self.df - self.df.mean()) / self.df.std())
            return (z_scores > threshold).sum()
        elif method == 'iqr':
            Q1 = self.df.quantile(0.25)
            Q3 = self.df.quantile(0.75)
            IQR = Q3 - Q1
            return ((self.df < (Q1 - 1.5 * IQR)) | (self.df > (Q3 + 1.5 * IQR))).sum()
        else:
            raise ValueError("Method must be 'zscore' or 'iqr'")

    def generate_report(self):
        """Generates a full profiling report."""
        report = {
            'summary_statistics': self.summary_statistics(),
            'missing_values': self.missing_values(),
            'outliers_zscore': self.detect_outliers(method='zscore'),
            'outliers_iqr': self.detect_outliers(method='iqr')
        }
        return report

# Example usage
if __name__ == "__main__":
    query = "SELECT * FROM historico"
    data = DataProfiler.fetch_data(query)
    profiler = DataProfiler(data)
    print(profiler.generate_report())
