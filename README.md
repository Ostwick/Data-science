# Data Science Tools Repository

Welcome to the Data Science Tools repository! This repository is a collection of codes and scripts designed to address common challenges in data science and data engineering projects. The goal is to provide modular, reusable, and easy-to-implement solutions while also engaging and supporting the development of my intern.

## Repository Structure

Below is a list of the planned codes for this repository:

1. **Automated Data Cleaning Pipeline**  
   A pipeline to automatically clean datasets by handling missing values, formatting data, and detecting outliers.

2. **Simple ETL (Extract, Transform, Load) Pipeline**  
   A basic pipeline for extracting, transforming, and loading data between different sources and destinations.

3. **Python Package for Data Profiling**  
   A tool to perform exploratory data analysis and generate detailed data profiling reports.

4. **CLI Tool for Setting Up Data Science Project Environments**  
   A command-line utility to quickly set up data science project environments with the required structure and dependencies.

5. **Automated Data Validation Pipeline**  
   A pipeline to verify data consistency, validity, and integrity.

6. **Performance Profiler for Python Functions**  
   A tool to measure and optimize the performance of Python functions.

7. **Data Version Control Tool for Machine Learning Models**  
   A solution to track changes in the data used for machine learning experiments.

---

## How to Use

### 1. Automated Data Cleaning Pipeline
The file `cleaning.py` contains the first code of this repository:

- **Features:**  
  - Handles missing values using strategies such as mean, median, mode, or row exclusion.
  - Formats data into specific types like `datetime` or `float`.
  - Detects and removes outliers using methods like Z-score and IQR.
  - Integrates with MySQL databases for reading and writing processed data.

- **How to Run:**
  1. Configure the database credentials in the `db_config` dictionary.
  2. Run the `cleaning.py` file with Python.
  3. The script fetches data from the `data` table, processes it, and saves the results in the `data_clean` table.

---

## Contributions
Contributions are welcome! Feel free to open issues or submit pull requests to improve or add new features.

## License
This repository is licensed under the MIT License. See the `LICENSE` file for more details.

