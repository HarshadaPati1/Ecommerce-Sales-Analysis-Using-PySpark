# Ecommerce-Sales-Analysis-Using-PySpark

This project is designed to demonstrate end-to-end data engineering and analysis for an e-commerce dataset using PySpark on Databricks. It consists of:

- **Data Ingestion and Preprocessing:** Cleans and ingests raw data into Delta Lake tables.
- **Sales Analysis:** Performs KPIs and trend analysis.
- **Product Recommendation:** Implements a basic recommendation engine using product co-occurrence.
- **Data Visualization:** Visualizes key metrics using interactive Databricks charts and Matplotlib graphs.
## Project Structure

- `notebooks/` – Databricks notebooks for interactive analysis.
- `python_code/` – Python modules for production-level scripts.
- `README.md` – Project overview and setup instructions.

## Setup and Running

1. **Databricks:** Import the notebooks into your Databricks workspace and run them sequentially.
2. **Local Execution:** Install PySpark locally. Then, run the scripts:
   - Data ingestion:  
     ```bash
     python src/data_ingestion.py <raw_data_path> <delta_table_path>
     ```
   - Sales analysis:  
     ```bash
     python src/sales_analysis.py
     ```
   - Recommendation:  
     ```bash
     python src/recommendation.py
     ```
   - Data visualization
     ```bash
     python src/data_visualization.py
     ```

## Requirements

- Python 3.x
- Apache Spark 3.x / PySpark
- Databricks environment for notebooks

