# CoinGecko Crypto API Data

This project uses the [CoinGecko API](https://docs.coingecko.com/v3.0.1/reference/introduction) cryptocurrency data. The analysis covers key metrics and insights, with data visualizations prepared using Tableau.

## Repository Structure

Below is an explanation of the key files and folders in this repository:

### Configuration
- **`.env.template`**:  
  A template for the environment configuration file.  
  **Steps to Use**:
  1. Copy this file and rename it as `.env`.
  2. Update the values inside the `.env` file with your specific configurations (e.g., API keys).

### Scripts and Folders
- **`fetch_data/`**:  
  Contains scripts for fetching data from the CoinGecko API using various endpoints.
  
- **`bi_function.py`**:  
  A collection of utility functions used across the project.

- **`cg_data_a_merge_init.py`**:  
  The script to initialize data fetching and merge the results into a single table.

### Analysis
- **`cg_data_b_analysis.ipynb`**:  
  A Jupyter Notebook for analyzing the processed data.  
  **Key Notes**:
  - This notebook uses a **static snapshot table** for analysis, ensuring consistency in observations.  
    - The static table represents a snapshot of the rolling data for the past 365 days at the time of analysis.  
    - The original table is dynamic and updates every time the data-fetching scripts run.
  - If you choose to re-run the notebook with updated data, the key observations in the notebook may not match the graphs or tables due to differences in the time period of the data.