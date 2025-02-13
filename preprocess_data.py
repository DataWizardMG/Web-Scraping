import pandas as pd
import time
import logging

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler('/home/mahesh/airflow/logs/data_preprocessing.log')]
)

def clean_data(btc_data, gold_data):
    """
    Cleans and processes the scraped Bitcoin and Gold price data.

    This function consolidates Bitcoin and Gold data into a Pandas DataFrame, adds the current timestamp,
    and fills any missing values with 0 to ensure data consistency.

    Args:
        btc_data (dict): A dictionary containing Bitcoin price and market capitalization.
            Example: {"btc_price": 50000.0, "btc_market_cap": 900000000000.0}
        gold_data (dict): A dictionary containing the gold price and percentage change.
            Example: {"Gold Price (USD)": 1800.0, "Percentage Change": 1.5}

    Returns:
        pd.DataFrame: A DataFrame containing the consolidated and cleaned data.
    """
    try:
        # Get the current timestamp
        current_time = time.strftime('%Y-%m-%d %H:%M:%S')

        # Log the received data
        logging.info(f"Cleaning data - BTC: {btc_data}, Gold: {gold_data}")

        # Create a DataFrame with the required columns
        df = pd.DataFrame({
            "Time": [current_time],
            "BTC Price": [btc_data['btc_price']],
            "BTC Market Cap": [btc_data['btc_market_cap']],
            "Gold Price": [gold_data['Gold Price (USD)']],
            "Gold Change": [gold_data['Percentage Change']]
        })

        # Handle missing values by filling them with 0
        df.fillna(0, inplace=True)

        logging.info("Data cleaned and missing values handled.")

        return df

    except KeyError as e:
        logging.error(f"Error in data cleaning due to missing key: {e}")
        raise

    except Exception as e:
        logging.error(f"Unexpected error in data cleaning: {e}")
        raise
