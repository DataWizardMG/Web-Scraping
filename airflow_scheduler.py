import sys
import os
import logging
import pandas as pd
import matplotlib.pyplot as plt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

sys.path.append('/home/mahesh/airflow/dags')

# Import custom modules for data scraping and preprocessing
from btc_data_scraping import get_btc_data
from gold_price_scraping import get_gold_price
from preprocess_data import clean_data, store_data

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler('/home/mahesh/airflow/logs/airflow_scheduler.log')]
)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG with a daily schedule at 10 AM
dag = DAG(
    'airflow_scheduler',
    default_args=default_args,
    description='DAG to scrape BTC and gold prices daily at 10 AM and calculate correlation',
    schedule_interval='0 10 * * *',  # Runs every day at 10 AM
)

def scrape_data(**kwargs):
    """
    Scrapes Bitcoin and Gold price data, cleans it, and stores it in a CSV file.
    
    This task fetches the latest BTC and Gold prices, preprocesses the data,
    and stores it in the specified location.
    """
    try:
        logging.info("Starting data scraping task...")
        
        # Fetch data
        btc_data = get_btc_data()
        gold_data = get_gold_price()
        logging.info("Scraped BTC and Gold data successfully.")
        
        # Preprocess the data
        df = clean_data(btc_data, gold_data)
        logging.info("Data cleaned and preprocessed successfully.")
        
        # Store the data
        store_data(df)
        logging.info("Data stored successfully.")
        
    except Exception as e:
        logging.error(f"Error in scrape_data: {e}")
        raise

def calculate_correlation():
    """
    Calculates the correlation between Bitcoin and Gold prices and logs the result.

    This task reads the preprocessed CSV file containing Bitcoin and Gold prices,
    calculates the correlation between the two, and appends the result to a text file.
    """
    try:
        logging.info("Starting correlation calculation task...")
        
        # Load the data from CSV
        df = pd.read_csv('/home/mahesh/airflow/crypto_gold_data.csv')
        df.dropna(inplace=True)  # Drop rows with missing values
        
        # Calculate correlation
        correlation = df['BTC Price'].corr(df['Gold Price'])
        logging.info(f"Correlation between BTC and Gold prices: {correlation}")
        
        # Store the correlation result in a text file
        with open('/home/mahesh/airflow/correlation_results.txt', 'a') as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Correlation: {correlation}\n")
        logging.info("Correlation result written to file.")
        
    except Exception as e:
        logging.error(f"Error in calculate_correlation: {e}")
        raise

def plot_correlation_over_time():
    """
    Plots the correlation between Bitcoin and Gold prices over time and saves the plot as an image.

    This task reads the historical correlation data and plots it as a time series graph.
    """
    try:
        logging.info("Starting correlation plotting task...")

        # Load the correlation results from the text file
        df = pd.read_csv('./correlation_results.txt', delimiter=' - ', names=['Datetime', 'Correlation'])
        df['Datetime'] = pd.to_datetime(df['Datetime'])
        df['Correlation'] = df['Correlation'].str.replace('Correlation: ', '').astype(float)
        # Remove duplicate timestamps, if any
        df.drop_duplicates(subset=['Datetime'], inplace=True)

        # Plot the correlation over time
        plt.figure(figsize=(12, 6))
        plt.plot(df['Datetime'], df['Correlation'], marker='o', linestyle='-', color='b', label='BTC vs Gold Correlation')
        plt.title('Correlation Between BTC and Gold Prices Over Time', fontsize=16)
        plt.xlabel('Time', fontsize=12)
        plt.ylabel('Correlation', fontsize=12)
        plt.xticks(rotation=45)  # Rotate the x-axis labels for better readability
        plt.grid(True)
        plt.legend()

        # Save the plot as a PNG file
        plt.tight_layout()  # Adjust layout for better appearance
        plt.savefig('./btc_gold_correlation_over_time.png')
        logging.info("Correlation plot saved successfully.")

    except Exception as e:
        logging.error(f"Error in plot_correlation_over_time: {e}")
        raise

# Define the task to scrape and store data
scraping_task = PythonOperator(
    task_id='scrape_and_store',
    python_callable=scrape_data,
    dag=dag,
)

# Define the task to calculate correlation between BTC and Gold prices
calculate_correlation_task = PythonOperator(
    task_id='calculate_correlation',
    python_callable=calculate_correlation,
    dag=dag,
)

# Define the task to plot the correlation between BTC and Gold prices over time
plot_correlation_task = PythonOperator(
    task_id='plot_correlation',
    python_callable=plot_correlation_over_time,
    dag=dag,
)

# Set the task dependencies
scraping_task >> calculate_correlation_task >> plot_correlation_task