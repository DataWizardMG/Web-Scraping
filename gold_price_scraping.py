import json
import requests
import logging

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler('/home/mahesh/airflow/logs/gold_price_scraping.log')]
)

def get_gold_price():
    """
    Fetches the latest gold price and percentage change in USD.

    This function sends a GET request to the gold price API, parses the response, and 
    returns the gold price in USD along with its percentage change.

    Returns:
        dict: A dictionary containing the gold price and its percentage change.
        Example: {"Gold Price (USD)": 1800.0, "Percentage Change": 1.5}
    
    Raises:
        Exception: If there is any error in fetching or processing the API response.
    """
    try:
        logging.info("Fetching gold price data from the API...")

        # API URL for fetching gold price in USD
        api_url = "https://data-asg.goldprice.org/dbXRates/USD"

        # Headers to mimic a real browser request
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0"
        }

        # Send a GET request to the API
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()  # Raise an error for bad responses (4xx, 5xx)

        # Parse the JSON response
        data = response.json()

        # Extract the gold price and percentage change
        gold_price = data["items"][0]["xauPrice"]
        gold_change = data["items"][0]["pcXau"]

        logging.info(f"Successfully retrieved Gold price data: Price = {gold_price}, Percentage Change = {gold_change}")
        
        return {"Gold Price (USD)": gold_price, "Percentage Change": gold_change}

    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        raise

    except KeyError as e:
        logging.error(f"Error parsing Gold price data: {e}")
        raise

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise
