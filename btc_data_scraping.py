import json
import requests
import logging

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler('/home/mahesh/airflow/logs/btc_data_scraping.log')]
)

def get_btc_data():
    """
    Fetches the latest Bitcoin price and market capitalization from the CoinMarketCap API.
    
    This function sends a GET request to the CoinMarketCap API to retrieve the latest Bitcoin data
    in USD. It returns the price and market capitalization of Bitcoin.

    Returns:
        dict: A dictionary containing the Bitcoin price and market capitalization.
        Example: {"btc_price": 50000.0, "btc_market_cap": 900000000000.0}

    Raises:
        Exception: If there is any error in fetching or processing the API response.
    """
    url = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest'
    parameters = {
        'symbol': 'BTC',
        'convert': 'USD'
    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': "da631029-9307-4e0e-983b-07d8ee57265c"
    }

    try:
        logging.info("Fetching Bitcoin data from CoinMarketCap API...")
        
        # Send a GET request to the API
        response = requests.get(url, headers=headers, params=parameters)
        response.raise_for_status()  # Raise an error for bad responses (4xx, 5xx)

        # Parse the JSON response
        btc_data = response.json()

        # Extract the price and market cap of Bitcoin
        btc_price = btc_data['data']['BTC'][0]['quote']['USD']['price']
        btc_market_cap = btc_data['data']['BTC'][0]['quote']['USD']['market_cap']

        logging.info(f"Successfully retrieved BTC data: Price = {btc_price}, Market Cap = {btc_market_cap}")
        
        return {"btc_price": btc_price, "btc_market_cap": btc_market_cap}

    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        raise

    except KeyError as e:
        logging.error(f"Error parsing BTC data: {e}")
        raise

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise


