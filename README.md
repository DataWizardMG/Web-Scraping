
# Bitcoin and Gold Price Data Scraping, Correlation Calculation, and Visualization

This project is designed to scrape Bitcoin and Gold price data at regular intervals, clean and store the data, calculate the correlation between the two assets, and visualize the correlation trend over time. The Airflow DAG orchestrates the tasks, which include data scraping, preprocessing, correlation calculation, and visualization.

## Project Structure

The project consists of the following main components:

### 1. `airflow_scheduler.py`

This file defines the Airflow Directed Acyclic Graph (DAG) that schedules and runs the following tasks:
- **Scrape Bitcoin and Gold prices** daily at 10 AM.
- **Preprocess the data** to clean and structure it.
- **Calculate the correlation** between Bitcoin and Gold prices.
- **Plot the correlation** between Bitcoin and Gold prices over time.

#### Key Features:
- The DAG is scheduled to run daily at 10 AM.
- Uses Python Operators to run the scraping, correlation calculation, and plotting tasks.
- Incorporates logging to monitor task execution and potential errors.
- Visualizes the correlation trend between Bitcoin and Gold prices over time using Matplotlib.

### 2. `btc_data_scraping.py`

This script is responsible for fetching the latest Bitcoin price and market capitalization using the CoinMarketCap API.

#### Key Features:
- Fetches **Bitcoin price** and **market capitalization**.
- Uses the CoinMarketCap API and handles potential API errors and response issues.
- Returns the data in a structured format for further processing.
- Logs any failures during API requests or data processing.

### 3. `gold_price_scraping.py`

This script fetches the current gold price and its percentage change using an API that provides gold prices in USD.

#### Key Features:
- Retrieves the **gold price** in USD and its **percentage change**.
- Handles errors like request failures and logs the results.
- Returns the data in a dictionary for further processing in the Airflow DAG.

### 4. `preprocess_data.py`

This script handles the preprocessing of Bitcoin and Gold price data by consolidating it into a single dataset.

#### Key Features:
- Takes Bitcoin and Gold data and merges it into a Pandas DataFrame.
- Adds a **timestamp** to each record for accurate tracking.
- Cleans the data by handling missing values.
- Returns the preprocessed data as a DataFrame for storage and analysis.

### 5. **Correlation Plotting**

The project also includes functionality to plot the correlation between Bitcoin and Gold prices over time using Matplotlib.

#### Key Features:
- **Plot Correlation Trend**: The correlation between Bitcoin and Gold prices over time is plotted and saved as a PNG file.
- **Time Series Visualization**: The plot shows how the correlation between the two assets changes over time.
- **File Output**: The correlation plot is saved as `btc_gold_correlation_over_time.png` for easy reference.

### How to Use

1. **Set up Airflow**: Make sure Airflow is properly installed and configured.
2. **Add the DAG**: Place `airflow_scheduler.py` in your Airflow DAGs directory.
3. **API Keys**: Ensure you have a valid CoinMarketCap API key and modify the `btc_data_scraping.py` with your API key.
4. **Run the DAG**: Start the Airflow web server and scheduler, and trigger the DAG from the UI.
5. **Visualization**: After the DAG runs, the correlation plot will be saved as an image file in the designated directory (`/home/mahesh/airflow/plots`).

### Logging and Error Handling

- Each script logs the key stages of the process (data fetching, cleaning, correlation calculation, plotting).
- Errors are logged to specific log files (e.g., `btc_data_scraping.log`, `gold_price_scraping.log`, `data_preprocessing.log`) for easy troubleshooting.

### Requirements

- **Airflow**: Used for task scheduling and DAG orchestration.
- **Pandas**: For data manipulation and cleaning.
- **Requests**: To handle API requests for Bitcoin and Gold data.
- **Matplotlib**: For plotting the correlation trend.
- **Python 3.8+**

### Example DAG Flow

1. The DAG starts by scraping Bitcoin and Gold data using the `btc_data_scraping.py` and `gold_price_scraping.py` scripts.
2. The scraped data is passed to `preprocess_data.py`, where it is cleaned and structured.
3. The cleaned data is then stored, and the correlation between Bitcoin and Gold prices is calculated.
4. The correlation is logged and stored in a text file for future reference.
5. The correlation trend is plotted over time, and the plot is saved as a PNG image.

### Files Included

- **airflow_scheduler.py**: Defines the DAG for scraping, cleaning, calculating correlation, and plotting the correlation over time.
- **btc_data_scraping.py**: Contains logic for fetching Bitcoin data from the CoinMarketCap API.
- **gold_price_scraping.py**: Contains logic for fetching Gold price data from a gold price API.
- **preprocess_data.py**: Cleans and structures the data for further analysis.
- **btc_gold_correlation_over_time.png**: The generated plot showing the correlation between Bitcoin and Gold prices over time.
