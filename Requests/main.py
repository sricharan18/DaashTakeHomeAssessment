import requests
import json
import pandas as pd
import numpy as np
import math
import time
import logging
from requests.exceptions import RequestException
from concurrent.futures import ThreadPoolExecutor, as_completed
import config
from faker import Faker 

def get_cookie(headers):
    """
    Fetches cookies from the website's response headers.
    """
    try:
        response = requests.get('https://www.lowes.com/', headers=headers, timeout=config.TIMEOUT)
        response.raise_for_status()
        cookies = response.headers.get('Set-Cookie', '')
        cookie = ''
        for item in cookies.split(','):
            if '=' in item.split(';')[0]:
                cookie += item.split(';')[0] + ';'
        return cookie
    except RequestException as e:
        logging.error(f"Failed to get cookie: {e}")
        return ''

def safe_append(dictionary, key_path, default_value='NA'):
    """
    Safely accesses nested dictionary keys, returns default_value if any key is missing.
    """
    try:
        for key in key_path:
            dictionary = dictionary[key]
        return dictionary
    except (KeyError, TypeError):
        return default_value

def get_data(adjusted_next_offset, headers, retries=0):
    """
    Fetches data from the target URL, includes retry logic for network errors.
    """
    # fake = Faker()
    # headers['User-Agent'] = fake.chrome()
    # headers['cookie'] = get_cookie(headers)

    url = f"https://www.lowes.com/pl/fall-decorations/fall-wreaths-garland/sullivans/1614047588/products?offset={adjusted_next_offset}&selectedStoreNumber=3284&ac=false&algoRulesAppliedInPageLoad=true"
    # url = f"https://www.lowes.com/pl/bathroom-accessories-hardware/shower-curtains-rods/4294639614/products?offset={adjusted_next_offset}&selectedStoreNumber=3284&ac=false&algoRulesAppliedInPageLoad=true"
    
    logging.info(f"Fetching: {url}")
    try:
        response = requests.get(url, headers=headers, timeout=config.TIMEOUT)
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        logging.error(f"Error fetching data from {url}: {e}")
        if retries < config.MAX_RETRIES:
            time.sleep(1)
            logging.info(f"Retrying ({retries + 1}/{config.MAX_RETRIES})...")
            return get_data(adjusted_next_offset, headers, retries + 1)
        else:
            logging.error(f"Max retries exceeded for URL: {url}")
            return None

def data_scraper(offset, headers):
    """
    Scrapes data for a given page offset.
    """
    data = get_data(offset, headers)
    if data is None:
        return pd.DataFrame()
    item_list = data.get('itemList', [])
    scraped_data = {'Brand': [], 'ModelId': [], 'Price': [], 'ProductURL': [], 'Description': []}
    for item in item_list:
        scraped_data['Brand'].append(safe_append(item, ['product', 'brand']))
        scraped_data['ModelId'].append(safe_append(item, ['product', 'modelId']))
        price = safe_append(item, ['location', 'price', 'sellingPrice'], safe_append(item, ['location', 'price', 'minPrice']))
        scraped_data['Price'].append(price)
        pd_url = safe_append(item, ['product', 'pdURL'])
        full_url = 'https://www.lowes.com' + pd_url if pd_url else np.nan
        scraped_data['ProductURL'].append(full_url)
        scraped_data['Description'].append(safe_append(item, ['product', 'description']))
    logging.info(f"Completed scraping offset {offset}")
    return pd.DataFrame(scraped_data)

def data_scraper_with_retry(offset, headers):
    """
    Wraps the data_scraper function with retry logic.
    """
    retries = 0
    while retries <= config.MAX_RETRIES:
        try:
            return data_scraper(offset, headers)
        except Exception as e:
            logging.error(f"Error scraping offset {offset}, attempt {retries + 1}: {e}")
            retries += 1
            if retries > config.MAX_RETRIES:
                logging.error(f"Max retries exceeded for offset {offset}")
                return pd.DataFrame()
            time.sleep(1)

def main():
    """
    Main function to orchestrate the scraping process.
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    fake = Faker()

    # Start timing the scraping process
    start_time = time.time()
    
    # Prepare headers with cookies
    headers = config.HEADERS.copy()
    headers['Cookie'] = get_cookie(headers)
    if not headers['Cookie']:
        logging.error("Failed to retrieve cookies. Exiting.")
        return
    
    # Fetch initial data to determine total pages
    data = get_data(0, headers)
    if data is None:
        logging.error("Failed to retrieve initial data. Exiting.")
        return
    total_items = data.get('itemCount', 0)
    total_items_per_offset = len(data.get('itemList', 1))
    total_pages = math.ceil(total_items / total_items_per_offset)
    offsets = [i * total_items_per_offset for i in range(total_pages)]
    df_list = []
    
    # Use ThreadPoolExecutor for concurrent scraping
    with ThreadPoolExecutor(max_workers=config.NUMBER_OF_THREADS) as executor:
        future_to_offset = {executor.submit(data_scraper_with_retry, offset, headers): offset for offset in offsets}
        for future in as_completed(future_to_offset):
            offset = future_to_offset[future]
            try:
                data_frame = future.result(timeout=config.TIMEOUT)
                if not data_frame.empty:
                    df_list.append(data_frame)
            except Exception as e:
                logging.error(f"Error in scraping offset {offset}: {e}")
    
    # Combine all data frames into one
    df = pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()
    logging.info("Data scraping completed.")

    end_time = time.time()
    total_time = end_time - start_time
    logging.info(f"Total time taken for scraping: {total_time:.2f} seconds")

    df.to_csv("Lowes_data_test_run.csv", index=False)

if __name__ == "__main__":
    main()
