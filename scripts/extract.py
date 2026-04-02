import os
import requests
import logging

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def extract_data(url, save_path):
    """Download the csv file from URL or confirm if a local version exists."""
    # Create the data directory if it doesn't exist
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    
    if os.path.exists(save_path):
        logging.info(f"File already exists at {save_path}. Skipping download.")
        return save_path
    
    try:
        logging.info(f"Fetching data from {url}...")
        # Headers as used here are to prevent 403 Forbidden errors
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, timeout=15, headers=headers)
        response.raise_for_status()
        
        with open(save_path, 'wb') as f:
            f.write(response.content)
            
        logging.info(f"Download complete✅. Saved to: {save_path}")
        return save_path
    
    except Exception as e:
        logging.error(f"Failed to extract data💥: {e}")
        raise
    
if __name__ == "__main__":
    # test URL for downlading the projects data set
    URL ="https://data.humdata.org/dataset/wfp-food-prices-for-kenya/resource/517ee1bf-2437-4f8c-aa1b-cb9925b9d437/download/wfp_food_prices_ken.csv"
    LOCAL_FILE = "data/wfp_food_prices_ken.csv"
    extract_data(URL, LOCAL_FILE)
    