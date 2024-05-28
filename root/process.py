import os
import zipfile
import tempfile
import requests
import logging
from datetime import datetime

log_file = 'processing.log'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(message)s')

def add_separator_to_log():
    separator = "\n" + "=" * 80 + "\n" + f"New Run: {datetime.now()}" + "\n" + "=" * 80 + "\n"
    with open(log_file, 'a') as log:
        log.write(separator)

def download_zip(url, download_path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(download_path, 'wb') as f:
            f.write(response.content)
        logging.info(f"Downloaded zip file from {url}")
    else:
        logging.error(f"Failed to download zip file from {url}, status code: {response.status_code}")
        raise Exception(f"Failed to download zip file from {url}")
    
def extract_zip(zip_file_path, output_dir):
    with tempfile.TemporaryDirectory() as temp_dir:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
            logging.info(f"Extracted zip file to {temp_dir}")
        

add_separator_to_log()

zip_file_url = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/1largeRjet1lep.zip"
zip_file_path = os.path.join(os.getcwd(), "1largeRjet1lep.zip")
output_dir = os.path.join(os.getcwd(), "output_1largeRjet1lep")
os.makedirs(output_dir, exist_ok=True)
download_zip(zip_file_url, zip_file_path)

extract_zip(zip_file_path, output_dir)
