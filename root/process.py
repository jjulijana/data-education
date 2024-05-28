import os
import zipfile
import tempfile
import requests
import ROOT
import pandas as pd
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
    
def process_root_file(root_file_path, output_dir):
    root_file = ROOT.TFile.Open(root_file_path)
    
    if not root_file or root_file.IsZombie():
        logging.error(f"Unable to open the ROOT file {root_file_path}.")
        return

    for key in root_file.GetListOfKeys():
        obj = key.ReadObj()
        if isinstance(obj, ROOT.TTree):
            tree = obj
            tree_name = tree.GetName()
            logging.info(f"Processing TTree: {tree_name}")

            output_file = os.path.join(output_dir, f"{os.path.basename(root_file_path)}_{tree_name}.csv")
            if os.path.exists(output_file):
                logging.info(f"CSV file already exists: {output_file}")
                return
            
            data = {}
            for branch in tree.GetListOfBranches():
                branch_name = branch.GetName()
                data[branch_name] = []

            for entry in tree:
                for branch_name in data.keys():
                    data[branch_name].append(getattr(entry, branch_name))

            df = pd.DataFrame(data)
            df.to_csv(output_file, index=False)
            logging.info(f"CSV file has been created: {output_file}")

def extract_and_process_zip(zip_file_path, output_dir):
    with tempfile.TemporaryDirectory() as temp_dir:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
            logging.info(f"Extracted zip file to {temp_dir}")
        
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                if file.endswith('.root'):
                    root_file_path = os.path.join(root, file)
                    process_root_file(root_file_path, output_dir)
        

add_separator_to_log()

zip_file_url = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/1largeRjet1lep.zip"
zip_file_path = os.path.join(os.getcwd(), "1largeRjet1lep.zip")
output_dir = os.path.join(os.getcwd(), "output_1largeRjet1lep")
os.makedirs(output_dir, exist_ok=True)
download_zip(zip_file_url, zip_file_path)

extract_and_process_zip(zip_file_path, output_dir)
