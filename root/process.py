import os
import zipfile
import tempfile
import hashlib
import requests
import ROOT
import pandas as pd
import logging
from datetime import datetime
import configparser
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String, DateTime, inspect

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
    
def read_db_config(filename='db_config.ini', section='postgresql'):
    parser = configparser.ConfigParser()
    parser.read(filename)

    db_config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db_config[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')

    return db_config

def hash_table_name(table_name, max_length=63):
    if len(table_name) > max_length:
        hash_object = hashlib.md5(table_name.encode())
        hash_suffix = hash_object.hexdigest()[:8]
        return f"{table_name[:max_length-9]}_{hash_suffix}"
    return table_name

def add_table_to_postgres(csv_file, table_name):
    db_config = read_db_config()
    db_uri = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(db_uri)
    connection = engine.connect()

    metadata = MetaData()

    df = pd.read_csv(csv_file)
    
    columns = []
    for col_name, col_type in zip(df.columns, df.dtypes):
        if col_type == 'int64':
            col = Column(col_name, Integer)
        elif col_type == 'float64':
            col = Column(col_name, Float)
        elif col_type == 'datetime64[ns]':
            col = Column(col_name, DateTime)
        else:
            col = Column(col_name, String)
        columns.append(col)

    table_name = hash_table_name(table_name)
    table = Table(
        table_name, metadata,
        *columns
    )

    inspector = inspect(engine)
    if inspector.has_table(table_name):
        logging.info(f"Table {table_name} already exists. Updating data.")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
    else:
        metadata.create_all(engine)
        logging.info(f"Table {table_name} created.")
        df.to_sql(table_name, engine, if_exists='append', index=False)
        logging.info(f"Data inserted into table {table_name}.")

    connection.close()
    
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

            add_table_to_postgres(output_file, table_name=f"{os.path.basename(root_file_path)}_{tree_name}")

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
        

if __name__ == "__main__":
    add_separator_to_log()

    zip_file_url = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/1largeRjet1lep.zip"
    zip_file_path = os.path.join(os.getcwd(), "1largeRjet1lep.zip")
    output_dir = os.path.join(os.getcwd(), "output_1largeRjet1lep")
    os.makedirs(output_dir, exist_ok=True)
    download_zip(zip_file_url, zip_file_path)

    extract_and_process_zip(zip_file_path, output_dir)
