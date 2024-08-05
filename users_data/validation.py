import time
from datetime import datetime
import logging
import pandas as pd
import numpy as np
import re
import random
import names
from typing import Union

pd.options.mode.chained_assignment = None 

def read_csv(file_path: str) -> Union[pd.DataFrame, None]:
    try:
        return pd.read_csv(file_path)
    except FileNotFoundError:
        logging.error(f"Error: File '{file_path}' not found.")
        return None
    except Exception as e:
        logging.error(f"An error occurred while reading '{file_path}': {e}")
        return None

log_file = 'valid.log'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(message)s')

def add_separator_to_log() -> None:
    separator = "\n" + "=" * 80 + "\n" + f"New Run: {datetime.now()}" + "\n" + "=" * 80 + "\n"
    with open(log_file, 'a') as log:
        log.write(separator)

def clean_data(data: pd.DataFrame) -> pd.DataFrame:
    threshold = 0.4
    min_count = int(threshold * len(data.columns))
    data_cleaned = data.dropna(thresh=min_count)
    logging.info(f'Rows with less than {threshold*100}% columns filled are removed. Remaining rows: {len(data_cleaned)}')
    return data_cleaned


def validate_email(email: str) -> bool:
    if isinstance(email, str):
        is_valid = re.match(r'^[a-zA-Z0-9]{3,}\d{2}@example\.com$', email)
        return is_valid
    return False

def generate_random_email() -> str:
    user = ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=3)) + str(random.randint(10, 99))
    random_email = f'{user}@example.com'
    logging.info(f'Generated random email: {random_email}')
    return random_email

def choose_password(row: pd.Series) -> str:
    main = row['main_password']
    backup = row['backup_password']
    criteria = [r'[A-Z]', r'[a-z]', r'[0-9]', r'[@!\*\?\#]']
    if pd.notna(main) and pd.notna(backup):
        main_criteria = sum(bool(re.search(p, main)) for p in criteria)
        backup_criteria = sum(bool(re.search(p, backup)) for p in criteria)
        if main_criteria > backup_criteria:
            chosen_password = main
        elif backup_criteria > main_criteria:
            chosen_password = backup
        else:
            chosen_password = main if len(main) >= len(backup) else backup
        return chosen_password
    chosen_password = main if pd.notna(main) else backup
    return chosen_password

def password_strength(password: str) -> str:
    criteria = [r'.{6,}', r'[A-Z]', r'[a-z]', r'[0-9]', r'[@!\*\?\#]']
    strength = sum(bool(re.search(p, password)) for p in criteria)
    if strength <= 1:
        strength_label = 'weak'
    elif strength <= 3:
        strength_label = 'medium'
    else:
        strength_label = 'strong'
    return strength_label

def validate_names(row: pd.Series) -> pd.Series:
    if pd.isna(row['first_name']) or pd.isna(row['last_name']) or len(str(row['first_name'])) < 2 or len(str(row['last_name'])) < 2:
        logging.warning(f'Invalid names found: {row["first_name"]}, {row["last_name"]}')
        return pd.Series([np.nan, np.nan], index=['first_name', 'last_name'])
    return row[['first_name', 'last_name']]


def validate_data(data: pd.DataFrame) -> pd.DataFrame:
    # 4a) 'login_email'
    invalid_emails = data['login_email'].apply(lambda x: not validate_email(x))
    invalid_email_addresses = data.loc[invalid_emails, 'login_email']
    logging.info(f'Invalid email addresses:\n{invalid_email_addresses.tolist()}')

    data.loc[invalid_emails, ('login_email')] = np.nan

    data['login_email'] = data['login_email'].apply(lambda x: x if pd.notna(x) else generate_random_email())

    # 4b) 'main_password' i 'backup_password'
    logging.info('Choosing passwords and calculating password strengths...')
    data['password'] = data.apply(choose_password, axis=1)
    data['password'] = data['password'].fillna('default')

    data['password_strength'] = data['password'].apply(password_strength)
    logging.info('Password strengths calculated.')

    data = data.drop(columns=['main_password', 'backup_password'])
    logging.info('Dropped columns: main_password, backup_password.')

    # 4c) 'first_name' i 'last_name'
    data[['first_name', 'last_name']] = data.apply(validate_names, axis=1)
    
    data['first_name'] = data['first_name'].apply(lambda x: x if pd.notna(x) else names.get_first_name())
    data['last_name'] = data['last_name'].apply(lambda x: x if pd.notna(x) else names.get_last_name())
    logging.info('Invalid names changed (generated valid).')


    # 4d) 'department'
    most_common_department = data['department'].mode().iloc[0]
    data['department'] = data['department'].fillna(most_common_department)
    department_counts = data['department'].value_counts()
    logging.info(f'Department counts:\n{department_counts}')

    return data


def drop_duplicates(data: pd.DataFrame) -> pd.DataFrame:    
    min_last_name = data.groupby('login_email')['last_name'].transform('min')
    data = data[data['last_name'] == min_last_name]
    
    data = data.drop_duplicates(subset='login_email', keep='first').reset_index(drop=True)

    return data


def generate_id(row: pd.Series, count: int) -> str: 
    dept_short = row['department'][:3].lower()
    initials = (row['first_name'][0] + row['last_name'][0]).lower()
    generated_id = f'{dept_short}{initials}{count}'
    return generated_id


def main():
    add_separator_to_log()
    csv_path = 'users_data.csv'
    users_data = read_csv(csv_path)
    
    if users_data is None:
        logging.info(f'Error reading CSV file: {csv_path}. Exiting.')
        return

    users_data = pd.read_csv(csv_path)
    logging.info(f'Read csv file: {csv_path}. Number of rows: {len(users_data)}, Number of columns: {len(users_data.columns)}')

    users_data_cleaned = clean_data(users_data)
    if len(users_data_cleaned) == 0:
        logging.warning(f'No data left after cleaning. Check data quality.')
        return

    missing_values = users_data_cleaned.isna().sum()
    logging.info(f'Missing values per column:\n{missing_values}')

    logging.info(f'Starting data validation.')
    users_data_validated = validate_data(users_data_cleaned)
    
    # users_data_unique = users_data_validated.sort_values(by=['login_email', 'last_name'], ascending=[True, True])
    # users_data_unique = users_data_unique.drop_duplicates(subset='login_email', keep='first').reset_index(drop=True)
    users_data_unique = drop_duplicates(users_data_validated)
    logging.info('Dropped columns with duplicated emails.')

    
    users_data_unique['id'] = users_data_unique.apply(lambda row: generate_id(row, users_data_unique.groupby(['first_name', 'last_name']).cumcount().loc[row.name]), axis=1)
    logging.info('Id column generated.')

    output_csv_path = 'users_data_validated.csv'
    users_data_unique.to_csv(output_csv_path, index=False)
    logging.info(f'Saved validated data to {output_csv_path}. Number of rows: {len(users_data_unique)}, Number of columns: {len(users_data_unique.columns)}')


if __name__ == "__main__":
    main()
