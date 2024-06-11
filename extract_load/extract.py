import os
import requests
from bs4 import BeautifulSoup
from utils import get_config
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import logging

# ######################## LOGGING SETUP ########################
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# console logger
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)
# file logger
log_file_path = get_config(section='path_files').get('logs')
os.makedirs(log_file_path, exist_ok=True)
file_handler = logging.FileHandler(f'{log_file_path}logs_extract_process.txt')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)
# ####################### LOGGING SETUP END  ########################

def file_exists(local_file_path, url_file):
    if os.path.isfile(local_file_path):
        response = requests.head(url_file)
        if response.status_code == 200:
            local_file_size = int(os.path.getsize(local_file_path))
            remote_file_size = int(response.headers.get('Content-Length'))

            if local_file_size == remote_file_size:
                logger.info(f'{local_file_path} already exists.')
                return True
            else: 
                return False
    else:
        return False

def download_file(url_file, save_to):
    """
    Downloads a file from a given URL and saves it to a specified directory.

    Parameters:
        url_file (str): The URL of the file to be downloaded.
        save_to (str): The directory where the downloaded file will be saved.

    Returns:
        None

    Raises:
        requests.exceptions.RequestException: If there is an error during the download process.

    """
    os.makedirs(save_to, exist_ok=True)

    filename = url_file.split('/')[-1]
    file_path = os.path.join(save_to, filename)
    
    if not file_exists(file_path, url_file):
        logger.info(f'Downloading {url_file}')
        try:
            with requests.get(url_file, stream=True) as r:
                r.raise_for_status()
                with open(file_path, 'wb') as file:
                    for chunk in r.iter_content():#(chunk_size=8192):
                        file.write(chunk)
            logger.info(f'{filename} downloaded successfully.')
        except requests.exceptions.RequestException as e:
            logger.error(f'Failed to download {url_file}. Error: {e}')
    else:
        logger.info(f'{filename} Skipping download.')

def list_zip_files(url):
    """
    Retrieves a list of zip file links from a given URL.

    Args:
        url (str): The URL from which to fetch the links.

    Returns:
        list: A list of zip file links found on the given URL.

    Raises:
        requests.exceptions.RequestException: If there is an error during the fetch process.

    This function sends a GET request to the given URL and retrieves the HTML content. It then parses the HTML using BeautifulSoup and finds all the anchor tags ('a'). It filters the links to only include those that end with '.zip' and returns the list of zip file links.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f'Failed to fetch URL: {url}. Error: {e}')
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a')

    # Filter zip file links
    zip_links = [url + link.get('href') for link in links \
                if link.get('href') and link.get('href').endswith('.zip')]
    return zip_links

def download_zip_files(url, save_to):
    """
    Downloads zip files from a given URL and saves them to a specified directory.

    Args:
        url (str): The URL from which to download the zip files.
        save_to (str): The directory where the downloaded zip files will be saved.

    Returns:
        None

    Raises:
        requests.exceptions.RequestException: If there is an error during the download process.

    """

    zip_links = list_zip_files(url)
    if not zip_links:
        logger.warning('No zip files found to download.')
        return

    logger.info(f'Found {len(zip_links)} zip files to download.')
    
    max_workers = multiprocessing.cpu_count() * 2
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(lambda url_file: download_file(url_file, save_to), zip_links)

def main():
    url = get_config(section='urls').get('receita_federal')
    zip_files_path = get_config(section='path_files').get('receita_federal')
    logger.info(f'START EXTRACT from {url} to {zip_files_path}')
    download_zip_files(url, zip_files_path)
    logger.info('EXTRACT process completed.')

if __name__ == '__main__':
    main()
