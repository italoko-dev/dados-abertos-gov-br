import os
import requests
from bs4 import BeautifulSoup
from utils import get_config

def download_zip_files(url, save_to):
    """
    Downloads zip files from a given URL and saves them to a specified directory.

    Args:
        url (str): The URL to download the zip files from.
        save_to (str): The directory to save the downloaded zip files to.

    Returns:
        None

    This function performs the following steps:
    1. Sends a GET request to the specified URL and retrieves the HTML content.
    2. Parses the HTML content using BeautifulSoup to extract all the anchor tags.
    3. Iterates over each anchor tag and checks if the href attribute ends with '.zip'.
    4. If the href attribute ends with '.zip', it constructs the URL for the zip file and the filename.
    5. Creates the directory specified in `save_to` if it doesn't exist.
    6. Downloads the zip file from the constructed URL and saves it to the specified directory.
    7. Prints a message indicating the download status.

    Note:
    - This function assumes that the HTML content contains anchor tags with href attributes pointing to zip files.
    - The function uses the requests library to send HTTP requests and the BeautifulSoup library to parse HTML content.
    - The function uses the os library to create directories and the requests library to download files.
    """
    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')

    links = soup.find_all('a')

    os.makedirs(save_to, exist_ok=True)
    for link in links:
        href = link.get('href')
        if href and href.endswith('.zip'):
            url_file = url + href
            filename = href.split('/')[-1]
            file_path = os.path.join(save_to, filename)
            
            print(f"Downloading {url_file}...")
            with requests.get(url_file, stream=True) as r:
                r.raise_for_status()
                with open(file_path, 'wb') as file:
                    for chunk in r.iter_content(chunk_size=8192):
                        file.write(chunk)
            print(f"{filename} downloaded successfully.")
def main():
    url = get_config(section='urls').get('receita_federal')
    zip_files_path = get_config(section='path_raw_files').get('receita_federal')
    download_zip_files(url, zip_files_path)

if __name__ == '__main__':
    main()
