# Program Initialization
import requests
import pandas as pd
from bs4 import BeautifulSoup
import time

data = []

# URL Initialization
with open('urls.txt', 'r') as file:
    url_list_raw = file.read()

url_list = url_list_raw.splitlines()

# URL Loop

def url_loop():
    for url_count in url_list:
        url = url_count
        data_gather(url)

# Data Gather Function
def data_gather(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    retries = 3

    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=100)
            if response.status_code == 200:
                break
            else:
                print(f"Failed to retrieve {url}, Status code: {response.status_code}")
                print("Retrying...")
                time.sleep(5)
        except requests.exceptions.Timeout:
            print(f"Timeout error on {url}, retrying...")
            time.sleep(5)
        except request.exceptions.RequestException as e:
            print(f"Request failed for {url}: {e}")
            print("Unrecoverable error occured :/")
            break

    if response.status_code == 200:
        dataSoup = response.text
        soup = BeautifulSoup(dataSoup, 'html.parser')

        print(f'Classes found in URL: {url}')
        tables = soup.find_all('table')

        if tables:
            for table in tables:
                class_name = table.get('class')
                if class_name:
                    print(class_name)
                    data_append(table, class_name, url, soup)
                else:
                    print("No class attributes found for table. Error?")
        else:
            print(f"No table found in {url}")

    else:
        print(f"Failed after {retries} retries for {url}")
    print("")

# Data Append Function

def data_append(table, class_name, url, soup):
    global data
    
    print(f"Appending data for {class_name}")

    table = soup.find('table', class_= class_name)

    for row in table.tbody.find_all('tr'):
        columns = row.find_all('td')
        if columns:
            date = columns[0].text.strip()
            location = columns[1].text.strip()
            work = columns[2].text.strip()
            reference = columns[3].text.strip()
            companypeople = columns[4].text.strip()

            data.append({'Date': date, 'Location': location, 'Work': work, 'Reference' : reference, 'Company/People' : companypeople, 'URL' : url})

        else:
            print("ERROR")


# Call Functions
url_loop()
df = pd.DataFrame(data)
df.to_csv('output.csv', index=False)