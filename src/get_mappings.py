from datetime import date, timedelta
from tqdm import tqdm
import requests
from collections import defaultdict 

import json

from bs4 import BeautifulSoup

from main import get_url_iter

def image_mappings(soup, repository):
    images = soup.find_all('img', {'class': 'cal_param_icon'})

    for image in images:
        image_components = image['src'].split('/')
        image_name = image_components[5].split('.')[0]
        image_data_desc = image['data-desc']

        repository[image_name] = image_data_desc

def attribute_mappings(soup, repository):
    table = soup.find('table', {'class': 'cal_table_bz'})
    spans = table.find_all('span')

    for span in spans:
        repository[span.text] = span['data-desc']


def scrape_mappings(start_date: date, end_date: date, map_function) -> None:
    url_iterator = tqdm(get_url_iter(start_date, end_date), total=(start_date-end_date).days + 1)
    url_iterator = iter(url_iterator)

    repository = {}
    for url, day in url_iterator:
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        map_function(soup, repository)

    
    print(repository)
    return repository

if __name__ == "__main__":
    scrape_mappings(date(2024, 1, 1), date(2024, 1, 30), attribute_mappings)
    
