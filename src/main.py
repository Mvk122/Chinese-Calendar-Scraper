"""
    Scrapes the calendar information from https://www.infengi.ru/calendar
    and stores each day's information in a json file in the results folder.
"""
import json
import asyncio
from typing import Any
from datetime import date, timedelta

from collections import defaultdict 

from bs4 import BeautifulSoup
import aiohttp

def get_day_url(day: int, month: int, year: int) -> str:
    return f"https://www.infengi.ru/calendar?day={day}&month={month}&year={year}"

def get_day_attributes(soup: BeautifulSoup) -> list[dict[str, str]]:
    table = soup.find('table', {'class': 'cal_table_bz'})
    spans = table.find_all('span')
    return [{'class': span['class'][0], 'text': span.text} for span in spans]

def get_lunar_day(soup: BeautifulSoup) -> str:
    div = soup.find('div', {'class': 'cal_luna_wrap'})
    span = div.find('span')
    return span.text

def get_good_bad_attributes(soup: BeautifulSoup) -> dict[str, list[str]]:
    images = soup.find_all('img', {'class': 'cal_param_icon'})
    images_by_attribute = defaultdict(list)
    for image in images:
        image_components = image['src'].split('/')
        image_name = image_components[5].split('.')[0]
        image_attribute = image_components[4]

        images_by_attribute[image_attribute].append(image_name)

    return images_by_attribute

def get_calendar_information_from_soup(soup: BeautifulSoup) -> dict[str, Any]:
    result = {}

    result['day_attributes'] = get_day_attributes(soup)
    result['lunar_day'] = get_lunar_day(soup)
    result['good_bad_attributes'] = get_good_bad_attributes(soup)

    return result

def store_calendar_information_to_json(calendar_information: dict[str, Any], date: date) -> None:
    with open(f"./results/{date.day}_{date.month}_{date.year}.json", 'w+') as f:
        json.dump(calendar_information, f, indent=4)


async def scrape_to_json(start_date: date, end_date: date, max_concurrent_requests: int = 10) -> None:
    delta = end_date - start_date
    connector = aiohttp.TCPConnector(limit=max_concurrent_requests)   
    
    async with aiohttp.ClientSession(connector=connector) as session:
        for i in range(delta.days + 1):
            day = start_date + timedelta(days=i)
            async with session.get(get_day_url(day.day, day.month, day.year)) as response:
                soup = BeautifulSoup(await response.text(), 'html.parser')
                calendar_information = get_calendar_information_from_soup(soup)
                store_calendar_information_to_json(calendar_information, day)

async def main(): 
    await scrape_to_json(date(2021, 9, 1), date(2021, 9, 30))

if __name__ == "__main__":
    asyncio.run(main())
