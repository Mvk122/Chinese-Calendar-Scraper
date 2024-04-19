import aiohttp
import asyncio

from datetime import date, timedelta
from tqdm import tqdm

from main import get_url_iter, store_calendar_information_to_json
from bs4 import BeautifulSoup

def get_good_bad(soup):
    div = soup.find('div', {'id': 'descBlok_d1'})
    return [p.text for p in div.find_all('p')]

def good_things_to_do(soup):
    good_things = soup.find('div', {'id': 'descBlok_d2'})
    if good_things:
        good_params = [gt.text for gt in good_things.find_all('div', {'class': 'cal_param_name param_good'})]
        good_param_descriptions = [gt.text for gt in good_things.find_all('div', {'class': 'cal_param_desc'})]
        return [(param, desc) for param, desc in zip(good_params, good_param_descriptions)]
    return []
    
def bad_things_to_do(soup):
    bad_things = soup.find('div', {'id': 'descBlok_d3'})
    if bad_things:
        good_params = [gt.text for gt in bad_things.find_all('div', {'class': 'cal_param_name param_bad'})]
        good_param_descriptions = [gt.text for gt in bad_things.find_all('div', {'class': 'cal_param_desc'})]
        return [(param, desc) for param, desc in zip(good_params, good_param_descriptions)]
    return []

def get_lunar_position(soup):
    lunar_position = soup.find('span', {'class' : 'cal_luna_sozvId'}).text
    lunar_position_name = soup.find('span', {'class' : 'cal_luna_sozvName'}).text
    lunar_position_desc = soup.find('div', {'id': 'descBlok_d5'}).text
    return {'position': lunar_position, 'name': lunar_position_name, 'desc': lunar_position_desc}

def get_lunar_day(soup):
    final = {}
    final['lunar_day'] = soup.find('div', {'class': 'cal_luna_wrap'}).text
    
    lunar_data = soup.find('div', {'id': 'descBlok_d6'})
    if lunar_data:
        lunar_data_list_ul = lunar_data.find('ul')
        if lunar_data_list_ul:
            lunar_data_list = lunar_data_list_ul.find_all('li')

            ret = []
            for list_item in lunar_data_list:
                list_item_text = list_item.text.strip()
                sp = list_item_text.split("-")
                if len(sp) == 2:
                    ret.append([sp[0].strip(), sp[1].strip()])
                else:
                    ret.append([sp[0].strip()])
            
            final['lunar_properties'] = ret
        
        paragraphs = lunar_data.find_all('p')
        paragraph_texts = "\n".join([p.text.strip() for p in paragraphs])

        final['lunar_description'] = paragraph_texts

    return final

def get_time_list(soup):
    time_list = soup.find('ul', {'id': 'cal_day_chas'})
    
    cal_days = []
    for i in range(1,14):
        cal_day = time_list.find('li', {'id': f'cal_day_chas_{i}'})
        if cal_day:
            cal_day_spans = cal_day.find_all("span")
            cal_day_data = []
            for span in cal_day_spans[:2]:
                cal_day_data.append({"text": span.text, "class": span["class"][0]})
            cal_days.append(cal_day_data)
        else:
            cal_days.append([])
    return cal_days

def get_time_good_bad(soup):
    ret = []
    for i in range(1,14):
        data_div = soup.find('div', {'id': f'cal_chas_tbl{i}'})
        if data_div:
            cal_day_right = data_div.find('div', {'class': 'cal_day_right'})
            good_params = cal_day_right.find_all('div', {'class': 'cal_param_name param_good'})
            bad_params = cal_day_right.find_all('div', {'class': 'cal_param_name param_bad'})
            param_descriptions = cal_day_right.find_all('div', {'class': 'cal_param_desc'})

            good = []
            bad = []
            # all the goods happen before bads
            for gp, pd in zip(good_params, param_descriptions):
                good.append((gp.text, pd.text))

            for bp, pd in zip(bad_params, param_descriptions[len(good):]):
                bad.append((bp.text, pd.text))

            ret.append({"good": good, "bad": bad})
        else:
            ret.append({"good": [], "bad": []})

    return ret

def get_time_table(soup):
    ret = []
    for i in range(1,14):
        day_data = []
        data_div = soup.find('div', {'id': f'cal_chas_tbl{i}'})
        if data_div:
            cal_day_left = data_div.find('div', {'class': 'cal_day_left'})
            table = cal_day_left.find('table', {'class': 'cal_table_bz_full'})

            rows = table.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                for col in cols:
                    col_span = col.find('span')
                    day_data.append({"text": col_span.text, "class": col_span["class"][0]})
        ret.append(day_data)
    return ret

def get_extended_calendar_information_from_soup(soup: BeautifulSoup):
    result = {}
    result['good_bad'] = get_good_bad(soup)
    result['good_to_do'] = good_things_to_do(soup)
    result['bad_to_do'] = bad_things_to_do(soup)
    result['get_lunar_position'] = get_lunar_position(soup)
    result['lunar_day'] = get_lunar_day(soup)
    result['time_list'] = get_time_list(soup)
    result['time_good_bad'] = get_time_good_bad(soup)
    result['time_table'] = get_time_table(soup)
    return result

async def get_and_store_extended_calendar_information(session, url: str, day: date):
    async with session.get(url) as response:
        soup = BeautifulSoup(await response.text(), 'html.parser')
        calendar_information = get_extended_calendar_information_from_soup(soup)
        store_calendar_information_to_json(calendar_information, day, folder="extended")


async def fast_scrape_to_json(start_date: date, end_date: date, max_concurrent_requests: int = 50) -> None:
    url_iterator = tqdm(get_url_iter(start_date, end_date), total=(end_date-start_date).days + 1)
    url_iterator = iter(url_iterator)
    keep_going = True
    async with aiohttp.ClientSession() as session:
        while keep_going:
            async with asyncio.TaskGroup() as tg:
                for _ in range(max_concurrent_requests):
                    try:
                        url, day = next(url_iterator)
                    except StopIteration:
                        keep_going = False
                        break
                    tg.create_task(get_and_store_extended_calendar_information(session, url, day))

async def main():
    await fast_scrape_to_json(date(2024, 1, 1), date(2024, 1, 2), 50)

if __name__ == "__main__":    
    asyncio.run(main())
