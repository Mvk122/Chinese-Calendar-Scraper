import pathlib
import json
import aiohttp
import asyncio

def get_image_names_from_result_files() -> tuple[set[str], set[str]]:
    good_attributes = set()
    bad_attributes = set()

    limit = 0
    for file in pathlib.Path('./results').iterdir():
        if limit == 1000:
            break
        if file.is_file():
            with open(file, 'r') as f:
                data = json.load(f)
                for attribute in data['good_bad_attributes']["good"]:
                    good_attributes.add(attribute)
                for attribute in data['good_bad_attributes']["bad"]:
                    bad_attributes.add(attribute)
        limit += 1
    return good_attributes, bad_attributes


async def scrape_images(good_attributes: set[str], bad_attributes: set[str], max_concurrent_requests: int = 20) -> None:
    connector = aiohttp.TCPConnector(limit=max_concurrent_requests)   
    async with aiohttp.ClientSession(connector=connector) as session:
        for attribute in good_attributes:
            async with session.get(f"https://www.infengi.ru/images/calendar/icon/good/{attribute}.png") as response:
                with open(f"./images/good/{attribute}.png", 'wb') as f:
                    f.write(await response.read())
        
        for attribute in bad_attributes:
            async with session.get(f"https://www.infengi.ru/images/calendar/icon/bad/{attribute}.png") as response:
                with open(f"./images/bad/{attribute}.png", 'wb') as f:
                    f.write(await response.read())

if __name__ == '__main__':
    good_attributes, bad_attributes = get_image_names_from_result_files()
    asyncio.run(scrape_images(good_attributes, bad_attributes, 20))