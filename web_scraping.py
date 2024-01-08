import aiohttp
import asyncio
import json
import pandas as pd
from aiohttp.client_exceptions import ClientResponseError
from bs4 import BeautifulSoup
import diskcache
from tqdm import tqdm


class AsyncWebScraper:
    def __init__(self, url, headers=None):
        self.url = url
        self.headers = headers or {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }

    async def fetch_page(self, session):
        for attempt in range(3):
            try:
                async with session.get(self.url, headers=self.headers, timeout=100000) as response:
                    response.raise_for_status()
                    return await response.text()
            except ClientResponseError as e:
                print(f'Error during download {self.url}: {e}. Re-attempting ... Attempt {attempt + 1}')
            except aiohttp.ClientConnectionError as e:
                print(f"Error during download {e}")

        print(f'Failed to download {self.url} after multiple attempts.')
        return None

    async def fetch_with_retry(self, session, url, headers, timeout=100000, retries=3):
        for _ in range(retries):
            try:
                async with session.get(url, headers=headers, timeout=timeout) as response:
                    # Przetwórz odpowiedź
                    return await response.text()
            except aiohttp.ClientConnectionError as e:
                print(f"Błąd połączenia: {e}")
                await asyncio.sleep(2)  # Poczekaj 2 sekundy przed ponowną próbą
            except asyncio.TimeoutError:
                print(f'Błąd czasu oczekiwania podczas pobierania {url}. Ponawianie próby...')

        return None

    def parse_html(self, html_content):
        soup = BeautifulSoup(html_content, "lxml")
        return soup


async def fetch_and_parse(session, url, cache, pbar=None):
    html_content = cache.get(url)

    if html_content is None:
        while html_content is None:
            html_content = await AsyncWebScraper(url).fetch_with_retry(session, url, AsyncWebScraper(url).headers)
            cache.set(url, html_content)

    if pbar:
        pbar.update(1)  # Aktualizacja paska postępu
    return AsyncWebScraper(url).parse_html(html_content)


async def get_number_of_pages(session, start_url, cache, pbar=None):
    html_content = await fetch_and_parse(session, start_url, cache, pbar)
    if html_content:
        last_page_links = html_content.find_all('a', {'class': 'eo9qioj1 css-pn5qf0 edo3iif1'})
        if last_page_links:
            last_page_number = int(last_page_links[-1]['href'].split('=')[-1])
            if pbar:
                pbar.set_postfix(pages=last_page_number)  # Display total pages in postfix
            print('\nNumber of pages to be analyzed:', last_page_number)
            return last_page_number
    return 0


async def get_listing_links_async(start_url, cache, pbar_total):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=1000)) as session:
        async with asyncio.Semaphore(500):  # Limit concurrent requests to 10
            num_pages_to_scrape = await get_number_of_pages(session, start_url, cache, pbar_total)

            if num_pages_to_scrape > 0:
                all_links = []
                tasks = [fetch_and_parse(session,
                                         f'{start_url}?ownerTypeSingleSelect=ALL&by=DEFAULT&direction=DESC&viewType=listing&page={page_num}/',
                                         cache, pbar_total)
                         for page_num in range(1, num_pages_to_scrape + 1)]

                # Get a unique part of the start_url for file names
                unique_name_taken_from_url = start_url.split('/pl/')[1].replace('/',
                                                                                '_')  # Replace slashes with underscores

                with tqdm(total=num_pages_to_scrape, desc="Fetching pages", position=1,
                          unit=' page') as pbar_fetch_pages:
                    soup_list = await asyncio.gather(*tasks)
                    for soup in soup_list:
                        if soup:
                            page_links = get_listing_links(soup)
                            all_links.extend(page_links)
                            pbar_fetch_pages.update(1)

                # Generate unique names for CSV and JSON files
                json_filename = f'{unique_name_taken_from_url}.json'
                csv_filename = f'{unique_name_taken_from_url}.csv'

                with tqdm(total=len(all_links), desc="Fetching details", position=0, unit='link') as pbar_fetch_details:
                    detailed_results = await asyncio.gather(
                        *[fetch_details(session, link, cache, pbar_fetch_details) for link in all_links]
                    )

                with open(json_filename, 'w', encoding='utf-8') as json_file:
                    json.dump(detailed_results, json_file, ensure_ascii=False, indent=2)
                    print(f'JSON file saved: {json_filename}')

                formatted_data = [{'link': item['link'], 'title': item['title'], 'price': item['price'],
                                   'price_per_m2': item['price_per_m2'], 'offer_location': item['offer_location'],
                                   'area': item['area'], 'plot_area': item['plot_area'],
                                   'type_of_development': item['type_of_development'],
                                   'number_of_rooms': item['number_of_rooms'],
                                   'heating': item['heating'], 'state_of_completion': item['state_of_completion'],
                                   'year_of_construction': item['year_of_construction'],
                                   'parking_space': item['parking_space'], 'rent': item['rent'],
                                   'floor': item['floor'], 'building_ownership': item['building_ownership'],
                                   'missing_info_button': item['missing_info_button'],
                                   'remote_handling': item['remote_handling'],
                                   'market': item['market'], 'advertiser_type': item['advertiser_type'],
                                   'free_from': item['free_from'], 'building_material': item['building_material'],
                                   'windows_type': item['windows_type'], 'floors_num': item['floors_num'],
                                   'recreational': item['recreational'], 'roof_type': item['roof_type'],
                                   'roofing': item['roofing'], 'garret_type': item['garret_type'],
                                   'media_types': item['media_types'], 'security_types': item['security_types'],
                                   'fence_types': item['fence_types'], 'access_types': item['access_types'],
                                   'location': item['location'], 'vicinity_types': item['vicinity_types'],
                                   'extras_types': item['extras_types'], 'lift': item['lift'],
                                   'equipment_types': item['equipment_types'],
                                   'rent_to_students': item['rent_to_students'], 'deposit': item['deposit'],
                                   'number_of_people_per_room': item['number_of_people_per_room'],
                                   'additional_cost': item['additional_cost'], 'description': item['description']}
                                  for item in detailed_results]

                csv_columns = ['link', 'title', 'price', 'price_per_m2', 'offer_location', 'area', 'plot_area',
                               'type_of_development', 'number_of_rooms', 'heating',
                               'state_of_completion', 'year_of_construction', 'parking_space', 'rent',
                               'floor', 'building_ownership', 'missing_info_button', 'remote_handling',
                               'market', 'advertiser_type', 'free_from', 'building_material',
                               'windows_type', 'floors_num', 'recreational', 'roof_type',
                               'roofing', 'garret_type', 'media_types', 'security_types',
                               'fence_types', 'access_types', 'location', 'vicinity_types',
                               'extras_types', 'lift', 'equipment_types', 'rent_to_students',
                               'number_of_people_per_room', 'deposit', 'additional_cost', 'description']
                df = pd.DataFrame(formatted_data, columns=csv_columns)
                df.to_csv(csv_filename, index=False, encoding='utf-8')

                print(f'CSV file saved: {csv_filename}')
                print('Number of offers found:', len(all_links))
                return all_links, detailed_results
            else:
                print("Error: Unable to determine the number of pages.")
                return []


async def fetch_details(session, link, cache, pbar=None):
    global description
    html_content = await fetch_and_parse(session, link, cache, pbar)
    if html_content:
        title_element = html_content.find('h1', class_='css-1wnihf5 efcnut38')
        price_element = html_content.find('strong', class_='css-t3wmkv e1l1avn10')
        price_per_m2_element = html_content.find('div', class_='css-1h1l5lm efcnut39')
        offer_location_element = html_content.find('a', class_='e1w8sadu0 css-1helwne exgq9l20')
        area_element = html_content.find('div', {'data-testid': 'table-value-area', 'class': 'css-1wi2w6s enb64yk5'})
        plot_area_element = html_content.find('div', {'data-testid': 'table-value-terrain_area'})
        type_of_development_element = html_content.find('div', {'data-testid': 'table-value-building_type'})
        number_of_rooms_element = html_content.find('a', {'data-cy': 'ad-information-link'})
        remote_handling_element = html_content.find('button',
                                                    {'data-cy': 'missing-info-button', 'class': 'css-x0kl3j e1k3ukdh0'})
        heating_element = html_content.find('div', {'data-testid': 'table-value-heating_types'})
        state_of_completion_element = html_content.find('div', {'data-testid': 'table-value-construction_status'})
        year_of_construction_element = html_content.find('div', {'data-testid': 'table-value-build_year'})
        parking_space_element = html_content.find('div', {'data-testid': 'table-value-car'})
        rent_element = html_content.find('button', {'data-cy': 'missing-info-button', 'class': 'css-x0kl3j e1k3ukdh0'})
        floor_element = html_content.find('div', {'data-testid': 'table-value-floor', 'class': 'css-1wi2w6s enb64yk5'})
        building_ownership_element = html_content.find('div', {'data-testid': 'table-value-building_ownership',
                                                               'class': 'css-1wi2w6s enb64yk5'})
        missing_info_button_element = html_content.find('button', {'data-cy': 'missing-info-button',
                                                                   'class': 'css-x0kl3j e1k3ukdh0'})
        market_element = html_content.find('div',
                                           {'data-testid': 'table-value-market', 'class': 'css-1wi2w6s enb64yk5'})
        advertiser_type_element = html_content.find('div', {'data-testid': 'table-value-advertiser_type',
                                                            'class': 'css-1wi2w6s enb64yk5'})
        free_from_element = html_content.find('div',
                                              {'data-testid': 'table-value-free_from', 'class': 'css-1wi2w6s enb64yk5'})
        building_material_element = html_content.find('div', {'data-testid': 'table-value-building_material',
                                                              'class': 'css-1wi2w6s enb64yk5'})
        windows_type_element = html_content.find('div', {'data-testid': 'table-value-windows_type',
                                                         'class': 'css-1wi2w6s enb64yk5'})
        floors_num_element = html_content.find('div', {'data-testid': 'table-value-floors_num',
                                                       'class': 'css-1wi2w6s enb64yk5'})
        recreational_element = html_content.find('div', {'data-testid': 'table-value-recreational',
                                                         'class': 'css-1wnyucs enb64yk5'})
        roof_type_element = html_content.find('div',
                                              {'data-testid': 'table-value-roof_type', 'class': 'css-1wi2w6s enb64yk5'})
        roofing_element = html_content.find('div',
                                            {'data-testid': 'table-value-roofing', 'class': 'css-1wi2w6s enb64yk5'})
        garret_type_element = html_content.find('div', {'data-testid': 'table-value-garret_type',
                                                        'class': 'css-1wi2w6s enb64yk5'})
        media_types_element = html_content.find('div', {'data-testid': 'table-value-media_types',
                                                        'class': 'css-1wi2w6s enb64yk5'})
        security_types_element = html_content.find('div', {'data-testid': 'table-value-security_types',
                                                           'class': 'css-1wi2w6s enb64yk5'})
        fence_types_element = html_content.find('div', {'data-testid': 'table-value-fence_types',
                                                        'class': 'css-1wi2w6s enb64yk5'})
        access_types_element = html_content.find('div', {'data-testid': 'table-value-access_types',
                                                         'class': 'css-1wi2w6s enb64yk5'})
        location_element = html_content.find('div',
                                             {'data-testid': 'table-value-location', 'class': 'css-1wi2w6s enb64yk5'})
        vicinity_types_element = html_content.find('div', {'data-testid': 'table-value-vicinity_types',
                                                           'class': 'css-1wi2w6s enb64yk5'})
        extras_types_element = html_content.find('div', {'data-testid': 'table-value-extras_types',
                                                         'class': 'css-1wi2w6s enb64yk5'})
        lift_element = html_content.find('div', {'data-testid': 'table-value-lift', 'class': 'css-1wi2w6s enb64yk5'})
        equipment_types_element = html_content.find('div', {'data-testid': 'table-value-equipment_types',
                                                            'class': 'css-1wi2w6s enb64yk5'})
        rent_to_students_element = html_content.find('div', {'data-testid': 'table-value-rent_to_students',
                                                             'class': 'css-1wi2w6s enb64yk5'})
        deposit_element = html_content.find('div',
                                            {'data-testid': 'table-value-deposit', 'class': 'css-1wi2w6s enb64yk5'})
        number_of_people_per_room_element = html_content.find('div', {'data-testid': 'table-value-roomsize',
                                                                      'class': 'css-1wi2w6s enb64yk5'})
        additional_cost_element = html_content.find('div', {'data-testid': 'table-value-additional_cost',
                                                            'class': 'css-1wi2w6s enb64yk5'})
        description_element = html_content.find('div',
                                                {'data-cy': 'adPageAdDescription', 'class': 'css-1wekrze e1lbnp621'})

        title = title_element.text if title_element else "N/A"
        price = price_element.text if price_element else "N/A"
        price_per_m2 = price_per_m2_element.text if price_per_m2_element else "N/A"
        offer_location = offer_location_element.text if offer_location_element else "N/A"
        area = area_element.text if area_element else "N/A"
        plot_area = plot_area_element.text if plot_area_element else "N/A"
        type_of_development = type_of_development_element.text if type_of_development_element else "N/A"
        number_of_rooms = number_of_rooms_element.text if number_of_rooms_element else "N/A"
        heating = heating_element.text if heating_element else "N/A"
        state_of_completion = state_of_completion_element.text if state_of_completion_element else "N/A"
        year_of_construction = year_of_construction_element.text if year_of_construction_element else "N/A"
        parking_space = parking_space_element.text if parking_space_element else "N/A"
        rent = rent_element.text if rent_element else "N/A"
        floor = floor_element.text if floor_element else "N/A"
        building_ownership = building_ownership_element.text if building_ownership_element else "N/A"
        missing_info_button = missing_info_button_element.text if missing_info_button_element else "N/A"
        remote_handling = remote_handling_element.text if remote_handling_element else "tak"
        market = market_element.text if market_element else "N/A"
        advertiser_type = advertiser_type_element.text if advertiser_type_element else "N/A"
        free_from = free_from_element.text if free_from_element else "N/A"
        building_material = building_material_element.text if building_material_element else "N/A"
        windows_type = windows_type_element.text if windows_type_element else "N/A"
        floors_num = floors_num_element.text if floors_num_element else "N/A"
        recreational = recreational_element.text if recreational_element else "N/A"
        roof_type = roof_type_element.text if roof_type_element else "N/A"
        roofing = roofing_element.text if roofing_element else "N/A"
        garret_type = garret_type_element.text if garret_type_element else "N/A"
        media_types = media_types_element.text if media_types_element else "N/A"
        security_types = security_types_element.text if security_types_element else "N/A"
        fence_types = fence_types_element.text if fence_types_element else "N/A"
        access_types = access_types_element.text if access_types_element else "N/A"
        location = location_element.text if location_element else "N/A"
        vicinity_types = vicinity_types_element.text if vicinity_types_element else "N/A"
        extras_types = extras_types_element.text if extras_types_element else "N/A"
        lift = lift_element.text if lift_element else "N/A"
        equipment_types = equipment_types_element.text if equipment_types_element else "N/A"
        rent_to_students = rent_to_students_element.text if rent_to_students_element else "N/A"
        deposit = deposit_element.text if deposit_element else "N/A"
        number_of_people_per_room = number_of_people_per_room_element.text if number_of_people_per_room_element else "N/A"
        additional_cost = additional_cost_element.text if additional_cost_element else "N/A"
        if description_element:
            ad_description_paragraphs = description_element.find_all('p')
            clean_text = []
            for paragraph in ad_description_paragraphs:
                # Remove &nbsp; from each paragraph's text
                cleaned_paragraph = paragraph.text.replace('\xa0', ' ')
                clean_text.append(cleaned_paragraph)

            # Combine paragraphs into a single string
            description = ' '.join(clean_text)
        else:
            'N/A'
        if pbar:
            pbar.update(1)  # Update progress bar
        return {'link': link, 'title': title, 'price': price, 'price_per_m2': price_per_m2,
                'offer_location': offer_location, 'area': area, 'plot_area': plot_area,
                'type_of_development': type_of_development, 'number_of_rooms': number_of_rooms,
                'heating': heating,
                'state_of_completion': state_of_completion, 'year_of_construction': year_of_construction,
                'parking_space': parking_space, 'rent': rent,
                'floor': floor, 'building_ownership': building_ownership,
                'missing_info_button': missing_info_button, 'remote_handling': remote_handling,
                'market': market, 'advertiser_type': advertiser_type,
                'free_from': free_from, 'building_material': building_material,
                'windows_type': windows_type, 'floors_num': floors_num,
                'recreational': recreational, 'roof_type': roof_type,
                'roofing': roofing, 'garret_type': garret_type,
                'media_types': media_types, 'security_types': security_types,
                'fence_types': fence_types, 'access_types': access_types,
                'location': location, 'vicinity_types': vicinity_types,
                'extras_types': extras_types, 'lift': lift,
                'equipment_types': equipment_types, 'rent_to_students': rent_to_students, 'deposit': deposit,
                'number_of_people_per_room': number_of_people_per_room, 'additional_cost': additional_cost,
                'description': description}

    else:
        return {'link': link, 'error': 'Failed to fetch details'}


def get_listing_links(soup):
    home_elements = soup.findAll('li', attrs={'class': 'css-o9b79t e1dfeild0'})
    links = []

    for info in home_elements[3:]:  # Skip the first three elements
        link_element = info.find('a', class_='css-lsw81o e1dfeild2')
        if link_element:
            link = link_element.get('href')
            full_link = 'https://www.otodom.pl' + link
            links.append(full_link)

    return links


class DiskCache:
    def __init__(self, cache_directory='./cache', expiration_time=86400):
        self.cache_directory = cache_directory
        self.expiration_time = expiration_time
        self.cache = diskcache.Cache(self.cache_directory, expire=self.expiration_time)

    def get(self, key):
        return self.cache.get(key)

    def set(self, key, value):
        self.cache.set(key, value)


