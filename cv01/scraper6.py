import asyncio
import aiofiles
import aiohttp
from bs4 import BeautifulSoup
import json
import os
import re

article_data_list = []
article_url_set = set()  # Množina na sledování zpracovaných URL článků
chunk_size = 1000  # Zvětšený chunk pro méně časté zápisy
max_file_size = 1 * 1024 * 1024 * 1024  # Maximální velikost souboru 1 GB (v bajtech)

async def load_existing_urls():
    """Načti existující URL z uloženého souboru JSONL."""
    if os.path.exists('scraped_articles.jsonl'):
        async with aiofiles.open('scraped_articles.jsonl', 'r', encoding='utf-8') as f:
            async for line in f:
                article_data = json.loads(line.strip())
                article_url_set.add(article_data['url'])

async def async_save_data_chunk():
    """Asynchronní ukládání dat do souboru a kontrola velikosti souboru."""
    global article_data_list

    if article_data_list:
        # Asynchronní zápis dat na disk
        async with aiofiles.open('scraped_articles.jsonl', 'a', encoding='utf-8') as f:
            await f.writelines(json.dumps(data, ensure_ascii=False) + '\n' for data in article_data_list)

        article_data_list = []  # Vyprázdníme seznam po zápisu

        # Zkontrolujeme velikost souboru
        file_size = os.path.getsize('scraped_articles.jsonl')
        if file_size >= max_file_size:
            print(f"Velikost souboru dosáhla {file_size / (1024 * 1024 * 1024):.2f} GB. Ukončuji program.")
            exit()  # Bezpečně ukončí program, pokud soubor přesáhne 1 GB

async def scrape_article(session, article_url):
    """Stáhneme a zpracujeme data z jednoho článku."""
    try:
        # Kontrola, zda článek již nebyl zpracován
        if article_url in article_url_set:
            print(f"Článek na URL {article_url} byl již zpracován, přeskočeno.")
            return None
        
        async with session.get(article_url) as response:
            if response.status != 200:
                return None  # Pokud článek nelze stáhnout, vrátíme None

            article_soup = BeautifulSoup(await response.text(), 'html.parser')
            article_data = {
                'url': article_url,
                'title': article_soup.find('h1').get_text(strip=True) if article_soup.find('h1') else 'Název nenalezen',
                'content': "\n".join(p.get_text(strip=True) for p in article_soup.find_all('p')) or 'Obsah nenalezen',
                'category': article_url.split("/")[3],
                'image_count': len(article_soup.find_all('img')),
                'publication_date': article_soup.find('time').get_text(strip=True) if article_soup.find('time') else 'Datum nenalezeno',
                'comment_count': int(re.search(r'\d+', article_soup.find('a', id='moot-linkin').find('span').get_text(strip=True)).group()) if article_soup.find('a', id='moot-linkin') and article_soup.find('a', id='moot-linkin').find('span') else 0
            }

            # Přidání URL článku do množiny, aby se zabránilo duplicitám
            article_url_set.add(article_url)
            return article_data  # Vracíme data článku
    except Exception as e:
        print(f"Chyba při zpracování článku {article_url}: {str(e)}")
        return None  # V případě chyby vracíme None

async def scrape_page(session, page_number):
    """Zpracujeme jednu stránku, vytáhneme URL článků a stáhneme jejich data."""
    url = f'https://www.idnes.cz/data.aspx?type=infinitesph&r=sph&section=sport&strana={page_number}&version=sph2024'
    async with session.get(url) as response:
        if response.status != 200:
            print(f"Chyba při volání API na stránce {page_number}: {response.status}")
            return []

        soup = BeautifulSoup(await response.text(), 'html.parser')
        articles = soup.find_all('a', class_='art-link')

        if not articles:
            print(f"Konec obsahu na stránce {page_number}.")
            return []

        tasks = [scrape_article(session, article.get('href')) for article in articles]
        print(f"Zpracovávám stránku {page_number} s {len(tasks)} články.")
        return await asyncio.gather(*tasks)

async def scrape_idnes(cookies, max_workers=50):
    """Hlavní funkce pro scrapování vícero stránek paralelně."""
    global article_data_list
    await load_existing_urls()  # Načti již zpracované URL
    async with aiohttp.ClientSession(cookies=cookies) as session:
        page_number = 1490

        while True:
            page_tasks = [scrape_page(session, i) for i in range(page_number, page_number + max_workers)]
            page_results = await asyncio.gather(*page_tasks)

            for result in page_results:
                if result:
                    article_data_list.extend([article for article in result if article])

            page_number += max_workers

            if all(not result for result in page_results):
                break

            # Uložení po každém zpracovaném chunku
            if len(article_data_list) >= chunk_size:
                await async_save_data_chunk()

        # Uložíme zbylá data na konci
        await async_save_data_chunk()

if __name__ == "__main__":
    cookies = {'kolbda': '1'}
    asyncio.run(scrape_idnes(cookies, max_workers=1))
