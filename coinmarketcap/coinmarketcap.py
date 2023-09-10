import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
from multiprocessing import Pool


def get_html(url: str) -> str:
    """Возвращает html страницы."""
    response = requests.get(url)  # объект Response
    return response.text


def get_all_links(html: str) -> list:
    """Возвращает ссылки на страницы криптовалют."""
    soup = BeautifulSoup(html, "html.parser")  # объект BeautifulSoup
    links = []

    try:
        trs = soup.find("tbody").find_all("tr", class_="cmc-table-row")  # объект ResultSet
    except AttributeError:
        pass
    else:
        for tr in trs:
            a = tr.find("a", class_="cmc-link").get("href", None)
            if a is not None:
                links.append("https://coinmarketcap.com" + a)
    return links


def get_page_data(html: str) -> dict:
    """Возвращает название и цену криптовалюты."""
    soup = BeautifulSoup(html, "html.parser")
    coin_info = soup.find("div", id="section-coin-overview")
    coin_name, coin_price = None, None

    if coin_info is not None:
        coin_name = coin_info.find("span", class_="coin-name-pc")
        coin_price = coin_info.find("span", class_="sc-16891c57-0 dxubiK base-text")

        if coin_name is not None:
            coin_name = coin_name.text.strip()
        if coin_price is not None:
            coin_price = coin_price.text.strip()

    return {"name": coin_name, "price": coin_price}


def write_csv(data: dict, file_name: str):
    """Запись данных о криптовалюте в csv файл."""
    with open(file_name, "a") as file:
        writer = csv.writer(file)
        writer.writerow((data["name"], data["price"]))


def main_one_process(links):
    """Парсинг сайта одним процессом."""
    start_time = datetime.now()

    for ind, link in enumerate(links):
        html = get_html(link)
        data = get_page_data(html)
        write_csv(data, "coinmarketcap/coins_one_process.csv")
        print(f"{ind}: {data['name']}\nTime: {datetime.now() - start_time}\n")

    print(f"Total time: {datetime.now() - start_time}\n")


def parse_and_write_coin(link):
    """Парсинг и запись информации о криптовалюте в файл."""
    html = get_html(link)
    data = get_page_data(html)
    write_csv(data, "coinmarketcap/coins_multiprocessing.csv")
    print(f"{data['name']}")


def main_multiprocessing(links):
    """Парсинг сайта несколькими процессами."""
    start_time = datetime.now()

    with Pool(20) as pool:
        pool.map(parse_and_write_coin, links)

    print(f"Total time: {datetime.now() - start_time}\n")


if __name__ == "__main__":
    base_url = "https://coinmarketcap.com/all/views/all/"

    all_links = get_all_links(get_html(base_url))

    main_one_process(all_links)  # 52 секунды
    main_multiprocessing(all_links)  # 27 секунд
