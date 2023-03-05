import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor as TPE
from concurrent.futures import as_completed

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from cache_to_disk import cache_to_disk
from settings import *

so = requests.Session()
so.headers.update({"User-Agent": USER_AGENT})


@cache_to_disk(DAYS_TO_CACHE)
def request_get(url):
    res = ""
    for i in range(5):
        try:
            res = so.get(url)
        except:
            time.sleep(1.5**i)
        else:
            break
    return res


def pretty_text(s: str) -> str:
    if hasattr(s, "text"):
        s = s.text
    s = unicodedata.normalize("NFKC", s)
    while "\n\n" in s:
        s = s.replace("\n\n", "\n")
    return s.strip().replace("\n", "/")


def get_options(soup):
    options = soup.select("#bkdt-option")[0].find("ul").find("li")
    details = pretty_text(options).replace(",", "、").replace("，", "、").split("、")
    details = list(map(lambda x: x.strip(), details))
    return details


def get_details(url):
    res = request_get(url)
    soup = BeautifulSoup(res.content, "html.parser")
    options = get_options(soup)

    data = {"options": options}
    table = soup.find("table", class_="table_gaiyou")
    for tr in table.find_all("tr"):
        for th, td in zip(tr.find_all("th"), tr.find_all("td")):
            th, td = map(pretty_text, [th, td])
            data[th] = td
    return data


def extract_listing(theads, listing):
    href = (
        listing.find("td", class_="ui-text--midium ui-text--bold").find("a").get("href")
    )
    url = f"https://suumo.jp{href}"

    row = {}
    tds = list(map(lambda x: pretty_text(x), listing.find_all("td")))
    for a, b in zip(theads, tds):
        if a and b and a not in ignore_rows:
            row[a] = b
    additional = get_details(url)
    row.update(additional)
    row["url"] = url
    row = pd.DataFrame.from_dict(row, orient="index").T
    return row


def do(page: int):
    assert 1 <= page and isinstance(page, int)
    page_url = URL.format(page)
    res = request_get(page_url)
    soup = BeautifulSoup(res.content, "html.parser")

    dfs = []
    for article in tqdm(list(soup.find_all("div", class_="cassetteitem"))):
        # bldg
        datum = {"page": page}
        for key, value in bldg_config.items():
            datum[key] = article.find(value["tag"], class_=value["class"])
            datum[key] = pretty_text(datum[key])
        df = pd.DataFrame(datum, index=[0])

        table = article.find("table", class_="cassetteitem_other")
        theads = list(map(lambda x: pretty_text(x), table.find_all("th")))
        listings = table.find_all("tbody")
        with TPE(max_workers=32) as executor:
            futures = []
            for listing in listings:
                future = executor.submit(extract_listing, theads, listing)
                futures.append(future)
            for future in as_completed(futures):
                row = future.result()
                row = pd.concat([df, row], axis=1)
                dfs.append(row)

    df = pd.concat(dfs, axis=0).reset_index(drop=True)
    return df


def main():
    gf = None
    dfs = []
    for page in range(1, 206):
        df = do(page)
        dfs.append(df)
        gf = pd.concat(dfs, axis=0)
        gf = gf.drop_duplicates(subset=["title", "url"]).reset_index(drop=True)
        gf.to_csv("data.csv", index=False)
    gf.to_csv("data.csv", index=False)


if __name__ == "__main__":
    main()
