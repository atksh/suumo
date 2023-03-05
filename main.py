import os
import shutil
import time
import traceback
import unicodedata
from concurrent.futures import ThreadPoolExecutor as TPE
from concurrent.futures import as_completed
from itertools import count

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
    for i in range(3):
        try:
            res = so.get(url)
        except:
            traceback.print_exc()
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
    print(tds)
    for a, b in zip(theads, tds):
        row[a] = b
    try:
        additional = get_details(url)
    except:
        pass
    else:
        row.update(additional)
    row = pd.DataFrame.from_dict(row, orient="index").T
    row["url"] = url
    return row


def do(page: int):
    assert 1 <= page and isinstance(page, int)
    page_url = URL.format(page)
    res = request_get(page_url)
    soup = BeautifulSoup(res.content, "html.parser")

    dfs = []
    for article in soup.find_all("div", class_="cassetteitem"):
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
                if len(row) > 0:
                    row = pd.concat([df, row], axis=1)
                    dfs.append(row)

    df = pd.concat(dfs, axis=0).reset_index(drop=True)
    return df


def main():
    shutil.rmtree("data", ignore_errors=True)
    os.mkdir("data")
    failed_count = 0
    for page in count(start=1):
        print("=== Page {} ===".format(page))
        df = []
        for _ in range(3):
            try:
                df = do(page)
            except:
                traceback.print_exc()
            else:
                if len(df) == 0:
                    print("No data found. Exiting...")
                    continue
                else:
                    break
        if len(df) == 0:
            failed_count += 1
            break
        df.to_csv(f"data/{page}.csv")
        if failed_count >= 10:
            return


if __name__ == "__main__":
    main()
