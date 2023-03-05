import os
import json
import random
import shutil
import unicodedata
from concurrent.futures import ThreadPoolExecutor as TPE
from concurrent.futures import as_completed
from itertools import count

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from retry import retry
from urllib3.util.retry import Retry

from cache_to_disk import cache_to_disk
from proxy import get_proxy_urls
from settings import *


@cache_to_disk(1)
def proxies():
    return get_proxy_urls()


def session() -> requests.Session:
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=5, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.headers.update({"User-Agent": USER_AGENT})
    return session


so = session()


@cache_to_disk(DAYS_TO_CACHE)
@retry(tries=3, backoff=2)
def request_get(url):
    def get(url, proxy_url=None):
        return so.get(
            url,
            timeout=(5, 10),
            proxies={scheme: f"http://{proxy_url}" for scheme in ("http", "https")}
            if proxy_url
            else None,
        )

    res = get(url)
    if res.ok:
        return res
    else:
        sampled_proxies = random.sample(proxies(), 5)
        for proxy_url in sampled_proxies:
            res = get(url, proxy_url)
            if res.ok:
                return res
    raise Exception(f"Failed to get {url}")


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
    try:
        options = get_options(soup)
    except IndexError:
        print(url)
        raise IndexError

    data = {"options": json.dumps(options)}
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
        row[a] = b
    additional = get_details(url)
    row.update(additional)
    row = pd.DataFrame.from_dict(row, orient="index").T
    row["url"] = url
    return row


@retry(tries=3)
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
        with TPE(max_workers=50) as executor:
            futures = []
            for listing in listings:
                future = executor.submit(extract_listing, theads, listing)
                futures.append(future)
            for future in as_completed(futures):
                row = future.result()
                if len(row) > 0:
                    row = pd.concat([df, row], axis=1)
                    dfs.append(row)

    if len(dfs) > 0:
        df = pd.concat(dfs, axis=0).reset_index(drop=True)
        return df
    else:
        print("No data found. Exiting...")
        return []


def main():
    shutil.rmtree("data", ignore_errors=True)
    os.mkdir("data")
    failed_count = 0
    for page in count(start=1):
        print("=== Page {} ===".format(page))
        df = do(page)
        if len(df) == 0:
            failed_count += 1
            break
        df.to_csv(f"data/{page}.csv")
        if failed_count >= 10:
            return


if __name__ == "__main__":
    main()
