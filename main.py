import unicodedata
from tqdm import tqdm

import pandas as pd
import requests
from bs4 import BeautifulSoup
from cache_to_disk import cache_to_disk

from settings import *

so = requests.Session()


@cache_to_disk(3)
def request_get(url):
    global so
    return so.get(url)


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
    


def main():
    res = request_get(URL)
    soup = BeautifulSoup(res.content, "html.parser")

    dfs = []
    for article in tqdm(list(soup.find_all("div", class_="cassetteitem"))):
        # bldg
        datum = {}
        for key, value in bldg_config.items():
            datum[key] = article.find(value["tag"], class_=value["class"])
            datum[key] = pretty_text(datum[key])
        df = pd.DataFrame(datum, index=[0])

        table = article.find("table", class_="cassetteitem_other")
        theads = list(map(lambda x: pretty_text(x), table.find_all("th")))
        listings = table.find_all("tbody")
        for listing in listings:
            href = (
                listing.find("td", class_="ui-text--midium ui-text--bold")
                .find("a")
                .get("href")
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
            row = pd.concat([df, row], axis=1)
            dfs.append(row)

    df = pd.concat(dfs, axis=0).reset_index(drop=True)
    df.to_csv("data.csv", index=False)


if __name__ == "__main__":
    main()
