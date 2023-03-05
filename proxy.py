from typing import List

import pandas as pd
import requests
from bs4 import BeautifulSoup


def get_proxy_df() -> pd.core.frame.DataFrame:
    content = requests.get("https://free-proxy-list.net/").content
    soup = BeautifulSoup(content, "html.parser")
    df = pd.read_html(str(soup), flavor="html5lib")[0]
    proxies = []
    for i, row in df.iterrows():
        ip = row["IP Address"]
        port = row["Port"]
        proxies.append(f"{ip}:{port}")
    df["url"] = proxies
    return df


def get_proxy_urls() -> List[str]:
    df = get_proxy_df()
    return list(df.url)
