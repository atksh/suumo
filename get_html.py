import random
from concurrent.futures import ThreadPoolExecutor
from functools import reduce
from typing import Callable, Dict, Iterable, List, Union

import pandas as pd
import requests
from bs4 import BeautifulSoup
from loguru import logger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .proxy import get_proxy_urls

requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += "HIGH:!DH:!aNULL"
try:
    requests.packages.urllib3.contrib.pyopenssl.DEFAULT_SSL_CIPHER_LIST += (
        "HIGH:!DH:!aNULL"
    )
except AttributeError:
    pass


def session() -> requests.Session:
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=5, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))
    return session


def get_ua_list(k: int = 5) -> Iterable[str]:
    """
    User-AgentのIterableを返す
    """
    # default first (iPhone)
    default_ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    yield default_ua

    uas = user_agents.copy()
    random.shuffle(uas)
    for ua in uas[:k]:
        yield ua


def get_html(url: str, allow_proxies: bool = True) -> str:
    return _get_html(url, False, allow_proxies)


def _get_html(
    url: str, ua: str, allow_redirects: bool, allow_proxies: bool = True
) -> str:
    headers = HEADERS.copy()
    headers["User-Agent"] = ua
    response = session().get(
        url,
        timeout=TIMEOUT,
        headers=headers,
        allow_redirects=allow_redirects,
    )
    if response.ok:
        content = response.content
        if len(content) == 0:
            # リダイレクト拒否したときに301等で抽出できない場合はエラーとする
            raise ValueError("Zero length content")
        return html_content_to_str(content)
    else:
        if allow_proxies:
            logger.info(
                f"Status code {response.status_code} of request for {url} is not ok. "
                "Retry with proxy."
            )
            return get_html_with_proxy(url, ua, allow_redirects)
        else:
            raise RequestWithoutProxyFailedError("Failed to fetch content")


def html_content_to_str(content: bytes) -> str:
    soup = BeautifulSoup(content, "html.parser")
    return str(soup)


def _get_html_content_with_proxy(
    url: str,
    proxy_url: str,
    ua: str,
    allow_redirects: bool,
) -> Union[bytes, None]:
    headers = HEADERS.copy()
    headers["User-Agent"] = ua
    try:
        logger.debug(f"url: {url}, headers: {headers}")
        response = session().get(
            url,
            timeout=TIMEOUT,
            headers=headers,
            proxies={scheme: f"http://{proxy_url}" for scheme in ("http", "https")},
            allow_redirects=allow_redirects,
        )
        if response.ok:
            return response.content
        else:
            logger.info(response)
            return None
    except (
        requests.exceptions.ProxyError,
        requests.exceptions.ConnectTimeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.ReadTimeout,
        requests.exceptions.ChunkedEncodingError,
    ) as e:
        logger.info(e)
        return None


def get_html_with_proxy(url: str, ua: str, allow_directs: bool) -> str:
    proxy_urls = get_proxy_urls()
    max_workers = 300
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        contents_with_proxy = list(
            executor.map(
                lambda proxy: _get_html_content_with_proxy(
                    url, proxy, ua, allow_directs
                ),
                proxy_urls,
            )
        )
    ok_response_counts = (
        pd.Series(contents_with_proxy).apply(bool).value_counts().to_dict()
    )
    logger.info(f"ok_response_counts = {ok_response_counts}")
    for content in contents_with_proxy:
        if content is not None:
            return html_content_to_str(content)
    error_message = "all requests with proxy failed."
    logger.error(error_message)
    raise AllRequestsWithProxyFailedError(error_message)
