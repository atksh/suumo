import json

ignore_rows = {"お気に入り"}
URL = (
    "https://suumo.jp/jj/chintai/"
    "ichiran/FR301FC001/"
    "?ar=030&bs=040&ra=013&cb=0.0"
    "&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999"
    "&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2="
    "&ek=000520110&ek=000519120&ek=000506000&ek=000520550"
    "&ek=000515330&ek=000523410&ek=000529160&rn=0005&srch_navi=1"
    "&page={}"
)
DAYS_TO_CACHE = 3
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"

with open("config_bldg.json") as f:
    bldg_config = json.load(f)
