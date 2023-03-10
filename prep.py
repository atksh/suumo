import json
import re
import numpy as np
import pandas as pd


def loads(x):
    x = str(x)
    try:
        return sorted(json.loads(x))
    except:
        return []


df = pd.read_csv("data.csv").drop(
    [
        "Unnamed: 6",
        "お気に入り",
        "取り扱い店舗物件コード",
        "保証会社",
        "ほか初期費用",
        "備考",
        "バルコニー面積",
        "取引態様",
        "SUUMO物件コード",
        "情報更新日",
        "次回更新日",
        "損保",
        "総戸数",
        "駐車場",
        "間取り詳細",
        "敷金積み増し",
    ],
    axis=1,
)
df["options"] = df["options"].apply(loads)
df = df[df["契約期間"].apply(lambda x: "普通借家" in str(x))].copy()


def extract_numbers(x) -> "list[int]":
    return re.findall(r"\d+", x)


def parse_fee(fee):
    fee = str(fee)
    if "万" in fee:
        fee = fee.replace("万", "")
        fee = float(fee) * 10_000
    elif "千" in fee:
        fee = fee.replace("千", "")
        fee = float(fee) * 1_000
    else:
        try:
            fee = float(fee)
        except:
            fee = 0
    return int(fee)


def parse_price(x: str):
    x = str(x).replace("円", "").replace("￥", "")
    rent, fee = x.split("/")
    return parse_fee(rent), parse_fee(fee)


def make_cat(df, col="options"):
    df = df.copy()
    all_options = set()
    for line in df[col].copy():
        if isinstance(line, list):
            all_options |= set(line)
        else:
            all_options.add(line)
    all_options = sorted(list(all_options))

    for opt in all_options:
        se = (
            df[col]
            .apply(lambda x: 1 if opt in x else 0)
            .rename(f"{col}_{opt}")
            .to_frame()
        )
        df = pd.concat([df, se], axis=1)
    return df.copy()


def parse_mid_fee(x):
    x = str(x)
    is_monthly = False
    if "ヶ月" in x:
        x = x.replace("ヶ月", "")
        x = float(x)
        is_monthly = True
    else:
        try:
            x = parse_fee(x)
        except:
            x = 0
    return (is_monthly, x)


def parse_age(x):
    try:
        a, h = extract_numbers(x)
    except:
        a, h = None, None
    return a, h


def parse_layout(x):
    x = str(x)
    l, area = x.split("/")
    area = area.replace("m2", "")
    area = float(area)
    return l, area


def parse_station(x):
    x = x.replace("徒歩", "歩")
    x = x.replace(" 歩", "-")
    x = x.replace("線/", "-")
    out = []
    for s in x.split("/"):
        try:
            line, sta, dist = s.split("-")
            line = line + "線"
            dist = dist.replace("分", "")
            dist = int(dist)
            out.append((line, sta, dist))
        except:
            pass
    out = sorted(out, key=lambda x: x[2])
    return out


df["monthly_price"] = df["賃料/管理費"].apply(parse_price).apply(lambda x: x[0] + x[1])
df["initial_cost"] = df["敷金/礼金"].apply(parse_price).apply(lambda x: x[0] + x[1])
df["proxy_cost"] = df["仲介手数料"].apply(parse_mid_fee)
df["proxy_cost_is_monthly"] = df["proxy_cost"].apply(lambda x: x[0])
df["proxy_cost_month_unit"] = df["proxy_cost"].apply(lambda x: x[1])
df["passed_years"] = df["age"].apply(parse_age).apply(lambda x: x[0])
df["max_floors"] = df["age"].apply(parse_age).apply(lambda x: x[1])
df = df[~df["max_floors"].isnull()].copy()
df["layout"] = df["間取り/専有面積"].apply(parse_layout).apply(lambda x: x[0])
df["area"] = df["間取り/専有面積"].apply(parse_layout).apply(lambda x: x[1])
df["arch"] = df["構造"]
df = df[df["階"].apply(lambda x: "B" not in x)].copy()
df["floor"] = df["階"].apply(
    lambda x: int(x.replace("階", "").split("-")[0]) if "階" in x else None
)
df = df[~df["floor"].isnull()].copy()
df["floor"] = df["floor"].astype(int)
df["max_floors"] = df["max_floors"].astype(int)
df["floor_ratio"] = df["floor"] / df["max_floors"]
df["diff_from_max"] = df["max_floors"] - df["floor"]

proxy_cost_is_monthly = df["proxy_cost_is_monthly"].astype(bool).values
monthly_price = df["monthly_price"].values
proxy_cost_month_unit = df["proxy_cost_month_unit"].values
df["proxy_cost"] = np.where(
    proxy_cost_is_monthly,
    monthly_price * proxy_cost_month_unit,
    proxy_cost_month_unit,
)
df["stations"] = df["stations"].apply(parse_station)
for i in range(3):
    df[f"line_{i}"] = df["stations"].apply(lambda x: x[i][0] if len(x) > i else "None")
    df[f"sta_{i}"] = df["stations"].apply(lambda x: x[i][1] if len(x) > i else "None")
    df[f"dist_{i}"] = df["stations"].apply(lambda x: x[i][2] if len(x) > i else 30)
    df = make_cat(df, f"line_{i}")
    df = make_cat(df, f"sta_{i}")
    df = df.drop([f"line_{i}", f"sta_{i}"], axis=1)

# df = make_cat(df, "options")
df = make_cat(df, "arch")
df = make_cat(df, "layout")
drop_cols = [
    "契約期間",
    "間取り/専有面積",
    "age",
    "構造",
    "賃料/管理費",
    "敷金/礼金",
    "options",
    "proxy_cost_is_monthly",
    "proxy_cost_month_unit",
    "仲介手数料",
    "ほか諸費用",
    "入居",
    "arch",
    "階",
    "階建",
    "築年月",
    "条件",
    "stations",
    "address",
    "layout",
]
for col in drop_cols:
    assert col in df.columns, col
df = df.drop(drop_cols, axis=1)
df.to_csv("cleaned.csv", index=False)
