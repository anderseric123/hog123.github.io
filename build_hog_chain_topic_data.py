#!/usr/bin/env python3
"""Build data bundle for hog industry chain monitoring topic page."""

from __future__ import annotations

import json
import re
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://www.moa.gov.cn/ztzl/szcpxx/jdsj"
WORKDIR = Path(__file__).resolve().parent
PROFIT_XLSX = Path("/Users/anders/Downloads/中国_养殖利润_自繁自养生猪.xlsx")
OUTPUT_JSON = WORKDIR / "hog-chain-topic-data.json"
OUTPUT_JS = WORKDIR / "hog-chain-topic-data.js"

def parse_pct(value: str) -> Optional[float]:
    text = (value or "").strip().replace("％", "%")
    if not text or "—" in text or "-" == text:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    return float(match.group()) / 100.0


def parse_num(value: str) -> Optional[float]:
    text = (value or "").replace(",", "").strip()
    if not text or "—" in text:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    return float(match.group())


def normalise_space(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def extract_indicator_date(indicator: str, page_ym: str) -> str:
    text = normalise_space(indicator)

    match = re.search(r"(\d{4})年\s*1[-—](\d{1,2})月", text)
    if match:
        return f"{int(match.group(1)):04d}-{int(match.group(2)):02d}-01"

    match = re.search(r"(\d{4})年\s*(\d{1,2})月(?:份|末)?", text)
    if match:
        return f"{int(match.group(1)):04d}-{int(match.group(2)):02d}-01"

    match = re.search(r"(\d{4})年\s*([1-4])季度末", text)
    if match:
        year = int(match.group(1))
        month = int(match.group(2)) * 3
        return f"{year:04d}-{month:02d}-01"

    match = re.search(r"(\d{4})年", text)
    if match:
        return f"{int(match.group(1)):04d}-12-01"

    return f"{page_ym}-01"


def classify_indicator(indicator: str) -> Optional[str]:
    text = normalise_space(indicator)
    rules = [
        (r"累计进口猪肉", "import_pork_cum"),
        (r"累计进口猪杂碎", "import_offal_cum"),
        (r"累计出口猪肉", "export_pork_cum"),
        (r"累计出口猪杂碎", "export_offal_cum"),
        (r"(?:规模以上)?生猪定点屠宰企业屠宰量.*1[-—]\d+月", "slaughter_volume_cum"),
        (r"1[-—]\d+月.*(?:规模以上)?生猪定点屠宰企业屠宰量", "slaughter_volume_cum"),
        (r"(?:规模以上)?生猪定点屠宰企业屠宰量", "slaughter_volume_monthly"),
        (r"进口猪肉", "import_pork_monthly"),
        (r"进口猪杂碎", "import_offal_monthly"),
        (r"出口猪肉", "export_pork_monthly"),
        (r"出口猪杂碎", "export_offal_monthly"),
        (r"能繁母猪存栏", "sow_inventory"),
        (r"生猪存栏", "hog_inventory"),
        (r"生猪出栏", "hog_slaughter"),
        (r"猪肉产量", "pork_output"),
        (r"二元母猪销售价格", "sow_sale_price"),
        (r"仔猪价格", "piglet_price"),
        (r"生猪出场价格", "hog_exit_price"),
        (r"全国批发市场白条猪价格", "wholesale_carcass_price"),
        (r"36个大中城市批发市场白条猪价格", "city_wholesale_carcass_price"),
        (r"精瘦肉）零售价格", "lean_meat_retail_price"),
        (r"后腿肉）零售价格", "hind_leg_retail_price"),
        (r"县乡集贸市场猪肉零售价格", "county_market_retail_price"),
        (r"居民(?:家庭)?人均猪肉消费量", "per_capita_pork_consumption"),
    ]
    for pattern, key in rules:
        if re.search(pattern, text):
            return key
    return None


def col_to_index(col: str) -> int:
    idx = 0
    for ch in col:
        idx = idx * 26 + (ord(ch) - 64)
    return idx - 1


def parse_simple_xlsx(path: Path) -> List[List[str]]:
    with zipfile.ZipFile(path) as zf:
        shared_strings: List[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
            for node in root.findall(f"{ns}si"):
                shared_strings.append("".join(t.text or "" for t in node.iter(f"{ns}t")))

        ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
        rel_ns = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
        wb_root = ET.fromstring(zf.read("xl/workbook.xml"))
        sheets = wb_root.find(f"{ns}sheets")
        first_sheet = sheets.find(f"{ns}sheet") if sheets is not None else None
        if first_sheet is None:
            return []

        rid = first_sheet.attrib.get(f"{rel_ns}id")
        rel_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        target = None
        for rel in rel_root:
            if rel.attrib.get("Id") == rid:
                target = rel.attrib.get("Target")
                break
        if not target:
            return []

        if not target.startswith("xl/"):
            target = f"xl/{target}"
        sheet = ET.fromstring(zf.read(target))

        rows: List[List[str]] = []
        sheet_data = sheet.find(f"{ns}sheetData")
        if sheet_data is None:
            return rows
        for row in sheet_data.findall(f"{ns}row"):
            cells: Dict[int, str] = {}
            for cell in row.findall(f"{ns}c"):
                ref = cell.attrib.get("r", "")
                match = re.match(r"([A-Z]+)\d+", ref)
                if not match:
                    continue
                col_idx = col_to_index(match.group(1))
                cell_type = cell.attrib.get("t")
                value_text = ""
                if cell_type == "s":
                    node = cell.find(f"{ns}v")
                    if node is not None and (node.text or "").strip():
                        shared_idx = int((node.text or "").strip())
                        if 0 <= shared_idx < len(shared_strings):
                            value_text = shared_strings[shared_idx]
                elif cell_type == "inlineStr":
                    inline = cell.find(f"{ns}is")
                    if inline is not None:
                        value_text = "".join(t.text or "" for t in inline.iter(f"{ns}t"))
                else:
                    node = cell.find(f"{ns}v")
                    value_text = (node.text or "").strip() if node is not None else ""
                cells[col_idx] = value_text
            if not cells:
                continue
            width = max(cells) + 1
            rows.append([cells.get(i, "") for i in range(width)])
        return rows


def parse_profit_weekly(path: Path) -> List[Dict[str, Any]]:
    rows = parse_simple_xlsx(path)
    if not rows:
        return []
    series: List[Dict[str, Any]] = []
    excel_origin = datetime(1899, 12, 30)
    for row in rows[1:]:
        if len(row) < 2:
            continue
        date_raw, value_raw = row[0], row[1]
        try:
            serial = float(date_raw)
            value = float(value_raw)
        except (TypeError, ValueError):
            continue
        date_obj = excel_origin + timedelta(days=serial)
        series.append(
            {
                "date": date_obj.strftime("%Y-%m-%d"),
                "value": round(value, 2),
            }
        )

    series.sort(key=lambda x: x["date"])
    prev = None
    for item in series:
        if prev is None or prev["value"] == 0:
            item["wow"] = None
        else:
            item["wow"] = round((item["value"] - prev["value"]) / abs(prev["value"]), 4)
        prev = item
    return series


def fetch_html(session: requests.Session, url: str, timeout: int = 12, retries: int = 2) -> str:
    last_error: Optional[Exception] = None
    for _ in range(retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            resp.raise_for_status()
            encoding = resp.apparent_encoding or resp.encoding or "utf-8"
            try:
                return resp.content.decode(encoding, errors="replace")
            except LookupError:
                return resp.content.decode("utf-8", errors="replace")
        except Exception as exc:  # noqa: BLE001
            last_error = exc
    raise RuntimeError(f"fetch failed: {url}") from last_error


def discover_month_urls(session: requests.Session, years: List[int]) -> List[str]:
    def resolve_html(url: str) -> str:
        html = fetch_html(session, url, timeout=20)
        refresh = re.search(r'content="0;url=([^"]+)"', html, flags=re.IGNORECASE)
        if refresh:
            jump = refresh.group(1).strip()
            next_url = requests.compat.urljoin(url, jump)
            html = fetch_html(session, next_url, timeout=20)
        return html

    url_set = set()
    for year in years:
        url = f"{BASE_URL}/{year}/"
        try:
            html = resolve_html(url)
        except Exception:  # noqa: BLE001
            continue
        soup = BeautifulSoup(html, "html.parser")
        for item in soup.select("li[objurl]"):
            objurl = (item.get("objurl") or "").strip()
            if not objurl:
                continue
            if re.search(rf"/jdsj/{year}/\d{{6}}/?$", objurl):
                url_set.add(objurl if objurl.endswith("/") else f"{objurl}/")
    return sorted(url_set)


def parse_moa_month_page(url: str, html: str) -> Tuple[str, List[Dict[str, Any]], Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    body_text = soup.get_text(" ", strip=True)
    title_match = re.search(r"生猪产品数据（(\d{4})年(\d{1,2})月）", body_text)
    if title_match:
        page_ym = f"{int(title_match.group(1)):04d}-{int(title_match.group(2)):02d}"
    else:
        url_match = re.search(r"/(\d{4})/(\d{6})/?$", url)
        if not url_match:
            raise ValueError(f"cannot infer month from url: {url}")
        ym = url_match.group(2)
        page_ym = f"{ym[:4]}-{ym[4:6]}"

    xlsx_url = None
    button = soup.select_one("a.redBtn")
    if button and button.get("href"):
        href = button["href"].strip()
        if href.startswith("http"):
            xlsx_url = href
        else:
            xlsx_url = requests.compat.urljoin(url, href)

    records: List[Dict[str, Any]] = []
    for table in soup.select("table.data_table.mobileNone"):
        for tr in table.select("tbody tr"):
            tds = tr.find_all("td")
            if len(tds) < 5:
                continue
            indicator = normalise_space(tds[1].get_text(" ", strip=True))
            value_text = normalise_space(tds[2].get_text(" ", strip=True))
            mom_text = normalise_space(tds[3].get_text(" ", strip=True))
            yoy_text = normalise_space(tds[4].get_text(" ", strip=True))
            key = classify_indicator(indicator)
            if not key:
                continue
            ratio = None
            ratio_match = re.search(r"相当于正常保有量\s*的\s*(\d+(?:\.\d+)?)%", value_text)
            if ratio_match:
                ratio = float(ratio_match.group(1)) / 100.0
            records.append(
                {
                    "key": key,
                    "date": extract_indicator_date(indicator, page_ym),
                    "value": parse_num(value_text),
                    "mom": parse_pct(mom_text),
                    "yoy": parse_pct(yoy_text),
                    "ratio_to_normal": ratio,
                    "indicator": indicator,
                    "value_text": value_text,
                    "source_page": url,
                    "source_month": page_ym,
                    "source_xlsx": xlsx_url,
                }
            )
    return page_ym, records, xlsx_url


def dedupe_records(records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    bucket: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    for row in sorted(records, key=lambda x: (x["source_month"], x["date"], x["key"])):
        key = row["key"]
        date = row["date"]
        prev = bucket[key].get(date)
        if prev is None:
            bucket[key][date] = row
            continue
        if row["source_month"] >= prev["source_month"]:
            bucket[key][date] = row

    result: Dict[str, List[Dict[str, Any]]] = {}
    for key, keyed in bucket.items():
        result[key] = sorted(keyed.values(), key=lambda x: x["date"])
    return result


def build_bundle() -> Dict[str, Any]:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
            )
        }
    )

    years = list(range(2021, datetime.now().year + 1))
    month_urls = discover_month_urls(session, years)

    all_records: List[Dict[str, Any]] = []
    month_index: List[Dict[str, Any]] = []
    for month_url in month_urls:
        try:
            html = fetch_html(session, month_url, timeout=12, retries=1)
            page_ym, records, xlsx_url = parse_moa_month_page(month_url, html)
            month_index.append(
                {"month": page_ym, "url": month_url, "xlsx": xlsx_url, "count": len(records), "status": "ok"}
            )
            all_records.extend(records)
        except Exception as exc:  # noqa: BLE001
            month_index.append({"month": None, "url": month_url, "xlsx": None, "count": 0, "status": str(exc)})

    series = dedupe_records(all_records)
    latest = {
        key: values[-1] if values else None
        for key, values in series.items()
    }

    profit_series = parse_profit_weekly(PROFIT_XLSX)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return {
        "meta": {
            "generated_at": now_str,
            "source": {
                "moa_base": BASE_URL,
                "profit_file": str(PROFIT_XLSX),
            },
            "month_count": len(month_urls),
            "series_count": len(series),
        },
        "month_index": month_index,
        "series": series,
        "latest": latest,
        "profit_weekly": profit_series,
    }


def main() -> None:
    bundle = build_bundle()
    payload = json.dumps(bundle, ensure_ascii=False, indent=2)
    OUTPUT_JSON.write_text(payload, encoding="utf-8")
    OUTPUT_JS.write_text(f"window.HOG_CHAIN_TOPIC_DATA = {payload};\n", encoding="utf-8")
    print(f"saved: {OUTPUT_JSON}")
    print(f"saved: {OUTPUT_JS}")
    print(f"months: {bundle['meta']['month_count']} series: {bundle['meta']['series_count']}")
    print("keys:", ", ".join(sorted(bundle["series"].keys())))
    print(f"profit points: {len(bundle['profit_weekly'])}")


if __name__ == "__main__":
    main()
