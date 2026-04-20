"""
Download PriceFull XML files for all 4 chains using direct HTTP/FTP.
Saves processed CSVs to data/YYYY-MM-DD/. Raw XMLs are never kept.
"""

import ftplib
import gzip
import io
import json
import re
import tempfile
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

SRC_DIR = Path(__file__).parent
CONFIG_DIR = SRC_DIR.parent / "config"
DATA_DIR = SRC_DIR.parent / "data"

CHAINS_CONFIG = json.loads((CONFIG_DIR / "chains.json").read_text(encoding="utf-8"))
FTP_HOST = "url.retail.publishedprices.co.il"


def load_branches() -> dict:
    path = CONFIG_DIR / "branches.json"
    if not path.exists():
        raise FileNotFoundError("branches.json not found. Run fetch_stores.py first.")
    return json.loads(path.read_text(encoding="utf-8"))


def decompress(content: bytes) -> bytes:
    try:
        return gzip.decompress(content)
    except Exception:
        return content


def parse_price_xml(content: bytes) -> tuple[dict, list[dict]]:
    xml_bytes = decompress(content)
    for enc in ("utf-8", "windows-1255", "iso-8859-8"):
        try:
            root = ET.fromstring(xml_bytes.decode(enc).encode("utf-8"))
            break
        except Exception:
            continue
    else:
        return {}, []

    meta = {
        "chain_id": (root.findtext("ChainId") or root.findtext("ChainID") or "").strip(),
        "store_id": (root.findtext("StoreId") or root.findtext("StoreID") or "").strip(),
    }

    products = []
    for item in root.iter("Item"):
        barcode = (item.findtext("ItemCode") or "").strip()
        name = (item.findtext("ItemName") or "").strip()
        price = (item.findtext("ItemPrice") or "").strip()
        unit = (item.findtext("UnitOfMeasure") or "").strip()
        update_date = (item.findtext("PriceUpdateDate") or "").strip()
        if barcode:
            products.append({
                "barcode": barcode, "name": name, "price": price,
                "unit": unit, "price_update_date": update_date,
            })

    return meta, products


def find_chain_key(chain_id: str) -> str | None:
    for key, cfg in CHAINS_CONFIG["chains"].items():
        if cfg["chain_id"] == chain_id:
            return key
    return None


def store_matches_filename(store_id: str, filename: str) -> bool:
    name = Path(filename).name
    sid = store_id.lstrip("0") or "0"
    return (
        f"-{store_id}-" in name
        or f"-{store_id}." in name
        or f"-{sid}-" in name
        or f"-{sid}." in name
        or f"-{store_id.zfill(3)}-" in name
        or f"-{store_id.zfill(7)}-" in name
    )


# ── FTP downloader ─────────────────────────────────────────────────────────────

def download_ftp_prices(username: str, store_ids: set[str]) -> list[bytes]:
    results = []
    try:
        ftp = ftplib.FTP(FTP_HOST, timeout=30)
        ftp.login(username, "")
        files = sorted(ftp.nlst())
        price_files = [f for f in files if "pricefull" in f.lower()]

        if store_ids:
            price_files = [f for f in price_files if any(store_matches_filename(s, f) for s in store_ids)]

        for filename in price_files:
            try:
                buf = io.BytesIO()
                ftp.retrbinary(f"RETR {filename}", buf.write)
                results.append(buf.getvalue())
                print(f"  Downloaded {filename}")
            except Exception as e:
                print(f"  Failed {filename}: {e}")

        ftp.quit()
    except Exception as e:
        print(f"  FTP error ({username}): {e}")
    return results


# ── Shufersal HTTP downloader ──────────────────────────────────────────────────

def _shufersal_price_links(store_ids: set[str]) -> list[str]:
    base = "https://prices.shufersal.co.il/FileObject/UpdateCategory"
    all_links: list[str] = []
    page = 1
    while True:
        try:
            resp = requests.get(base, params={"catID": "2", "page": str(page)}, timeout=30)
            links = re.findall(r'href="(/FileObject/[^"]+)"', resp.text)
            price_links = [l for l in links if "pricefull" in l.lower()]
            if not price_links:
                break
            all_links.extend(price_links)
            if 'page=' not in resp.text or len(price_links) < 5:
                break
            page += 1
        except Exception as e:
            print(f"  Shufersal page {page} error: {e}")
            break

    if store_ids:
        all_links = [l for l in all_links if any(store_matches_filename(s, l) for s in store_ids)]
    return list(set(all_links))


def download_shufersal_prices(store_ids: set[str]) -> list[bytes]:
    results = []
    links = _shufersal_price_links(store_ids)
    for link in links:
        try:
            url = "https://prices.shufersal.co.il" + link
            r = requests.get(url, timeout=60)
            results.append(r.content)
            print(f"  Downloaded {link.split('/')[-1]}")
        except Exception as e:
            print(f"  Failed {link}: {e}")
    return results


# ── Carrefour HTTP downloader ──────────────────────────────────────────────────

def download_carrefour_prices(store_ids: set[str]) -> list[bytes]:
    results = []
    try:
        resp = requests.get("https://prices.carrefour.co.il/", timeout=30)
        path_m = re.search(r"const path = ['\"]([^'\"]+)['\"]", resp.text)
        files_m = re.search(r"const files = (\[.*?\]);", resp.text, re.DOTALL)
        if not path_m or not files_m:
            print("  Could not parse Carrefour file list")
            return results
        base_path = path_m.group(1)
        files = json.loads(files_m.group(1))
        price_files = [f["name"] for f in files if "pricefull" in f["name"].lower()]
        if store_ids:
            price_files = [f for f in price_files if any(store_matches_filename(s, f) for s in store_ids)]
        for name in price_files:
            try:
                url = f"https://prices.carrefour.co.il/{base_path}/{name}"
                r = requests.get(url, timeout=60)
                results.append(r.content)
                print(f"  Downloaded {name}")
            except Exception as e:
                print(f"  Failed {name}: {e}")
    except Exception as e:
        print(f"  Carrefour error: {e}")
    return results


# ── Processing ─────────────────────────────────────────────────────────────────

def process_and_save(all_contents: list[tuple[str, bytes]], run_date: date, branches: dict):
    out_dir = DATA_DIR / run_date.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)

    branch_store_ids: dict[str, set[str]] = {
        k: {s["store_id"] for s in v} for k, v in branches.items()
    }
    branch_map: dict[tuple[str, str], dict] = {
        (k, s["store_id"]): s for k, v in branches.items() for s in v
    }

    saved = 0
    for chain_key, content in all_contents:
        meta, products = parse_price_xml(content)
        if not products:
            continue

        store_id = meta.get("store_id", "")
        if store_id not in branch_store_ids.get(chain_key, set()):
            continue

        info = branch_map.get((chain_key, store_id), {})
        chain_display = CHAINS_CONFIG["chains"][chain_key]["display_name"]

        df = pd.DataFrame(products)
        df["chain"] = chain_display
        df["chain_key"] = chain_key
        df["store_id"] = store_id
        df["store_name"] = info.get("store_name", store_id)
        df["city"] = info.get("city", "")
        df["date"] = run_date.isoformat()

        safe_name = f"{chain_key}_{store_id}_{info.get('city','')}.csv".replace(" ", "_")
        df.to_csv(out_dir / safe_name, index=False, encoding="utf-8-sig")
        print(f"  Saved {len(products)} products: {safe_name}")
        saved += 1

    return saved


def main(run_date: date | None = None):
    if run_date is None:
        run_date = date.today() - timedelta(days=1)

    branches = load_branches()
    print(f"Downloading prices for {run_date}...")

    chain_contents: list[tuple[str, bytes]] = []

    for chain_key, store_list in branches.items():
        if not store_list:
            print(f"\n[{chain_key}] No branches — skipping")
            continue
        store_ids = {s["store_id"] for s in store_list}
        print(f"\n[{chain_key}] {len(store_ids)} branches")

        if chain_key == "OSHER_AD":
            contents = download_ftp_prices("osherad", store_ids)
        elif chain_key == "RAMI_LEVY":
            contents = download_ftp_prices("RamiLevi", store_ids)
        elif chain_key == "SHUFERSAL":
            contents = download_shufersal_prices(store_ids)
        elif chain_key == "YAYNO_BITAN_AND_CARREFOUR":
            contents = download_carrefour_prices(store_ids)
        else:
            contents = []

        chain_contents.extend((chain_key, c) for c in contents)

    print("\nProcessing...")
    saved = process_and_save(chain_contents, run_date, branches)
    print(f"Done. Saved {saved} branch files to data/{run_date}/")


if __name__ == "__main__":
    main()
