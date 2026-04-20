"""
Download StoresFull XML for all 4 chains using direct HTTP/FTP.
Filters by target cities and subchain, writes config/branches.json.
"""

import ftplib
import gzip
import io
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

CONFIG_DIR = Path(__file__).parent.parent / "config"
CHAINS_CONFIG = json.loads((CONFIG_DIR / "chains.json").read_text(encoding="utf-8"))
TARGET_CITIES = CHAINS_CONFIG["target_cities"]
FTP_HOST = "url.retail.publishedprices.co.il"


def decompress(content: bytes) -> bytes:
    try:
        return gzip.decompress(content)
    except Exception:
        return content


def parse_stores_xml(content: bytes) -> list[dict]:
    xml_bytes = decompress(content)
    for enc in ("utf-8", "windows-1255", "iso-8859-8"):
        try:
            root = ET.fromstring(xml_bytes.decode(enc).encode("utf-8"))
            break
        except Exception:
            continue
    else:
        return []

    stores = []
    for store in root.iter("Store"):
        stores.append({
            "store_id": (store.findtext("StoreId") or store.findtext("StoreID") or "").strip(),
            "store_name": (store.findtext("StoreName") or "").strip(),
            "subchain": (store.findtext("SubChainName") or "").strip(),
            "city": (store.findtext("City") or "").strip(),
        })
    return stores


def fetch_ftp(username: str, pattern: str) -> bytes | None:
    try:
        ftp = ftplib.FTP(FTP_HOST, timeout=30)
        ftp.login(username, "")
        files = sorted(ftp.nlst())
        matching = [f for f in files if pattern.lower() in f.lower()]
        if not matching:
            print(f"  No files matching '{pattern}'")
            ftp.quit()
            return None
        filename = matching[-1]
        print(f"  Downloading {filename}...")
        buf = io.BytesIO()
        ftp.retrbinary(f"RETR {filename}", buf.write)
        ftp.quit()
        return buf.getvalue()
    except Exception as e:
        print(f"  FTP error ({username}): {e}")
        return None


def fetch_shufersal(pattern: str) -> bytes | None:
    try:
        base = "https://prices.shufersal.co.il/FileObject/UpdateCategory"
        # catID=5 is the stores category — all files on this page are store files
        resp = requests.get(base, params={"catID": "5", "page": "1"}, timeout=30)
        # Links look like href="/File/Get?fileId=12345" with anchor text = filename
        pairs = re.findall(r'href="([^"]*File/Get[^"]*)"[^>]*>([^<]+)<', resp.text)
        matching = [(href, text) for href, text in pairs if pattern.lower() in text.lower()]
        if not matching:
            # Fall back: take any File/Get link (catID=5 should be only store files)
            matching = [(href, text) for href, text in pairs]
        if not matching:
            print(f"  No Shufersal file found (pattern='{pattern}')")
            return None
        href, name = sorted(matching)[-1]
        url = "https://prices.shufersal.co.il" + href if href.startswith("/") else href
        print(f"  Downloading {name.strip()}...")
        r = requests.get(url, timeout=60)
        return r.content
    except Exception as e:
        print(f"  Shufersal error: {e}")
        return None


def fetch_carrefour(pattern: str) -> bytes | None:
    try:
        resp = requests.get("https://prices.carrefour.co.il/", timeout=30)
        path_m = re.search(r"const path = ['\"]([^'\"]+)['\"]", resp.text)
        files_m = re.search(r"const files = (\[.*?\]);", resp.text, re.DOTALL)
        if not path_m or not files_m:
            print("  Could not parse Carrefour file list")
            return None
        base_path = path_m.group(1)
        files = json.loads(files_m.group(1))
        names = sorted([f["name"] for f in files if pattern.lower() in f["name"].lower()])
        if not names:
            print(f"  No Carrefour file matching '{pattern}'")
            return None
        url = f"https://prices.carrefour.co.il/{base_path}/{names[-1]}"
        print(f"  Downloading {names[-1]}...")
        r = requests.get(url, timeout=60)
        return r.content
    except Exception as e:
        print(f"  Carrefour error: {e}")
        return None


CHAIN_FETCHERS = {
    "OSHER_AD": lambda: fetch_ftp("osherad", "stores"),
    "RAMI_LEVY": lambda: fetch_ftp("RamiLevi", "stores"),
    "SHUFERSAL": lambda: fetch_shufersal("stores"),
    "YAYNO_BITAN_AND_CARREFOUR": lambda: fetch_carrefour("stores"),
}
SUBCHAIN_FILTERS = {
    "SHUFERSAL": "שופרסל דיל",
    "YAYNO_BITAN_AND_CARREFOUR": "קרפור",
}


def main():
    branches = {k: [] for k in CHAIN_FETCHERS}

    for chain_key, fetch_fn in CHAIN_FETCHERS.items():
        print(f"\n[{chain_key}]")
        content = fetch_fn()
        if not content:
            print("  Skipped.")
            continue

        stores = parse_stores_xml(content)
        subchain_filter = SUBCHAIN_FILTERS.get(chain_key)
        matched = [
            s for s in stores
            if s["city"] in TARGET_CITIES
            and (subchain_filter is None or subchain_filter in s["subchain"])
        ]

        branches[chain_key] = [
            {"store_id": s["store_id"], "store_name": s["store_name"],
             "subchain": s["subchain"], "city": s["city"]}
            for s in matched
        ]
        print(f"  Found {len(matched)} matching branches:")
        for b in branches[chain_key]:
            print(f"    [{b['store_id']}] {b['store_name']} - {b['city']}")

    out_path = CONFIG_DIR / "branches.json"
    out_path.write_text(json.dumps(branches, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nSaved branch list to {out_path}")


if __name__ == "__main__":
    main()
