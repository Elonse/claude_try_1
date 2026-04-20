"""
Download StoresFull XML for all 4 chains, extract branches in our target cities,
and save the result to config/branches.json.
Run once from an Israeli IP to build the branch list.
"""

import json
import os
import tempfile
from pathlib import Path

from il_supermarket_scarper import ScarpingTask
from il_supermarket_scarper.scrappers_factory import ScraperFactory
from il_supermarket_scarper.utils import DiskFileOutput
from il_supermarket_scarper.utils.file_types import FileTypesFilters

CONFIG_DIR = Path(__file__).parent.parent / "config"
CHAINS_CONFIG = json.loads((CONFIG_DIR / "chains.json").read_text(encoding="utf-8"))

TARGET_CITIES = set(CHAINS_CONFIG["target_cities"])

CHAIN_SCRAPERS = [
    ScraperFactory.OSHER_AD,
    ScraperFactory.RAMI_LEVY,
    ScraperFactory.SHUFERSAL,
    ScraperFactory.YAYNO_BITAN_AND_CARREFOUR,
]

CHAIN_KEY_MAP = {
    "OSHER_AD": "OSHER_AD",
    "RAMI_LEVY": "RAMI_LEVY",
    "SHUFERSAL": "SHUFERSAL",
    "YAYNO_BITAN_AND_CARREFOUR": "YAYNO_BITAN_AND_CARREFOUR",
}


def download_stores(tmp_dir: str):
    task = ScarpingTask(
        enabled_scrapers=CHAIN_SCRAPERS,
        files_types=[FileTypesFilters.STORE_FILE],
        output_configuration=DiskFileOutput(tmp_dir),
        multiprocessing=1,
    )
    task.start()


def parse_store_xml(xml_path: Path) -> list[dict]:
    import xml.etree.ElementTree as ET

    try:
        tree = ET.parse(xml_path)
    except ET.ParseError:
        return []

    root = tree.getroot()
    stores = []
    for store in root.iter("Store"):
        city = (store.findtext("City") or "").strip()
        store_id = store.findtext("StoreId") or store.findtext("StoreID") or ""
        store_name = store.findtext("StoreName") or ""
        subchain = store.findtext("SubChainName") or ""
        chain_id = store.findtext("ChainId") or store.findtext("ChainID") or ""
        stores.append({
            "store_id": store_id.strip(),
            "store_name": store_name.strip(),
            "subchain": subchain.strip(),
            "city": city,
            "chain_id": chain_id.strip(),
        })
    return stores


def find_chain_key(chain_id: str) -> str | None:
    for key, cfg in CHAINS_CONFIG["chains"].items():
        if cfg["chain_id"] == chain_id:
            return key
    return None


def matches_subchain_filter(store: dict, chain_key: str) -> bool:
    cfg = CHAINS_CONFIG["chains"].get(chain_key, {})
    filt = cfg.get("subchain_filter")
    if not filt:
        return True
    return filt in store["subchain"]


def build_branch_list(tmp_dir: str) -> dict:
    branches = {key: [] for key in CHAINS_CONFIG["chains"]}

    for xml_file in Path(tmp_dir).rglob("*.xml"):
        stores = parse_store_xml(xml_file)
        if not stores:
            continue

        for store in stores:
            city = store["city"]
            if city not in TARGET_CITIES:
                continue

            chain_key = find_chain_key(store["chain_id"])
            if not chain_key:
                continue

            if not matches_subchain_filter(store, chain_key):
                continue

            branches[chain_key].append({
                "store_id": store["store_id"],
                "store_name": store["store_name"],
                "subchain": store["subchain"],
                "city": city,
            })

    return branches


def main():
    with tempfile.TemporaryDirectory() as tmp_dir:
        print("Downloading stores files...")
        download_stores(tmp_dir)

        print("Parsing stores...")
        branches = build_branch_list(tmp_dir)

        output_path = CONFIG_DIR / "branches.json"
        output_path.write_text(
            json.dumps(branches, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"Saved branch list to {output_path}")

        for chain_key, stores in branches.items():
            print(f"\n{chain_key} ({len(stores)} branches):")
            for s in stores:
                print(f"  [{s['store_id']}] {s['store_name']} - {s['city']} ({s['subchain']})")


if __name__ == "__main__":
    main()
