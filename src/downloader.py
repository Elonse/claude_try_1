"""
Download PriceFull XML files for all 4 chains.
Reads config/branches.json to know which store IDs we care about.
Saves processed CSVs to data/YYYY-MM-DD/.
Raw XMLs are never kept.
"""

import json
import os
import tempfile
from datetime import date, timedelta
from pathlib import Path

from il_supermarket_scarper import ScarpingTask
from il_supermarket_scarper.scrappers_factory import ScraperFactory
from il_supermarket_scarper.utils import DiskFileOutput
from il_supermarket_scarper.utils.file_types import FileTypesFilters

SRC_DIR = Path(__file__).parent
CONFIG_DIR = SRC_DIR.parent / "config"
DATA_DIR = SRC_DIR.parent / "data"

CHAINS_CONFIG = json.loads((CONFIG_DIR / "chains.json").read_text(encoding="utf-8"))

CHAIN_SCRAPERS = [
    ScraperFactory.OSHER_AD,
    ScraperFactory.RAMI_LEVY,
    ScraperFactory.SHUFERSAL,
    ScraperFactory.YAYNO_BITAN_AND_CARREFOUR,
]


def load_branches() -> dict:
    path = CONFIG_DIR / "branches.json"
    if not path.exists():
        raise FileNotFoundError("branches.json not found. Run fetch_stores.py first.")
    return json.loads(path.read_text(encoding="utf-8"))


def download_prices(tmp_dir: str):
    task = ScarpingTask(
        enabled_scrapers=CHAIN_SCRAPERS,
        files_types=[FileTypesFilters.PRICE_FULL_FILE],
        output_configuration=DiskFileOutput(tmp_dir),
        multiprocessing=2,
    )
    task.start()


def parse_price_xml(xml_path: Path) -> tuple[dict, list[dict]]:
    """Returns (file_meta, list of product dicts)."""
    import xml.etree.ElementTree as ET

    try:
        tree = ET.parse(xml_path)
    except ET.ParseError:
        return {}, []

    root = tree.getroot()

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
        products.append({
            "barcode": barcode,
            "name": name,
            "price": price,
            "unit": unit,
            "price_update_date": update_date,
        })

    return meta, products


def find_chain_key(chain_id: str) -> str | None:
    for key, cfg in CHAINS_CONFIG["chains"].items():
        if cfg["chain_id"] == chain_id:
            return key
    return None


def process_and_save(tmp_dir: str, run_date: date, branches: dict):
    import pandas as pd

    out_dir = DATA_DIR / run_date.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)

    branch_store_ids = {}
    for chain_key, store_list in branches.items():
        branch_store_ids[chain_key] = {s["store_id"] for s in store_list}

    for xml_file in Path(tmp_dir).rglob("*.xml"):
        meta, products = parse_price_xml(xml_file)
        if not products:
            continue

        chain_key = find_chain_key(meta.get("chain_id", ""))
        if not chain_key:
            continue

        store_id = meta.get("store_id", "")
        if store_id not in branch_store_ids.get(chain_key, set()):
            continue

        branch_info = next(
            (s for s in branches[chain_key] if s["store_id"] == store_id), {}
        )
        city = branch_info.get("city", "")
        store_name = branch_info.get("store_name", store_id)
        chain_display = CHAINS_CONFIG["chains"][chain_key]["display_name"]

        df = pd.DataFrame(products)
        df["chain"] = chain_display
        df["chain_key"] = chain_key
        df["store_id"] = store_id
        df["store_name"] = store_name
        df["city"] = city
        df["date"] = run_date.isoformat()

        safe_name = f"{chain_key}_{store_id}_{city}.csv".replace(" ", "_")
        df.to_csv(out_dir / safe_name, index=False, encoding="utf-8-sig")
        print(f"  Saved {len(products)} products: {safe_name}")


def main(run_date: date | None = None):
    if run_date is None:
        run_date = date.today() - timedelta(days=1)

    branches = load_branches()
    print(f"Downloading prices for {run_date}...")

    with tempfile.TemporaryDirectory() as tmp_dir:
        download_prices(tmp_dir)
        print("Processing...")
        process_and_save(tmp_dir, run_date, branches)

    print(f"Done. Data saved to data/{run_date}/")


if __name__ == "__main__":
    main()
