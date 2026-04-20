"""
Calculate basket prices per branch from processed CSVs.
Reads config/baskets/state_basket.json and data/YYYY-MM-DD/*.csv.
"""

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

SRC_DIR = Path(__file__).parent
CONFIG_DIR = SRC_DIR.parent / "config"
DATA_DIR = SRC_DIR.parent / "data"


def load_state_basket() -> list[str]:
    path = CONFIG_DIR / "baskets" / "state_basket.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    barcodes = []
    for item in data.get("items", []):
        if "barcodes" in item:
            barcodes.extend(str(b) for b in item["barcodes"])
        elif "barcode" in item:
            barcodes.append(str(item["barcode"]))
    return barcodes


def load_branch_data(run_date: date) -> pd.DataFrame:
    data_dir = DATA_DIR / run_date.isoformat()
    if not data_dir.exists():
        raise FileNotFoundError(f"No data for {run_date}. Run downloader.py first.")

    dfs = []
    for csv_file in data_dir.glob("*.csv"):
        dfs.append(pd.read_csv(csv_file, dtype={"barcode": str}))

    if not dfs:
        raise ValueError(f"No CSV files found in data/{run_date}/")

    return pd.concat(dfs, ignore_index=True)


def calculate_state_basket(df: pd.DataFrame, barcodes: list[str]) -> pd.DataFrame:
    if not barcodes:
        return pd.DataFrame()

    basket_df = df[df["barcode"].isin(barcodes)].copy()
    basket_df["price"] = pd.to_numeric(basket_df["price"], errors="coerce")

    results = (
        basket_df.groupby(["chain", "chain_key", "store_id", "store_name", "city"])
        .agg(
            items_found=("barcode", "nunique"),
            total_price=("price", "sum"),
        )
        .reset_index()
    )
    results["basket_coverage"] = results["items_found"] / len(barcodes)
    results["basket_name"] = "סל המדינה"
    return results.sort_values("total_price")


def main(run_date: date | None = None):
    if run_date is None:
        run_date = date.today() - timedelta(days=1)

    print(f"Calculating baskets for {run_date}...")
    df = load_branch_data(run_date)
    barcodes = load_state_basket()

    if not barcodes:
        print("State basket is empty — upload state_basket.json first.")
        return pd.DataFrame()

    results = calculate_state_basket(df, barcodes)
    print(results.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
