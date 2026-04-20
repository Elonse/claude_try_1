"""
Generate Excel outputs:
  - outputs/excel/YYYY-MM-DD/branch_{chain}_{store_id}.xlsx  (full price list per branch)
  - outputs/excel/YYYY-MM-DD/basket_comparison.xlsx          (basket summary across all branches)
"""

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

SRC_DIR = Path(__file__).parent
CONFIG_DIR = SRC_DIR.parent / "config"
DATA_DIR = SRC_DIR.parent / "data"
OUTPUTS_DIR = SRC_DIR.parent / "outputs" / "excel"

CHAINS_CONFIG = json.loads((CONFIG_DIR / "chains.json").read_text(encoding="utf-8"))


def export_branch_files(run_date: date, df: pd.DataFrame):
    out_dir = OUTPUTS_DIR / run_date.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)

    for (chain_key, store_id), group in df.groupby(["chain_key", "store_id"]):
        store_name = group["store_name"].iloc[0]
        city = group["city"].iloc[0]
        chain_display = group["chain"].iloc[0]

        cols = ["barcode", "name", "price", "unit", "price_update_date"]
        sheet_df = group[cols].copy()
        sheet_df["price"] = pd.to_numeric(sheet_df["price"], errors="coerce")

        filename = f"branch_{chain_key}_{store_id}_{city}.xlsx".replace(" ", "_")
        filepath = out_dir / filename

        with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
            sheet_df.to_excel(writer, sheet_name="מחירים", index=False)
            ws = writer.sheets["מחירים"]
            ws.column_dimensions["A"].width = 16
            ws.column_dimensions["B"].width = 40
            ws.column_dimensions["C"].width = 12

        print(f"  {filename} ({len(sheet_df)} products)")


def export_basket_comparison(run_date: date, basket_results: pd.DataFrame):
    if basket_results.empty:
        return

    out_dir = OUTPUTS_DIR / run_date.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    filepath = out_dir / "basket_comparison.xlsx"

    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        basket_results.to_excel(writer, sheet_name="השוואת סלים", index=False)
        ws = writer.sheets["השוואת סלים"]
        for col in ws.columns:
            ws.column_dimensions[col[0].column_letter].width = 20

    print(f"  basket_comparison.xlsx")


def main(run_date: date | None = None, basket_results: pd.DataFrame | None = None):
    from basket_calculator import load_branch_data

    if run_date is None:
        run_date = date.today() - timedelta(days=1)

    print(f"Exporting Excel for {run_date}...")
    df = load_branch_data(run_date)
    if df.empty:
        print("No data to export.")
        return
    export_branch_files(run_date, df)

    if basket_results is not None and not basket_results.empty:
        export_basket_comparison(run_date, basket_results)

    print("Done.")


if __name__ == "__main__":
    main()
