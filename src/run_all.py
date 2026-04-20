"""
Main entry point. Runs the full pipeline:
  1. Download prices (from Israeli IP)
  2. Calculate baskets
  3. Export Excel
  4. Generate HTML report

Usage:
    python run_all.py                    # uses yesterday's date
    python run_all.py --date 2025-01-15  # specific date
    python run_all.py --stores-only      # only fetch branch list (run first time)
"""

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--stores-only", action="store_true",
                        help="Only fetch branch list, don't download prices")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.stores_only:
        from fetch_stores import main as fetch_stores
        fetch_stores()
        return

    run_date = date.fromisoformat(args.date) if args.date else None

    from downloader import main as download
    from basket_calculator import main as calc_baskets
    from excel_exporter import main as export_excel
    from html_report import main as render_html

    download(run_date)
    basket_results = calc_baskets(run_date)
    export_excel(run_date, basket_results)
    render_html(run_date, basket_results)


if __name__ == "__main__":
    main()
