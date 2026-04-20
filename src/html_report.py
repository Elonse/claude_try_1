"""
Generate a simple HTML summary page at outputs/html/YYYY-MM-DD/index.html.
Shows basket comparison across all branches and cities.
"""

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

SRC_DIR = Path(__file__).parent
CONFIG_DIR = SRC_DIR.parent / "config"
OUTPUTS_DIR = SRC_DIR.parent / "outputs" / "html"


def render_html(run_date: date, basket_results: pd.DataFrame) -> str:
    rows = ""
    if not basket_results.empty:
        for _, row in basket_results.sort_values(["city", "total_price"]).iterrows():
            coverage_pct = f"{row['basket_coverage']:.0%}"
            rows += f"""
        <tr>
          <td>{row['city']}</td>
          <td>{row['chain']}</td>
          <td>{row['store_name']}</td>
          <td class="price">₪{row['total_price']:.2f}</td>
          <td>{int(row['items_found'])} ({coverage_pct})</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html dir="rtl" lang="he">
<head>
  <meta charset="UTF-8">
  <title>השוואת מחירים — {run_date}</title>
  <style>
    body {{ font-family: Arial, sans-serif; max-width: 900px; margin: 40px auto; padding: 0 20px; }}
    h1 {{ color: #2c3e50; }}
    .date {{ color: #7f8c8d; font-size: 0.9em; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
    th {{ background: #2c3e50; color: white; padding: 10px; text-align: right; }}
    td {{ padding: 8px 10px; border-bottom: 1px solid #eee; }}
    tr:hover {{ background: #f5f5f5; }}
    .price {{ font-weight: bold; color: #27ae60; }}
    .chain-tag {{ background: #ecf0f1; border-radius: 4px; padding: 2px 6px; font-size: 0.85em; }}
  </style>
</head>
<body>
  <h1>השוואת מחירים — סל המדינה</h1>
  <p class="date">תאריך נתונים: {run_date}</p>

  <table>
    <thead>
      <tr>
        <th>עיר</th>
        <th>רשת</th>
        <th>סניף</th>
        <th>מחיר סל כולל</th>
        <th>פריטים שנמצאו</th>
      </tr>
    </thead>
    <tbody>{rows}
    </tbody>
  </table>
</body>
</html>"""


def main(run_date: date | None = None, basket_results: pd.DataFrame | None = None):
    if run_date is None:
        run_date = date.today() - timedelta(days=1)

    if basket_results is None or basket_results.empty:
        print("No basket results to render.")
        return

    out_dir = OUTPUTS_DIR / run_date.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    filepath = out_dir / "index.html"
    filepath.write_text(render_html(run_date, basket_results), encoding="utf-8")
    print(f"HTML report saved to {filepath}")


if __name__ == "__main__":
    main()
