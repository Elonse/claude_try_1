# השוואת מחירים — רשתות שיווק

השוואת מחירים בין אושר עד, רמי לוי, שופרסל דיל וקרפור ב-5 ערים.

## דרישות

```bash
pip install -r requirements.txt
```

## הרצה ראשונה — בניית רשימת סניפים

חייב לרוץ ממחשב בישראל:

```bash
python src/run_all.py --stores-only
```

יוצר `config/branches.json` עם רשימת הסניפים הרלוונטיים ב-5 הערים.

## הורדת מחירים יומית

```bash
python src/run_all.py
```

מוריד מחירי אתמול, מחשב סלים, מייצא Excel ו-HTML.

תאריך ספציפי:
```bash
python src/run_all.py --date 2025-01-15
```

## מבנה הפרויקט

```
config/
  chains.json          # הגדרות רשתות וערים
  branches.json        # רשימת סניפים (נוצר אחרי run --stores-only)
  baskets/
    state_basket.json  # ברקודים של סל המדינה
src/
  run_all.py           # נקודת כניסה ראשית
  fetch_stores.py      # הורדת רשימת סניפים
  downloader.py        # הורדת מחירים
  basket_calculator.py # חישוב מחיר הסלים
  excel_exporter.py    # יצוא Excel
  html_report.py       # יצוא HTML
data/
  YYYY-MM-DD/          # CSV מעובד לכל סניף
outputs/
  excel/YYYY-MM-DD/    # קבצי Excel
  html/YYYY-MM-DD/     # דוח HTML
```

## ערים נבדקות

חיפה / קריית ביאליק, בית שמש, אשקלון, באר שבע, פתח תקווה
