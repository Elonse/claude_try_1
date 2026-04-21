# ================================================================
# 01_download_data.R
#
# שלב 1: הורדת נתוני מחירים ועיבודם לדאטאפריימים מקומיים.
# אין צורך ב-PAT או Git — עובד לבד.
#
# פלט:
#   data/YYYY-MM-DD/all_prices.csv       — כל המחירים מכל הסניפים
#   data/YYYY-MM-DD/all_prices.rds       — אותו דבר בפורמט R (טעינה מהירה)
#   data/YYYY-MM-DD/basket_results.csv   — השוואת סל המדינה
#   data/YYYY-MM-DD/basket_results.rds
#   outputs/excel/YYYY-MM-DD/            — קבצי Excel
#   outputs/html/YYYY-MM-DD/index.html   — דשבורד HTML
#
# הרץ ב-RStudio: לחץ Source (Ctrl+Shift+S)
# ================================================================

# ── 0. הגדרות — שנה כאן בלבד ────────────────────────────────

REPO_DIR <- if (.Platform$OS.type == "windows") {
  file.path(Sys.getenv("USERPROFILE"), "Desktop", "claude_try_1")
} else {
  file.path(path.expand("~"), "Desktop", "claude_try_1")
}

RUN_DATE <- format(Sys.Date() - 1, "%Y-%m-%d")   # אתמול — או שנה ל-"2025-01-15"

# ── 1. חבילות ────────────────────────────────────────────────

needed <- c("httr", "curl", "xml2", "jsonlite", "rvest",
            "stringr", "dplyr", "readr", "openxlsx")
new_pkg <- needed[!sapply(needed, requireNamespace, quietly = TRUE)]
if (length(new_pkg) > 0) {
  message("מתקין חבילות: ", paste(new_pkg, collapse = ", "))
  install.packages(new_pkg, quiet = TRUE)
}
invisible(lapply(needed, library, character.only = TRUE))

# ── 2. נתיבים וקונפיג ────────────────────────────────────────

if (!dir.exists(REPO_DIR))
  stop("תיקיית הריפו לא נמצאה: ", REPO_DIR,
       "\nוודא שהנתיב ב-REPO_DIR נכון.")

CONFIG_FILE       <- file.path(REPO_DIR, "config", "chains.json")
BRANCHES_FILE     <- file.path(REPO_DIR, "config", "branches.json")
STATE_BASKET_FILE <- file.path(REPO_DIR, "config", "baskets", "state_basket.json")
DATA_DIR          <- file.path(REPO_DIR, "data", RUN_DATE)
EXCEL_DIR         <- file.path(REPO_DIR, "outputs", "excel", RUN_DATE)
HTML_DIR          <- file.path(REPO_DIR, "outputs", "html",  RUN_DATE)
TMP_DIR           <- tempdir()

dir.create(DATA_DIR,  showWarnings = FALSE, recursive = TRUE)
dir.create(EXCEL_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(HTML_DIR,  showWarnings = FALSE, recursive = TRUE)

config        <- fromJSON(CONFIG_FILE)
TARGET_CITIES <- config$target_cities
FTP_HOST      <- "url.retail.publishedprices.co.il"

CHAIN_META <- list(
  OSHER_AD = list(
    display_name = "אושר עד",   chain_id = "7290103152017",
    engine = "ftp",             ftp_username = "osherad"
  ),
  RAMI_LEVY = list(
    display_name = "רמי לוי",   chain_id = "7290058140886",
    engine = "ftp",             ftp_username = "RamiLevi"
  ),
  SHUFERSAL = list(
    display_name = "שופרסל דיל", chain_id = "7290027600007",
    engine = "shufersal",       subchain_filter = "שופרסל דיל"
  ),
  YAYNO_BITAN_AND_CARREFOUR = list(
    display_name = "קרפור",     chain_id = "7290055700007",
    engine = "carrefour",       subchain_filter = "קרפור"
  )
)

# ── 3. פונקציות עזר ──────────────────────────────────────────

## 3a. FTP

ftp_list_files <- function(username) {
  h <- new_handle(username = username, password = "")
  # dirlistonly = TRUE forces NLST — returns plain filenames, not long LIST format
  handle_setopt(h, dirlistonly = TRUE)
  res <- tryCatch(
    curl_fetch_memory(paste0("ftp://", FTP_HOST, "/"), handle = h),
    error = function(e) { message("  FTP שגיאה (", username, "): ", e$message); NULL }
  )
  if (is.null(res)) return(character(0))
  lines <- strsplit(rawToChar(res$content), "\r?\n")[[1]]
  lines <- trimws(lines)
  lines[nchar(lines) > 0]
}

ftp_download_file <- function(username, filename, dest) {
  h <- new_handle(username = username, password = "")
  tryCatch(
    { curl_download(paste0("ftp://", FTP_HOST, "/", filename), dest, handle = h); dest },
    error = function(e) { message("  הורדה נכשלה: ", filename, " — ", e$message); "" }
  )
}

## 3b. Shufersal HTTP
# מחזיר data.frame עם עמודות: url, filename
# filename = טקסט הקישור (מכיל שם הקובץ כולל תאריך וסניף)

shufersal_list_files <- function(cat_id) {
  base <- paste0("https://prices.shufersal.co.il/FileObject/UpdateCategory?catID=", cat_id)

  get_page <- function(p) {
    resp <- tryCatch(GET(paste0(base, "&page=", p)), error = function(e) NULL)
    if (is.null(resp)) return(data.frame(url = character(), filename = character(),
                                         stringsAsFactors = FALSE))
    if (p == 1) message("  Shufersal HTTP status: ", status_code(resp))
    page <- tryCatch(read_html(resp), error = function(e) NULL)
    if (is.null(page)) return(data.frame(url = character(), filename = character(),
                                         stringsAsFactors = FALSE))
    # נסה קודם a[href*='File/Get'], אחר כך a[href*='file']
    nodes <- html_nodes(page, "a[href*='File/Get'], a[href*='file'], a[href*='.gz'], a[href*='.xml']")
    if (length(nodes) == 0 && p == 1) {
      # הדפס חלק מהHTML לאבחון
      html_preview <- substr(as.character(page), 1, 600)
      message("  Shufersal HTML preview: ", html_preview)
    }
    hrefs     <- html_attr(nodes, "href")
    filenames <- trimws(html_text(nodes))
    hrefs_full <- ifelse(grepl("^http", hrefs),
                         hrefs,
                         paste0("https://prices.shufersal.co.il", hrefs))
    data.frame(url = hrefs_full, filename = filenames, stringsAsFactors = FALSE)
  }

  # מציאת מספר עמודות
  first <- tryCatch(read_html(GET(base)), error = function(e) NULL)
  total <- 1
  if (!is.null(first)) {
    pnums <- first %>%
      html_nodes("#gridContainer tfoot a") %>%
      html_attr("href") %>%
      str_match("page=(\\d+)") %>%
      { .[, 2] } %>%
      as.integer()
    if (length(pnums) > 0 && any(!is.na(pnums)))
      total <- max(pnums, na.rm = TRUE)
  }

  result <- data.frame(url = character(), filename = character(), stringsAsFactors = FALSE)
  for (p in seq_len(total)) {
    result <- rbind(result, get_page(p))
    Sys.sleep(0.3)
  }
  result[!duplicated(result$url), ]
}

## 3c. Carrefour HTTP
# מחזיר data.frame עם עמודות: url, filename

carrefour_list_files <- function() {
  page_text <- tryCatch(
    content(GET("https://prices.carrefour.co.il/"), as = "text", encoding = "UTF-8"),
    error = function(e) { message("  Carrefour שגיאה: ", e$message); NULL }
  )
  if (is.null(page_text)) return(data.frame(url = character(), filename = character(),
                                             stringsAsFactors = FALSE))
  path  <- str_match(page_text, "const path = ['\"]([^'\"]+)['\"]")[, 2]
  fjson <- str_match(page_text, "const files = (\\[.*?\\]);")[, 2]
  if (is.na(path) || is.na(fjson)) {
    message("  לא ניתן לפרסר רשימת קבצי Carrefour")
    return(data.frame(url = character(), filename = character(), stringsAsFactors = FALSE))
  }
  files <- fromJSON(fjson)
  data.frame(
    url      = paste0("https://prices.carrefour.co.il/", path, "/", files$name),
    filename = files$name,
    stringsAsFactors = FALSE
  )
}

## 3d. סינון קבצים — בודק גם ב-url וגם ב-filename
filter_by_date <- function(df, d) {
  if (is.character(df)) {   # תמיכה ב-FTP שמחזיר character vector
    return(grep(format(as.Date(d), "%Y%m%d"), df, value = TRUE))
  }
  pat <- format(as.Date(d), "%Y%m%d")
  df[grepl(pat, df$url) | grepl(pat, df$filename), ]
}

filter_by_store <- function(df, ids) {
  if (length(ids) == 0) return(if (is.character(df)) df else df[FALSE, ])
  pat <- paste(ids, collapse = "|")
  if (is.character(df)) return(grep(pat, df, value = TRUE))
  df[grepl(pat, df$url) | grepl(pat, df$filename), ]
}

## 3e. XML parsing

read_xml_gz <- function(path) {
  if (grepl("\\.gz$", path, ignore.case = TRUE)) {
    tmp <- tempfile(fileext = ".xml")
    con <- gzfile(path, "rb")
    writeBin(readBin(con, "raw", n = 100e6), tmp)
    close(con)
    path <- tmp
  }
  for (enc in c("UTF-8", "Windows-1255", "ISO-8859-8")) {
    doc <- tryCatch(read_xml(path, encoding = enc), error = function(e) NULL)
    if (!is.null(doc)) return(doc)
  }
  NULL
}

parse_stores_xml <- function(path) {
  doc <- read_xml_gz(path)
  if (is.null(doc)) return(data.frame())
  stores <- xml_find_all(doc, "//Store")
  if (length(stores) == 0) return(data.frame())
  chain_id <- xml_text(xml_find_first(doc, "//ChainId|//ChainID"))
  data.frame(
    chain_id   = chain_id,
    store_id   = xml_text(xml_find_first(stores, ".//StoreId|.//StoreID")),
    store_name = xml_text(xml_find_first(stores, ".//StoreName")),
    subchain   = xml_text(xml_find_first(stores, ".//SubChainName")),
    city       = xml_text(xml_find_first(stores, ".//City")),
    stringsAsFactors = FALSE
  )
}

parse_price_xml <- function(path) {
  doc <- read_xml_gz(path)
  if (is.null(doc)) return(NULL)
  chain_id <- xml_text(xml_find_first(doc, "//ChainId|//ChainID"))
  store_id <- xml_text(xml_find_first(doc, "//StoreId|//StoreID"))
  items    <- xml_find_all(doc, "//Item")
  if (length(items) == 0) return(NULL)
  list(
    chain_id = chain_id,
    store_id = store_id,
    products = data.frame(
      barcode           = xml_text(xml_find_first(items, ".//ItemCode")),
      name              = xml_text(xml_find_first(items, ".//ItemName")),
      price             = suppressWarnings(as.numeric(
                            xml_text(xml_find_first(items, ".//ItemPrice")))),
      unit              = xml_text(xml_find_first(items, ".//UnitOfMeasure")),
      price_update_date = xml_text(xml_find_first(items, ".//PriceUpdateDate")),
      stringsAsFactors  = FALSE
    )
  )
}

# ── 4. fetch_stores (רק אם branches.json ריק) ─────────────────

branches_empty <- function() {
  if (!file.exists(BRANCHES_FILE)) return(TRUE)
  b <- tryCatch(fromJSON(BRANCHES_FILE, simplifyVector = FALSE),
                error = function(e) list())
  all(sapply(b, length) == 0)
}

if (branches_empty()) {
  message("\n=== שלב א: בניית רשימת סניפים ===")
  branches <- setNames(lapply(names(CHAIN_META), function(x) list()), names(CHAIN_META))

  for (chain_key in names(CHAIN_META)) {
    cfg <- CHAIN_META[[chain_key]]
    message("[", chain_key, "]")

    xml_path <- switch(cfg$engine,
      ftp = {
        files <- ftp_list_files(cfg$ftp_username)
        f     <- tail(sort(grep("^Stores", files, value = TRUE, ignore.case = TRUE)), 1)
        if (length(f) == 0) { message("  לא נמצאו קבצי חנויות"); NULL }
        else {
          dest <- file.path(TMP_DIR, f)
          ftp_download_file(cfg$ftp_username, f, dest)
          if (nchar(dest) > 0 && file.exists(dest)) dest else NULL
        }
      },
      shufersal = {
        df <- shufersal_list_files("5")
        df <- df[grepl("store", df$filename, ignore.case = TRUE) |
                 grepl("store", df$url,      ignore.case = TRUE), ]
        if (nrow(df) == 0) { message("  לא נמצאו קבצי חנויות Shufersal"); NULL }
        else {
          u    <- tail(sort(df$url), 1)
          dest <- file.path(TMP_DIR, basename(u))
          tryCatch({ curl_download(u, dest); dest },
                   error = function(e) { message("  ", e$message); NULL })
        }
      },
      carrefour = {
        df <- carrefour_list_files()
        df <- df[grepl("store", df$filename, ignore.case = TRUE), ]
        if (nrow(df) == 0) { message("  לא נמצאו קבצי חנויות Carrefour"); NULL }
        else {
          u    <- tail(sort(df$url), 1)
          dest <- file.path(TMP_DIR, basename(u))
          tryCatch({ curl_download(u, dest); dest },
                   error = function(e) { message("  ", e$message); NULL })
        }
      }
    )

    if (is.null(xml_path) || !file.exists(xml_path)) next

    stores <- parse_stores_xml(xml_path)
    if (nrow(stores) == 0) { message("  לא פורסרו חנויות"); next }

    mask <- stores$city %in% TARGET_CITIES
    if (!is.null(cfg$subchain_filter))
      mask <- mask & grepl(cfg$subchain_filter, stores$subchain, fixed = TRUE)

    matched <- stores[mask, ]
    branches[[chain_key]] <- lapply(seq_len(nrow(matched)), function(i)
      list(store_id   = matched$store_id[i],
           store_name = matched$store_name[i],
           subchain   = matched$subchain[i],
           city       = matched$city[i]))

    message("  נמצאו ", nrow(matched), " סניפים:")
    for (b in branches[[chain_key]])
      message("    [", b$store_id, "] ", b$store_name, " — ", b$city)
  }

  write(toJSON(branches, pretty = TRUE, auto_unbox = TRUE, ensure_ascii = FALSE),
        BRANCHES_FILE)
  message("נשמר: config/branches.json\n")
}

# ── 5. הורדת מחירים ──────────────────────────────────────────

message("\n=== שלב ב: הורדת מחירים ל-", RUN_DATE, " ===")
branches  <- fromJSON(BRANCHES_FILE, simplifyVector = FALSE)
all_paths <- character(0)   # נתיבים מקומיים לקבצי XML שהורדו

for (chain_key in names(CHAIN_META)) {
  cfg       <- CHAIN_META[[chain_key]]
  store_ids <- sapply(branches[[chain_key]], `[[`, "store_id")
  if (length(store_ids) == 0) {
    message("[", chain_key, "] אין סניפים — מדלג")
    next
  }
  message("[", chain_key, "] מוריד עבור ", length(store_ids), " סניפים...")

  paths <- switch(cfg$engine,

    ftp = {
      files <- ftp_list_files(cfg$ftp_username)
      files <- grep("^PriceFull", files, value = TRUE, ignore.case = TRUE)
      files <- filter_by_date(files, RUN_DATE)
      files <- filter_by_store(files, store_ids)
      if (length(files) == 0) { message("  לא נמצאו קבצים"); character(0) }
      else vapply(files, function(f) {
        ftp_download_file(cfg$ftp_username, f, file.path(TMP_DIR, f))
      }, character(1))
    },

    shufersal = {
      df <- shufersal_list_files("2")
      df <- df[grepl("pricefull", df$filename, ignore.case = TRUE) |
               grepl("pricefull", df$url,      ignore.case = TRUE), ]
      df <- filter_by_date(df, RUN_DATE)
      df <- filter_by_store(df, store_ids)
      if (nrow(df) == 0) { message("  לא נמצאו קבצים"); character(0) }
      else vapply(df$url, function(u) {
        dest <- file.path(TMP_DIR, basename(u))
        tryCatch({ curl_download(u, dest); dest },
                 error = function(e) { message("  ", e$message); "" })
      }, character(1))
    },

    carrefour = {
      df <- carrefour_list_files()
      df <- df[grepl("pricefull", df$filename, ignore.case = TRUE), ]
      df <- filter_by_date(df, RUN_DATE)
      df <- filter_by_store(df, store_ids)
      if (nrow(df) == 0) { message("  לא נמצאו קבצים"); character(0) }
      else vapply(df$url, function(u) {
        dest <- file.path(TMP_DIR, basename(u))
        tryCatch({ curl_download(u, dest); dest },
                 error = function(e) { message("  ", e$message); "" })
      }, character(1))
    }
  )

  all_paths <- c(all_paths, paths[nchar(paths) > 0])
}

# ── 6. עיבוד XML → דאטאפריים ─────────────────────────────────

message("\n=== שלב ג: עיבוד ", length(all_paths), " קבצים ===")

all_data <- list()

for (path in all_paths) {
  res <- parse_price_xml(path)
  if (is.null(res)) next

  # מצא chain_key לפי chain_id
  ck <- names(Filter(function(m) m$chain_id == res$chain_id, CHAIN_META))[1]
  if (is.na(ck) || is.null(ck)) next

  # מצא מידע על הסניף
  bi_list <- Filter(function(b) b$store_id == res$store_id, branches[[ck]])
  if (length(bi_list) == 0) {
    message("  סניף ", res$store_id, " לא ברשימה — מדלג")
    next
  }
  bi <- bi_list[[1]]

  df <- res$products
  df$chain      <- CHAIN_META[[ck]]$display_name
  df$chain_key  <- ck
  df$store_id   <- res$store_id
  df$store_name <- bi$store_name
  df$city       <- bi$city
  df$date       <- RUN_DATE

  fname <- paste0(ck, "_", res$store_id, "_", gsub(" ", "_", bi$city), ".csv")
  write_csv(df, file.path(DATA_DIR, fname))
  message("  ✓ ", fname, " (", nrow(df), " מוצרים)")
  all_data[[fname]] <- df
}

if (length(all_data) == 0) {
  warning("לא עובד נתונים. בדוק: חיבור לאינטרנט, config/branches.json, ותאריך.")
  message("הסקריפט סיים ללא נתונים. הרץ שוב לאחר בדיקת הבעיה.")
  stop("No data processed.")
}

all_df <- bind_rows(all_data)
message("\nסה\"כ: ", nrow(all_df), " שורות מ-", length(all_data), " סניפים")

# שמירה ב-R native format לעבודה נוחה
saveRDS(all_df, file.path(DATA_DIR, "all_prices.rds"))
write_csv(all_df, file.path(DATA_DIR, "all_prices.csv"))
message("נשמר: all_prices.rds + all_prices.csv")

# ── 7. חישוב סל המדינה ───────────────────────────────────────

message("\n=== שלב ד: חישוב סל המדינה ===")

basket   <- fromJSON(STATE_BASKET_FILE)
# כל פריט יכול להכיל כמה ברקודים (וריאנטים)
barcodes <- unique(as.character(unlist(lapply(basket$items, `[[`, "barcodes"))))
n_items  <- length(basket$items)
message("סל המדינה: ", n_items, " פריטים, ", length(barcodes), " ברקודים")

basket_results <- if (length(barcodes) > 0) {
  all_df %>%
    filter(barcode %in% barcodes) %>%
    group_by(chain, chain_key, store_id, store_name, city) %>%
    summarise(
      items_found = n_distinct(barcode),
      total_price = sum(price, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(coverage = paste0(round(items_found / n_items * 100), "%")) %>%
    arrange(total_price)
} else {
  message("סל המדינה ריק — מדלג")
  NULL
}

if (!is.null(basket_results)) {
  saveRDS(basket_results, file.path(DATA_DIR, "basket_results.rds"))
  write_csv(basket_results, file.path(DATA_DIR, "basket_results.csv"))
  message("נשמר: basket_results.rds + basket_results.csv")
  print(basket_results %>% select(city, chain, store_name, total_price, coverage))
}

# ── 8. יצוא Excel ────────────────────────────────────────────

message("\n=== שלב ה: יצוא Excel ===")

# קובץ אחד לכל סניף
for (key in unique(paste(all_df$chain_key, all_df$store_id, sep = "||"))) {
  parts <- strsplit(key, "\\|\\|")[[1]]
  sub   <- all_df %>% filter(chain_key == parts[1], store_id == parts[2])
  fname <- paste0("branch_", parts[1], "_", parts[2], "_",
                  gsub(" ", "_", sub$city[1]), ".xlsx")
  write.xlsx(sub[, c("barcode", "name", "price", "unit", "price_update_date",
                      "store_name", "city", "chain")],
             file.path(EXCEL_DIR, fname), rowNames = FALSE)
  message("  ", fname)
}

# השוואת סלים
if (!is.null(basket_results) && nrow(basket_results) > 0) {
  write.xlsx(as.data.frame(basket_results),
             file.path(EXCEL_DIR, "basket_comparison.xlsx"), rowNames = FALSE)
  message("  basket_comparison.xlsx")
}

# ── 9. יצוא HTML ─────────────────────────────────────────────

message("\n=== שלב ו: יצוא HTML ===")

if (!is.null(basket_results) && nrow(basket_results) > 0) {
  rows <- basket_results %>%
    arrange(city, total_price) %>%
    rowwise() %>%
    mutate(html_row = sprintf(
      "<tr><td>%s</td><td>%s</td><td>%s</td><td class='price'>&#8362;%.2f</td><td>%s</td></tr>",
      htmltools_escape(city), htmltools_escape(chain),
      htmltools_escape(store_name), total_price, coverage
    )) %>%
    pull(html_row) %>%
    paste(collapse = "\n")

  writeLines(sprintf(
'<!DOCTYPE html>
<html dir="rtl" lang="he">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>השוואת מחירים — %s</title>
  <style>
    body { font-family: Arial, sans-serif; max-width: 960px; margin: 40px auto; padding: 0 20px; }
    h1   { color: #2c3e50; }
    p.sub { color: #7f8c8d; margin-top: -10px; }
    table { width: 100%%; border-collapse: collapse; margin-top: 20px; }
    th    { background: #2c3e50; color: white; padding: 12px 10px; text-align: right; }
    td    { padding: 10px; border-bottom: 1px solid #ecf0f1; }
    tr:hover td { background: #f8f9fa; }
    .price { font-weight: bold; color: #27ae60; font-size: 1.1em; }
    tr:first-child td { border-top: 2px solid #27ae60; }
  </style>
</head>
<body>
  <h1>השוואת מחירים — סל המדינה</h1>
  <p class="sub">תאריך: %s | %d פריטים</p>
  <table>
    <thead>
      <tr><th>עיר</th><th>רשת</th><th>סניף</th><th>מחיר סל</th><th>כיסוי</th></tr>
    </thead>
    <tbody>
%s
    </tbody>
  </table>
</body>
</html>', RUN_DATE, RUN_DATE, n_items, rows),
    file.path(HTML_DIR, "index.html"), useBytes = FALSE)

  message("  index.html")
}

# ── סיום ─────────────────────────────────────────────────────

message("\n", strrep("=", 50))
message("הכל הסתיים בהצלחה!")
message("נתונים מקומיים: ", DATA_DIR)
message("Excel:          ", EXCEL_DIR)
message("HTML:           ", HTML_DIR)
message(strrep("=", 50))
message("\nכעת הרץ את 02_push_to_github.R כדי לדחוף לגיטהאב.")

# פונקציית עזר — escape HTML
htmltools_escape <- function(x) {
  x <- gsub("&", "&amp;", x, fixed = TRUE)
  x <- gsub("<", "&lt;",  x, fixed = TRUE)
  x <- gsub(">", "&gt;",  x, fixed = TRUE)
  x
}
