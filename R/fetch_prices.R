# fetch_prices.R
# Downloads PriceFull XML files for the branches in config/branches.json,
# processes them to CSV, computes the state basket comparison, and exports
# Excel + HTML outputs.
#
# Run daily from an Israeli machine (after fetch_stores.R has been run once).
#
# Usage:
#   source("R/fetch_prices.R")          # uses yesterday's date
#   fetch_all(run_date = "2025-04-19")  # specific date

library(httr)
library(curl)
library(xml2)
library(jsonlite)
library(rvest)
library(stringr)
library(dplyr)
library(readr)
library(openxlsx)

# ── Config ────────────────────────────────────────────────────────────────────

REPO_ROOT <- dirname(dirname(rstudioapi::getSourceEditorContext()$path))
# If not in RStudio, set manually:
# REPO_ROOT <- "/path/to/claude_try_1"

BRANCHES_FILE     <- file.path(REPO_ROOT, "config", "branches.json")
STATE_BASKET_FILE <- file.path(REPO_ROOT, "config", "baskets", "state_basket.json")
DATA_DIR          <- file.path(REPO_ROOT, "data")
EXCEL_OUT_DIR     <- file.path(REPO_ROOT, "outputs", "excel")
HTML_OUT_DIR      <- file.path(REPO_ROOT, "outputs", "html")
TMP_DIR           <- tempdir()

FTP_HOST <- "url.retail.publishedprices.co.il"

CHAIN_META <- list(
  OSHER_AD = list(
    display_name = "אושר עד", chain_id = "7290103152017",
    engine = "ftp", ftp_username = "osherad"
  ),
  RAMI_LEVY = list(
    display_name = "רמי לוי", chain_id = "7290058140886",
    engine = "ftp", ftp_username = "RamiLevi"
  ),
  SHUFERSAL = list(
    display_name = "שופרסל דיל", chain_id = "7290027600007",
    engine = "shufersal"
  ),
  YAYNO_BITAN_AND_CARREFOUR = list(
    display_name = "קרפור", chain_id = "7290055700007",
    engine = "carrefour"
  )
)

# ── Helpers: FTP ──────────────────────────────────────────────────────────────

ftp_list_files <- function(username) {
  h   <- new_handle(username = username, password = "")
  res <- curl_fetch_memory(paste0("ftp://", FTP_HOST, "/"), handle = h)
  strsplit(rawToChar(res$content), "\r?\n")[[1]]
}

ftp_download <- function(username, filename, dest) {
  h <- new_handle(username = username, password = "")
  curl_download(paste0("ftp://", FTP_HOST, "/", filename), dest, handle = h)
}

# ── Helpers: Shufersal HTTP ───────────────────────────────────────────────────

shufersal_list_price_files <- function() {
  base_url <- "https://prices.shufersal.co.il/FileObject/UpdateCategory?catID=2"

  get_links <- function(page_num) {
    page <- read_html(GET(paste0(base_url, "&page=", page_num)))
    page %>% html_nodes("a[href*='File/Get']") %>% html_attr("href")
  }

  first  <- read_html(GET(base_url))
  pnums  <- first %>%
    html_nodes("#gridContainer tfoot a") %>%
    html_attr("href") %>%
    str_extract("page=(\\d+)", group = 1) %>%
    as.integer()
  total  <- if (length(pnums) > 0) max(pnums, na.rm = TRUE) else 1

  links <- character(0)
  for (p in seq_len(total)) {
    links <- c(links, get_links(p))
    Sys.sleep(0.3)
  }
  unique(links)
}

# ── Helpers: Carrefour HTTP ───────────────────────────────────────────────────

carrefour_list_price_files <- function() {
  page  <- content(GET("https://prices.carrefour.co.il/"), "text", encoding = "UTF-8")
  path  <- str_match(page, "const path = ['\"]([^'\"]+)['\"]")[, 2]
  fjson <- str_match(page, "const files = (\\[.*?\\]);")[, 2]
  if (is.na(path) || is.na(fjson)) stop("Could not parse Carrefour file list")
  files <- fromJSON(fjson)
  paste0("https://prices.carrefour.co.il/", path, "/", files$name)
}

# ── Helpers: XML parsing ──────────────────────────────────────────────────────

read_xml_gz <- function(path) {
  if (grepl("\\.gz$", path)) {
    tmp    <- tempfile(fileext = ".xml")
    con_in <- gzfile(path, "rb")
    writeBin(readBin(con_in, "raw", n = 100e6), tmp)
    close(con_in)
    path <- tmp
  }
  tryCatch(read_xml(path, encoding = "UTF-8"), error = function(e)
    tryCatch(read_xml(path, encoding = "Windows-1255"), error = function(e2) NULL))
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
      price             = xml_text(xml_find_first(items, ".//ItemPrice")),
      unit              = xml_text(xml_find_first(items, ".//UnitOfMeasure")),
      price_update_date = xml_text(xml_find_first(items, ".//PriceUpdateDate")),
      stringsAsFactors  = FALSE
    )
  )
}

# ── Filter helpers ────────────────────────────────────────────────────────────

filter_price_files_by_date <- function(filenames, target_date) {
  date_str <- format(as.Date(target_date), "%Y%m%d")
  grep(date_str, filenames, value = TRUE)
}

filter_price_files_by_store <- function(filenames, store_ids) {
  pattern <- paste(store_ids, collapse = "|")
  grep(pattern, filenames, value = TRUE)
}

# ── Per-chain price downloaders ───────────────────────────────────────────────

download_chain_prices_ftp <- function(chain_key, cfg, store_ids, target_date) {
  cat("  FTP listing for", chain_key, "...\n")
  all_files <- tryCatch(ftp_list_files(cfg$ftp_username), error = function(e) {
    message("  FTP error: ", e$message); character(0)
  })

  price_files <- grep("^PriceFull", all_files, value = TRUE, ignore.case = TRUE)
  price_files <- filter_price_files_by_date(price_files, target_date)
  price_files <- filter_price_files_by_store(price_files, store_ids)

  paths <- character(0)
  for (f in price_files) {
    dest <- file.path(TMP_DIR, f)
    cat("  Downloading", f, "...\n")
    tryCatch(
      { ftp_download(cfg$ftp_username, f, dest); paths <- c(paths, dest) },
      error = function(e) message("  Error: ", e$message)
    )
  }
  paths
}

download_chain_prices_shufersal <- function(store_ids, target_date) {
  cat("  Fetching Shufersal price file list...\n")
  links <- tryCatch(shufersal_list_price_files(), error = function(e) {
    message("  Error: ", e$message); character(0)
  })

  links <- filter_price_files_by_date(links, target_date)
  links <- filter_price_files_by_store(links, store_ids)

  paths <- character(0)
  for (url in links) {
    dest <- file.path(TMP_DIR, basename(url))
    cat("  Downloading", basename(url), "...\n")
    tryCatch(
      { curl_download(url, dest); paths <- c(paths, dest) },
      error = function(e) message("  Error: ", e$message)
    )
  }
  paths
}

download_chain_prices_carrefour <- function(store_ids, target_date) {
  cat("  Fetching Carrefour price file list...\n")
  links <- tryCatch(carrefour_list_price_files(), error = function(e) {
    message("  Error: ", e$message); character(0)
  })

  links <- filter_price_files_by_date(links, target_date)
  links <- filter_price_files_by_store(links, store_ids)

  paths <- character(0)
  for (url in links) {
    dest <- file.path(TMP_DIR, basename(url))
    cat("  Downloading", basename(url), "...\n")
    tryCatch(
      { curl_download(url, dest); paths <- c(paths, dest) },
      error = function(e) message("  Error: ", e$message)
    )
  }
  paths
}

# ── Processing ────────────────────────────────────────────────────────────────

process_files <- function(xml_paths, branches, run_date) {
  dir.create(file.path(DATA_DIR, run_date), showWarnings = FALSE, recursive = TRUE)
  all_data <- list()

  for (path in xml_paths) {
    cat("  Parsing", basename(path), "...\n")
    result <- parse_price_xml(path)
    if (is.null(result)) next

    chain_key  <- names(Filter(function(m) m$chain_id == result$chain_id, CHAIN_META))[1]
    if (is.na(chain_key) || is.null(chain_key)) next

    branch_info <- Filter(function(b) b$store_id == result$store_id,
                          branches[[chain_key]])
    if (length(branch_info) == 0) next
    branch_info <- branch_info[[1]]

    df <- result$products
    df$chain       <- CHAIN_META[[chain_key]]$display_name
    df$chain_key   <- chain_key
    df$store_id    <- result$store_id
    df$store_name  <- branch_info$store_name
    df$city        <- branch_info$city
    df$date        <- run_date
    df$price       <- suppressWarnings(as.numeric(df$price))

    safe_name <- paste0(chain_key, "_", result$store_id, "_",
                        gsub(" ", "_", branch_info$city), ".csv")
    write_csv(df, file.path(DATA_DIR, run_date, safe_name))
    cat("    Saved", nrow(df), "products →", safe_name, "\n")
    all_data[[safe_name]] <- df
  }

  bind_rows(all_data)
}

# ── Basket calculation ────────────────────────────────────────────────────────

calc_state_basket <- function(df) {
  basket <- fromJSON(STATE_BASKET_FILE)
  barcodes <- as.character(sapply(basket$items, `[[`, "barcode"))
  if (length(barcodes) == 0) {
    message("State basket is empty — upload state_basket.json first.")
    return(NULL)
  }

  df %>%
    filter(barcode %in% barcodes) %>%
    group_by(chain, chain_key, store_id, store_name, city) %>%
    summarise(
      items_found  = n_distinct(barcode),
      total_price  = sum(price, na.rm = TRUE),
      .groups      = "drop"
    ) %>%
    mutate(
      basket_coverage = items_found / length(barcodes),
      basket_name     = "סל המדינה"
    ) %>%
    arrange(total_price)
}

# ── Excel export ──────────────────────────────────────────────────────────────

export_excel <- function(all_data, basket_results, run_date) {
  out_dir <- file.path(EXCEL_OUT_DIR, run_date)
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

  # One file per branch
  for (key in unique(paste(all_data$chain_key, all_data$store_id, sep = "_"))) {
    parts  <- strsplit(key, "_")[[1]]
    ck     <- parts[1]; sid <- parts[2]
    branch <- all_data %>% filter(chain_key == ck, store_id == sid)
    city   <- branch$city[1]
    fname  <- paste0("branch_", ck, "_", sid, "_", gsub(" ", "_", city), ".xlsx")
    cols   <- c("barcode", "name", "price", "unit", "price_update_date")
    write.xlsx(branch[, cols], file.path(out_dir, fname), rowNames = FALSE)
    cat("  Exported", fname, "\n")
  }

  # Basket comparison
  if (!is.null(basket_results) && nrow(basket_results) > 0) {
    write.xlsx(as.data.frame(basket_results),
               file.path(out_dir, "basket_comparison.xlsx"), rowNames = FALSE)
    cat("  Exported basket_comparison.xlsx\n")
  }
}

# ── HTML report ───────────────────────────────────────────────────────────────

export_html <- function(basket_results, run_date) {
  out_dir <- file.path(HTML_OUT_DIR, run_date)
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

  if (is.null(basket_results) || nrow(basket_results) == 0) return(invisible())

  rows <- paste(apply(
    basket_results %>% arrange(city, total_price), 1,
    function(r) sprintf(
      "<tr><td>%s</td><td>%s</td><td>%s</td><td class='price'>₪%.2f</td><td>%d (%.0f%%)</td></tr>",
      r["city"], r["chain"], r["store_name"],
      as.numeric(r["total_price"]), as.integer(r["items_found"]),
      as.numeric(r["basket_coverage"]) * 100
    )
  ), collapse = "\n")

  html <- sprintf('<!DOCTYPE html>
<html dir="rtl" lang="he">
<head>
  <meta charset="UTF-8">
  <title>השוואת מחירים — %s</title>
  <style>
    body{font-family:Arial,sans-serif;max-width:900px;margin:40px auto;padding:0 20px}
    h1{color:#2c3e50}
    table{width:100%%;border-collapse:collapse;margin-top:20px}
    th{background:#2c3e50;color:white;padding:10px;text-align:right}
    td{padding:8px 10px;border-bottom:1px solid #eee}
    tr:hover{background:#f5f5f5}
    .price{font-weight:bold;color:#27ae60}
  </style>
</head>
<body>
  <h1>השוואת מחירים — סל המדינה</h1>
  <p>תאריך נתונים: %s</p>
  <table>
    <thead>
      <tr><th>עיר</th><th>רשת</th><th>סניף</th><th>מחיר סל</th><th>פריטים</th></tr>
    </thead>
    <tbody>%s</tbody>
  </table>
</body></html>', run_date, run_date, rows)

  writeLines(html, file.path(out_dir, "index.html"), useBytes = FALSE)
  cat("  HTML report →", file.path(out_dir, "index.html"), "\n")
}

# ── Main ──────────────────────────────────────────────────────────────────────

fetch_all <- function(run_date = NULL) {
  if (is.null(run_date)) run_date <- format(Sys.Date() - 1, "%Y-%m-%d")
  cat("=== Price download for", run_date, "===\n")

  branches <- fromJSON(BRANCHES_FILE, simplifyVector = FALSE)
  all_xml_paths <- character(0)

  for (chain_key in names(CHAIN_META)) {
    cfg      <- CHAIN_META[[chain_key]]
    store_ids <- sapply(branches[[chain_key]], `[[`, "store_id")
    if (length(store_ids) == 0) { cat("[", chain_key, "] no branches configured\n"); next }

    cat("\n[", chain_key, "]\n")
    paths <- switch(cfg$engine,
      ftp       = download_chain_prices_ftp(chain_key, cfg, store_ids, run_date),
      shufersal = download_chain_prices_shufersal(store_ids, run_date),
      carrefour = download_chain_prices_carrefour(store_ids, run_date)
    )
    all_xml_paths <- c(all_xml_paths, paths)
  }

  cat("\n=== Processing", length(all_xml_paths), "files ===\n")
  all_data <- process_files(all_xml_paths, branches, run_date)

  if (nrow(all_data) == 0) { message("No data processed."); return(invisible()) }

  cat("\n=== Calculating baskets ===\n")
  basket_results <- calc_state_basket(all_data)

  cat("\n=== Exporting outputs ===\n")
  export_excel(all_data, basket_results, run_date)
  export_html(basket_results, run_date)

  cat("\nDone! Push data/ and outputs/ to GitHub.\n")
  invisible(all_data)
}

# Run immediately when sourced
fetch_all()
