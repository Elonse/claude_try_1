# get_prices.R
# ─────────────────────────────────────────────────────────────────────────────
# מוריד מחירים מ-4 רשתות, מחזיר dataframe אחד: all_prices
# הרץ מ-RStudio על מחשב בישראל (Source → Ctrl+Shift+S)
# ─────────────────────────────────────────────────────────────────────────────

# ── הגדרות ────────────────────────────────────────────────────────────────────

TARGET_CITIES <- c("חיפה", "קריית ביאליק", "בית שמש", "אשקלון", "באר שבע", "פתח תקווה")
RUN_DATE      <- format(Sys.Date() - 1, "%Y%m%d")    # אתמול בפורמט 20260420
TMP           <- tempdir()

# ── חבילות ────────────────────────────────────────────────────────────────────

for (pkg in c("curl", "httr", "xml2", "rvest", "stringr", "dplyr", "jsonlite")) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  suppressPackageStartupMessages(library(pkg, character.only = TRUE))
}

# ── פונקציות עזר ──────────────────────────────────────────────────────────────

# קורא XML (גם gz)
read_gz_xml <- function(path) {
  if (grepl("\\.gz$", path, ignore.case = TRUE)) {
    tmp <- tempfile(fileext = ".xml")
    con <- gzfile(path, "rb")
    writeBin(readBin(con, "raw", n = 100e6), tmp)
    close(con)
    path <- tmp
  }
  for (enc in c("UTF-8", "Windows-1255")) {
    doc <- tryCatch(read_xml(path, encoding = enc), error = function(e) NULL)
    if (!is.null(doc)) return(doc)
  }
  message("  ✗ לא ניתן לפרסר: ", basename(path))
  NULL
}

# מחלץ טבלת חנויות מ-XML
xml_to_stores <- function(path) {
  doc <- read_gz_xml(path)
  if (is.null(doc)) return(NULL)
  nodes <- xml_find_all(doc, "//Store")
  if (length(nodes) == 0) return(NULL)
  data.frame(
    store_id   = xml_text(xml_find_first(nodes, ".//StoreId|.//StoreID")),
    store_name = xml_text(xml_find_first(nodes, ".//StoreName")),
    subchain   = xml_text(xml_find_first(nodes, ".//SubChainName")),
    city       = xml_text(xml_find_first(nodes, ".//City")),
    stringsAsFactors = FALSE
  )
}

# מחלץ טבלת מחירים מ-XML, מוסיף עמודות זיהוי
xml_to_prices <- function(path, chain, store_id, store_name, city) {
  doc <- read_gz_xml(path)
  if (is.null(doc)) return(NULL)
  items <- xml_find_all(doc, "//Item")
  if (length(items) == 0) return(NULL)
  data.frame(
    barcode    = xml_text(xml_find_first(items, ".//ItemCode")),
    name       = xml_text(xml_find_first(items, ".//ItemName")),
    price      = suppressWarnings(as.numeric(xml_text(xml_find_first(items, ".//ItemPrice")))),
    unit       = xml_text(xml_find_first(items, ".//UnitOfMeasure")),
    chain      = chain,
    store_id   = store_id,
    store_name = store_name,
    city       = city,
    stringsAsFactors = FALSE
  )
}

# מוריד URL לקובץ זמני — עוקף בעיית שינוי שם ב-Windows
http_fetch <- function(url) {
  ext  <- tools::file_ext(basename(url))
  dest <- tempfile(fileext = if (nchar(ext) > 0) paste0(".", ext) else ".tmp")
  res  <- curl_fetch_memory(url)
  writeBin(res$content, dest)
  dest
}

# ── FTP ───────────────────────────────────────────────────────────────────────

FTP_HOST <- "url.retail.publishedprices.co.il"

ftp_ls <- function(user) {
  h <- new_handle(username = user, password = "")
  handle_setopt(h, dirlistonly = TRUE)
  r <- tryCatch(
    curl_fetch_memory(paste0("ftp://", FTP_HOST, "/"), handle = h),
    error = function(e) { message("  FTP error: ", e$message); NULL }
  )
  if (is.null(r)) return(character(0))
  x <- strsplit(rawToChar(r$content), "\r?\n")[[1]]
  x <- trimws(x); x[nchar(x) > 0]
}

ftp_get <- function(user, fname) {
  h <- new_handle(username = user, password = "")
  tryCatch({
    # curl_fetch_memory avoids Windows file-rename permission errors
    res  <- curl_fetch_memory(paste0("ftp://", FTP_HOST, "/", fname), handle = h)
    dest <- tempfile(fileext = paste0(".", tools::file_ext(fname)))
    writeBin(res$content, dest)
    dest
  }, error = function(e) { message("  FTP download failed: ", e$message); NULL })
}

ftp_download_stores <- function(user) {
  files <- ftp_ls(user)
  sf    <- tail(sort(grep("store", files, ignore.case = TRUE, value = TRUE)), 1)
  if (length(sf) == 0) { message("  אין קובץ חנויות"); return(NULL) }
  message("  מוריד: ", sf)
  ftp_get(user, sf)
}

ftp_download_prices <- function(user, store_ids) {
  files  <- ftp_ls(user)
  pfiles <- grep("pricefull", files, ignore.case = TRUE, value = TRUE)
  pfiles <- grep(RUN_DATE, pfiles, value = TRUE)
  pfiles <- grep(paste(store_ids, collapse = "|"), pfiles, value = TRUE)
  if (length(pfiles) == 0) { message("  אין קבצי מחירים לתאריך ", RUN_DATE); return(character(0)) }
  paths <- character(0)
  for (f in pfiles) {
    message("  מוריד: ", f)
    p <- ftp_get(user, f)
    if (!is.null(p)) paths <- c(paths, p)
  }
  paths
}

# ── Shufersal HTTP ────────────────────────────────────────────────────────────

shufersal_file_list <- function(cat_id) {
  # User-Agent נחוץ כדי שהשרת לא יחסום
  resp <- tryCatch(
    GET(paste0("https://prices.shufersal.co.il/FileObject/UpdateCategory?catID=", cat_id, "&page=1"),
        add_headers("User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")),
    error = function(e) { message("  Shufersal error: ", e$message); NULL }
  )
  if (is.null(resp) || status_code(resp) != 200) {
    message("  Shufersal HTTP ", if (!is.null(resp)) status_code(resp) else "N/A")
    return(data.frame(url = character(), name = character(), stringsAsFactors = FALSE))
  }
  pg    <- read_html(content(resp, as = "text", encoding = "UTF-8"))
  nodes <- html_nodes(pg, "a[href*='File/Get']")
  hrefs <- html_attr(nodes, "href")
  names <- trimws(html_text(nodes))
  hrefs_full <- ifelse(startsWith(hrefs, "http"), hrefs,
                       paste0("https://prices.shufersal.co.il", hrefs))
  data.frame(url = hrefs_full, name = names, stringsAsFactors = FALSE)
}

# ── Carrefour HTTP ────────────────────────────────────────────────────────────

carrefour_file_list <- function() {
  resp <- tryCatch(GET("https://prices.carrefour.co.il/"), error = function(e) NULL)
  if (is.null(resp)) { message("  Carrefour לא נגיש"); return(NULL) }
  txt   <- content(resp, as = "text", encoding = "UTF-8")
  path  <- str_match(txt, "const path\\s*=\\s*['\"]([^'\"]+)['\"]")[, 2]
  fjson <- str_match(txt, "const files\\s*=\\s*(\\[.*?\\]);")[, 2]
  if (is.na(path) || is.na(fjson)) { message("  לא ניתן לפרסר Carrefour"); return(NULL) }
  files <- fromJSON(fjson)
  data.frame(
    url  = paste0("https://prices.carrefour.co.il/", path, "/", files$name),
    name = files$name,
    stringsAsFactors = FALSE
  )
}

# ══════════════════════════════════════════════════════════════════════════════
# שלב 1: הורדת רשימות חנויות
# ══════════════════════════════════════════════════════════════════════════════

message("\n══ שלב 1: חנויות ══")
branches <- list()

# Osher Ad
message("\n[אושר עד]")
path <- ftp_download_stores("osherad")
if (!is.null(path)) {
  s <- xml_to_stores(path)
  if (!is.null(s)) {
    branches$OSHER_AD <- s[s$city %in% TARGET_CITIES, ]
    message("  נמצאו: ", nrow(branches$OSHER_AD), " סניפים")
  }
}

# Rami Levy
message("\n[רמי לוי]")
path <- ftp_download_stores("RamiLevi")
if (!is.null(path)) {
  s <- xml_to_stores(path)
  if (!is.null(s)) {
    branches$RAMI_LEVY <- s[s$city %in% TARGET_CITIES, ]
    message("  נמצאו: ", nrow(branches$RAMI_LEVY), " סניפים")
  }
}

# Shufersal
message("\n[שופרסל דיל]")
sf_files <- shufersal_file_list("5")   # catID=5 = store files
sf_stores <- sf_files[grepl("store", sf_files$name, ignore.case = TRUE), ]
if (nrow(sf_stores) > 0) {
  url <- tail(sort(sf_stores$url), 1)
  message("  מוריד: ", basename(url))
  tryCatch({
    dest <- http_fetch(url)
    s <- xml_to_stores(dest)
    if (!is.null(s)) {
      branches$SHUFERSAL <- s[s$city %in% TARGET_CITIES &
                               grepl("שופרסל דיל", s$subchain, fixed = TRUE), ]
      message("  נמצאו: ", nrow(branches$SHUFERSAL), " סניפים")
    }
  }, error = function(e) message("  הורדה נכשלה: ", e$message))
} else {
  message("  לא נמצאו קבצי חנויות (", nrow(sf_files), " קישורים בדף catID=5)")
}

# Carrefour
message("\n[קרפור]")
cf_files <- carrefour_file_list()
if (!is.null(cf_files)) {
  cf_stores <- cf_files[grepl("store", cf_files$name, ignore.case = TRUE), ]
  if (nrow(cf_stores) > 0) {
    url <- tail(sort(cf_stores$url), 1)
    message("  מוריד: ", basename(url))
    tryCatch({
      dest <- http_fetch(url)
      s <- xml_to_stores(dest)
      if (!is.null(s)) {
        branches$CARREFOUR <- s[s$city %in% TARGET_CITIES &
                                 grepl("קרפור", s$subchain, fixed = TRUE), ]
        message("  נמצאו: ", nrow(branches$CARREFOUR), " סניפים")
      }
    }, error = function(e) message("  הורדה נכשלה: ", e$message))
  }
}

# הדפסת סיכום סניפים
message("\n── סיכום סניפים ──")
for (ch in names(branches)) {
  b <- branches[[ch]]
  message(ch, ": ", nrow(b))
  if (nrow(b) > 0) message(paste0("  ", b$store_id, "  ", b$store_name, "  (", b$city, ")", collapse = "\n"))
}

# ══════════════════════════════════════════════════════════════════════════════
# שלב 2: הורדת מחירים
# ══════════════════════════════════════════════════════════════════════════════

message("\n\n══ שלב 2: מחירים לתאריך ", RUN_DATE, " ══")
all_prices <- list()

# Osher Ad
if (!is.null(branches$OSHER_AD) && nrow(branches$OSHER_AD) > 0) {
  message("\n[אושר עד]")
  ids   <- branches$OSHER_AD$store_id
  paths <- ftp_download_prices("osherad", ids)
  for (p in paths) {
    si  <- str_extract(basename(p), paste(ids, collapse = "|"))
    row <- branches$OSHER_AD[branches$OSHER_AD$store_id == si, ][1, ]
    df  <- xml_to_prices(p, "אושר עד", row$store_id, row$store_name, row$city)
    if (!is.null(df)) { all_prices[[basename(p)]] <- df; message("  ✓ ", nrow(df), " מוצרים — ", row$store_name) }
  }
}

# Rami Levy
if (!is.null(branches$RAMI_LEVY) && nrow(branches$RAMI_LEVY) > 0) {
  message("\n[רמי לוי]")
  ids   <- branches$RAMI_LEVY$store_id
  paths <- ftp_download_prices("RamiLevi", ids)
  for (p in paths) {
    si  <- str_extract(basename(p), paste(ids, collapse = "|"))
    row <- branches$RAMI_LEVY[branches$RAMI_LEVY$store_id == si, ][1, ]
    df  <- xml_to_prices(p, "רמי לוי", row$store_id, row$store_name, row$city)
    if (!is.null(df)) { all_prices[[basename(p)]] <- df; message("  ✓ ", nrow(df), " מוצרים — ", row$store_name) }
  }
}

# Shufersal
if (!is.null(branches$SHUFERSAL) && nrow(branches$SHUFERSAL) > 0) {
  message("\n[שופרסל דיל]")
  ids      <- branches$SHUFERSAL$store_id
  pf_files <- shufersal_file_list("2")   # catID=2 = price files
  pf_files <- pf_files[grepl("pricefull", pf_files$name, ignore.case = TRUE) &
                        grepl(RUN_DATE, pf_files$name), ]
  pf_files <- pf_files[grepl(paste(ids, collapse = "|"), pf_files$name), ]
  for (i in seq_len(nrow(pf_files))) {
    message("  מוריד: ", pf_files$name[i])
    tryCatch({
      dest <- http_fetch(pf_files$url[i])
      si  <- str_extract(pf_files$name[i], paste(ids, collapse = "|"))
      row <- branches$SHUFERSAL[branches$SHUFERSAL$store_id == si, ][1, ]
      df  <- xml_to_prices(dest, "שופרסל דיל", row$store_id, row$store_name, row$city)
      if (!is.null(df)) { all_prices[[pf_files$name[i]]] <- df; message("  ✓ ", nrow(df), " מוצרים") }
    }, error = function(e) message("  נכשל: ", e$message))
  }
}

# Carrefour
if (!is.null(branches$CARREFOUR) && nrow(branches$CARREFOUR) > 0) {
  message("\n[קרפור]")
  ids      <- branches$CARREFOUR$store_id
  cf_all   <- carrefour_file_list()
  pf_files <- cf_all[grepl("pricefull", cf_all$name, ignore.case = TRUE) &
                     grepl(RUN_DATE, cf_all$name) &
                     grepl(paste(ids, collapse = "|"), cf_all$name), ]
  for (i in seq_len(nrow(pf_files))) {
    message("  מוריד: ", pf_files$name[i])
    tryCatch({
      dest <- http_fetch(pf_files$url[i])
      si  <- str_extract(pf_files$name[i], paste(ids, collapse = "|"))
      row <- branches$CARREFOUR[branches$CARREFOUR$store_id == si, ][1, ]
      df  <- xml_to_prices(dest, "קרפור", row$store_id, row$store_name, row$city)
      if (!is.null(df)) { all_prices[[pf_files$name[i]]] <- df; message("  ✓ ", nrow(df), " מוצרים") }
    }, error = function(e) message("  נכשל: ", e$message))
  }
}

# ══════════════════════════════════════════════════════════════════════════════
# תוצאה סופית
# ══════════════════════════════════════════════════════════════════════════════

if (length(all_prices) == 0) {
  warning("לא הורדו נתונים. בדוק חיבור לאינטרנט ורשימת סניפים למעלה.")
} else {
  all_prices_df <- bind_rows(all_prices)
  message("\n══ סיים ══")
  message("all_prices_df: ", nrow(all_prices_df), " שורות, ",
          length(unique(all_prices_df$store_id)), " סניפים")
  message("עמודות: ", paste(names(all_prices_df), collapse = ", "))
}
