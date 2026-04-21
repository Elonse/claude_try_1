# download_prices.R
# מוריד מחירים ומבצעים מ-4 רשתות לסניפים בערים הנבחרות
# מבוסס על המודל שעבד: curl_fetch_memory + writeBin
# תוצאה: all_prices_df, all_promos_df

library(curl)
library(xml2)
library(rvest)
library(stringr)
library(dplyr)
library(jsonlite)

# ══════════════════════════════════════════════════════════════
# הגדרות
# ══════════════════════════════════════════════════════════════

TARGET_CITIES <- c("חיפה", "קריית ביאליק", "בית שמש", "אשקלון", "באר שבע", "פתח תקווה")
FTP_HOST      <- "url.retail.publishedprices.co.il"

CHAINS <- list(
  rami_levy = list(
    name     = "רמי לוי",
    engine   = "ftp",
    user     = "RamiLevi",
    subchain = NULL
  ),
  osher_ad = list(
    name     = "אושר עד",
    engine   = "ftp",
    user     = "osherad",
    subchain = NULL
  ),
  shufersal = list(
    name     = "שופרסל דיל",
    engine   = "shufersal",
    subchain = "שופרסל דיל"
  ),
  carrefour = list(
    name     = "קרפור",
    engine   = "carrefour",
    subchain = "קרפור"
  )
)

# ══════════════════════════════════════════════════════════════
# פונקציות עזר
# ══════════════════════════════════════════════════════════════

# הורדת URL לקובץ זמני — ללא curl_download (עוקף בעיית Windows)
fetch_to_tmp <- function(url, handle = new_handle()) {
  raw  <- curl_fetch_memory(url, handle = handle)
  ext  <- tools::file_ext(sub("\\?.*", "", basename(url)))
  dest <- tempfile(fileext = if (nchar(ext) > 0) paste0(".", ext) else ".tmp")
  writeBin(raw$content, dest)
  dest
}

# פתיחת XML (כולל gz)
open_xml <- function(path) {
  if (grepl("\\.gz$", path, ignore.case = TRUE)) {
    out <- tempfile(fileext = ".xml")
    con <- gzfile(path, "rb")
    writeBin(readBin(con, "raw", n = 100e6), out)
    close(con)
    path <- out
  }
  for (enc in c("UTF-8", "Windows-1255")) {
    doc <- tryCatch(read_xml(path, encoding = enc), error = function(e) NULL)
    if (!is.null(doc)) return(doc)
  }
  NULL
}

# XML חנויות → data.frame
xml_to_stores <- function(path) {
  doc   <- open_xml(path)
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

# XML מחירים → data.frame
xml_to_prices <- function(path, chain, store_id, store_name, city) {
  doc   <- open_xml(path)
  if (is.null(doc)) return(NULL)
  items <- xml_find_all(doc, "//Item")
  if (length(items) == 0) return(NULL)
  data.frame(
    barcode    = xml_text(xml_find_first(items, ".//ItemCode")),
    name       = xml_text(xml_find_first(items, ".//ItemName")),
    price      = suppressWarnings(as.numeric(xml_text(xml_find_first(items, ".//ItemPrice")))),
    unit       = xml_text(xml_find_first(items, ".//UnitOfMeasure")),
    chain      = chain, store_id = store_id, store_name = store_name, city = city,
    stringsAsFactors = FALSE
  )
}

# XML מבצעים → data.frame
xml_to_promos <- function(path, chain, store_id, store_name, city) {
  doc    <- open_xml(path)
  if (is.null(doc)) return(NULL)
  promos <- xml_find_all(doc, "//Promotion")
  if (length(promos) == 0) return(NULL)
  data.frame(
    promo_id    = xml_text(xml_find_first(promos, ".//PromotionId")),
    description = xml_text(xml_find_first(promos, ".//PromotionDescription")),
    start_date  = xml_text(xml_find_first(promos, ".//PromotionStartDate")),
    end_date    = xml_text(xml_find_first(promos, ".//PromotionEndDate")),
    min_qty     = xml_text(xml_find_first(promos, ".//MinQty")),
    discount    = xml_text(xml_find_first(promos, ".//DiscountedPrice|.//DiscountRate")),
    chain       = chain, store_id = store_id, store_name = store_name, city = city,
    stringsAsFactors = FALSE
  )
}

# ══════════════════════════════════════════════════════════════
# FTP helpers
# ══════════════════════════════════════════════════════════════

ftp_ls <- function(user) {
  h <- new_handle(username = user, password = "")
  handle_setopt(h, dirlistonly = TRUE)
  r <- tryCatch(
    curl_fetch_memory(paste0("ftp://", FTP_HOST, "/"), handle = h),
    error = function(e) { message("  FTP שגיאה (", user, "): ", e$message); NULL }
  )
  if (is.null(r)) return(character(0))
  x <- trimws(strsplit(rawToChar(r$content), "\r?\n")[[1]])
  x[nchar(x) > 0]
}

ftp_fetch <- function(user, fname) {
  h <- new_handle(username = user, password = "")
  tryCatch(
    fetch_to_tmp(paste0("ftp://", FTP_HOST, "/", fname), handle = h),
    error = function(e) { message("  FTP הורדה נכשלה: ", fname); NULL }
  )
}

# ══════════════════════════════════════════════════════════════
# Shufersal HTTP helpers
# ══════════════════════════════════════════════════════════════

SHUFERSAL_UA <- "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

shufersal_ls <- function(cat_id) {
  # catID=5 → חנויות | catID=2 → מחירים | catID=1 → מבצעים
  base <- paste0("https://prices.shufersal.co.il/FileObject/UpdateCategory?catID=", cat_id)
  result <- data.frame(url = character(), name = character(), stringsAsFactors = FALSE)

  # מציאת מספר עמודות
  h     <- new_handle()
  handle_setopt(h, useragent = SHUFERSAL_UA)
  raw   <- tryCatch(curl_fetch_memory(paste0(base, "&page=1"), handle = h),
                    error = function(e) NULL)
  if (is.null(raw)) return(result)
  pg    <- read_html(rawToChar(raw$content))
  total <- tryCatch({
    nums <- pg %>% html_nodes("#gridContainer tfoot a") %>%
      html_attr("href") %>% str_match("page=(\\d+)") %>% { .[,2] } %>%
      as.integer()
    if (length(nums) > 0 && any(!is.na(nums))) max(nums, na.rm = TRUE) else 1
  }, error = function(e) 1)

  for (p in seq_len(total)) {
    h2  <- new_handle(); handle_setopt(h2, useragent = SHUFERSAL_UA)
    raw2 <- tryCatch(curl_fetch_memory(paste0(base, "&page=", p), handle = h2),
                     error = function(e) NULL)
    if (is.null(raw2)) next
    pg2   <- read_html(rawToChar(raw2$content))
    nodes <- html_nodes(pg2, "a[href*='File/Get']")
    hrefs <- html_attr(nodes, "href")
    names <- trimws(html_text(nodes))
    hrefs_full <- ifelse(startsWith(hrefs, "http"), hrefs,
                         paste0("https://prices.shufersal.co.il", hrefs))
    result <- rbind(result, data.frame(url = hrefs_full, name = names, stringsAsFactors = FALSE))
    Sys.sleep(0.3)
  }
  result[!duplicated(result$url), ]
}

# ══════════════════════════════════════════════════════════════
# Carrefour HTTP helpers
# ══════════════════════════════════════════════════════════════

carrefour_ls <- function() {
  h   <- new_handle()
  raw <- tryCatch(curl_fetch_memory("https://prices.carrefour.co.il/", handle = h),
                  error = function(e) { message("  Carrefour לא נגיש"); NULL })
  if (is.null(raw)) return(NULL)
  txt   <- rawToChar(raw$content)
  path  <- str_match(txt, "const path\\s*=\\s*['\"]([^'\"]+)['\"]")[, 2]
  fjson <- str_match(txt, "const files\\s*=\\s*(\\[.*?\\]);")[, 2]
  if (is.na(path) || is.na(fjson)) { message("  לא ניתן לפרסר Carrefour"); return(NULL) }
  files <- fromJSON(fjson)
  data.frame(url  = paste0("https://prices.carrefour.co.il/", path, "/", files$name),
             name = files$name, stringsAsFactors = FALSE)
}

# ══════════════════════════════════════════════════════════════
# שלב 1 — רשימת סניפים
# ══════════════════════════════════════════════════════════════

message("══ שלב 1: סניפים ══")
branches <- list()

for (key in names(CHAINS)) {
  cfg <- CHAINS[[key]]
  message("\n[", cfg$name, "]")

  path <- switch(cfg$engine,
    ftp = {
      files <- ftp_ls(cfg$user)
      sf    <- tail(sort(grep("store", files, ignore.case = TRUE, value = TRUE)), 1)
      if (length(sf) == 0) { message("  אין קובץ חנויות"); NULL }
      else { message("  מוריד: ", sf); ftp_fetch(cfg$user, sf) }
    },
    shufersal = {
      df <- shufersal_ls("5")
      sf <- df[grepl("store", df$name, ignore.case = TRUE), ]
      if (nrow(sf) == 0) { message("  אין קובץ חנויות"); NULL }
      else { url <- tail(sort(sf$url), 1); message("  מוריד: ", basename(url))
             tryCatch(fetch_to_tmp(url), error = function(e) NULL) }
    },
    carrefour = {
      df <- carrefour_ls()
      if (is.null(df)) NULL
      else {
        sf <- df[grepl("store", df$name, ignore.case = TRUE), ]
        if (nrow(sf) == 0) { message("  אין קובץ חנויות"); NULL }
        else { url <- tail(sort(sf$url), 1); message("  מוריד: ", basename(url))
               tryCatch(fetch_to_tmp(url), error = function(e) NULL) }
      }
    }
  )

  if (is.null(path)) next
  stores <- xml_to_stores(path)
  if (is.null(stores)) next

  matched <- stores[stores$city %in% TARGET_CITIES, ]
  if (!is.null(cfg$subchain))
    matched <- matched[grepl(cfg$subchain, matched$subchain, fixed = TRUE), ]

  if (nrow(matched) == 0) { message("  אין סניפים בערים הנבחרות"); next }
  branches[[key]] <- matched
  message("  נמצאו ", nrow(matched), " סניפים:")
  for (i in seq_len(nrow(matched)))
    message("    [", matched$store_id[i], "] ", matched$store_name[i], " — ", matched$city[i])
}

# ══════════════════════════════════════════════════════════════
# שלב 2 — הורדת מחירים ומבצעים
# ══════════════════════════════════════════════════════════════

message("\n\n══ שלב 2: מחירים ומבצעים ══")
all_prices <- list()
all_promos <- list()

for (key in names(branches)) {
  cfg     <- CHAINS[[key]]
  br      <- branches[[key]]
  ids     <- br$store_id
  message("\n[", cfg$name, "] — ", length(ids), " סניפים")

  # ── קבלת רשימת קבצים ──
  file_list <- switch(cfg$engine,
    ftp       = ftp_ls(cfg$user),
    shufersal = {
      pf <- shufersal_ls("2"); pm <- shufersal_ls("1")
      list(prices = pf, promos = pm)
    },
    carrefour = { cf <- carrefour_ls(); list(prices = cf, promos = cf) }
  )

  for (i in seq_len(nrow(br))) {
    sid   <- br$store_id[i]
    sname <- br$store_name[i]
    scity <- br$city[i]

    # ── מחירים ──
    price_file <- switch(cfg$engine,
      ftp = {
        candidates <- grep("pricefull", file_list, ignore.case = TRUE, value = TRUE)
        candidates <- grep(sid, candidates, value = TRUE)
        if (length(candidates) == 0) NULL else tail(sort(candidates), 1)
      },
      shufersal = , carrefour = {
        df <- file_list$prices
        if (is.null(df)) NULL else {
          rows <- df[grepl("pricefull", df$name, ignore.case = TRUE) &
                     grepl(sid, df$name), ]
          if (nrow(rows) == 0) NULL else tail(sort(rows$url), 1)
        }
      }
    )

    if (!is.null(price_file)) {
      message("  מחירים [", sid, "] ", sname)
      path <- switch(cfg$engine,
        ftp       = ftp_fetch(cfg$user, price_file),
        shufersal = , carrefour = tryCatch(fetch_to_tmp(price_file), error = function(e) NULL)
      )
      if (!is.null(path)) {
        df <- xml_to_prices(path, cfg$name, sid, sname, scity)
        if (!is.null(df)) { all_prices[[paste(key, sid)]] <- df; message("    ✓ ", nrow(df), " מוצרים") }
      }
    } else message("  אין קובץ מחירים ל-[", sid, "]")

    # ── מבצעים ──
    promo_file <- switch(cfg$engine,
      ftp = {
        candidates <- grep("promofull", file_list, ignore.case = TRUE, value = TRUE)
        candidates <- grep(sid, candidates, value = TRUE)
        if (length(candidates) == 0) NULL else tail(sort(candidates), 1)
      },
      shufersal = , carrefour = {
        df <- file_list$promos
        if (is.null(df)) NULL else {
          rows <- df[grepl("promofull", df$name, ignore.case = TRUE) &
                     grepl(sid, df$name), ]
          if (nrow(rows) == 0) NULL else tail(sort(rows$url), 1)
        }
      }
    )

    if (!is.null(promo_file)) {
      message("  מבצעים  [", sid, "] ", sname)
      path <- switch(cfg$engine,
        ftp       = ftp_fetch(cfg$user, promo_file),
        shufersal = , carrefour = tryCatch(fetch_to_tmp(promo_file), error = function(e) NULL)
      )
      if (!is.null(path)) {
        df <- xml_to_promos(path, cfg$name, sid, sname, scity)
        if (!is.null(df)) { all_promos[[paste(key, sid)]] <- df; message("    ✓ ", nrow(df), " מבצעים") }
      }
    }
  }
}

# ══════════════════════════════════════════════════════════════
# תוצאות
# ══════════════════════════════════════════════════════════════

all_prices_df <- if (length(all_prices) > 0) bind_rows(all_prices) else data.frame()
all_promos_df <- if (length(all_promos) > 0) bind_rows(all_promos) else data.frame()

message("\n══ סיום ══")
message("all_prices_df: ", nrow(all_prices_df), " שורות | ",
        n_distinct(all_prices_df$store_id), " סניפים | ",
        n_distinct(all_prices_df$chain), " רשתות")
message("all_promos_df: ", nrow(all_promos_df), " שורות")
