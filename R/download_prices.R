# download_prices.R
library(curl); library(xml2); library(rvest); library(stringr); library(dplyr); library(jsonlite)

TARGET_CITIES <- c("חיפה", "קריית ביאליק", "קרית ביאליק",
                   "בית שמש", "אשקלון", "באר שבע", "פתח תקווה")
FTP_HOST <- "url.retail.publishedprices.co.il"

# ── עזר: הורדה לזיכרון → קובץ זמני ─────────────────────────────────────────
mem_fetch <- function(url, handle = new_handle()) {
  r    <- curl_fetch_memory(url, handle = handle)
  ext  <- tools::file_ext(sub("\\?.*", "", basename(url)))
  dest <- tempfile(fileext = if (nchar(ext) > 0) paste0(".", ext) else ".tmp")
  writeBin(r$content, dest)
  dest
}

# ── עזר: פתיחת XML (גם gz) ───────────────────────────────────────────────────
open_xml <- function(path) {
  if (grepl("\\.gz$", path, ignore.case = TRUE)) {
    out <- tempfile(fileext = ".xml")
    con <- gzfile(path, "rb"); writeBin(readBin(con, "raw", n = 100e6), out); close(con)
    path <- out
  }
  for (enc in c("UTF-8", "Windows-1255")) {
    d <- tryCatch(read_xml(path, encoding = enc), error = function(e) NULL)
    if (!is.null(d)) return(d)
  }
  NULL
}

# ── עזר: XML → dataframe מחירים ─────────────────────────────────────────────
to_prices <- function(path, chain, store_id, store_name, city) {
  doc <- open_xml(path); if (is.null(doc)) return(NULL)
  it  <- xml_find_all(doc, "//Item"); if (length(it) == 0) return(NULL)
  data.frame(
    barcode = xml_text(xml_find_first(it, ".//ItemCode")),
    name    = xml_text(xml_find_first(it, ".//ItemName")),
    price   = suppressWarnings(as.numeric(xml_text(xml_find_first(it, ".//ItemPrice")))),
    unit    = xml_text(xml_find_first(it, ".//UnitOfMeasure")),
    chain = chain, store_id = store_id, store_name = store_name, city = city,
    stringsAsFactors = FALSE)
}

# ── עזר: XML → dataframe מבצעים ──────────────────────────────────────────────
to_promos <- function(path, chain, store_id, store_name, city) {
  doc <- open_xml(path); if (is.null(doc)) return(NULL)
  pr  <- xml_find_all(doc, "//Promotion"); if (length(pr) == 0) return(NULL)
  data.frame(
    promo_id    = xml_text(xml_find_first(pr, ".//PromotionId")),
    description = xml_text(xml_find_first(pr, ".//PromotionDescription")),
    start_date  = xml_text(xml_find_first(pr, ".//PromotionStartDate")),
    end_date    = xml_text(xml_find_first(pr, ".//PromotionEndDate")),
    discount    = xml_text(xml_find_first(pr, ".//DiscountedPrice|.//DiscountRate")),
    chain = chain, store_id = store_id, store_name = store_name, city = city,
    stringsAsFactors = FALSE)
}

# ── FTP ───────────────────────────────────────────────────────────────────────
ftp_ls <- function(user) {
  h <- new_handle(username = user, password = ""); handle_setopt(h, dirlistonly = TRUE)
  r <- tryCatch(curl_fetch_memory(paste0("ftp://", FTP_HOST, "/"), handle = h),
                error = function(e) { message("FTP error: ", e$message); NULL })
  if (is.null(r)) return(character(0))
  x <- trimws(strsplit(rawToChar(r$content), "\r?\n")[[1]]); x[nchar(x) > 0]
}
ftp_get <- function(user, fname) {
  h <- new_handle(username = user, password = "")
  tryCatch(mem_fetch(paste0("ftp://", FTP_HOST, "/", fname), h),
           error = function(e) { message("  FTP get failed: ", e$message); NULL })
}

# ── Shufersal ─────────────────────────────────────────────────────────────────
UA <- "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
shufersal_ls <- function(cat_id) {
  # catID: 5=חנויות  2=מחירים  1=מבצעים
  base   <- paste0("https://prices.shufersal.co.il/FileObject/UpdateCategory?catID=", cat_id)
  result <- data.frame(url = character(), name = character(), stringsAsFactors = FALSE)
  h <- new_handle(); handle_setopt(h, useragent = UA)
  r <- tryCatch(curl_fetch_memory(paste0(base, "&page=1"), handle = h), error = function(e) NULL)
  if (is.null(r)) { message("  Shufersal לא נגיש"); return(result) }
  pg    <- read_html(rawToChar(r$content))
  total <- tryCatch({
    n <- pg %>% html_nodes("#gridContainer tfoot a") %>% html_attr("href") %>%
         str_match("page=(\\d+)") %>% { .[,2] } %>% as.integer()
    if (length(n) > 0 && any(!is.na(n))) max(n, na.rm = TRUE) else 1
  }, error = function(e) 1)
  for (p in seq_len(total)) {
    h2 <- new_handle(); handle_setopt(h2, useragent = UA)
    r2 <- tryCatch(curl_fetch_memory(paste0(base, "&page=", p), handle = h2), error = function(e) NULL)
    if (is.null(r2)) next
    nd <- html_nodes(read_html(rawToChar(r2$content)), "a[href*='File/Get']")
    hr <- html_attr(nd, "href")
    nm <- trimws(html_text(nd))
    hr <- ifelse(startsWith(hr, "http"), hr, paste0("https://prices.shufersal.co.il", hr))
    result <- rbind(result, data.frame(url = hr, name = nm, stringsAsFactors = FALSE))
    Sys.sleep(0.2)
  }
  result[!duplicated(result$url), ]
}

# ── Carrefour ─────────────────────────────────────────────────────────────────
carrefour_ls <- function() {
  r <- tryCatch(curl_fetch_memory("https://prices.carrefour.co.il/"),
                error = function(e) { message("Carrefour error: ", e$message); NULL })
  if (is.null(r)) return(NULL)
  txt   <- rawToChar(r$content)
  path  <- str_match(txt, "const path\\s*=\\s*['\"]([^'\"]+)['\"]")[,2]
  fjson <- str_match(txt, "const files\\s*=\\s*(\\[.*?\\]);")[,2]
  if (is.na(path) || is.na(fjson)) { message("Carrefour parse failed"); return(NULL) }
  files <- fromJSON(fjson)
  data.frame(url  = paste0("https://prices.carrefour.co.il/", path, "/", files$name),
             name = files$name, stringsAsFactors = FALSE)
}

# ══════════════════════════════════════════════════════════════════════════════
# שלב 1 — רשימת סניפים
# ══════════════════════════════════════════════════════════════════════════════
message("══ שלב 1: סניפים ══")
branches <- list()

## רמי לוי (FTP)
message("\n[רמי לוי]")
rl_files <- ftp_ls("RamiLevi")
message("  קבצים ב-FTP: ", length(rl_files))
rl_stores_f <- tail(sort(grep("store", rl_files, ignore.case = TRUE, value = TRUE)), 1)
if (length(rl_stores_f) > 0) {
  message("  מוריד: ", rl_stores_f)
  path <- ftp_get("RamiLevi", rl_stores_f)
  if (!is.null(path)) {
    doc  <- open_xml(path)
    stores <- if (!is.null(doc)) {
      nd <- xml_find_all(doc, "//Store")
      data.frame(store_id   = xml_text(xml_find_first(nd, ".//StoreId|.//StoreID")),
                 store_name = xml_text(xml_find_first(nd, ".//StoreName")),
                 city       = xml_text(xml_find_first(nd, ".//City")),
                 stringsAsFactors = FALSE)
    } else NULL
    if (!is.null(stores)) {
      message("  כל הערים ב-XML: ", paste(unique(stores$city), collapse = ", "))
      branches$rami_levy <- stores[stores$city %in% TARGET_CITIES, ]
      message("  סניפים נבחרים: ", nrow(branches$rami_levy))
    }
  }
}

## אושר עד (FTP)
message("\n[אושר עד]")
oa_files <- ftp_ls("osherad")
message("  קבצים ב-FTP: ", length(oa_files))
oa_stores_f <- tail(sort(grep("store", oa_files, ignore.case = TRUE, value = TRUE)), 1)
if (length(oa_stores_f) > 0) {
  message("  מוריד: ", oa_stores_f)
  path <- ftp_get("osherad", oa_stores_f)
  if (!is.null(path)) {
    doc  <- open_xml(path)
    stores <- if (!is.null(doc)) {
      nd <- xml_find_all(doc, "//Store")
      data.frame(store_id   = xml_text(xml_find_first(nd, ".//StoreId|.//StoreID")),
                 store_name = xml_text(xml_find_first(nd, ".//StoreName")),
                 city       = xml_text(xml_find_first(nd, ".//City")),
                 stringsAsFactors = FALSE)
    } else NULL
    if (!is.null(stores)) {
      message("  כל הערים ב-XML: ", paste(unique(stores$city), collapse = ", "))
      branches$osher_ad <- stores[stores$city %in% TARGET_CITIES, ]
      message("  סניפים נבחרים: ", nrow(branches$osher_ad))
    }
  }
}

## שופרסל (HTTP)
message("\n[שופרסל]")
shuf_store_files <- shufersal_ls("5")
message("  קישורים בדף catID=5: ", nrow(shuf_store_files))
if (nrow(shuf_store_files) > 0) {
  sf <- shuf_store_files[grepl("store", shuf_store_files$name, ignore.case = TRUE), ]
  message("  קבצי חנויות שנמצאו: ", nrow(sf))
  if (nrow(sf) > 0) {
    url  <- tail(sort(sf$url), 1)
    message("  מוריד: ", basename(url))
    path <- tryCatch(mem_fetch(url), error = function(e) NULL)
    if (!is.null(path)) {
      doc <- open_xml(path)
      stores <- if (!is.null(doc)) {
        nd <- xml_find_all(doc, "//Store")
        data.frame(store_id   = xml_text(xml_find_first(nd, ".//StoreId|.//StoreID")),
                   store_name = xml_text(xml_find_first(nd, ".//StoreName")),
                   subchain   = xml_text(xml_find_first(nd, ".//SubChainName")),
                   city       = xml_text(xml_find_first(nd, ".//City")),
                   stringsAsFactors = FALSE)
      } else NULL
      if (!is.null(stores)) {
        message("  כל הערים ב-XML: ", paste(unique(stores$city), collapse = ", "))
        matched <- stores[stores$city %in% TARGET_CITIES &
                          grepl("שופרסל דיל", stores$subchain, fixed = TRUE), ]
        branches$shufersal <- matched
        message("  סניפים נבחרים (שופרסל דיל): ", nrow(matched))
      }
    }
  }
}

## קרפור (HTTP)
message("\n[קרפור]")
cf_all <- carrefour_ls()
if (!is.null(cf_all)) {
  message("  קבצים ב-Carrefour: ", nrow(cf_all))
  sf <- cf_all[grepl("store", cf_all$name, ignore.case = TRUE), ]
  message("  קבצי חנויות: ", nrow(sf))
  if (nrow(sf) > 0) {
    url  <- tail(sort(sf$url), 1)
    message("  מוריד: ", basename(url))
    path <- tryCatch(mem_fetch(url), error = function(e) NULL)
    if (!is.null(path)) {
      doc <- open_xml(path)
      stores <- if (!is.null(doc)) {
        nd <- xml_find_all(doc, "//Store")
        data.frame(store_id   = xml_text(xml_find_first(nd, ".//StoreId|.//StoreID")),
                   store_name = xml_text(xml_find_first(nd, ".//StoreName")),
                   subchain   = xml_text(xml_find_first(nd, ".//SubChainName")),
                   city       = xml_text(xml_find_first(nd, ".//City")),
                   stringsAsFactors = FALSE)
      } else NULL
      if (!is.null(stores)) {
        message("  כל הערים ב-XML: ", paste(unique(stores$city), collapse = ", "))
        matched <- stores[stores$city %in% TARGET_CITIES &
                          grepl("קרפור", stores$subchain, fixed = TRUE), ]
        branches$carrefour <- matched
        message("  סניפים נבחרים (קרפור): ", nrow(matched))
      }
    }
  }
}

message("\n── סיכום: ", sum(sapply(branches, nrow)), " סניפים סה\"כ ──")
for (ch in names(branches)) {
  b <- branches[[ch]]
  message(ch, ": ", nrow(b))
  if (nrow(b) > 0) for (i in seq_len(nrow(b)))
    message("  [", b$store_id[i], "] ", b$store_name[i], " (", b$city[i], ")")
}

# ══════════════════════════════════════════════════════════════════════════════
# שלב 2 — מחירים ומבצעים
# ══════════════════════════════════════════════════════════════════════════════
message("\n══ שלב 2: מחירים ומבצעים ══")
all_prices <- list()
all_promos <- list()

## רמי לוי ─────────────────────────────────────────────────────────────────────
if (!is.null(branches$rami_levy) && nrow(branches$rami_levy) > 0) {
  for (i in seq_len(nrow(branches$rami_levy))) {
    sid <- branches$rami_levy$store_id[i]
    # PriceFull
    pf <- tail(sort(grep(sid, grep("pricefull", rl_files, ignore.case=TRUE, value=TRUE), value=TRUE)), 1)
    if (length(pf) > 0) {
      path <- ftp_get("RamiLevi", pf)
      if (!is.null(path)) {
        df <- to_prices(path, "רמי לוי", sid, branches$rami_levy$store_name[i], branches$rami_levy$city[i])
        if (!is.null(df)) { all_prices[[paste0("rl_",sid)]] <- df; message("רמי לוי [",sid,"] מחירים: ", nrow(df)) }
      }
    }
    # PromoFull
    pmf <- tail(sort(grep(sid, grep("promofull", rl_files, ignore.case=TRUE, value=TRUE), value=TRUE)), 1)
    if (length(pmf) > 0) {
      path <- ftp_get("RamiLevi", pmf)
      if (!is.null(path)) {
        df <- to_promos(path, "רמי לוי", sid, branches$rami_levy$store_name[i], branches$rami_levy$city[i])
        if (!is.null(df)) { all_promos[[paste0("rl_",sid)]] <- df; message("רמי לוי [",sid,"] מבצעים: ", nrow(df)) }
      }
    }
  }
}

## אושר עד ─────────────────────────────────────────────────────────────────────
if (!is.null(branches$osher_ad) && nrow(branches$osher_ad) > 0) {
  for (i in seq_len(nrow(branches$osher_ad))) {
    sid <- branches$osher_ad$store_id[i]
    pf  <- tail(sort(grep(sid, grep("pricefull", oa_files, ignore.case=TRUE, value=TRUE), value=TRUE)), 1)
    if (length(pf) > 0) {
      path <- ftp_get("osherad", pf)
      if (!is.null(path)) {
        df <- to_prices(path, "אושר עד", sid, branches$osher_ad$store_name[i], branches$osher_ad$city[i])
        if (!is.null(df)) { all_prices[[paste0("oa_",sid)]] <- df; message("אושר עד [",sid,"] מחירים: ", nrow(df)) }
      }
    }
    pmf <- tail(sort(grep(sid, grep("promofull", oa_files, ignore.case=TRUE, value=TRUE), value=TRUE)), 1)
    if (length(pmf) > 0) {
      path <- ftp_get("osherad", pmf)
      if (!is.null(path)) {
        df <- to_promos(path, "אושר עד", sid, branches$osher_ad$store_name[i], branches$osher_ad$city[i])
        if (!is.null(df)) { all_promos[[paste0("oa_",sid)]] <- df; message("אושר עד [",sid,"] מבצעים: ", nrow(df)) }
      }
    }
  }
}

## שופרסל ──────────────────────────────────────────────────────────────────────
if (!is.null(branches$shufersal) && nrow(branches$shufersal) > 0) {
  shuf_price_files <- shufersal_ls("2")   # catID=2: מחירים
  shuf_promo_files <- shufersal_ls("1")   # catID=1: מבצעים
  for (i in seq_len(nrow(branches$shufersal))) {
    sid <- branches$shufersal$store_id[i]
    # מחירים
    pf <- shuf_price_files[grepl("pricefull", shuf_price_files$name, ignore.case=TRUE) &
                            grepl(sid, shuf_price_files$name), ]
    if (nrow(pf) > 0) {
      path <- tryCatch(mem_fetch(tail(sort(pf$url),1)), error=function(e) NULL)
      if (!is.null(path)) {
        df <- to_prices(path, "שופרסל דיל", sid, branches$shufersal$store_name[i], branches$shufersal$city[i])
        if (!is.null(df)) { all_prices[[paste0("sh_",sid)]] <- df; message("שופרסל [",sid,"] מחירים: ", nrow(df)) }
      }
    }
    # מבצעים
    pmf <- shuf_promo_files[grepl("promo", shuf_promo_files$name, ignore.case=TRUE) &
                             grepl(sid, shuf_promo_files$name), ]
    if (nrow(pmf) > 0) {
      path <- tryCatch(mem_fetch(tail(sort(pmf$url),1)), error=function(e) NULL)
      if (!is.null(path)) {
        df <- to_promos(path, "שופרסל דיל", sid, branches$shufersal$store_name[i], branches$shufersal$city[i])
        if (!is.null(df)) { all_promos[[paste0("sh_",sid)]] <- df; message("שופרסל [",sid,"] מבצעים: ", nrow(df)) }
      }
    }
  }
}

## קרפור ───────────────────────────────────────────────────────────────────────
if (!is.null(branches$carrefour) && nrow(branches$carrefour) > 0) {
  # Carrefour: "Price" לא "PriceFull", "NULLPromo" לא "PromoFull"
  for (i in seq_len(nrow(branches$carrefour))) {
    sid <- branches$carrefour$store_id[i]
    # מחירים — grep ל-"price" (לא "pricefull")
    pf <- cf_all[grepl("^price", cf_all$name, ignore.case=TRUE) & grepl(sid, cf_all$name), ]
    if (nrow(pf) > 0) {
      path <- tryCatch(mem_fetch(tail(sort(pf$url),1)), error=function(e) NULL)
      if (!is.null(path)) {
        df <- to_prices(path, "קרפור", sid, branches$carrefour$store_name[i], branches$carrefour$city[i])
        if (!is.null(df)) { all_prices[[paste0("cf_",sid)]] <- df; message("קרפור [",sid,"] מחירים: ", nrow(df)) }
      }
    }
    # מבצעים — grep ל-"promo"
    pmf <- cf_all[grepl("promo", cf_all$name, ignore.case=TRUE) & grepl(sid, cf_all$name), ]
    if (nrow(pmf) > 0) {
      path <- tryCatch(mem_fetch(tail(sort(pmf$url),1)), error=function(e) NULL)
      if (!is.null(path)) {
        df <- to_promos(path, "קרפור", sid, branches$carrefour$store_name[i], branches$carrefour$city[i])
        if (!is.null(df)) { all_promos[[paste0("cf_",sid)]] <- df; message("קרפור [",sid,"] מבצעים: ", nrow(df)) }
      }
    }
  }
}

# ══════════════════════════════════════════════════════════════════════════════
all_prices_df <- if (length(all_prices) > 0) bind_rows(all_prices) else data.frame()
all_promos_df <- if (length(all_promos) > 0) bind_rows(all_promos) else data.frame()
message("\n══ סיום ══")
message("all_prices_df: ", nrow(all_prices_df), " שורות | ", n_distinct(all_prices_df$store_id), " סניפים")
message("all_promos_df: ", nrow(all_promos_df), " שורות")
