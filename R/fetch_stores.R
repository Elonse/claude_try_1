# fetch_stores.R
# Downloads StoresFull files for all 4 chains, finds branches in our 5 target
# cities, and writes config/branches.json.
#
# Run ONCE from an Israeli machine before the first price download.
#
# Required packages (install once):
#   install.packages(c("httr", "curl", "xml2", "jsonlite", "rvest", "stringr"))

library(httr)
library(curl)
library(xml2)
library(jsonlite)
library(rvest)
library(stringr)

# ── Config ────────────────────────────────────────────────────────────────────

REPO_ROOT    <- dirname(dirname(rstudioapi::getSourceEditorContext()$path))
# If not in RStudio, set manually:
# REPO_ROOT <- "/path/to/claude_try_1"

CONFIG_FILE  <- file.path(REPO_ROOT, "config", "chains.json")
OUTPUT_FILE  <- file.path(REPO_ROOT, "config", "branches.json")
TMP_DIR      <- tempdir()

config       <- fromJSON(CONFIG_FILE)
TARGET_CITIES <- config$target_cities

FTP_HOST <- "url.retail.publishedprices.co.il"

CHAINS <- list(
  OSHER_AD = list(
    display_name   = "אושר עד",
    chain_id       = "7290103152017",
    engine         = "ftp",
    ftp_username   = "osherad"
  ),
  RAMI_LEVY = list(
    display_name   = "רמי לוי",
    chain_id       = "7290058140886",
    engine         = "ftp",
    ftp_username   = "RamiLevi"
  ),
  SHUFERSAL = list(
    display_name     = "שופרסל דיל",
    chain_id         = "7290027600007",
    engine           = "shufersal",
    subchain_filter  = "שופרסל דיל"
  ),
  YAYNO_BITAN_AND_CARREFOUR = list(
    display_name     = "קרפור",
    chain_id         = "7290055700007",
    engine           = "carrefour",
    subchain_filter  = "קרפור"
  )
)

# ── FTP helpers ───────────────────────────────────────────────────────────────

ftp_list_files <- function(username) {
  url <- paste0("ftp://", FTP_HOST, "/")
  h   <- new_handle(username = username, password = "")
  res <- curl_fetch_memory(url, handle = h)
  strsplit(rawToChar(res$content), "\r?\n")[[1]]
}

ftp_download <- function(username, filename, dest) {
  url <- paste0("ftp://", FTP_HOST, "/", filename)
  h   <- new_handle(username = username, password = "")
  curl_download(url, dest, handle = h)
}

# ── Shufersal HTTP helper ─────────────────────────────────────────────────────

shufersal_list_files <- function(cat_id = "5") {
  base_url <- paste0(
    "https://prices.shufersal.co.il/FileObject/UpdateCategory?catID=", cat_id
  )

  get_page <- function(page_num) {
    url  <- paste0(base_url, "&page=", page_num)
    page <- read_html(GET(url))
    links <- page %>%
      html_nodes("a[href*='File/Get']") %>%
      html_attr("href")
    links
  }

  # Find total pages
  first_page <- read_html(GET(base_url))
  page_links <- first_page %>%
    html_nodes("#gridContainer tfoot a") %>%
    html_attr("href")
  page_nums <- as.integer(str_extract(page_links, "page=(\\d+)", group = 1))
  total_pages <- if (length(page_nums) > 0) max(page_nums, na.rm = TRUE) else 1

  all_links <- character(0)
  for (p in seq_len(total_pages)) {
    all_links <- c(all_links, get_page(p))
    Sys.sleep(0.3)
  }
  unique(all_links)
}

# ── Carrefour HTTP helper ─────────────────────────────────────────────────────

carrefour_list_files <- function() {
  page <- content(GET("https://prices.carrefour.co.il/"), "text", encoding = "UTF-8")

  path  <- str_match(page, "const path = ['\"]([^'\"]+)['\"]")[, 2]
  files_json <- str_match(page, "const files = (\\[.*?\\]);")[, 2]

  if (is.na(path) || is.na(files_json)) stop("Could not parse Carrefour file list")

  files <- fromJSON(files_json)
  paste0("https://prices.carrefour.co.il/", path, "/", files$name)
}

# ── XML parsing ───────────────────────────────────────────────────────────────

read_xml_gz <- function(path) {
  if (grepl("\\.gz$", path)) {
    tmp <- tempfile(fileext = ".xml")
    on.exit(unlink(tmp))
    con_in  <- gzfile(path, "rb")
    raw     <- readBin(con_in, "raw", n = 50e6)
    close(con_in)
    writeBin(raw, tmp)
    path <- tmp
  }
  tryCatch(read_xml(path, encoding = "UTF-8"), error = function(e) {
    tryCatch(read_xml(path, encoding = "Windows-1255"), error = function(e2) NULL)
  })
}

parse_stores_xml <- function(path) {
  doc <- read_xml_gz(path)
  if (is.null(doc)) return(data.frame())

  chain_id <- xml_text(xml_find_first(doc, "//ChainId|//ChainID"))
  stores   <- xml_find_all(doc, "//Store")

  data.frame(
    chain_id   = chain_id,
    store_id   = xml_text(xml_find_first(stores, ".//StoreId|.//StoreID")),
    store_name = xml_text(xml_find_first(stores, ".//StoreName")),
    subchain   = xml_text(xml_find_first(stores, ".//SubChainName")),
    city       = xml_text(xml_find_first(stores, ".//City")),
    stringsAsFactors = FALSE
  )
}

# ── Per-chain downloaders ─────────────────────────────────────────────────────

fetch_stores_ftp <- function(chain_key, cfg) {
  cat("  FTP listing for", chain_key, "...\n")
  files <- tryCatch(ftp_list_files(cfg$ftp_username), error = function(e) {
    message("  FTP error: ", e$message); character(0)
  })

  store_files <- grep("^Stores", files, value = TRUE, ignore.case = TRUE)
  if (length(store_files) == 0) { warning("No store files for ", chain_key); return(NULL) }

  # Take the most recent one
  f    <- tail(sort(store_files), 1)
  dest <- file.path(TMP_DIR, f)
  cat("  Downloading", f, "...\n")
  tryCatch(ftp_download(cfg$ftp_username, f, dest), error = function(e) {
    message("  Download error: ", e$message); return(NULL)
  })
  dest
}

fetch_stores_shufersal <- function() {
  cat("  Fetching Shufersal store file list...\n")
  links <- tryCatch(shufersal_list_files("5"), error = function(e) {
    message("  Error: ", e$message); character(0)
  })

  store_links <- grep("Stores", links, value = TRUE, ignore.case = TRUE)
  if (length(store_links) == 0) { warning("No Shufersal store files found"); return(NULL) }

  url  <- tail(sort(store_links), 1)
  dest <- file.path(TMP_DIR, basename(url))
  cat("  Downloading", basename(url), "...\n")
  tryCatch(curl_download(url, dest), error = function(e) {
    message("  Download error: ", e$message); return(NULL)
  })
  dest
}

fetch_stores_carrefour <- function() {
  cat("  Fetching Carrefour store file list...\n")
  links <- tryCatch(carrefour_list_files(), error = function(e) {
    message("  Error: ", e$message); character(0)
  })

  store_links <- grep("Stores", links, value = TRUE, ignore.case = TRUE)
  if (length(store_links) == 0) { warning("No Carrefour store files found"); return(NULL) }

  url  <- tail(sort(store_links), 1)
  dest <- file.path(TMP_DIR, basename(url))
  cat("  Downloading", basename(url), "...\n")
  tryCatch(curl_download(url, dest), error = function(e) {
    message("  Download error: ", e$message); return(NULL)
  })
  dest
}

# ── Main ──────────────────────────────────────────────────────────────────────

main <- function() {
  branches <- setNames(
    lapply(names(CHAINS), function(x) list()),
    names(CHAINS)
  )

  for (chain_key in names(CHAINS)) {
    cfg <- CHAINS[[chain_key]]
    cat("\n[", chain_key, "]\n")

    xml_path <- switch(cfg$engine,
      ftp        = fetch_stores_ftp(chain_key, cfg),
      shufersal  = fetch_stores_shufersal(),
      carrefour  = fetch_stores_carrefour()
    )

    if (is.null(xml_path) || !file.exists(xml_path)) next

    cat("  Parsing XML...\n")
    stores <- parse_stores_xml(xml_path)
    if (nrow(stores) == 0) next

    # Filter by target cities
    city_match <- stores$city %in% TARGET_CITIES
    # Filter by subchain if specified
    if (!is.null(cfg$subchain_filter)) {
      city_match <- city_match & grepl(cfg$subchain_filter, stores$subchain, fixed = TRUE)
    }

    matched <- stores[city_match, ]

    branches[[chain_key]] <- lapply(seq_len(nrow(matched)), function(i) {
      list(
        store_id   = matched$store_id[i],
        store_name = matched$store_name[i],
        subchain   = matched$subchain[i],
        city       = matched$city[i]
      )
    })

    cat("  Found", nrow(matched), "matching branches:\n")
    for (b in branches[[chain_key]]) {
      cat("    [", b$store_id, "]", b$store_name, "-", b$city, "\n")
    }
  }

  write(toJSON(branches, pretty = TRUE, auto_unbox = TRUE, ensure_ascii = FALSE),
        OUTPUT_FILE)
  cat("\nSaved branches to", OUTPUT_FILE, "\n")
}

main()
