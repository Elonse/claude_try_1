# ================================================================
# run_everything.R
#
# הרץ כ-Source ב-RStudio (כפתור Source למעלה מימין).
# עושה הכל: מתקין חבילות, מוריד נתונים, מייצא Excel+HTML,
# ודוחף את התוצאות לגיטהאב.
#
# דרישה: מחשב בישראל + RStudio + Python לא נדרש.
# ================================================================

# ── 0. קונפיגורציה ────────────────────────────────────────────
GITHUB_USER <- "Elonse"
REPO_NAME   <- "claude_try_1"
REPO_URL    <- paste0("https://github.com/", GITHUB_USER, "/", REPO_NAME, ".git")
REPO_DIR    <- if (.Platform$OS.type == "windows") {
  file.path(Sys.getenv("USERPROFILE"), "Desktop", REPO_NAME)
} else {
  file.path(path.expand("~"), "Desktop", REPO_NAME)
}
RUN_DATE    <- format(Sys.Date() - 1, "%Y-%m-%d")   # אתמול

# ── 1. חבילות ─────────────────────────────────────────────────
needed <- c("httr", "curl", "xml2", "jsonlite", "rvest",
            "stringr", "dplyr", "readr", "openxlsx", "git2r")
to_install <- needed[!sapply(needed, requireNamespace, quietly = TRUE)]
if (length(to_install) > 0) {
  message("Installing: ", paste(to_install, collapse = ", "))
  install.packages(to_install, quiet = TRUE)
}
invisible(lapply(needed, library, character.only = TRUE))

# ── 2. Clone / Pull ───────────────────────────────────────────
PAT <- rstudioapi::askForPassword(
  "הכנס את ה-GitHub Personal Access Token שלך (לא יוצג על המסך)"
)
auth_url <- sub("https://", paste0("https://", GITHUB_USER, ":", PAT, "@"),
                REPO_URL)

if (!dir.exists(REPO_DIR)) {
  message("Cloning repository to ", REPO_DIR, " ...")
  git2r::clone(auth_url, REPO_DIR)
} else {
  message("Pulling latest changes...")
  repo <- git2r::repository(REPO_DIR)
  tryCatch(git2r::pull(repo), error = function(e) message("Pull skipped: ", e$message))
}

setwd(REPO_DIR)
CONFIG_FILE       <- file.path(REPO_DIR, "config", "chains.json")
BRANCHES_FILE     <- file.path(REPO_DIR, "config", "branches.json")
STATE_BASKET_FILE <- file.path(REPO_DIR, "config", "baskets", "state_basket.json")
DATA_DIR          <- file.path(REPO_DIR, "data")
EXCEL_OUT_DIR     <- file.path(REPO_DIR, "outputs", "excel")
HTML_OUT_DIR      <- file.path(REPO_DIR, "outputs", "html")
TMP_DIR           <- tempdir()

config        <- fromJSON(CONFIG_FILE)
TARGET_CITIES <- config$target_cities
FTP_HOST      <- "url.retail.publishedprices.co.il"

CHAIN_META <- list(
  OSHER_AD = list(
    display_name = "אושר עד",  chain_id = "7290103152017",
    engine = "ftp", ftp_username = "osherad"
  ),
  RAMI_LEVY = list(
    display_name = "רמי לוי",   chain_id = "7290058140886",
    engine = "ftp", ftp_username = "RamiLevi"
  ),
  SHUFERSAL = list(
    display_name = "שופרסל דיל", chain_id = "7290027600007",
    engine = "shufersal", subchain_filter = "שופרסל דיל"
  ),
  YAYNO_BITAN_AND_CARREFOUR = list(
    display_name = "קרפור",     chain_id = "7290055700007",
    engine = "carrefour", subchain_filter = "קרפור"
  )
)

# ── 3. פונקציות עזר ──────────────────────────────────────────

## FTP
ftp_list_files <- function(username) {
  h   <- new_handle(username = username, password = "")
  res <- curl_fetch_memory(paste0("ftp://", FTP_HOST, "/"), handle = h)
  strsplit(rawToChar(res$content), "\r?\n")[[1]]
}
ftp_download <- function(username, filename, dest) {
  h <- new_handle(username = username, password = "")
  curl_download(paste0("ftp://", FTP_HOST, "/", filename), dest, handle = h)
}

## Shufersal HTTP
shufersal_list_files <- function(cat_id) {
  base <- paste0("https://prices.shufersal.co.il/FileObject/UpdateCategory?catID=", cat_id)
  get_links <- function(p) {
    read_html(GET(paste0(base, "&page=", p))) %>%
      html_nodes("a[href*='File/Get']") %>% html_attr("href")
  }
  pnums <- read_html(GET(base)) %>%
    html_nodes("#gridContainer tfoot a") %>% html_attr("href") %>%
    str_extract("page=(\\d+)", group = 1) %>% as.integer()
  total <- if (length(pnums) > 0) max(pnums, na.rm = TRUE) else 1
  links <- character(0)
  for (p in seq_len(total)) { links <- c(links, get_links(p)); Sys.sleep(0.3) }
  unique(links)
}

## Carrefour HTTP
carrefour_list_files <- function() {
  page  <- content(GET("https://prices.carrefour.co.il/"), "text", encoding = "UTF-8")
  path  <- str_match(page, "const path = ['\"]([^'\"]+)['\"]")[, 2]
  fjson <- str_match(page, "const files = (\\[.*?\\]);")[, 2]
  if (is.na(path) || is.na(fjson)) stop("Could not parse Carrefour file list")
  files <- fromJSON(fjson)
  paste0("https://prices.carrefour.co.il/", path, "/", files$name)
}

## XML parsing
read_xml_gz <- function(path) {
  if (grepl("\\.gz$", path)) {
    tmp <- tempfile(fileext = ".xml")
    con <- gzfile(path, "rb"); writeBin(readBin(con, "raw", n = 100e6), tmp); close(con)
    path <- tmp
  }
  tryCatch(read_xml(path, encoding = "UTF-8"),
           error = function(e) tryCatch(read_xml(path, encoding = "Windows-1255"),
                                        error = function(e2) NULL))
}

parse_stores_xml <- function(path) {
  doc <- read_xml_gz(path); if (is.null(doc)) return(data.frame())
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

parse_price_xml <- function(path) {
  doc <- read_xml_gz(path); if (is.null(doc)) return(NULL)
  chain_id <- xml_text(xml_find_first(doc, "//ChainId|//ChainID"))
  store_id <- xml_text(xml_find_first(doc, "//StoreId|//StoreID"))
  items    <- xml_find_all(doc, "//Item")
  if (length(items) == 0) return(NULL)
  list(
    chain_id = chain_id, store_id = store_id,
    products = data.frame(
      barcode           = xml_text(xml_find_first(items, ".//ItemCode")),
      name              = xml_text(xml_find_first(items, ".//ItemName")),
      price             = suppressWarnings(as.numeric(xml_text(xml_find_first(items, ".//ItemPrice")))),
      unit              = xml_text(xml_find_first(items, ".//UnitOfMeasure")),
      price_update_date = xml_text(xml_find_first(items, ".//PriceUpdateDate")),
      stringsAsFactors  = FALSE
    )
  )
}

filter_by_date  <- function(files, d) grep(format(as.Date(d), "%Y%m%d"), files, value = TRUE)
filter_by_store <- function(files, ids) grep(paste(ids, collapse = "|"), files, value = TRUE)

# ── 4. fetch_stores (רק אם branches.json חסר / ריק) ──────────
branches_empty <- function() {
  if (!file.exists(BRANCHES_FILE)) return(TRUE)
  b <- fromJSON(BRANCHES_FILE, simplifyVector = FALSE)
  all(sapply(b, length) == 0)
}

if (branches_empty()) {
  message("\n=== שלב א: הורדת רשימת סניפים ===")
  branches <- setNames(lapply(names(CHAIN_META), function(x) list()), names(CHAIN_META))

  for (chain_key in names(CHAIN_META)) {
    cfg <- CHAIN_META[[chain_key]]
    message("[", chain_key, "]")

    dl_store <- function(url_or_fname, username = NULL) {
      dest <- file.path(TMP_DIR, basename(url_or_fname))
      if (!is.null(username))
        tryCatch(ftp_download(username, url_or_fname, dest), error = function(e) { message(e$message); return(NULL) })
      else
        tryCatch(curl_download(url_or_fname, dest), error = function(e) { message(e$message); return(NULL) })
      dest
    }

    xml_path <- switch(cfg$engine,
      ftp = {
        files <- tryCatch(ftp_list_files(cfg$ftp_username), error = function(e) character(0))
        f     <- tail(sort(grep("^Stores", files, value = TRUE, ignore.case = TRUE)), 1)
        if (length(f) == 0) NULL else dl_store(f, cfg$ftp_username)
      },
      shufersal = {
        links <- tryCatch(shufersal_list_files("5"), error = function(e) character(0))
        f     <- tail(sort(grep("Stores", links, value = TRUE, ignore.case = TRUE)), 1)
        if (length(f) == 0) NULL else dl_store(f)
      },
      carrefour = {
        links <- tryCatch(carrefour_list_files(), error = function(e) character(0))
        f     <- tail(sort(grep("Stores", links, value = TRUE, ignore.case = TRUE)), 1)
        if (length(f) == 0) NULL else dl_store(f)
      }
    )

    if (is.null(xml_path) || !file.exists(xml_path)) next
    stores <- parse_stores_xml(xml_path)
    if (nrow(stores) == 0) next

    mask <- stores$city %in% TARGET_CITIES
    if (!is.null(cfg$subchain_filter))
      mask <- mask & grepl(cfg$subchain_filter, stores$subchain, fixed = TRUE)

    matched <- stores[mask, ]
    branches[[chain_key]] <- lapply(seq_len(nrow(matched)), function(i)
      list(store_id = matched$store_id[i], store_name = matched$store_name[i],
           subchain = matched$subchain[i], city = matched$city[i]))

    message("  נמצאו ", nrow(matched), " סניפים:")
    for (b in branches[[chain_key]])
      message("    [", b$store_id, "] ", b$store_name, " — ", b$city)
  }

  write(toJSON(branches, pretty = TRUE, auto_unbox = TRUE, ensure_ascii = FALSE),
        BRANCHES_FILE)
  message("נשמר: config/branches.json")
}

# ── 5. הורדת מחירים ──────────────────────────────────────────
message("\n=== שלב ב: הורדת מחירים ל-", RUN_DATE, " ===")
branches    <- fromJSON(BRANCHES_FILE, simplifyVector = FALSE)
all_xml     <- character(0)

for (chain_key in names(CHAIN_META)) {
  cfg      <- CHAIN_META[[chain_key]]
  store_ids <- sapply(branches[[chain_key]], `[[`, "store_id")
  if (length(store_ids) == 0) next
  message("[", chain_key, "]")

  paths <- switch(cfg$engine,
    ftp = {
      files <- tryCatch(ftp_list_files(cfg$ftp_username), error = function(e) character(0))
      files <- filter_by_store(filter_by_date(
        grep("^PriceFull", files, value = TRUE, ignore.case = TRUE), RUN_DATE), store_ids)
      vapply(files, function(f) {
        dest <- file.path(TMP_DIR, f)
        tryCatch({ ftp_download(cfg$ftp_username, f, dest); dest },
                 error = function(e) { message(e$message); "" })
      }, character(1))
    },
    shufersal = {
      links <- tryCatch(shufersal_list_files("2"), error = function(e) character(0))
      links <- filter_by_store(filter_by_date(links, RUN_DATE), store_ids)
      vapply(links, function(u) {
        dest <- file.path(TMP_DIR, basename(u))
        tryCatch({ curl_download(u, dest); dest },
                 error = function(e) { message(e$message); "" })
      }, character(1))
    },
    carrefour = {
      links <- tryCatch(carrefour_list_files(), error = function(e) character(0))
      links <- filter_by_store(filter_by_date(links, RUN_DATE), store_ids)
      vapply(links, function(u) {
        dest <- file.path(TMP_DIR, basename(u))
        tryCatch({ curl_download(u, dest); dest },
                 error = function(e) { message(e$message); "" })
      }, character(1))
    }
  )
  all_xml <- c(all_xml, paths[nchar(paths) > 0])
}

# ── 6. עיבוד ─────────────────────────────────────────────────
message("\n=== שלב ג: עיבוד ", length(all_xml), " קבצים ===")
dir.create(file.path(DATA_DIR, RUN_DATE), showWarnings = FALSE, recursive = TRUE)
all_data <- list()

for (path in all_xml) {
  res <- parse_price_xml(path); if (is.null(res)) next
  ck  <- names(Filter(function(m) m$chain_id == res$chain_id, CHAIN_META))[1]
  if (is.na(ck)) next
  bi  <- Filter(function(b) b$store_id == res$store_id, branches[[ck]])
  if (length(bi) == 0) next; bi <- bi[[1]]

  df <- res$products
  df$chain <- CHAIN_META[[ck]]$display_name; df$chain_key <- ck
  df$store_id <- res$store_id; df$store_name <- bi$store_name
  df$city <- bi$city; df$date <- RUN_DATE

  fname <- paste0(ck, "_", res$store_id, "_", gsub(" ", "_", bi$city), ".csv")
  write_csv(df, file.path(DATA_DIR, RUN_DATE, fname))
  message("  ", fname, " (", nrow(df), " מוצרים)")
  all_data[[fname]] <- df
}

if (length(all_data) == 0) stop("לא עובד נתונים — בדוק חיבור אינטרנט ורשימת סניפים")
all_df <- bind_rows(all_data)

# ── 7. חישוב סל המדינה ───────────────────────────────────────
basket      <- fromJSON(STATE_BASKET_FILE)
barcodes    <- as.character(sapply(basket$items, `[[`, "barcode"))
basket_results <- if (length(barcodes) > 0) {
  all_df %>%
    filter(barcode %in% barcodes) %>%
    group_by(chain, store_id, store_name, city) %>%
    summarise(items_found = n_distinct(barcode),
              total_price = sum(price, na.rm = TRUE), .groups = "drop") %>%
    mutate(coverage = paste0(round(items_found / length(barcodes) * 100), "%")) %>%
    arrange(total_price)
} else { message("סל המדינה ריק — דלג על חישוב"); NULL }

# ── 8. יצוא Excel ────────────────────────────────────────────
message("\n=== שלב ד: יצוא Excel ===")
excel_dir <- file.path(EXCEL_OUT_DIR, RUN_DATE)
dir.create(excel_dir, showWarnings = FALSE, recursive = TRUE)

for (key in unique(paste(all_df$chain_key, all_df$store_id, sep = "||"))) {
  parts <- strsplit(key, "\\|\\|")[[1]]
  sub   <- all_df %>% filter(chain_key == parts[1], store_id == parts[2])
  fname <- paste0("branch_", parts[1], "_", parts[2], "_",
                  gsub(" ", "_", sub$city[1]), ".xlsx")
  write.xlsx(sub[, c("barcode","name","price","unit","price_update_date")],
             file.path(excel_dir, fname), rowNames = FALSE)
  message("  ", fname)
}

if (!is.null(basket_results)) {
  write.xlsx(as.data.frame(basket_results),
             file.path(excel_dir, "basket_comparison.xlsx"), rowNames = FALSE)
  message("  basket_comparison.xlsx")
}

# ── 9. יצוא HTML ─────────────────────────────────────────────
message("\n=== שלב ה: יצוא HTML ===")
html_dir <- file.path(HTML_OUT_DIR, RUN_DATE)
dir.create(html_dir, showWarnings = FALSE, recursive = TRUE)

if (!is.null(basket_results)) {
  rows <- paste(apply(basket_results %>% arrange(city, total_price), 1, function(r)
    sprintf("<tr><td>%s</td><td>%s</td><td>%s</td><td class='p'>₪%.2f</td><td>%s</td></tr>",
            r["city"], r["chain"], r["store_name"],
            as.numeric(r["total_price"]), r["coverage"])), collapse = "\n")

  writeLines(sprintf(
    '<!DOCTYPE html><html dir="rtl" lang="he"><head><meta charset="UTF-8">
<title>השוואת מחירים %s</title>
<style>body{font-family:Arial;max-width:900px;margin:40px auto}
table{width:100%%;border-collapse:collapse}
th{background:#2c3e50;color:white;padding:10px;text-align:right}
td{padding:8px;border-bottom:1px solid #eee}.p{font-weight:bold;color:#27ae60}</style>
</head><body><h1>השוואת מחירים — סל המדינה</h1><p>%s</p>
<table><thead><tr><th>עיר</th><th>רשת</th><th>סניף</th><th>מחיר סל</th><th>כיסוי</th></tr></thead>
<tbody>%s</tbody></table></body></html>', RUN_DATE, RUN_DATE, rows),
    file.path(html_dir, "index.html"))
  message("  index.html")
}

# ── 10. Git commit + push ─────────────────────────────────────
message("\n=== שלב ו: שמירה לגיטהאב ===")
repo <- git2r::repository(REPO_DIR)
git2r::add(repo, c("data", "outputs", "config/branches.json"))
tryCatch({
  git2r::commit(repo, message = paste("Price data:", RUN_DATE),
                author    = git2r::signature("Claude", "noreply@anthropic.com"),
                committer = git2r::signature("Claude", "noreply@anthropic.com"))
  creds <- git2r::cred_user_pass(GITHUB_USER, PAT)
  git2r::push(repo, credentials = creds)
  message("נדחף לגיטהאב בהצלחה!")
}, error = function(e) message("שגיאת Git: ", e$message))

message("\n✓ הכל הסתיים! קבצי Excel ב: ", excel_dir)
