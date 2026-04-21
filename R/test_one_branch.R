library(curl)
library(xml2)
library(dplyr)

# ── הורדת רשימת קבצים מ-FTP ──────────────────────────────────
h <- new_handle(username = "RamiLevi", password = "")
handle_setopt(h, dirlistonly = TRUE)
res <- curl_fetch_memory("ftp://url.retail.publishedprices.co.il/", handle = h)
files <- strsplit(rawToChar(res$content), "\r?\n")[[1]]
files <- trimws(files[nchar(trimws(files)) > 0])
message("סה\"כ קבצים: ", length(files))

# ── בחר את קובץ PriceFull הראשון הזמין ───────────────────────
price_file <- tail(sort(grep("PriceFull", files, value = TRUE, ignore.case = TRUE)), 1)
message("מוריד: ", price_file)

# ── הורדה לזיכרון → קובץ זמני ────────────────────────────────
h2  <- new_handle(username = "RamiLevi", password = "")
raw <- curl_fetch_memory(paste0("ftp://url.retail.publishedprices.co.il/", price_file), handle = h2)
tmp <- tempfile(fileext = ".gz")
writeBin(raw$content, tmp)

# ── פריסת XML ─────────────────────────────────────────────────
xml_tmp <- tempfile(fileext = ".xml")
con <- gzfile(tmp, "rb")
writeBin(readBin(con, "raw", n = 100e6), xml_tmp)
close(con)

doc   <- read_xml(xml_tmp)
items <- xml_find_all(doc, "//Item")
message("מוצרים: ", length(items))

# ── דאטאפריים ─────────────────────────────────────────────────
prices <- data.frame(
  barcode = xml_text(xml_find_first(items, ".//ItemCode")),
  name    = xml_text(xml_find_first(items, ".//ItemName")),
  price   = as.numeric(xml_text(xml_find_first(items, ".//ItemPrice"))),
  stringsAsFactors = FALSE
)

head(prices, 10)
