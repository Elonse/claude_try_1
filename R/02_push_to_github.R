# ================================================================
# 02_push_to_github.R
#
# שלב 2: דחיפת הנתונים שהורדו ב-01_download_data.R לגיטהאב.
# הרץ רק אחרי שסקריפט 1 הסתיים בהצלחה.
#
# הרץ ב-RStudio: לחץ Source (Ctrl+Shift+S)
# ================================================================

# ── 0. הגדרות — שנה כאן בלבד ────────────────────────────────

GITHUB_USER <- "Elonse"
REPO_NAME   <- "claude_try_1"
REPO_DIR    <- if (.Platform$OS.type == "windows") {
  file.path(Sys.getenv("USERPROFILE"), "Desktop", "claude_try_1")
} else {
  file.path(path.expand("~"), "Desktop", "claude_try_1")
}
RUN_DATE <- format(Sys.Date() - 1, "%Y-%m-%d")   # חייב להתאים לסקריפט 1

# ── 1. חבילות ────────────────────────────────────────────────

if (!requireNamespace("git2r", quietly = TRUE))
  install.packages("git2r", quiet = TRUE)
library(git2r)

# ── 2. PAT ───────────────────────────────────────────────────

PAT <- if (requireNamespace("rstudioapi", quietly = TRUE) &&
           rstudioapi::isAvailable()) {
  rstudioapi::askForPassword("הכנס GitHub Personal Access Token")
} else {
  readline("הכנס GitHub Personal Access Token: ")
}

if (nchar(trimws(PAT)) == 0) stop("לא הוזן PAT.")

# ── 3. וידוא שהריפו קיים ─────────────────────────────────────

if (!dir.exists(REPO_DIR))
  stop("תיקיית הריפו לא נמצאה: ", REPO_DIR)

repo <- tryCatch(
  git2r::repository(REPO_DIR),
  error = function(e) stop("לא ניתן לפתוח ריפו Git ב-", REPO_DIR, "\n", e$message)
)

# ── 4. Pull לפני ה-commit ────────────────────────────────────

message("מושך שינויים מ-GitHub...")
auth_url <- paste0("https://", GITHUB_USER, ":", PAT,
                   "@github.com/", GITHUB_USER, "/", REPO_NAME, ".git")
tryCatch(
  git2r::pull(repo, credentials = git2r::cred_user_pass(GITHUB_USER, PAT)),
  error = function(e) message("Pull נכשל (ממשיך): ", e$message)
)

# ── 5. הוספת קבצים ───────────────────────────────────────────

message("מוסיף קבצים...")

# רשימת כל הקבצים שנוצרו
paths_to_add <- c(
  file.path("data",    RUN_DATE),
  file.path("outputs", "excel", RUN_DATE),
  file.path("outputs", "html",  RUN_DATE),
  file.path("config",  "branches.json")
)

# git2r::add מקבל נתיבים יחסיים לשורש הריפו
existing <- paths_to_add[dir.exists(file.path(REPO_DIR, paths_to_add)) |
                          file.exists(file.path(REPO_DIR, paths_to_add))]

if (length(existing) == 0) {
  stop("לא נמצאו קבצי פלט. וודא שהרצת קודם את 01_download_data.R")
}

tryCatch(
  git2r::add(repo, path = existing),
  error = function(e) {
    message("git2r::add נכשל, מנסה git מערכתי...")
    for (p in existing) {
      system2("git", c("-C", shQuote(REPO_DIR), "add", shQuote(p)))
    }
  }
)

# בדוק אם יש שינויים staged
status <- git2r::status(repo)
n_staged <- length(status$staged)

if (n_staged == 0) {
  message("אין שינויים חדשים — הכל כבר עדכני בגיטהאב.")
  message("סיום.")
  stop("Nothing to commit.", call. = FALSE)
}

message("  ", n_staged, " קבצים/שינויים ל-commit")

# ── 6. Commit ────────────────────────────────────────────────

commit_msg <- paste("Price data:", RUN_DATE)
message("מבצע commit: ", commit_msg)

tryCatch(
  git2r::commit(repo,
    message   = commit_msg,
    author    = git2r::signature(name  = GITHUB_USER,
                                  email = paste0(GITHUB_USER, "@users.noreply.github.com")),
    committer = git2r::signature(name  = GITHUB_USER,
                                  email = paste0(GITHUB_USER, "@users.noreply.github.com"))
  ),
  error = function(e) stop("Commit נכשל: ", e$message)
)

# ── 7. Push ──────────────────────────────────────────────────

message("דוחף לגיטהאב...")
creds <- git2r::cred_user_pass(GITHUB_USER, PAT)

push_ok <- tryCatch({
  git2r::push(repo, credentials = creds)
  TRUE
}, error = function(e) {
  message("git2r::push נכשל, מנסה git מערכתי...")
  # שיטה חלופית: embed PAT ב-URL
  ret <- system2("git", c("-C", shQuote(REPO_DIR), "push",
                           shQuote(auth_url), "main"),
                 stdout = TRUE, stderr = TRUE)
  if (!is.null(attr(ret, "status")) && attr(ret, "status") != 0) {
    message(paste(ret, collapse = "\n"))
    FALSE
  } else TRUE
})

# ── סיום ─────────────────────────────────────────────────────

if (isTRUE(push_ok)) {
  message("\n", strrep("=", 50))
  message("נדחף לגיטהאב בהצלחה!")
  message("https://github.com/", GITHUB_USER, "/", REPO_NAME)
  message(strrep("=", 50))
} else {
  warning("הדחיפה נכשלה. בדוק את ה-PAT ואת החיבור לאינטרנט.")
}
