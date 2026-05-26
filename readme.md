# FinDataScrap — Financial Data Scraper

Daily scraping pipeline that collects financial market data (futures curves, ETF ratios, govies, swaps, FX rates, technicals…) and saves it to a MySQL database.

Runs on **PythonAnywhere** on a daily schedule. Can also be run locally for testing.

> **Local folder:** `Finance/FinDashboard/ImportData/GCP/`
> **On PythonAnywhere:** `~/FinDataScrap/`

---

## Folder structure

```
GCP/
├── main.py              # Entry point — orchestrates all scrapers
├── import_common.py     # Shared date helpers (tod, last_bd, need_reimport)
├── common/              # Shared definitions synced from Finance/common/ — see Deployment
├── imports/             # One file per data source (import_CMEfuts.py, import_yahoo.py, …)
├── databases/           # DB read/write helpers (database_mysql.py, classes.py, …)
├── utils/               # Logging, email, environment detection (isLocal())
└── email_report.py      # Sends summary email after each run
```

---

## Running

**Locally:**
```bash
cd "d:\OneDrive\Python Scripts\Finance\FinDashboard\ImportData\GCP"
python main.py
```

**On PythonAnywhere** — triggered by a scheduled task. To run manually via the PA console:
```bash
cd ~/FinDataScrap
python main.py
```

---

## Deployment to PythonAnywhere

When you change files in this folder, upload them to the server. The easiest way is via the PA **Files** tab or by running `rsync` from a local terminal that has SSH access:

```bash
rsync -av "d:/OneDrive/Python Scripts/Finance/FinDashboard/ImportData/GCP/" \
  <pa-user>@ssh.pythonanywhere.com:~/FinDataScrap/
```

### Deploying the shared `common/` package

`Finance/common/` is a shared package used by scripts here. It is **not** auto-synced — deploy it separately whenever it changes:

```bash
rsync -av "d:/OneDrive/Python Scripts/Finance/common/" \
  <pa-user>@ssh.pythonanywhere.com:~/FinDataScrap/common/
```

See also: [`Finance/common/README.md`](../../../../common/README.md)

---

## Adding a new scraper

1. Create `imports/import_<name>.py` — expose a `Scrap` instance (see `databases/classes.py`)
2. Import and add it to `lstScrap` in `main.py`
3. Deploy the new file to `~/FinDataScrap/imports/` on PA
