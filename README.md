# RetailPulse — Final Submission (Real Data + PySpark RDD)

## What this project does
RetailPulse loads the genuine UCI Online Retail transaction dataset, validates the eight original columns, audits data quality, cleans positive sales lines with Apache Spark RDD operations, calculates business KPIs, exports analysis files, and provides a Streamlit dashboard.

## Dataset (real public data)
- Name: Online Retail
- Provider: UCI Machine Learning Repository, dataset 352
- Official page: https://archive.ics.uci.edu/dataset/352/online-retail
- DOI: https://doi.org/10.24432/C5BW33
- Citation: Chen, D. (2015). *Online Retail* [Dataset]. UCI Machine Learning Repository.
- License: CC BY 4.0
- Period: 1 Dec 2010 to 9 Dec 2011; UCI reports 541,909 instances.
- Currency: GBP

The original workbook is intentionally not bundled. The notebook downloads the official UCI archive, or prompts for the original `Online Retail.xlsx` if download fails. Never substitute an unrelated six-column file.

## Files
- `RetailPulse_RealData_PySpark_RDD.ipynb` — complete Colab notebook, from download through cleaning, RDD methods, KPIs, exports and website launch.
- `app.py` — Streamlit dashboard; accepts the original UCI Excel workbook or its CSV export and processes records with PySpark RDD.
- `requirements.txt` — dependencies.
- `DATA_SOURCE.txt` — provenance, license, citation and limitations.
- `data_dictionary.csv` — source and derived field descriptions.
- `PROJECT_REPORT.md` — submission-ready project report outline.

## Run in Google Colab
1. Download this ZIP and extract it on your computer.
2. Open Google Colab: https://colab.research.google.com/.
3. Upload `RetailPulse_RealData_PySpark_RDD.ipynb` and run cells from top to bottom.
4. The notebook attempts to download the original UCI workbook. If that fails, upload `Online Retail.xlsx` from the official UCI page when prompted.
5. For the website, upload `app.py` into the Colab Files panel, then run the final notebook cell. Open the temporary URL and upload the original workbook in the website.

## Run locally
Install Java compatible with your PySpark version, then:
```bash
pip install -r requirements.txt
streamlit run app.py
```
Upload the original UCI workbook or its CSV export.

## Cleaning and business rules
1. Required fields: InvoiceNo, StockCode, Quantity, InvoiceDate, UnitPrice.
2. Exclude cancellation invoice numbers beginning with `C` and non-positive quantities/prices from positive-sales KPIs.
3. Missing customer IDs are retained as `Anonymous` for total sales, but excluded from customer-level metrics.
4. Remove exact duplicate full transaction lines only. An invoice number is not unique per product line.
5. Calculate line revenue = Quantity × UnitPrice in GBP.

## RDD concepts demonstrated
`map`, `filter`, `flatMap`, `distinct`, `reduceByKey`, `values`, `take`, `count`, `collect`, and `cache`.

## Important limitations
This is genuine historical data from one UK retailer, not live sales, not Pakistani sales, and not representative of all retailers. Revenue is GBP. Returns/cancellations are excluded from positive-sales measures; this is a defined reporting choice. Spark uses `local[*]` in Colab/local mode: genuine RDD processing, but not a multi-node cluster. Colab tunnels are temporary; do not expose confidential company data through a public tunnel.
