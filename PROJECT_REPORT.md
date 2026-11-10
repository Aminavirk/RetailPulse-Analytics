# RetailPulse: Real-World Retail Analytics Using Apache PySpark RDD

## Abstract
RetailPulse is a reproducible retail analytics project built with Apache PySpark RDD and Streamlit. It uses the public UCI Online Retail dataset, validates the original transaction schema, audits data quality, cleans positive sale lines, computes revenue and business KPIs, and presents interactive analysis.

## Dataset
The project uses UCI Online Retail (Chen, 2015), dataset ID 352, DOI https://doi.org/10.24432/C5BW33, licensed CC BY 4.0. The records cover one UK online retailer from December 2010 through December 2011. The dataset is historical and currency is GBP.

## Methodology
The original eight columns are validated before processing. RDD `map` converts source rows into normalized records; `filter` excludes invalid lines, cancellations and returns from positive-sales reporting; exact full-row duplicates are removed without collapsing legitimate multi-product invoices; `reduceByKey` aggregates revenue by country and month. Revenue is quantity multiplied by unit price.

## Analytics
The pipeline reports total sales revenue, invoice count, known customers, products, units sold, monthly revenue, country revenue, product revenue and customer revenue. Anonymous transactions contribute to overall revenue but not identified-customer counts.

## Technology
Python, pandas, Apache Spark RDD, Streamlit and Plotly. In Colab the Spark master is `local[*]`, which executes Spark locally and demonstrates RDD APIs; it is not a distributed multi-node deployment.

## Limitations and ethics
The source is a single retailer's historical data, not current market data or a representative sample of the retail sector. Returns and cancellations are excluded from positive-sales KPIs according to an explicit rule. Do not describe GBP revenue as PKR or imply this is live company data.

## Reproduction
Run the notebook in Google Colab from the first cell. It retrieves the original workbook from UCI, with manual upload fallback. Upload `app.py` into the Colab Files panel before launching the Streamlit tunnel. Refer to README.md for detailed steps.

## Reference
Chen, D. (2015). *Online Retail* [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C5BW33
