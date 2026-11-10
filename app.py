import os, tempfile
import pandas as pd
import plotly.express as px
import streamlit as st
from pyspark.sql import SparkSession

st.set_page_config(page_title="RetailPulse | Retail Analytics", page_icon="📊", layout="wide")
st.title("RetailPulse")
st.caption("Real-world retail transaction analytics powered by Apache PySpark RDD")

@st.cache_resource
def get_spark():
    return (SparkSession.builder.appName("RetailPulse-Dashboard").master("local[*]")
            .config("spark.sql.shuffle.partitions", "8").getOrCreate())
spark = get_spark()

with st.sidebar:
    st.header("Upload retail data")
    upload = st.file_uploader("Upload UCI Online Retail Excel or CSV", type=["xlsx", "xls", "csv"])
    st.caption("Required fields: InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country")
    st.caption("This dashboard currently supports the UCI Online Retail schema, not arbitrary unrelated datasets.")

if upload is None:
    st.info("Upload the original UCI Online Retail.xlsx workbook or its CSV export to view the dashboard.")
    st.stop()

with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(upload.name)[1]) as tmp:
    tmp.write(upload.getvalue()); temp_path = tmp.name
try:
    if upload.name.lower().endswith(".csv"):
        source_df = spark.read.option("header", True).option("inferSchema", True).csv(temp_path)
    else:
        pandas_source = pd.read_excel(temp_path, engine="openpyxl")
        source_df = spark.createDataFrame(pandas_source.astype(object).where(pd.notna(pandas_source), None))
finally:
    try: os.remove(temp_path)
    except OSError: pass

# Normalize incoming headers, preserving original field meanings.
import re
rename_map = {"invoiceno":"InvoiceNo", "stockcode":"StockCode", "description":"Description", "quantity":"Quantity", "invoicedate":"InvoiceDate", "unitprice":"UnitPrice", "customerid":"CustomerID", "country":"Country"}
def norm(s): return re.sub(r"[^a-z0-9]", "", str(s).strip().lower())
actual = {c: rename_map.get(norm(c), c) for c in source_df.columns}
for old, new in actual.items():
    if old != new: source_df = source_df.withColumnRenamed(old, new)
required = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
missing = [c for c in required if c not in source_df.columns]
if missing:
    st.error(f"Missing required UCI columns: {missing}. Please upload the original Online Retail dataset with all eight fields.")
    st.stop()

import pandas as pd
def clean_row(row):
    d = row.asDict(recursive=True)
    try:
        inv, stock = str(d["InvoiceNo"]).strip(), str(d["StockCode"]).strip()
        qty, price = float(d["Quantity"]), float(d["UnitPrice"])
        date = pd.to_datetime(d["InvoiceDate"], errors="coerce")
        if pd.isna(date) or not inv or not stock or inv.upper().startswith("C") or qty <= 0 or price <= 0: return None
        cid = d.get("CustomerID")
        try: cid = "Anonymous" if cid is None or pd.isna(cid) else str(int(float(cid)))
        except (ValueError, TypeError): cid = "Anonymous"
        desc, country = d.get("Description"), d.get("Country")
        return {"InvoiceNo":inv,"StockCode":stock,"Description":str(desc).strip() if desc is not None and not pd.isna(desc) else "Unknown product","Quantity":qty,"InvoiceDate":date.to_pydatetime(),"UnitPrice":price,"CustomerID":cid,"Country":str(country).strip() if country is not None and not pd.isna(country) else "Unknown","Revenue":qty*price}
    except (KeyError, ValueError, TypeError, OverflowError): return None

raw_rdd = source_df.rdd
clean_rdd = raw_rdd.map(clean_row).filter(lambda x: x is not None)
clean_rdd = (clean_rdd.map(lambda r:(tuple(sorted((k,str(v)) for k,v in r.items())),r))
             .reduceByKey(lambda a,b:a).values().cache())
records = clean_rdd.collect()
if not records:
    st.error("No valid sale lines found. Verify the uploaded file and its values.")
    st.stop()
df = pd.DataFrame(records)
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
df["Month"] = df["InvoiceDate"].dt.to_period("M").astype(str)

st.sidebar.divider()
countries = sorted(df.Country.dropna().unique().tolist())
selected = st.sidebar.multiselect("Country filter", countries, default=countries)
min_date, max_date = df.InvoiceDate.min().date(), df.InvoiceDate.max().date()
date_range = st.sidebar.date_input("Invoice date range", value=(min_date,max_date), min_value=min_date, max_value=max_date)
view = df[df.Country.isin(selected)].copy()
if isinstance(date_range, tuple) and len(date_range)==2:
    view = view[view.InvoiceDate.dt.date.between(date_range[0], date_range[1])]
if view.empty:
    st.warning("No transactions match the selected filters.")
    st.stop()

m1,m2,m3,m4 = st.columns(4)
m1.metric("Sales revenue", f"£{view.Revenue.sum():,.2f}")
m2.metric("Orders", f"{view.InvoiceNo.nunique():,}")
m3.metric("Known customers", f"{view.loc[view.CustomerID!='Anonymous','CustomerID'].nunique():,}")
m4.metric("Units sold", f"{view.Quantity.sum():,.0f}")
st.caption(f"Uploaded rows: {source_df.count():,} | Valid unique sale lines: {len(df):,} | Currency: GBP | Cancellations/returns and invalid sale lines excluded")

left,right = st.columns(2)
with left:
    st.subheader("Monthly revenue")
    monthly = view.groupby("Month",as_index=False).Revenue.sum().sort_values("Month")
    st.plotly_chart(px.line(monthly,x="Month",y="Revenue",markers=True),use_container_width=True)
with right:
    st.subheader("Revenue by country")
    country = view.groupby("Country",as_index=False).Revenue.sum().nlargest(15,"Revenue")
    st.plotly_chart(px.bar(country,x="Country",y="Revenue"),use_container_width=True)
left,right = st.columns(2)
with left:
    st.subheader("Top products")
    products = view.groupby("Description",as_index=False).Revenue.sum().nlargest(15,"Revenue")
    st.plotly_chart(px.bar(products,x="Revenue",y="Description",orientation="h"),use_container_width=True)
with right:
    st.subheader("Top known customers")
    cust = view[view.CustomerID!="Anonymous"].groupby("CustomerID",as_index=False).Revenue.sum().nlargest(15,"Revenue")
    st.plotly_chart(px.bar(cust,x="CustomerID",y="Revenue"),use_container_width=True)

st.subheader("Cleaned transaction data")
st.dataframe(view,use_container_width=True,height=350)
st.download_button("Download cleaned transactions CSV",view.to_csv(index=False).encode("utf-8"),"retailpulse_cleaned_transactions.csv","text/csv")
st.download_button("Download monthly summary CSV",monthly.to_csv(index=False).encode("utf-8"),"retailpulse_monthly_sales.csv","text/csv")
with st.expander("RDD audit details"):
    st.write({"Spark version":spark.version,"RDD methods used":["map","filter","reduceByKey","values","cache","collect"],"raw rows":source_df.count(),"clean sale lines":len(df)})
st.caption("Source dataset: UCI Online Retail (Chen, 2015), 2010–2011. Historical single-retailer data; not live market data. Currency GBP.")
