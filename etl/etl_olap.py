import os
import pandas as pd
import boto3
from dotenv import load_dotenv
from sqlalchemy import create_engine

# ------------------------
# Load config from .env
# ------------------------
def load_env():
    from pathlib import Path
    dotenv_path = Path(__file__).parent.parent / "config" / ".env"
    load_dotenv(dotenv_path)
    return {
        "aws_region": os.getenv("AWS_REGION"),
        "s3_bucket": os.getenv("S3_BUCKET_NAME"),
        "s3_key": os.getenv("S3_KEY"),
        "db_url": f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
                  f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}?sslmode=require"
    }

# ------------------------
# Load CSV from S3
# ------------------------
def read_csv_from_s3(bucket, key, region):
    session = boto3.session.Session(region_name=region)
    s3 = session.client('s3')
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except s3.exceptions.NoSuchKey:
        raise Exception(f"S3 key '{key}' not found in bucket '{bucket}'")
    return pd.read_csv(obj['Body'])

# ------------------------
# Data Cleaning + Initial Transform
# ------------------------
def transform_data(df):
    df = df.rename(columns={
        "TransactionNo": "transaction_id",
        "Date": "date",
        "ProductNo": "product_id",
        "ProductName": "name",
        "Price": "price",
        "Quantity": "quantity",
        "CustomerNo": "customer_id",
        "Country": "country"
    })
    df.dropna(subset=["customer_id"], inplace=True)
    df = df[~df["transaction_id"].astype(str).str.contains("C")]
    df = df[df["quantity"] > 0]
    df = df.astype({"transaction_id": int, "customer_id": int})
    df["date"] = pd.to_datetime(df["date"], format="%m/%d/%Y", errors='coerce')
    df["country_id"] = df["country"].str.upper().str.slice(stop=3) + df["country"].str.len().astype(str)
    df = df.groupby([
        "transaction_id", "date", "product_id", "name", "price", "customer_id",
        "country", "country_id"
    ])['quantity'].sum().reset_index()
    df["year"] = df["date"].dt.year
    df["quarter"] = df["date"].dt.quarter
    df["month"] = df["date"].dt.month
    df["week"] = df["date"].dt.isocalendar().week
    df["day"] = df["date"].dt.day
    df["day_name"] = df["date"].dt.day_name()
    return df

# ------------------------
# Build Dimensional Tables
# ------------------------
def build_customer_dim(df):
    dim = df[["customer_id"]].drop_duplicates().sort_values("customer_id").reset_index(drop=True)
    dim["customer_key"] = dim.index
    return dim

def build_transaction_dim(df):
    dim = df[["transaction_id"]].drop_duplicates().sort_values("transaction_id").reset_index(drop=True)
    dim["transaction_key"] = dim.index
    return dim

def build_date_dim(df):
    dim = df[["date", "year", "quarter", "month", "week", "day", "day_name"]] \
           .drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
    dim["date_key"] = dim.index
    return dim

def build_country_dim(df):
    df_clean = df.groupby("country_id", as_index=False).agg({"country": "first"}) \
                 .sort_values("country_id").reset_index(drop=True)
    df_clean["country_key"] = df_clean.index
    return df_clean

def build_product_dim(df):
    df_clean = df.groupby("product_id", as_index=False).agg({"name": "first", "price": "first"}) \
                 .sort_values("product_id").reset_index(drop=True)
    df_clean["product_key"] = df_clean.index
    return df_clean

# ------------------------
# Build Fact Table
# ------------------------
def build_sales_fact(df, customer_dim, transaction_dim, date_dim, product_dim, country_dim):
    df_fact = df.merge(customer_dim, on="customer_id", how="left", validate="many_to_one") \
                .merge(transaction_dim, on="transaction_id", how="left", validate="many_to_one") \
                .merge(date_dim, on="date", how="left", validate="many_to_one") \
                .merge(product_dim, on="product_id", how="left", validate="many_to_one") \
                .merge(country_dim, on="country_id", how="left", validate="many_to_one")
    return df_fact[["customer_key", "transaction_key", "date_key", "product_key", "country_key", "quantity"]]

# ------------------------
# Write Tables to PostgreSQL
# ------------------------
def write_to_postgres(df, table_name, db_url):
    engine = create_engine(db_url)
    df.to_sql(table_name, engine, if_exists="replace", index=False, method='multi')
    print(f"✅ Loaded table: {table_name} ({len(df)} rows)")

# ------------------------
# Main ETL Process
# ------------------------
def main():
    config = load_env()
    df = read_csv_from_s3(config["s3_bucket"], config["s3_key"], config["aws_region"])
    df = transform_data(df)
    # df = df.head(10)  # Limit for preview/test

    customer_dim = build_customer_dim(df)
    transaction_dim = build_transaction_dim(df)
    date_dim = build_date_dim(df)
    country_dim = build_country_dim(df)
    product_dim = build_product_dim(df)
    sales_fact = build_sales_fact(df, customer_dim, transaction_dim, date_dim, product_dim, country_dim)

    db_url = config["db_url"]
    df.head(100).sort_values(by=["transaction_id", "product_id"]).to_csv("script_output.csv", index=False)

    write_to_postgres(customer_dim, "customer_dim", db_url)
    write_to_postgres(transaction_dim, "transaction_dim", db_url)
    write_to_postgres(date_dim, "date_dim", db_url)
    write_to_postgres(country_dim, "country_dim", db_url)
    write_to_postgres(product_dim, "product_dim", db_url)
    write_to_postgres(sales_fact, "sales_fact", db_url)

if __name__ == "__main__":
    main()
