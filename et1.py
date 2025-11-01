import os
import requests
import pandas as pd
import duckdb
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("DATA_GOV_API_KEY")
if not API_KEY:
    raise ValueError("DATA_GOV_API_KEY not found in .env file")

DB_FILE = 'samarth.db'

DATASETS = {
    "agriculture": {
        "resource_id": "35be999b-0208-4354-b557-f6ca9a5355de",
        "url": "https://data.gov.in/resource/district-wise-season-wise-crop-production-statistics-1997",
        "limit": 50000 
    },
    "climate": {
        "resource_id": "8e0bd482-4aba-4d99-9cb9-ff124f6f1c2f",
        "url": "https://www.data.gov.in/resource/sub-divisional-monthly-rainfall-1901-2017",
        "limit": 5000 
    }
}

def fetch_data(resource_id, limit):
    """Fetches data from the data.gov.in API."""
    base_url = "https://api.data.gov.in/resource/"
    url = f"{base_url}{resource_id}?api-key={API_KEY}&format=json&limit={limit}"
    
    print(f"Fetching data from {resource_id}...")
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        if 'records' not in data:
            print(f"Error: 'records' key not in response. Response: {data}")
            return None
            
        print(f"Successfully fetched {len(data['records'])} records.")
        return data['records']
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def transform_agri_data(records, source_url):
    """Transforms raw agriculture data into a clean DataFrame."""
    print("Transforming agriculture data...")
    df = pd.DataFrame(records)

    df.rename(columns={
        "state_name": "state",
        "district_name": "district",
        "crop_year": "year",
        "season": "season",
        "crop": "crop",
        "area_": "area_hectare",
        "production_": "production_tonnes"
    }, inplace=True)
    
    df = df[['state', 'district', 'crop', 'year', 'season', 'area_hectare', 'production_tonnes']]
    
    df['source_url'] = source_url
    
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df['production_tonnes'] = pd.to_numeric(df['production_tonnes'], errors='coerce')
    
    df.dropna(subset=['year', 'production_tonnes', 'state', 'district', 'crop'], inplace=True)
    
    df['year'] = df['year'].astype(int) 
    
    df['state'] = df['state'].str.strip().str.title()
    df['district'] = df['district'].str.strip().str.title()
    df['crop'] = df['crop'].str.strip().str.title()
    
    print(f"Agriculture data transformed. {len(df)} clean records.")
    return df

def transform_climate_data(records, source_url):
    """Transforms raw climate data into a clean, 'tidy' DataFrame."""
    print("Transforming climate data...")
    df = pd.DataFrame(records)
    
    df.rename(columns={
        "subdivision": "subdivision", 
        "year": "year"
    }, inplace=True)
    
    month_columns_raw = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    
    for col in month_columns_raw:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
    df_tidy = df.melt(
        id_vars=["subdivision", "year"],
        value_vars=month_columns_raw, 
        var_name="month",
        value_name="rainfall_mm"
    )
    
    df_tidy['source_url'] = source_url
    
    df_tidy['year'] = pd.to_numeric(df_tidy['year'], errors='coerce')
    df_tidy.dropna(subset=['year', 'rainfall_mm', 'subdivision'], inplace=True)
    df_tidy['year'] = df_tidy['year'].astype(int) 
    
    df_tidy['subdivision'] = df_tidy['subdivision'].str.strip().str.title()
    
    print(f"Climate data transformed. {len(df_tidy)} clean records.")
    return df_tidy

def load_to_duckdb(dataframes, db_file):
    """Loads a dictionary of DataFrames into DuckDB tables."""
    con = duckdb.connect(database=db_file, read_only=False)
    
    for table_name, df in dataframes.items():
        print(f"Loading data into table: {table_name}...")
        con.register('temp_df', df)
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
        print(f"Table '{table_name}' created successfully.")
        
    print("\nTables in database:")
    print(con.execute("SHOW TABLES").fetchall())
    
    con.close()


if __name__ == "__main__":
    
    agri_records = fetch_data(
        DATASETS["agriculture"]["resource_id"], 
        DATASETS["agriculture"]["limit"]
    )
    climate_records = fetch_data(
        DATASETS["climate"]["resource_id"],
        DATASETS["climate"]["limit"]
    )
    
    if agri_records and climate_records:
        
        df_agri = transform_agri_data(agri_records, DATASETS["agriculture"]["url"])
        df_climate = transform_climate_data(climate_records, DATASETS["climate"]["url"])
        
      
        load_to_duckdb({
            "agriculture_production": df_agri,
            "climate_rainfall": df_climate
        }, DB_FILE)
        
        print(f"\n✅ ETL process complete. Database '{DB_FILE}' is ready.")
    else:
        print("\n❌ ETL process failed. Check API key or network connection.")