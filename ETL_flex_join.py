import pandas as pd
from sqlalchemy import create_engine

def extract_data(file_paths):
    dataframes = []
    for file in file_paths:
        df = pd.read_csv(file)
        df.columns = df.columns.str.strip().str.upper()
        dataframes.append(df)
    return dataframes

def transform_data(dataframes, join_keys):
    """Join multiple dataframes on given keys."""
    if len(dataframes) - 1 != len(join_keys): #DataFrame N ตัว ต้องมี join key N-1 ตัวพอดี เช่น DataFrame 3 ตัว ต้องมี 2 join keys
        raise ValueError("You must provide exactly one join key per join operation (number of dataframes - 1).")
    
    df_merged = dataframes[0] #DataFrame ตัวแรกไว้เป็นฐานในการเริ่ม join
    
    for idx, df in enumerate(dataframes[1:]): #เอา DataFrame ถัด ๆ ไปมาทีละตัว พร้อมกับดึง key ที่ใช้ join ออกมาใช้ในแต่ละรอบ
        key = join_keys[idx]
        print(f"Joining on key: {key}")
        if key not in df_merged.columns or key not in df.columns:
            raise KeyError(f"Join key '{key}' not found in both dataframes.")
        df_merged = pd.merge(df_merged, df, on=key, how='inner')
    
    # Example transformation
    if 'DAY' in df_merged.columns:
        df_merged_select = df_merged['DAY']  # dummy transformation
    
    return df_merged_select

def load_data(df, db_url, table_name):
    """Load data into the database."""
    try:
        engine = create_engine(db_url)
        with engine.connect() as connection:
            df.to_sql(table_name, con=connection, if_exists='fail', index=False)
        print("Data loaded successfully.")
    except Exception as e:
        print(f"Error loading data: {e}")

def run_etl():
    """Run the full ETL process."""
    file_paths =[
        'C:\\Users\\USER\\Downloads\\DA BTSG Test\\transaction_data.csv',
        'C:\\Users\\USER\\Downloads\\DA BTSG Test\\day_key.csv',
        'C:\\Users\\USER\\Downloads\\DA BTSG Test\\product.csv'
    ]
    join_keys = [
        'DAY',  # join results.csv and stats.csv on 'season'
        'PRODUCT_ID'  # join results.csv and stats.csv on 'season'
    ]

    db_url = 'postgresql+psycopg2://postgres:012545@localhost/postgres'
    table_name = 'result'
    

    dataframes = extract_data(file_paths)
    transformed_data = transform_data(dataframes, join_keys)
    load_data(transformed_data, db_url, table_name)
    print("ETL process completed successfully.")

if __name__ == "__main__":
    run_etl()