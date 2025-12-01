import pandas as pd
from sqlalchemy import create_engine

def extract_data(file_paths):
    return pd.read_json(file_paths, lines=True)

def transform_data(df):
    # df['name'] = df['name']
    # return df[['name']]
    df_transformed = df[['name','height']].copy()
    df_transformed['height'] = df_transformed['height'] * 2
    return df_transformed

def load_data(df, db_url, table_name):
    try:
        engine = create_engine(db_url)
        with engine.connect() as connection:
            df.to_sql(table_name, con=connection, if_exists='fail', index=False)
            print(f'Successful')
    except Exception as e:
        print(f'error')

def run_etl():
    file_path = "D:\Python\source\source1.json"
    db_url = 'postgresql+psycopg2://postgres:012545@localhost/postgres'
    table_name = 'json_file'

    df = extract_data(file_path)
    transformed_data = transform_data(df)
    load_data(transformed_data, db_url, table_name)
    print("ETL process completed successfully.")

if __name__ == "__main__":
    run_etl()