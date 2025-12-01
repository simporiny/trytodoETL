import pandas as pd
from sqlalchemy import create_engine

# EXTRACT
# def extract_data(file_path):
#     """Extract data from CSV file."""
#     return pd.read_csv(file_path)
def extract_data(file_path_1, file_path_2):
    """Extract data from two CSV files."""
    df1 = pd.read_csv(file_path_1)
    df2 = pd.read_csv(file_path_2)
    return df1, df2

# TRANSFORM
# def transform_data(df):
#     """Transform the data as needed."""
#     df['home_goals'] = df['home_goals']
#     return df
def transform_data(df1, df2):
    """Transform and join the data as needed."""
    # Example: Join on a common column, like 'match_id'
    df_joined = pd.merge(df1, df2, on='season', how='inner')  # You can use 'left', 'right', 'outer', or 'inner'
    
    # Further transformation (optional)
    df_only_home_goals =  df_joined['home_goals']
    
    return df_only_home_goals

# LOAD
def load_data(df, db_url, table_name):
    """Load data into the database."""
    try:
        engine = create_engine(db_url)
        with engine.connect() as connection:
            df.to_sql(table_name, con=connection, if_exists='fail', index=False)
        print("Data loaded successfully.")
    except Exception as e:
        print(f"Error loading data: {e}")

# MAIN
def run_etl():
    """Run the full ETL process."""
    file_path_1 = 'C:\\Users\\USER\\Downloads\\Tableau\\results.csv'
    file_path_2 = 'C:\\Users\\USER\\Downloads\\Tableau\\stats.csv'
    db_url = 'postgresql+psycopg2://postgres:012545@localhost/postgres'
    table_name = 'result'

    # data = extract_data(file_path)
    df1, df2 = extract_data(file_path_1, file_path_2)
    # transformed_data = transform_data(data)
    transformed_data = transform_data(df1, df2)
    load_data(transformed_data, db_url, table_name)
    print("ETL process completed successfully.")

if __name__ == "__main__":
    run_etl()
