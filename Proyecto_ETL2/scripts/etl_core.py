import pyodbc
import pandas as pd
from datetime import datetime

def create_connection(config):
    conn_str = (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['server']};"
        f"DATABASE={config['database']};"
        f"UID={config['user']};"
        f"PWD={config['password']};"
        f"PORT={config['port']};"
    )
    return pyodbc.connect(conn_str, autocommit=config['autocommit'])


def extract_data(conn, query):
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        raise Exception(f"Error during extraction: {e}")


def apply_transformations(df, transformations):
    for col, ops in transformations.items():
        for op in ops:
            if op == 'lowercase':
                df[col] = df[col].str.lower()
            elif op == 'uppercase':
                df[col] = df[col].str.upper()
            elif op == 'extract_date':
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
            elif op == 'extract_year':
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.year
            elif op == 'extract_month':
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.month
            elif op == 'extract_day':
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.day
            elif isinstance(op, dict) and 'concat' in op:
                df[col] = df[col].astype(str) + str(op['concat'])
    return df


def get_existing_data(conn, dest_table, key_columns):
    try:
        keys = ", ".join(key_columns)
        query = f"SELECT {keys} FROM {dest_table}"
        return pd.read_sql(query, conn)
    except Exception as e:
        raise Exception(f"Error fetching existing records: {e}")


def load_data(conn, dest_table, df, key_columns):
    cursor = conn.cursor()
    try:
        placeholders = ", ".join("?" * len(df.columns))
        columns = ", ".join(df.columns)
        insert_query = f"INSERT INTO {dest_table} ({columns}) VALUES ({placeholders})"
        
        # Only insert rows that are not in existing DW
        existing = get_existing_data(conn, dest_table, key_columns)
        if not existing.empty:
            df = df.merge(existing, on=key_columns, how='left', indicator=True)
            df = df[df['_merge'] == 'left_only']
            df.drop(columns=['_merge'], inplace=True)

        if df.empty:
            return 0  # Nothing to insert

        cursor.fast_executemany = True
        cursor.executemany(insert_query, df.values.tolist())
        conn.commit()
        return len(df)

    except Exception as e:
        conn.rollback()
        raise Exception(f"Error loading data: {e}")
