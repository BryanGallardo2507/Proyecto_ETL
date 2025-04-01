import pyodbc
import pandas as pd
from datetime import datetime
import hashlib

# --------------------------------------
# Database Connection
# --------------------------------------
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

# --------------------------------------
# Data Extraction
# --------------------------------------
def extract_data(conn, query):
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        raise Exception(f"Error during extraction: {e}")

# --------------------------------------
# Transformations
# --------------------------------------
def apply_transformations(df, transformations):
    for col, ops in transformations.items():
        for op in ops:
            if op == 'lowercase':
                df[col] = df[col].str.lower()
            elif op == 'uppercase':
                df[col] = df[col].str.upper()
            elif op == 'extract_year':
                df['ANIO'] = pd.to_datetime(df[col], errors='coerce').dt.year
            elif op == 'extract_month_name':
                df['MES'] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%B')
            elif op == 'extract_trimester':
                df['TRIMESTRE'] = pd.to_datetime(df[col], errors='coerce').dt.quarter
            elif op == 'extract_day_name':
                df['DIA_SEMANA'] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%A')
            elif isinstance(op, dict) and 'concat' in op:
                df[col] = df[col].astype(str) + str(op['concat'])

    # Ensure object columns are safe for SQL insert
    for c in df.columns:
        if df[c].dtype == 'object':
            df[c] = df[c].astype(str)

    return df

# --------------------------------------
# Row Hashing for Deduplication
# --------------------------------------
def generate_row_hash(row, cols):
    row_string = '|'.join(str(row[col]) for col in cols)
    return hashlib.sha256(row_string.encode()).hexdigest()

# --------------------------------------
# Get Existing Records (based on keys)
# --------------------------------------
def get_existing_data(conn, dest_table, key_columns):
    try:
        query = f"SELECT {', '.join(key_columns)} FROM {dest_table}"
        return pd.read_sql(query, conn)
    except Exception as e:
        raise Exception(f"Error fetching existing records: {e}")

# --------------------------------------
# Data Load with Hash-Based Deduplication
# --------------------------------------
def load_data(conn, dest_table, df, key_columns):
    cursor = conn.cursor()
    try:
        # Step 1: Create ROW_HASH column for input data
        df['ROW_HASH'] = df.apply(lambda row: generate_row_hash(row, key_columns), axis=1)

        # Step 2: Get existing rows (only key columns)
        existing = get_existing_data(conn, dest_table, key_columns)

        if not existing.empty:
            # Step 3: Generate ROW_HASH for existing records
            existing['ROW_HASH'] = existing.apply(lambda row: generate_row_hash(row, key_columns), axis=1)

            # Step 4: Drop duplicates
            df = df[~df['ROW_HASH'].isin(existing['ROW_HASH'])]

        if df.empty:
            return 0  # No new rows to insert

        # Step 5: Remove ROW_HASH before insert
        df_to_insert = df.drop(columns=['ROW_HASH'])

        placeholders = ", ".join("?" * len(df_to_insert.columns))
        columns = ", ".join(df_to_insert.columns)
        insert_query = f"INSERT INTO {dest_table} ({columns}) VALUES ({placeholders})"

        cursor.fast_executemany = True
        cursor.executemany(insert_query, df_to_insert.values.tolist())
        conn.commit()
        return len(df_to_insert)

    except Exception as e:
        conn.rollback()
        raise Exception(f"Error loading data: {e}")
