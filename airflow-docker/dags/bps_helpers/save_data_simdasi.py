import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from typing import Tuple
from bps_helpers.config.db_config import DB_CONFIG

def save_to_postgres(df: pd.DataFrame, full_table_name: str) -> Tuple[bool, str]:
    """
    Save Pandas DataFrame to PostgreSQL database, creating schema if needed.
    Returns tuple of (success: bool, message: str)
    """
    if df.empty:
        return False, "DataFrame kosong. Tidak ada yang disimpan ke database."

    if '.' in full_table_name:
        schema_name, table_name = full_table_name.split('.', 1)
    else:
        schema_name = 'public'
        table_name = full_table_name

    print(f"Skema: '{schema_name}', Tabel: '{table_name}'")

    try:
        conn_str_psycopg = f"dbname='{DB_CONFIG['dbname']}' user='{DB_CONFIG['user']}' password='{DB_CONFIG['password']}' host='{DB_CONFIG['host']}' port='{DB_CONFIG['port']}'"
        with psycopg2.connect(conn_str_psycopg) as conn:
            with conn.cursor() as cursor:
                print(f"Memastikan skema '{schema_name}' ada...")
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        print(f"✅ Skema '{schema_name}' siap.")

        conn_str_sqlalchemy = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        engine = create_engine(conn_str_sqlalchemy)

        print(f"Menulis data ke tabel '{schema_name}.{table_name}'...")
        df.to_sql(table_name, engine, schema=schema_name, if_exists='replace', index=False)
        return True, f"Data berhasil disimpan ke PostgreSQL: {schema_name}.{table_name}"

    except Exception as e:
        return False, f"Terjadi kesalahan saat operasi database: {e}"
    finally:
        if 'engine' in locals():
            engine.dispose()