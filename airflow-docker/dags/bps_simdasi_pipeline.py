from datetime import datetime, timedelta
from airflow import DAG
# from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
# from airflow.utils.task_group import TaskGroup
from airflow.sdk import TaskGroup
# from airflow.decorators import dag, task
from airflow.sdk import task, dag
from airflow.exceptions import AirflowSkipException 

import pandas as pd
import numpy as np
from bps_helpers.config.db_config import DB_CONFIG
from bps_helpers.get_data_simdasi import handle_simdasi_detail_table
from bps_helpers.save_data_simdasi import save_to_postgres

CSV_URL = "https://docs.google.com/spreadsheets/d/1hJ02dmxIVXZd7_i0ue3SvurxKNUPDvpKQqT5PSmJ_2g/gviz/tq?tqx=out:csv&gid=1551122677"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_api_urls_from_sheet():
    """
    Fetch API URLs from Google Sheets.
    Juga membaca kolom D untuk flag perkalian.
    """
    try:
        df = pd.read_csv(CSV_URL, header=None)
        #Tambahkan 'X' untuk flag perkalian (kolom D)
        df = df.rename(columns={
            df.columns[0]: "url",
            df.columns[1]: "schema",
            df.columns[2]: "table",
            df.columns[3]: "multiply_flag" 
        })
        
        # Pastikan kolom flag ada, jika tidak, buat kolom kosong
        if 'multiply_flag' not in df.columns:
            df['multiply_flag'] = np.nan
            
        # Ubah flag menjadi boolean, anggap 'X' (case-insensitive) berarti True
        df['multiply_flag'] = df['multiply_flag'].str.strip().str.upper() == 'X'
        
        records = df.dropna(subset=["url"]).to_dict(orient="records")
        return records
    except Exception as e:
        print(f"❌ Gagal ambil dari Google Sheet: {e}")
        return []

#parameter 'multiply' pada fungsi
def process_simdasi_url(url: str, schema: str, table: str, multiply: bool = False):
    """
    Memanggil helper untuk mengambil data, lalu menerapkan logika perkalian
    pada kolom ke-5 dan seterusnya.
    """
    print(f"🔁 Processing URL: {url}")
    # Memanggil fungsi dari file get_simdasi.py
    result = handle_simdasi_detail_table(url, schema, table)

    if result is None:
        print("⚠️ No data processed from helper")
        return

    # mengalikan kolom berdasarkan posisinya
    if multiply:
        print(f"🔎 Menerapkan perkalian 1000 pada data dari {url}...")
        
        # Proses untuk original_df
        if result.get("original_df") is not None and not result["original_df"].empty:
            df_orig = result["original_df"]
            cols_to_multiply = df_orig.columns[4:] #Mulai dari index 4
            print(f"✅ Mengalikan kolom di original_df: {cols_to_multiply.tolist()}")
            for col in cols_to_multiply:
                #pastikan kolom adalah numerik sebelum dikalikan
                df_orig[col] = pd.to_numeric(df_orig[col], errors='coerce') * 1000
            result["original_df"] = df_orig

        # Cek dan kalikan di transposed_df (jika ada)
        if result.get("transposed_df") is not None and not result["transposed_df"].empty:
            df_trans = result["transposed_df"]
            cols_to_multiply = df_trans.columns[5:] # <-- Aturan 2: Mulai dari index 5
            print(f"✅ Mengalikan kolom di transposed_df: {cols_to_multiply.tolist()}")
            for col in cols_to_multiply:
                # Pastikan kolom adalah numerik sebelum dikalikan
                df_trans[col] = pd.to_numeric(df_trans[col], errors='coerce') * 1000
            result["transposed_df"] = df_trans
    
    #Menyimpan data yang sudah (atau tidak) dimodifikasi ke database
    full_table_original = f"{schema}.{table}"
    full_table_transposed = f"{schema}.{table}_cl"

    if result["original_df"] is not None and not result["original_df"].empty:
        success, message = save_to_postgres(result["original_df"], full_table_original)
        print(f"{'✅YEY BISA' if success else '❌YAH GABISA'} {message}")
    else:
        print("⚠️ No original data to save")

    # Save transposed data
    if result["transposed_df"] is not None and not result["transposed_df"].empty:
        success, message = save_to_postgres(result["transposed_df"], full_table_transposed)
        print(f"{'✅CONGRATS' if success else '❌YAH GABISA'} {message}")
    else:
        print("⚠️ No transposed data to save")

with DAG(
    'bps_simdasi_pipeline',
    default_args=default_args,
    schedule='0 0 * * *', 
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['bps', 'data', 'serial', 'last_sunday']
) as dag:
    @task
    def task_0_check_is_last_sunday(ds=None, dag_run=None):
        """
        Task Gerbang:
        1. SELALU LOLOS jika di-trigger manual.
        2. Jika di-schedule, cek apakah hari ini Minggu terakhir bulan ini.
        'ds' adalah execution date string (YYYY-MM-DD)
        'dag_run' adalah objek yang berisi info tentang run DAG ini.
        """
        
        # dag_run akan otomatis diisi oleh Airflow
        if dag_run and dag_run.run_type == 'manual':
            print(f"✅ Triggered MANUAL. Melanjutkan pipeline.")
            return  # Langsung lolos tanpa cek tanggal

        # Kode ini hanya akan berjalan jika run_type BUKAN 'manual'
        print(f"Memeriksa tanggal eksekusi schedule: {ds}")
        dt = datetime.fromisoformat(ds)
        
        # 1. Cek apakah hari ini hari Minggu (Senin=0 ... Minggu=6)
        is_sunday = (dt.weekday() == 6)
        # 2. Cek apakah 7 hari dari sekarang sudah bulan baru
        is_last_sunday_in_month = ((dt + timedelta(days=7)).month != dt.month)
        if is_sunday and is_last_sunday_in_month:
            print(f"✅ HARI MINGGU TERAKHIR (Scheduled). Melanjutkan pipeline.")
            return
        else:
            print(f"SKIP. Tanggal schedule {ds} bukan hari Minggu terakhir bulan ini.")
            raise AirflowSkipException("Bukan hari Minggu terakhir, pipeline di-skip.")

    # Memanggil fungsi @task
    # Ini untuk membuat instance task dari definisi fungsi di atas.
    # Variabel 'gate_task' sekarang terisi dan bisa digunakan untuk dependensi.
    gate_task = task_0_check_is_last_sunday()

    with TaskGroup('process_simdasi_apis') as process_group:
        previous_task = None
        api_urls = get_api_urls_from_sheet()
        
        if not api_urls:
            # Handle kasus jika GSheet kosong atau gagal diakses
            print("⚠️ Tidak ada URL API yang ditemukan dari Google Sheet.")
        
        for i, cfg in enumerate(api_urls):
             task_operator = PythonOperator(
                task_id=f'process_api_{i+1}',
                python_callable=process_simdasi_url,
                op_kwargs={
                    'url': cfg['url'],
                    'schema': cfg['schema'],
                    'table': cfg['table'],
                    'multiply': cfg.get('multiply_flag', False) 
                }
            )
             if previous_task:
                previous_task >> task_operator
             
             # Set task saat ini sebagai 'previous' untuk iterasi berikutnya
             previous_task = task_operator
    
    # Task 'gate_task' (pengecekan hari Minggu) harus berhasil
    # sebelum 'process_group' (seluruh grup pemrosesan API) dimulai.
    gate_task >> process_group