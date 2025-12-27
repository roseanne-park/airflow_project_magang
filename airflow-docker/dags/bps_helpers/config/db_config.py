import psycopg2
DB_CONFIG = {
    "dbname": "result_cleansing",
    "user": "postgres",
    "password": "2lNyRKW3oc9kan8n",
    "host": "103.183.92.158",
    "port": "5432"
}

def get_db_connection():
    """
    Membuat dan mengembalikan sebuah koneksi baru ke database.
    Fungsi ini akan dipanggil oleh setiap file processor yang membutuhkan
    akses ke database.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Koneksi database berhasil dibuat.")
        return conn
    except Exception as e:
        print(f"❌ Gagal menyambung ke database: {str(e)}")
        raise