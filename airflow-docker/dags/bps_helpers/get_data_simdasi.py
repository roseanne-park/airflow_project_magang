import requests
import pandas as pd
import re
from urllib.parse import urlparse
import json
from datetime import datetime
from typing import Optional, List, Dict, Any                  

def get_available_years(base_url: str) -> Optional[List[int]]:
    """Automatically try to fetch available years for a table from endpoint id/23."""
    print("\n🔄 Mencoba mengambil daftar tahun yang tersedia secara otomatis...")
    try:
        id_tabel_match = re.search(r'id_tabel/([^/]+)', base_url)
        wilayah_match = re.search(r'wilayah/([^/]+)', base_url)
        key_match = re.search(r'key/([^/]+)', base_url)

        if not (id_tabel_match and wilayah_match and key_match):
            print("⚠️ URL tidak mengandung id_tabel, wilayah, atau key yang diperlukan.")
            return None

        id_tabel = id_tabel_match.group(1)
        wilayah = wilayah_match.group(1)
        key = key_match.group(1)

        list_url = f"https://webapi.bps.go.id/v1/api/interoperabilitas/datasource/simdasi/id/23/wilayah/{wilayah}/key/{key}/"

        response = requests.get(list_url, timeout=30)
        if response.status_code != 200:
            print("⚠️ Gagal menghubungi endpoint daftar tabel.")
            return None

        list_data = response.json()
        if list_data.get("data-availability") != "available":
            print("⚠️ Daftar tabel tidak tersedia untuk wilayah ini.")
            return None

        for table_info in list_data.get('data', [{}, {}])[1].get('data', []):
            if table_info.get('id_tabel') == id_tabel:
                years = table_info.get('ketersediaan_tahun')
                if years:
                    print(f"✅ Tahun yang tersedia ditemukan: {years}")
                    return sorted(years, reverse=True)

        print("⚠️ Tabel tidak ditemukan dalam daftar, tidak bisa menentukan tahun.")
        return None
    except Exception as e:
        print(f"❌ Terjadi kesalahan saat mengambil tahun otomatis: {e}")
        return None
    
def handle_simdasi_list(json_data):
    """Menangani respons API SIMDASI berbasis daftar sederhana."""
    try:
        data_list = json_data['data'][1]['data']
        df = pd.DataFrame(data_list)
        df.columns = [normalize_column_name(col) for col in df.columns]
        return df
    except (KeyError, IndexError, TypeError) as e:
        print(f"❌ Tidak dapat mengurai struktur daftar SIMDASI. Kesalahan: {e}")
        return pd.DataFrame(), None
        
def process_url(url):
    """Menganalisis URL, mengambil data, dan memanggil penangan yang sesuai."""
    # Ganti placeholder {tahun} dengan tahun sekarang untuk validasi awal
    url_for_check = re.sub(r'\{tahun\}', str(datetime.now().year), url, flags=re.IGNORECASE)

    parsed_url = urlparse(url_for_check)
    path = parsed_url.path

    if '/datasource/simdasi/' in path:
        print("== Terdeteksi API SIMDASI ==")
        match = re.search(r'/id/(\d+)/', url_for_check)
        if not match:
            print("❌ URL SIMDASI tidak valid. Tidak dapat menemukan ID endpoint (contoh: '/id/25/').")
            return pd.DataFrame(), None

        endpoint_id = match.group(1)

        if endpoint_id == '25':
            # Kirim URL asli dengan placeholder {tahun} ke penangan
            return handle_simdasi_detail_table(url)
        else:
            try:
                response = requests.get(url_for_check, timeout=30)
                if response.status_code != 200:
                    print(f"❌ Kesalahan HTTP: {response.status_code} - {response.text}")
                    return pd.DataFrame(), None
                json_data = response.json()
                if json_data.get("status") != "OK":
                    print(f"❌ Kesalahan API: {json_data.get('message', 'Kesalahan tidak diketahui')}")
                    return pd.DataFrame(), None
            except Exception as e:
                print(f"❌ Gagal mengambil data untuk URL list: {e}")
                return pd.DataFrame(), None

            simdasi_map = {'26': "mfd_provinsi", '27': "mfd_regency", '28': "mfd_district", '22': "simdasi_subjects", '34': "simdasi_master_table", '36': "simdasi_master_table_detail", '23': "simdasi_tables_by_area", '24': "simdasi_tables_by_area_subject"}
            table_name = simdasi_map.get(endpoint_id, f"unknown_simdasi_{endpoint_id}")
            return handle_simdasi_list(json_data, table_name)

    else:
        print("❌ Format URL API tidak dikenali. Skrip ini hanya menangani URL SIMDASI.")
        return pd.DataFrame(), None    
def handle_simdasi_detail_table(url_template: str, schema: str, table: str) -> Dict[str, Any]:
    """Handle complex structure from 'Detail of SIMDASI Table' endpoint (/id/25/)."""
    tahun_range = get_available_years(url_template)
    if not tahun_range:
        current_year = datetime.now().year
        tahun_range = range(current_year - 10, current_year + 1)
        print(f"⚠️ Gagal mendeteksi tahun. Menggunakan rentang tahun default: {list(tahun_range)}")

    all_data = []
    label_column_name = 'label'
    id_kategori_value = None

    print("\n--- Memulai Loop Pengambilan Data SIMDASI ---")
    for tahun in sorted(tahun_range, reverse=True):
        print(f"🔄 Mengambil tahun {tahun}...")
        url = re.sub(r'tahun/\d{4}', f'tahun/{tahun}', url_template)
        try:
            response = requests.get(url, timeout=30)
            if response.status_code != 200 or response.json().get("data-availability") != "available":
                print(f"-> Data tidak tersedia untuk tahun {tahun}.")
                continue

            json_data = response.json()
            data_info = json_data['data'][1]
            lingkup_id = data_info.get("lingkup_id")
            mms_id = data_info.get("mms_id")
            if id_kategori_value is None:
                id_kategori_value = mms_id
                if lingkup_id:
                    label_column_name = normalize_column_name(lingkup_id)

            kolom_metadata = data_info.get('kolom', {})
            kolom_order = list(kolom_metadata.keys())
            kolom_labels = [kolom_metadata[k]['nama_variabel'] for k in kolom_order]

            for row in data_info.get('data', []):
                clean_label = ' '.join(re.sub(r'<[^>]+>', ' ', row.get('label', '')).split())
                      # Jika lingkup_id adalah 'kabupaten/kota'
                if lingkup_id and lingkup_id.lower() == "kabupaten/kota":
                    # Periksa apakah label BUKAN 'kota', 'kabupaten', DAN juga bukan 'jawa timur'
                    if (not clean_label.lower().startswith('kota ') and
                        not clean_label.lower().startswith('kabupaten ') and
                        clean_label.lower() != 'jawa timur'):
                        
                        # Jika semua kondisi terpenuhi, tambahkan 'Kabupaten' di depan
                        clean_label = f"Kabupaten {clean_label}"
                record = {
                    label_column_name: clean_label,
                    'tahun': tahun,
                    'id_kategori': id_kategori_value
                }

                variables_data = row.get('variables')
                for key, label in zip(kolom_order, kolom_labels):
                    value = None
                    if isinstance(variables_data, dict):
                        value_dict = variables_data.get(key, {})
                        if isinstance(value_dict, dict):
                            value = value_dict.get('value_raw')

                    cleaned_value = 0.0
                    if isinstance(value, (int, float)):
                        cleaned_value = float(value)
                    elif isinstance(value, str):
                        try:
                            cleaned_string = value.replace(".", "").replace(",", ".")
                            cleaned_value = float(cleaned_string)
                        except (ValueError, TypeError):
                            cleaned_value = value

                    record[label] = cleaned_value

                all_data.append(record)
            print(f"-> Berhasil memproses data untuk tahun {tahun}.")
        except requests.exceptions.RequestException as e:
            print(f"❌ Kesalahan jaringan untuk tahun {tahun}: {e}")
        except (json.JSONDecodeError, KeyError, IndexError, TypeError) as e:
            print(f"❌ Gagal mengurai data untuk tahun {tahun}. Kesalahan: {e}")

    if not all_data:
        print("\n⚠️ Tidak ada data yang berhasil dikumpulkan dari rentang tahun yang ditentukan.")
        return None

    df = pd.DataFrame(all_data)
    df['id'] = range(1, 1 + len(df))

    if 'id_kategori' in df.columns:
        df['id_kategori'] = pd.to_numeric(df['id_kategori'], errors='coerce').fillna(0).astype(int)
    else:
        df['id_kategori'] = 0

    cols = df.columns.tolist()
    cols.remove('id')
    if 'id_kategori' in cols:
        cols.remove('id_kategori')
    new_order = ['id', 'id_kategori'] + [col for col in cols if col not in ['id', 'id_kategori']]
    df = df[new_order]

    df = df.fillna(0)
    df.columns = [normalize_column_name(col) for col in df.columns]

    original_full_table_name = f"{schema}.{table}"
    df_transposed, cleansing_full_table_name = transpose_if_needed(df.copy(), table, schema)

    return {
        "original_df": df,
        "original_table": original_full_table_name,
        "transposed_df": df_transposed,
        "transposed_table": cleansing_full_table_name
    }


def normalize_column_name(col: str) -> str:
    """Normalize string into valid column name."""
    col = str(col).lower().strip()
    col = re.sub(r'<[^>]+>', ' ', col)
    col = re.sub(r'[^a-z0-9\s_]', '', col)
    col = re.sub(r'\s+', '_', col)
    return col

def transpose_if_needed(df: pd.DataFrame, table: str, schema: str) -> tuple:
    """Transpose (unpivot) dataframe if column count > 5."""
    if df.shape[1] <= 5:
        print("ℹ️ Data tidak ditranspose karena jumlah kolom <= 5.")
        return None, None

    id_cols = ['id', 'id_kategori', 'tahun']
    potential_label_cols = [col for col in df.columns if col not in id_cols and df[col].dtype == 'object']
    
    if potential_label_cols:
        label_col_name = potential_label_cols[0]
        id_cols.append(label_col_name)
    else:
        print("⚠️ Could not identify the label column for transposition. Skipping transpose.")
        return None, None

    value_cols = [col for col in df.columns if col not in id_cols]

    if not value_cols:
        print("⚠️ No value columns found for transposition. Skipping transpose.")
        return None, None

    df_transposed = pd.melt(
        df,
        id_vars=id_cols,
        value_vars=value_cols,
        var_name='kategori',
        value_name='jumlah'
    )

    max_base_length = 63 - len("_cl") - 1
    short_table_name = table[:max_base_length]
    cleansing_table_name = f"{short_table_name.rstrip('_')}_cl"

    print(f"✅Yeay Data berhasil ditranspose menjadi format long (jumlah baris: {len(df_transposed)}).")
    return df_transposed, f"{schema}.{cleansing_table_name}"