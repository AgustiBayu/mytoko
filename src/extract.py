import pandas as pd
import os
from src.utils import load_config

def extract_data():
    print("--- Transform: Memulai Pemrosesan Data ---")

    config = load_config()
    files_map = config['etl_settings']['input_file']
    raw_path = config['etl_settings']['path']['raw']

    data_frames = {}
    for key, file_name in files_map.items():
        file_path = os.path.join(raw_path, file_name)
        
        if not os.path.exists(file_path):
            print(f"Error: File {file_name} tidak ditemukan di {file_path}")
            continue

        print(f"--- Memproses: {file_name} ---")
        try:            
            if file_name.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_name.endswith('.json'):
                df = pd.read_json(file_path)
            elif file_name.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            else:
                print(f"Format tidak didukung: {file_name}")
                continue
            df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
            data_frames[key] = df
            print(f"Berhasil memuat '{key}' dengan {len(df)} baris.")

        except Exception as e:
            print(f"Gagal membaca {file_name}: {e}")

    return data_frames