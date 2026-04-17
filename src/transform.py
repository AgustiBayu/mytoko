import pandas as pd
import os
from src.utils import load_config

def transform_data(df_dict):

    config = load_config()
    staging_path = config['etl_settings']['path']['staging']
    final_path = config['etl_settings']['path']['final']

    print("--- Mengambil Data ---")
    df_kategori = df_dict['kategori']
    df_produk = df_dict['produk']
    df_transaksi = df_dict['penjualan']

    for df_item in [df_kategori, df_produk, df_transaksi]:
        df_item.columns = df_item.columns.str.strip().str.lower()

    print("--- Transformasi: Membersihkan Data ---")

    kategori_unique = df_kategori[['content','category']].drop_duplicates().reset_index(drop=True)
    df_dim_kategori = pd.DataFrame({
        'id': range(1, len(kategori_unique) + 1),
        'isi_kategori': kategori_unique['content'],
        'tipe_kategori': kategori_unique['category']
    })

    tanggal_unique = df_transaksi[['hari', 'tanggal']].drop_duplicates().reset_index(drop=True)
    hari_map = {
        0: 'Sunday',
        1: 'Monday',
        2: 'Tuesday',
        3: 'Wednesday',
        4: 'Thursday',
        5: 'Friday',
        6: 'Saturday'
    }
    df_dim_tanggal = pd.DataFrame({
        'id': range(1, len(tanggal_unique) + 1),
        'tahun': '9999',
        'bulan': '99',
        'hari': tanggal_unique['hari'].map(hari_map),
        'tanggal': tanggal_unique['tanggal']
    })

    produk_unique = df_produk[['brand','description','seller','title','actual_price','discount','average_rating','category']].drop_duplicates().reset_index(drop=True)
    df_produk_temp = pd.merge(produk_unique, df_dim_kategori, left_on=['category'], right_on=['tipe_kategori'],how='left')
    df_dim_produk = pd.DataFrame({
        'id': range(1, len(df_produk_temp) + 1),
        'kategori_id': df_produk_temp['id'],
        'brand': produk_unique['brand'],
        'deskripsi_produk': produk_unique['description'],
        'nama_penjual': produk_unique['seller'],
        'judul_produk': produk_unique['title'],
        'harga_barang_asli': produk_unique['actual_price'],
        'diskon': df_produk_temp['discount'].astype(str).str.extract('(\d+)').squeeze().fillna(0).astype(int),
        'rating_produk': produk_unique['average_rating']
    })

    df_transaksi['hari_str'] = df_transaksi['hari'].map(hari_map)
    df_fact_temp = pd.merge(df_transaksi, df_dim_tanggal[['id', 'hari', 'tanggal']], left_on=['hari_str', 'tanggal'], right_on=['hari', 'tanggal'], how='left')

    df_fact_transaksi_sales = pd.DataFrame({
        'id': range(1, len(df_fact_temp) + 1),
        'produk_id': None,         
        'tanggal_id': df_fact_temp['id'],
        'curah_hujan_mm': df_fact_temp['curah hujan (mm)'],
        'penjualan_barang_pcs': df_fact_temp['penjualan (pcs)'],
        'harga_barang': 0,  
        'total_penghasilan': 0 
    })

    df_dim_kategori.to_csv(os.path.join(staging_path, 'dim_kategori'), index=False)
    df_dim_produk.to_csv(os.path.join(staging_path, 'dim_produk'), index=False)
    df_dim_tanggal.to_csv(os.path.join(staging_path, 'dim_tanggal'), index=False)
    df_fact_transaksi_sales.to_csv(os.path.join(final_path, 'fact_transaksi'), index=False)

    print('Berhasil memisahkan 3 Tabel Dimensi (Staging) dan 1 Tabel Fakta (Final).')
    return df_dim_kategori, df_dim_produk, df_dim_tanggal, df_fact_transaksi_sales