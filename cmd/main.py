from src.extract import extract_data
from src.transform import transform_data

def main():
    df = extract_data()
    if df is not None:
        df_dim_kategori, df_dim_produk, df_dim_tanggal, df_fact_transaksi_sales = transform_data(df)
        print("Pipeline ETL lokal selesai! Cek folder staging dan final.")
    
if __name__ == "__main__":
    main()  