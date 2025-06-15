import requests
import pandas as pd
import os
import json
from datetime import datetime

# Konfigurasi
URL_TEMPLATE = "https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4={kode_wilayah}"
KODE_WILAYAH = "36.71.07.1003"  # Karawaci Baru
OUTPUT_CSV = "data-raw/dummy.csv"
MAX_ROWS = 1000  # Batas maksimal baris

def fetch_and_process_data():
    try:
        # Mengambil data dari API
        url = URL_TEMPLATE.format(kode_wilayah=KODE_WILAYAH)
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        print("Data berhasil diambil dari API")
        
        # ===== KONVERSI JSON KE DATAFRAME =====
        lokasi_df = pd.json_normalize(data['lokasi'])
        
        # Proses data cuaca yang nested
        cuaca_rows = []
        
        # Iterasi melalui setiap item dalam 'data'
        for item in data['data']:
            # Iterasi melalui setiap periode dalam 'cuaca'
            for period in item['cuaca']:
                # Iterasi melalui setiap entri cuaca dalam periode
                for entry in period:
                    # Buat dictionary baru dengan menggabungkan data lokasi dan cuaca
                    row = {
                        **lokasi_df.iloc[0].to_dict(),  # Data lokasi
                        **entry  # Data cuaca
                    }
                    cuaca_rows.append(row)
        
        # Konversi ke DataFrame
        df = pd.DataFrame(cuaca_rows)
        
        # Konversi kolom waktu
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['utc_datetime'] = pd.to_datetime(df['utc_datetime'])
        df['local_datetime'] = pd.to_datetime(df['local_datetime'])
        df['analysis_date'] = pd.to_datetime(df['analysis_date'])
        
        # Pemilihan kolom yang diperlukan
        columns_to_keep = [
            'provinsi', 'kotkab', 'kecamatan', 'desa', 'lon', 'lat',
            'datetime', 'local_datetime', 't', 'hu', 'ws', 'wd', 'wd_deg',
            'tcc', 'tp', 'weather', 'weather_desc', 'vs'
        ]
        df = df[columns_to_keep]
        
        # Rename kolom
        df = df.rename(columns={
            't': 'temperature',
            'hu': 'humidity',
            'ws': 'wind_speed',
            'wd': 'wind_direction',
            'wd_deg': 'wind_degree',
            'tcc': 'cloud_cover',
            'tp': 'precipitation',
            'vs': 'visibility',
            'weather_desc': 'weather_description'
        })
        
        # Tambahkan kolom turunan
        df['hour'] = df['local_datetime'].dt.hour
        df['date'] = df['local_datetime'].dt.date
        
        # Urutkan berdasarkan waktu
        df = df.sort_values('local_datetime').reset_index(drop=True)
        
        # Tambahkan timestamp pengambilan data
        df['fetch_time'] = datetime.now()
        
        # Tambahkan unique key untuk mencegah duplikasi
        df['unique_key'] = df['local_datetime'].astype(str) + '_' + df['hour'].astype(str)
        
        return df
        
    except Exception as e:
        print(f"Error processing data: {str(e)}")
        exit(1)

def update_csv(new_data):
    try:
        # Buat direktori jika belum ada
        os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
        
        # Baca data existing jika file ada
        if os.path.exists(OUTPUT_CSV):
            existing_df = pd.read_csv(OUTPUT_CSV, parse_dates=['local_datetime', 'fetch_time'])
        else:
            existing_df = pd.DataFrame()
        
        # Cek jika ada data yang sudah ada sebelumnya
        if not existing_df.empty:
            # Buat unique key untuk data existing
            existing_df['unique_key'] = existing_df['local_datetime'].astype(str) + '_' + existing_df['hour'].astype(str)
            
            # Filter data baru yang belum ada di data existing
            new_data = new_data[~new_data['unique_key'].isin(existing_df['unique_key'])]
            
            # Jika tidak ada data baru, hentikan proses
            if new_data.empty:
                print("Tidak ada data baru untuk ditambahkan.")
                return existing_df
        
        # Gabungkan data baru dengan data existing
        combined_df = pd.concat([existing_df, new_data], ignore_index=True)
        
        # Urutkan berdasarkan waktu
        combined_df = combined_df.sort_values('local_datetime')
        
        # Pertahankan hanya MAX_ROWS terbaru
        if len(combined_df) > MAX_ROWS:
            combined_df = combined_df.iloc[-MAX_ROWS:]
        
        # Simpan ke CSV
        combined_df.to_csv(OUTPUT_CSV, index=False)
        print(f"Data berhasil disimpan ke CSV. Total baris: {len(combined_df)}")
        
        return combined_df
        
    except Exception as e:
        print(f"Error updating CSV: {str(e)}")
        exit(1)

if __name__ == "__main__":
    # Ambil dan proses data baru
    new_df = fetch_and_process_data()
    
    # Update file CSV
    updated_df = update_csv(new_df)
    
    # Tampilkan ringkasan
    print("\nRingkasan Data:")
    print(f"- Baris terbaru: {updated_df.iloc[-1]['local_datetime']}")
    print(f"- Baris terlama: {updated_df.iloc[0]['local_datetime']}")
    print(f"- Rentang waktu: {updated_df.iloc[0]['local_datetime']} hingga {updated_df.iloc[-1]['local_datetime']}")