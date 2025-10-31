"""LTA Carpark Availability Data Collector - Tampines Area"""

import requests
import pandas as pd
import time
from datetime import datetime
import os

API_URL = "https://datamall2.mytransport.sg/ltaodataservice/CarParkAvailabilityv2"
API_KEY = "zpcE7lfaRa2g9OU3YIaotA=="
DATA_FOLDER = "data"

TAMPINES_LAT_MIN = 1.335
TAMPINES_LAT_MAX = 1.375
TAMPINES_LON_MIN = 103.925
TAMPINES_LON_MAX = 103.965

TAMPINES_KEYWORDS = ['tampines', 'tmn', 'simei', 'pasir ris', 'bedok north', 'tanah merah']

os.makedirs(DATA_FOLDER, exist_ok=True)


def is_tampines_carpark(carpark):
    development = carpark.get('Development', '').lower()
    for keyword in TAMPINES_KEYWORDS:
        if keyword in development:
            return True
    
    location = carpark.get('Location', '')
    if location:
        try:
            coords = location.split()
            if len(coords) >= 2:
                lat = float(coords[0])
                lon = float(coords[1])
                if (TAMPINES_LAT_MIN <= lat <= TAMPINES_LAT_MAX and 
                    TAMPINES_LON_MIN <= lon <= TAMPINES_LON_MAX):
                    return True
        except (ValueError, IndexError):
            pass
    return False


def fetch_carpark_data():
    headers = {'AccountKey': API_KEY, 'accept': 'application/json'}
    try:
        response = requests.get(API_URL, headers=headers, timeout=30)
        if response.status_code == 200:
            data = response.json()
            all_carparks = data.get('value', [])
            return [cp for cp in all_carparks if is_tampines_carpark(cp)]
        return None
    except:
        return None


def process_carpark_data(carparks):
    if not carparks:
        return None
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    processed_records = []
    
    for carpark in carparks:
        location = carpark.get('Location', '')
        lat, lon = None, None
        if location:
            coords = location.split()
            if len(coords) >= 2:
                lat = float(coords[0])
                lon = float(coords[1])
        
        processed_records.append({
            'timestamp': timestamp,
            'carpark_id': carpark.get('CarParkID', ''),
            'area': carpark.get('Area', ''),
            'development': carpark.get('Development', ''),
            'latitude': lat,
            'longitude': lon,
            'available_lots': carpark.get('AvailableLots', 0),
            'lot_type': carpark.get('LotType', ''),
            'agency': carpark.get('Agency', '')
        })
    
    return pd.DataFrame(processed_records)


def save_to_csv(df):
    if df is None or df.empty:
        return
    
    filename = f"carpark_data_tampines_{datetime.now().strftime('%Y%m%d')}.csv"
    filepath = os.path.join(DATA_FOLDER, filename)
    file_exists = os.path.isfile(filepath)
    df.to_csv(filepath, mode='a', header=not file_exists, index=False)


def collect_continuously():
    while True:
        try:
            carparks = fetch_carpark_data()
            if carparks:
                df = process_carpark_data(carparks)
                save_to_csv(df)
            time.sleep(600)
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    collect_continuously()
