import os
import pandas as pd
from helper import get_city_zone_info

# Paths to data directories
openmeteo_dir = "../OpenMeteo"
gridstatusio_dir = "../GridStatusIO"
# Load city/zone info from CSV
city_zone_df = pd.read_csv("../city_zone_info.csv")

# Helper: parse datetime with/without timezone
def parse_datetime(dt):
    try:
        return pd.to_datetime(dt, utc=True)
    except Exception:
        return pd.to_datetime(dt)

def merge_isone():
    # Load ISONE grid data
    forecast_path = os.path.join(gridstatusio_dir, "isone_reliability_region_load_forecast.csv")
    zonal_load_path = os.path.join(gridstatusio_dir, "isone_zonal_load_real_time_hourly.csv")
    if not (os.path.exists(forecast_path) and os.path.exists(zonal_load_path)):
        print("ISONE grid data not found.")
        return

    df_forecast = pd.read_csv(forecast_path)
    df_zonal = pd.read_csv(zonal_load_path)

    # Parse datetimes
    df_forecast['interval_start_utc'] = pd.to_datetime(df_forecast['interval_start_utc'], utc=True)
    df_zonal['interval_start_utc'] = pd.to_datetime(df_zonal['interval_start_utc'], utc=True)

    merged_rows = []
    for _, row in city_zone_df.iterrows():
        # Ignore missing zones (e.g., CAISO rows)
        if pd.isna(row['zone']):
            continue
        if not str(row['zone']).startswith('.Z'):
            continue  # Only NEISO/ISONE
        city = row['city']
        state = row['state']
        zone = row['zone']

        # Load weather/aqi
        weather_path = os.path.join(openmeteo_dir, f"{city}_{state}_hourly_weather.csv")
        aqi_path = os.path.join(openmeteo_dir, f"{city}_{state}_hourly_aqi.csv")
        if not (os.path.exists(weather_path) and os.path.exists(aqi_path)):
            print(f"Missing weather/aqi for {city}, {state}")
            continue

        df_weather = pd.read_csv(weather_path)
        df_aqi = pd.read_csv(aqi_path)

        # Merge weather and AQI on datetime, date, and time
        # Ensure all three columns exist in both dataframes
        for col in ['datetime', 'date', 'time']:
            if col not in df_weather.columns:
                raise ValueError(f"{col} column missing in weather data")
            if col not in df_aqi.columns:
                raise ValueError(f"{col} column missing in AQI data")

        # Standardize datetime columns, handling timezone-aware strings
        df_weather['datetime'] = pd.to_datetime(df_weather['datetime'], utc=True)
        df_aqi['datetime'] = pd.to_datetime(df_aqi['datetime'], utc=True)

        # Merge on all three columns
        df_wa = pd.merge(
            df_weather,
            df_aqi,
            on=['datetime', 'date', 'time'],
            suffixes=('', '_aqi')
        )

        # Add city, state, zone
        df_wa['city'] = city
        df_wa['state'] = state
        df_wa['zone'] = zone

        # Convert local datetime to UTC for join
        if df_wa['datetime'].dt.tz is None:
            # Assume local time, need to localize then convert to UTC
            # Try to infer timezone from weather file (OpenMeteo saves in local time)
            # For now, treat as naive and localize to US/Eastern, then convert to UTC
            df_wa['datetime_utc'] = df_wa['datetime'].dt.tz_localize('US/Eastern').dt.tz_convert('UTC')
        else:
            df_wa['datetime_utc'] = df_wa['datetime'].dt.tz_convert('UTC')

        # Merge in forecast and zonal load
        # The 'location' column in grid data matches the zone
        df_zone_forecast = df_forecast[df_forecast['location'] == zone].copy()
        df_zone_zonal = df_zonal[df_zonal['location'] == zone].copy()

        # Merge on UTC datetime
        df_zone_forecast = df_zone_forecast.rename(columns={'interval_start_utc': 'datetime_utc'})
        df_zone_zonal = df_zone_zonal.rename(columns={'interval_start_utc': 'datetime_utc'})

        # Only keep relevant columns
        df_zone_forecast = df_zone_forecast[['datetime_utc', 'load_forecast', 'regional_percentage']]
        df_zone_zonal = df_zone_zonal[['datetime_utc', 'load']]

        # Merge with weather/aqi
        df_wa = pd.merge(df_wa, df_zone_forecast, on='datetime_utc', how='inner')
        df_wa = pd.merge(df_wa, df_zone_zonal, on='datetime_utc', how='inner')

        # Reorder columns
        cols = (
            ['datetime', 'datetime_utc', 'date', 'time', 'city', 'state', 'zone']
            + [c for c in df_wa.columns if c not in ['datetime', 'datetime_utc', 'date', 'time', 'city', 'state', 'zone', 'load_forecast', 'regional_percentage', 'load']]
            + ['load_forecast', 'regional_percentage', 'load']
        )
        df_wa = df_wa[[c for c in cols if c in df_wa.columns]]

        merged_rows.append(df_wa)

    if merged_rows:
        df_isone_final = pd.concat(merged_rows, ignore_index=True)
        # Add load_to_forecast_diff column
        if 'load' in df_isone_final.columns and 'load_forecast' in df_isone_final.columns:
            df_isone_final['load_to_forecast_diff'] = df_isone_final['load'] - df_isone_final['load_forecast']
        df_isone_final.to_csv("../Merged/merged_isone_causal.csv", index=False)
        print("Saved merged ISONE data to ../Merged/merged_isone_causal.csv")
    else:
        print("No ISONE data merged.")

def merge_nyiso():
    # Load NYISO grid data
    forecast_path = os.path.join(gridstatusio_dir, "nyiso_zonal_load_forecast_hourly.csv")
    load_path = os.path.join(gridstatusio_dir, "nyiso_load.csv")
    if not (os.path.exists(forecast_path) and os.path.exists(load_path)):
        print("NYISO grid data not found.")
        return

    df_forecast = pd.read_csv(forecast_path)
    df_load = pd.read_csv(load_path)

    # Parse datetimes
    if 'interval_start_utc' in df_forecast.columns:
        df_forecast['interval_start_utc'] = pd.to_datetime(df_forecast['interval_start_utc'], utc=True)
    elif 'datetime_utc' in df_forecast.columns:
        df_forecast['interval_start_utc'] = pd.to_datetime(df_forecast['datetime_utc'], utc=True)
    else:
        raise ValueError("No interval_start_utc in NYISO forecast")

    if 'interval_start_utc' in df_load.columns:
        df_load['interval_start_utc'] = pd.to_datetime(df_load['interval_start_utc'], utc=True)
    elif 'datetime_utc' in df_load.columns:
        df_load['interval_start_utc'] = pd.to_datetime(df_load['datetime_utc'], utc=True)
    else:
        raise ValueError("No interval_start_utc in NYISO load")

    merged_rows = []
    for _, row in city_zone_df.iterrows():
        # Ignore missing zones (e.g., CAISO rows)
        if pd.isna(row['zone']):
            continue
        if str(row['zone']).startswith('.Z'):
            continue  # Only NYISO
        city = row['city']
        state = row['state']
        zone = row['zone']

        # Load weather/aqi
        weather_path = os.path.join(openmeteo_dir, f"{city}_{state}_hourly_weather.csv")
        aqi_path = os.path.join(openmeteo_dir, f"{city}_{state}_hourly_aqi.csv")
        if not (os.path.exists(weather_path) and os.path.exists(aqi_path)):
            print(f"Missing weather/aqi for {city}, {state}")
            continue

        df_weather = pd.read_csv(weather_path)
        df_aqi = pd.read_csv(aqi_path)

        # Merge weather and AQI on all three columns: datetime, date, and time (if present)
        # Identify columns to merge on
        merge_cols = []
        for col in ['datetime', 'date', 'time']:
            if col in df_weather.columns and col in df_aqi.columns:
                merge_cols.append(col)
        if not merge_cols:
            raise ValueError("No common columns (datetime/date/time) to merge on between weather and AQI data")

        # Standardize datetime columns if present
        if 'datetime' in merge_cols:
            df_weather['datetime'] = pd.to_datetime(df_weather['datetime'], utc=True)
            df_aqi['datetime'] = pd.to_datetime(df_aqi['datetime'], utc=True)
        if 'date' in merge_cols:
            df_weather['date'] = pd.to_datetime(df_weather['date']).dt.date
            df_aqi['date'] = pd.to_datetime(df_aqi['date']).dt.date
        if 'time' in merge_cols:
            # If time is not already string, convert to string for merge
            df_weather['time'] = df_weather['time'].astype(str)
            df_aqi['time'] = df_aqi['time'].astype(str)

        df_wa = pd.merge(df_weather, df_aqi, on=merge_cols, suffixes=('', '_aqi'))

        # Add city, state, zone
        df_wa['city'] = city
        df_wa['state'] = state
        df_wa['zone'] = zone

        # Convert local datetime to UTC for join
        if df_wa['datetime'].dt.tz is None:
            df_wa['datetime_utc'] = df_wa['datetime'].dt.tz_localize('US/Eastern').dt.tz_convert('UTC')
        else:
            df_wa['datetime_utc'] = df_wa['datetime'].dt.tz_convert('UTC')

        # In NYISO, each zone is a column in forecast/load
        # Get forecast and load for this zone
        zone_col = zone
        if zone_col not in df_forecast.columns or zone_col not in df_load.columns:
            print(f"Zone {zone_col} not found in NYISO grid data columns.")
            continue

        # Build forecast dataframe: datetime_utc, load_forecast, regional_percentage
        df_zone_forecast = df_forecast[['interval_start_utc', zone_col]].copy()
        df_zone_forecast = df_zone_forecast.rename(columns={zone_col: 'load_forecast', 'interval_start_utc': 'datetime_utc'})
        # Regional percentage: load_forecast for this zone / sum across all zones
        zone_cols = ['west', 'genese', 'centrl', 'north', 'mhk_vl', 'capitl', 'hud_vl', 'millwd', 'dunwod', 'nyc', 'longil']
        df_zone_forecast['regional_percentage'] = df_zone_forecast['load_forecast'] / df_forecast[zone_cols].sum(axis=1)

        # Build load dataframe: datetime_utc, load
        df_zone_load = df_load[['interval_start_utc', zone_col]].copy()
        df_zone_load = df_zone_load.rename(columns={zone_col: 'load', 'interval_start_utc': 'datetime_utc'})

        # Merge with weather/aqi
        df_wa = pd.merge(df_wa, df_zone_forecast, left_on='datetime_utc', right_on='datetime_utc', how='inner')
        df_wa = pd.merge(df_wa, df_zone_load, left_on='datetime_utc', right_on='datetime_utc', how='inner')

        # Reorder columns
        cols = (
            ['datetime', 'datetime_utc', 'date', 'time', 'city', 'state', 'zone']
            + [c for c in df_wa.columns if c not in ['datetime', 'datetime_utc', 'date', 'time', 'city', 'state', 'zone', 'load_forecast', 'regional_percentage', 'load']]
            + ['load_forecast', 'regional_percentage', 'load']
        )
        df_wa = df_wa[[c for c in cols if c in df_wa.columns]]

        merged_rows.append(df_wa)

    if merged_rows:
        df_nyiso_final = pd.concat(merged_rows, ignore_index=True)
        # Add load_to_forecast_diff column
        if 'load' in df_nyiso_final.columns and 'load_forecast' in df_nyiso_final.columns:
            df_nyiso_final['load_to_forecast_diff'] = df_nyiso_final['load'] - df_nyiso_final['load_forecast']
        df_nyiso_final.to_csv("../Merged/merged_nyiso_causal.csv", index=False)
        print("Saved merged NYISO data to ../Merged/merged_nyiso_causal.csv")
    else:
        print("No NYISO data merged.")

if __name__ == "__main__":
    merge_isone()
    merge_nyiso()