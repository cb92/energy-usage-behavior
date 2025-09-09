import openmeteo_requests
from openmeteo_sdk.Unit import Unit
from openmeteo_sdk.Variable import Variable
import pandas as pd

import requests
from datetime import date

import os

def get_unit_name(unit_value):
    for name, value in Unit.__dict__.items():
        if value == unit_value and not name.startswith("__"):
            return name
    return f"unknown({unit_value})"


def get_variable_name(var_value):
    for name, value in Variable.__dict__.items():
        if value == var_value and not name.startswith("__"):
            return name
    return f"unknown({var_value})"

def extract_data_from_api_response(timeseries, cols, timezone = 'America/Los_Angeles'):
    datetime_index = pd.date_range(
        start=pd.to_datetime(timeseries.Time(), unit="s", utc=True),
        end=pd.to_datetime(timeseries.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=timeseries.Interval()),
        inclusive="left"
    )

    # Convert DatetimeIndex to timezone-aware datetime
    datetime_index = datetime_index.tz_convert(timezone)  # e.g., "America/Los_Angeles"

    # Construct DataFrame from converted index
    df_dict = {}
    df_dict["datetime"] = datetime_index
    df_dict["date"] = datetime_index.date
    df_dict["time"] = datetime_index.time

    i = 0
    while True:
        try: 
            col_name = cols[i] + "__" + \
                get_unit_name(timeseries.Variables(i).Unit())
            df_dict[col_name] = timeseries.Variables(i).ValuesAsNumpy()
            i+=1
            continue
        except:
            break
    return pd.DataFrame(data = df_dict)

def get_historical_hourly_data(lat, long, city_timezone, city, state, col_names=None, save_csv=True):
    if col_names is None:
        col_names = [
            "temperature_2m", "dew_point_2m", "rain", "snowfall", "cloud_cover", "wind_speed_10m"
        ]
    
    # Define filenames
    weather_filename = f"../OpenMeteo/{city}_{state}_hourly_weather.csv"
    aqi_filename = f"../OpenMeteo/{city}_{state}_hourly_aqi.csv"
    
    # Check if files already exist
    weather_exists = os.path.exists(weather_filename)
    aqi_exists = os.path.exists(aqi_filename)
    
    if weather_exists and aqi_exists:
        print(f"Loading existing data for {city}, {state} from files...")
        df_historical_weather = pd.read_csv(weather_filename)
        df_historical_aqi = pd.read_csv(aqi_filename)
        print(f"Loaded weather data from: {weather_filename}")
        print(f"Loaded AQI data from: {aqi_filename}")
        return df_historical_weather, df_historical_aqi, weather_filename, aqi_filename
    
    print(f"Fetching historical hourly weather for {city}, {state} from API...")
    openmeteo = openmeteo_requests.Client()

    # Historical hourly weather (last 4 years)
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": long,
        "start_date": date(date.today().year - 4, 1, 1),
        "end_date": date.today(),
        "hourly": col_names,
        "timezone": 'GMT'
    }
    responses = openmeteo.weather_api(url, params=params)
    df_historical_weather = extract_data_from_api_response(responses[0].Hourly(), col_names, timezone=city_timezone)

    # AQI (pm2.5, hourly) - last 4 years
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    aqi_col_names = ["pm2_5"]
    params = {
        "latitude": lat,
        "longitude": long,
        "hourly": aqi_col_names,
        "start_date": date(date.today().year - 4, 1, 1),
        "end_date": date.today(),
        "timezone": 'GMT'
    }
    responses = openmeteo.weather_api(url, params=params)
    df_historical_aqi = extract_data_from_api_response(responses[0].Hourly(), aqi_col_names, timezone=city_timezone)

    # Save to CSV files
    if save_csv:
        # Ensure Data directory exists
        os.makedirs("Data", exist_ok=True)
        
        df_historical_weather.to_csv(weather_filename, index=False)
        df_historical_aqi.to_csv(aqi_filename, index=False)
        print(f"Saved weather data to: {weather_filename}")
        print(f"Saved AQI data to: {aqi_filename}")

    return df_historical_weather, df_historical_aqi, weather_filename, aqi_filename

def get_city_info(city, state):
    url = f'https://geocoding-api.open-meteo.com/v1/search?name={city}&count=100'
    response = requests.get(url)
    data = response.json()
    filtered_data = [item for item in data['results'] if item['country_code'] == 'US' and item['admin1'] == state]
    if not filtered_data:
        raise ValueError(f"No results found for {city}, {state}")
    info = filtered_data[0]
    return info['latitude'], info['longitude'], info['timezone'], info.get('population', None)


def get_city_zone_info():
    # Define city/state/zone info
    city_data = [
        # NEISO
        {"city": "Portland", "state": "Maine", "zone": ".Z.MAINE"},
        {"city": "Manchester", "state": "New Hampshire", "zone": ".Z.NEWHAMPSHIRE"},
        {"city": "Burlington", "state": "Vermont", "zone": ".Z.VERMONT"},
        {"city": "Providence", "state": "Rhode Island", "zone": ".Z.RHODEISLAND"},
        {"city": "Bridgeport", "state": "Connecticut", "zone": ".Z.CONNECTICUT"},
        {"city": "Brockton", "state": "Massachusetts", "zone": ".Z.WCMASS"},
        {"city": "Springfield", "state": "Massachusetts", "zone": ".Z.SEMASS"},
        {"city": "Boston", "state": "Massachusetts", "zone": ".Z.NEMASSBOST"},
        # NYISO
        {"city": "Buffalo", "state": "New York", "zone": "west"},
        {"city": "Rochester", "state": "New York", "zone": "genese"},
        {"city": "Syracuse", "state": "New York", "zone": "centrl"},
        {"city": "Plattsburgh", "state": "New York", "zone": "north"},
        {"city": "Utica", "state": "New York", "zone": "mhk_vl"},
        {"city": "Albany", "state": "New York", "zone": "capitl"},
        {"city": "Poughkeepsie", "state": "New York", "zone": "hud_vl"},
        {"city": "White Plains", "state": "New York", "zone": "millwd"},
        {"city": "Yonkers", "state": "New York", "zone": "dunwod"},
        {"city": "New York City", "state": "New York", "zone": "nyc"},
        {"city": "Hempstead", "state": "New York", "zone": "longil"},
        # CAISO
        {"city": "San Jose", "state": "California", "zone": None},
        {"city": "Los Angeles", "state": "California", "zone": None},
        {"city": "Truckee", "state": "California", "zone": None},
        {"city": "Fresno", "state": "California", "zone": None},
        {"city": "Sacramento", "state": "California", "zone": None},
        {"city": "Redding", "state": "California", "zone": None},
    ]

    records = []
    for entry in city_data:
        try:
            lat, lon, tz, pop = get_city_info(entry["city"], entry["state"])
            
            # Fetch historical weather and AQ data for each city
            if lat is not None and lon is not None and tz is not None:
                try:
                    weather_df, aqi_df, weather_filename, aqi_filename = get_historical_hourly_data(
                        lat, lon, tz, entry["city"], entry["state"]
                    )
                except Exception as e:
                    print(f"Error fetching weather/AQ data for {entry['city']}, {entry['state']}: {e}")
                    weather_filename = None
                    aqi_filename = None
            else:
                weather_filename = None
                aqi_filename = None
                
        except Exception as e:
            print(f"Error getting city info for {entry['city']}, {entry['state']}: {e}")
            lat, lon, tz, pop = None, None, None, None
            weather_filename = None
            aqi_filename = None
            
        records.append({
            "city": entry["city"],
            "state": entry["state"],
            "zone": entry["zone"],
            "latitude": lat,
            "longitude": lon,
            "timezone": tz,
            "population": pop,
            "weather_filename": weather_filename,
            "aqi_filename": aqi_filename
        })

    df = pd.DataFrame(records)
    df.to_csv("../city_zone_info.csv", index=False)
    print("City zone info saved to Data/city_zone_info.csv")


def main():
    get_city_zone_info()

if __name__ == "__main__":
    main()
