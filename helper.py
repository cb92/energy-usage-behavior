# from openmeteo_sdk.Unit import Unit
# from openmeteo_sdk.Variable import Variable
import pandas as pd
# import numpy as np

import requests
# import datetime as dt
# from datetime import date
# import openmeteo_requests

# import os
# import hashlib
# import json

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
        except Exception as e:
            print(e)
            lat, lon, tz, pop = None, None, None, None
        records.append({
            "city": entry["city"],
            "state": entry["state"],
            "zone": entry["zone"],
            "latitude": lat,
            "longitude": lon,
            "timezone": tz,
            "population": pop
        })

    df = pd.DataFrame(records)
    df.to_csv("Data/city_zone_info.csv", index=False)


def main():
    get_city_zone_info()

if __name__ == "__main__":
    main()
