
import os
from datetime import date
from gridstatusio import GridStatusClient

# Set up date range
start_date = "2021-01-01"
end_date = date.today().strftime("%Y-%m-%d")

# Ensure output directory exists
output_dir = "../GridStatusIO"
os.makedirs(output_dir, exist_ok=True)

# Recommended: set GRIDSTATUS_API_KEY as an environment variable instead of hardcoding
client = GridStatusClient("a40502a0863e463984dae0439b84759e")

# List of datasets and their parameters
datasets = [
    # {
    #     "dataset": "isone_zonal_load_real_time_hourly",
    #     "params": {"timezone": "GMT"},
    #     "filename": "isone_zonal_load_real_time_hourly.csv"
    # },
    # {
    #     "dataset": "isone_reliability_region_load_forecast",
    #     "params": {"publish_time": "latest", "timezone": "GMT"},
    #     "filename": "isone_reliability_region_load_forecast.csv"
    # # },
    # {
    #     "dataset": "isone_lmp_real_time_hourly_final",
    #     "params": {"timezone": "GMT"},
    #     "filename": "isone_lmp_real_time_hourly_final.csv"
    # },
    {
        "dataset": "isone_fuel_mix",
        "params": {"timezone": "GMT"},
        "filename": "isone_fuel_mix.csv"
    },
    {
        "dataset": "nyiso_load",
        "params": {"timezone": "GMT"},
        "filename": "nyiso_load.csv"
    },
    {
        "dataset": "nyiso_zonal_load_forecast_hourly",
        "params": {"publish_time": "latest", "timezone": "GMT"},
        "filename": "nyiso_zonal_load_forecast_hourly.csv"
    },
    # {
    #     "dataset": "nyiso_lmp_real_time_15_min",
    #     "params": {"timezone": "GMT"},
    #     "filename": "nyiso_lmp_real_time_15_min.csv"
    # },
    {
        "dataset": "nyiso_fuel_mix",
        "params": {"timezone": "GMT"},
        "filename": "nyiso_fuel_mix.csv"
    },
    {
        "dataset": "caiso_standardized_hourly",
        "params": {"timezone": "GMT"},
        "filename": "caiso_standardized_hourly.csv"
    },
]

for entry in datasets:
    params = entry["params"].copy()
    params["start"] = start_date
    params["end"] = end_date
    print(f"Fetching {entry['dataset']} from {start_date} to {end_date} ...")
    df = client.get_dataset(
        dataset=entry["dataset"],
        **params
    )
    output_path = os.path.join(output_dir, entry["filename"])
    df.to_csv(output_path, index=False)
    print(f"Saved to {output_path}")