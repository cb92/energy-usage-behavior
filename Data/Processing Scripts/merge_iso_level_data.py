import pandas as pd
import numpy as np

# Load merged ISONE and NYISO data
df_isone = pd.read_csv("../Merged/merged_isone_causal.csv", parse_dates=["datetime_utc"])
df_nyiso = pd.read_csv("../Merged/merged_nyiso_causal.csv", parse_dates=["datetime_utc"])

# Load fuel mix data
df_isone_fuel = pd.read_csv("../GridStatusIO/isone_fuel_mix.csv", parse_dates=["interval_start_utc"])
df_nyiso_fuel = pd.read_csv("../GridStatusIO/nyiso_fuel_mix.csv", parse_dates=["interval_start_utc"])

# Columns to aggregate (weighted average by regional_percentage)
weather_aqi_cols = [
    "temperature_2m__celsius",
    "dew_point_2m__celsius",
    "rain__millimetre",
    "snowfall__centimetre",
    "cloud_cover__percentage",
    "wind_speed_10m__kilometres_per_hour",
    "pm2_5__micrograms_per_cubic_metre"
]

def aggregate_iso(df, group_col="datetime_utc"):
    # Only keep rows with non-null regional_percentage
    df = df[df["regional_percentage"].notnull()]
    agg_dict = {}
    for col in weather_aqi_cols:
        agg_dict[col] = lambda x, col=col: np.average(x, weights=df.loc[x.index, "regional_percentage"])
    agg_dict["load"] = "sum"
    agg_dict["load_forecast"] = "sum"
    agg_dict["regional_percentage"] = "sum"  # for reference, should be 1.0
    return (
        df.groupby(group_col)
        .agg(agg_dict)
        .reset_index()
    )

# Aggregate ISONE and NYISO
df_isone_agg = aggregate_iso(df_isone)
df_nyiso_agg = aggregate_iso(df_nyiso)

# Add load_to_forecast_diff
df_isone_agg["load_to_forecast_diff"] = df_isone_agg["load"] - df_isone_agg["load_forecast"]
df_nyiso_agg["load_to_forecast_diff"] = df_nyiso_agg["load"] - df_nyiso_agg["load_forecast"]

# Join with fuel mix data
df_isone_agg = pd.merge(
    df_isone_agg,
    df_isone_fuel,
    left_on="datetime_utc",
    right_on="interval_start_utc",
    how="inner"
)
df_nyiso_agg = pd.merge(
    df_nyiso_agg,
    df_nyiso_fuel,
    left_on="datetime_utc",
    right_on="interval_start_utc",
    how="inner"
)

# Carbon intensity values (g CO2 eq per kWh)
# source: https://en.wikipedia.org/wiki/Emission_intensity
# solar: assumes 70/30 split between grid scale (22) and rooftop (46)
CARBON_INTENSITY = {
    "coal": 1001,
    "hydro": 4,
    "landfill_gas": 469,
    "natural_gas": 469,
    "nuclear": 16,
    "oil": 893,
    "other": 600,
    "refuse": 469,
    "solar": 29,
    "wind": 12,
    "wood": 230,
    "dual_fuel": 600,
    "other_fossil_fuels": 600,
    "other_renewables": 30,
}

def carbon_intensity_row_isone(row):
    total = 0
    total_mw = 0
    for col in [
        "coal", "hydro", "landfill_gas", "natural_gas", "nuclear", "oil",
        "other", "refuse", "solar", "wind", "wood"
    ]:
        mw = row.get(col, 0)
        ci = CARBON_INTENSITY.get(col, 0)
        total += mw * ci
        total_mw += mw
    return total / total_mw if total_mw > 0 else np.nan

def carbon_intensity_row_nyiso(row):
    total = 0
    total_mw = 0
    for col in [
        "dual_fuel", "hydro", "natural_gas", "nuclear", "other_fossil_fuels",
        "other_renewables", "wind"
    ]:
        mw = row.get(col, 0)
        ci = CARBON_INTENSITY.get(col, 0)
        total += mw * ci
        total_mw += mw
    return total / total_mw if total_mw > 0 else np.nan

df_isone_agg["carbon_intensity__gco2eq_per_kwh"] = df_isone_agg.apply(carbon_intensity_row_isone, axis=1)
df_nyiso_agg["carbon_intensity__gco2eq_per_kwh"] = df_nyiso_agg.apply(carbon_intensity_row_nyiso, axis=1)

# Save results
df_isone_agg.to_csv("../Merged/iso_level_isone_agg.csv", index=False)
df_nyiso_agg.to_csv("../Merged/iso_level_nyiso_agg.csv", index=False)