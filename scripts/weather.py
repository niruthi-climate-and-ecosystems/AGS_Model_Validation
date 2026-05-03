import os
from functools import lru_cache
import requests
import xarray as xr
import traceback
from io import BytesIO


class weather:
    def __init__(self, file_url: str, latitude: float, longitude: float) -> None:
        self.file = file_url
        self.lat = latitude
        self.long = longitude

    @staticmethod
    @lru_cache(maxsize=5)  # cache up to 5 files
    def load_dataset(file_url: str):
        print("Loading dataset (only once per file)...")
        content = requests.get(file_url, stream=True).content
        return xr.open_dataset(BytesIO(content), engine="scipy").load()

    def get_data(self) -> dict:
        try:
            self.weather = self.load_dataset(self.file)

            ds = self.weather.sel(Latitude=self.lat, Longitude=self.long, method='nearest')

            rain = ds['Rainfall'].resample(Date_time="W")
            temp_min = ds['Tmin'].resample(Date_time="W").mean().values[0]
            temp_max = ds['Tmax'].resample(Date_time="W").mean().values[0]
            rh_min = ds['RH_min'].resample(Date_time="W").mean().values[0]
            rh_max = ds['RH_max'].resample(Date_time="W").mean().values[0]

            return dict(
                rain_sum=rain.sum().values[0],
                temp_min=temp_min,
                temp_max=temp_max,
                rh_min=rh_min,
                rh_max=rh_max
            )

        except Exception as e:
            print(f"⚠️ Could not open file: {self.file}\n{e}")
            traceback.print_exc()
            return dict(rain_sum=0, temp_min=0, temp_max=0, rh_min=0, rh_max=0)