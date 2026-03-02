<<<<<<< HEAD
## Weekly = True for weekly advisory
## Weekly = False for daily adviosry

IS_WEEKLY = True

import os
from pandas import *
import json
from datetime import datetime,timedelta,date
import numpy as np
import sys
import traceback
import warnings
import tqdm
import requests
import fsspec
from io import BytesIO

warnings.filterwarnings("ignore")

from fastapi import FastAPI
from fastapi.responses import JSONResponse
import math
import xarray as xr

app = FastAPI()

from pydantic import BaseModel
from typing import List, Dict, Any

class WeeklyAdvisoryRequest(BaseModel):
    season: str
    crop_name: str
    sowing_date: str
    current_date: str
    weather_json: List[Dict]
    weather_input: str = "manual"
    lat: float = 21.44
    lon: float = 85.15
    elevation: int = 100

@app.post("/validate/weekly_advisory/")
async def crop_advisory(request: WeeklyAdvisoryRequest):

    if request.elevation is None:
        request.elevation = 100

    if request.current_date is None:
        request.current_date = datetime.now().strftime("%Y-%m-%d")

    print(
        "current date, sowing date:",
        request.season,
        request.crop_name,
        request.sowing_date,
        request.lat,
        request.lon,
        request.current_date,
        request.elevation
    )

    get_response = weekly_adviosry(
        season=request.season,
        crop_name=request.crop_name,
        sowing_date=request.sowing_date,
        latitude=request.lat,
        longitude=request.lon,
        elevation=request.elevation,
        current_date=datetime.strptime(request.current_date, "%Y-%m-%d"),
        weather_input=request.weather_input,
        weather_dict=request.weather_json,
    )

    raw_data = get_response.generate()
    cleaned_data = clean_nan(raw_data)
    print("cleaned data: ",type(cleaned_data))

    return cleaned_data.to_json()


# @app.post("/validate/weekly_advisory/")
# async def crop_advisory(
#     season: str,
#     crop_name: str,
#     sowing_date: str,
#     current_date: str,
#     weather_json: dict,
#     weather_input = "manual",
#     lat: float=21.44,
#     lon: float=85.15,
#     elevation: int=100, #| None = None,
#      #| None = None
# ):
#     """
#     season: str,\n
#     crop_name: str,\n
#     sowing_date: "YYYY-MM-DD",\n
#     current_date: "YYYY-MM-DD"\n
#     """
#     # Default fallbacks
#     if elevation is None:
#         elevation = 100
#     if current_date is None:
#         current_date = datetime.now().strftime("%Y-%m-%d")

#     print("current date, sowing date:", season, crop_name, sowing_date, lat, lon, current_date,elevation)

#     # Call advisory function
#     get_response = weekly_adviosry(
#         season=season,
#         crop_name=crop_name,
#         sowing_date=sowing_date,
#         latitude=lat,
#         longitude=lon,
#         elevation=elevation,
#         current_date=datetime.strptime(current_date,"%Y-%m-%d"),
#         weather_input=weather_input,
#         weather_dict=weather_json,
#     )

#     raw_data = get_response.generate()
#     print(raw_data)
#     cleaned_data = clean_nan(raw_data)

#     # Convert DataFrame → dict for valid JSON
#     data_dict = json.loads(cleaned_data.to_json())#type:ignore

#     return JSONResponse(content=data_dict, status_code=200)

def clean_nan(obj):
    if isinstance(obj, float) and math.isnan(obj):
        return None
    elif isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan(i) for i in obj]
    return obj

def get_config():
    return json.load(open("scripts/setup/input_config.json"))

def get_crop_attributes_bydate(input_date:datetime,crop_db:DataFrame)->DataFrame:
    """
    To get attributes in non-growing season of crop, \ntakes date as input to fetch the adviosry content
    """
    # day_of_month = input_date.timetuple().tm_mday
    # month_of_year = input_date.timetuple().tm_mon
    current_year = input_date.year
    fixed_df = crop_db[crop_db['advisory_type']=='Fixed']
    date_time_values = list(zip(fixed_df.day_of_month,fixed_df.month_of_year))
    fixed_df['Date_col'] = [datetime(year=current_year,month=int(i[1]),day=int(i[0])) for i in date_time_values]
    fixed_df['date_difference'] = abs((fixed_df["Date_col"] - input_date)).apply(lambda x: x.days) # type: ignore
    # print(fixed_df[['advisory_title','date_difference','day_of_month','month_of_year']])
    # return crop_db[(crop_db['day_of_month']==day_of_month)&(crop_db['month_of_year']==month_of_year)]
    return fixed_df.loc[fixed_df['date_difference']==min(fixed_df['date_difference'])]

def get_crop_attributes_byweek(start_date:datetime,end_date:datetime,crop_db:DataFrame)->DataFrame:
    """
    Takes week (float) as input,\n filters andr returns crop_db dataframe by its crop week
    """
    crop_week = (end_date.timetuple().tm_yday-start_date.timetuple().tm_yday)/7
    return crop_week,crop_db.loc[(crop_db['cropstage_week_start']<crop_week)&(crop_db['cropstage_week_end']>=crop_week)].copy(deep=True) # type: ignore

class dynamic_classes:
    #advisory_classes = ["Generic","Variety_selection","Nursery","Pest","Irrigation"]
    def __init__(self,index_filtered_db:DataFrame,elevation:str|None,sowing_date:datetime,advisory_date:datetime,
                 latitude:float,longitude:float,config,weather_dict:List[Dict[str, Any]] ,weather_input='Forecast')->None:
        """
        weather_input = 'forecast' or 'manual', default value = ['forecast'], if 'manual' is selected, user is expected to provide weather input
        """
        self.database = index_filtered_db.copy(deep=True)
        self.elevation = elevation
        self.advisory_date = advisory_date
        self.sowing_date = sowing_date
        self.lat = latitude
        self.long = longitude
        self.config = config
        self.weather_input = weather_input
        self.weather_dict = weather_dict
    def manual_weather(self):
        df = DataFrame(self.weather_dict)
        # print(df)
        rain_sum = df["Rainfall (mm)"].sum()
        tmin = df["Tmin (°C)"].mean()
        tmax = df["Tmax (°C)"].mean()
        rh_min = df["RH_min (%)"].mean()
        rh_max = df["RH_max (%)"].mean()
        return dict(rain_sum=rain_sum, temp_min=tmin, temp_max=tmax, rh_min=rh_min, rh_max=rh_max)

    def generate(self):
        # BASED ON MANUAL INPUT
        if self.weather_input == "Manual":
            if self.database['advisory_class'].values[0] == "Pest":
                print("checking in pest...")
                if len(self.database)==1:
                    infestsnap_info = infestsnap(state_name=self.database['state'].values[0],
                                                crop_name=self.database['crop_name'].values[0],
                                                lat=self.lat,lon=self.long,
                                                sowing_date=self.sowing_date,season=self.database['season'].values[0],config=self.config).request()
                    adv_content = format_text(pest_filepath=self.config['pest_info'],season=self.database['season'].values[0],crop=self.database['crop_name'].values[0],infestsnap_result=infestsnap_info)
                    self.database['advisory_content'] = adv_content
                    return self.database.to_dict(orient="records")
                else:
                    raise IndexError("\n Expecting one row in the database passed, received more than one row.")
            if self.database['advisory_class'].values[0] == "Variety_selection":
                if isinstance(self.elevation,int):
                    if self.elevation <= 200:
                        land_type = "low land"
                    elif self.elevation >200 and self.elevation<= 800:
                        land_type = "medium land"
                    else:
                        land_type = "up land"
                return self.database.loc[self.database['landtype']==land_type].to_dict(orient='records')
            if self.database['advisory_class'].values[0] == "Nursery":
                try:
                    forecast_data = forecast_data = dynamic_classes.manual_weather(self)
                    # Weather Probability
                    advisory_probability = probability(df=self.database,weather_dict=forecast_data,config=self.config).get_probability()
                    matching_record = self.database.iloc[np.where(advisory_probability==advisory_probability.max())[0],:].to_dict(orient='records')
                    return matching_record

                except Exception as e:
                    traceback.print_exc()
                    print("\n Exception has occured in Dynamic-Nursery advisory class.\n",e)
                    sys.exit()

            if self.database['advisory_class'].values[0] == "Irrigation":
                try:
                    forecast_data = dynamic_classes.manual_weather(self)
                    # If forecast_data is empty (file missing), handle gracefully
                    if not forecast_data:
                        return []  # or return some default advisory record

                    advisory_probability = probability(df=self.database,weather_dict=forecast_data,config=self.config).get_probability()
                    matching_record = self.database.iloc[np.where(advisory_probability==advisory_probability.max())[0],:].to_dict(orient="records")
                    return matching_record
                except Exception as e:
                    traceback.print_exc()
                    print("\n Exception occured in Dynamic- Irrigation adviosry class. \n",e)
                    sys.exit()
            else:    
                try:
                    forecast_data = dynamic_classes.manual_weather(self)
                    print("Manual Input Data: \n",forecast_data,"\n")
                    # If forecast_data is empty (file missing), handle gracefully
                    if not forecast_data:
                        return []  # or return some default advisory record

                    advisory_probability = probability(df=self.database,weather_dict=forecast_data,config=self.config).get_probability()
                    matching_record = self.database.iloc[np.where(advisory_probability==advisory_probability.max())[0],:].to_dict(orient="records")
                    return matching_record
                except Exception as e:
                    traceback.print_exc()
                    print(dynamic_classes.manual_weather(self))
                    print("\n Exception occured in Dynamic- Manual weather input section. \n",e)
                    sys.exit()
        
        # Based on FORECASTED DATA
        if self.weather_input == "Forecast":
            if self.database['advisory_class'].values[0] == "Variety_selection":
                if isinstance(self.elevation,int):
                    if self.elevation <= 200:
                        land_type = "low land"
                    elif self.elevation >200 and self.elevation<= 800:
                        land_type = "medium land"
                    else:
                        land_type = "up land"
                return self.database.loc[self.database['landtype']==land_type].to_dict(orient='records')
            if self.database['advisory_class'].values[0] == "Nursery":
                try:
                    # read file
                    wth_folder = self.advisory_date.strftime("%Y%m%d")
                    wth_file_path = os.path.join(self.config['weather_dir'],os.path.join(f"{wth_folder}",f"{wth_folder}_daily.nc"))
                    # wth_file_url = f"https://nc.niruthi.in/ncfiles/{wth_folder}/{wth_folder}_daily.nc"  
                    forecast_data = weather(file_url=wth_file_path,latitude=self.lat,longitude=self.long).get_data()
                    # Weather Probability
                    advisory_probability = probability(df=self.database,weather_dict=forecast_data,config=self.config).get_probability()
                    matching_record = self.database.iloc[np.where(advisory_probability==advisory_probability.max())[0],:].to_dict(orient='records')
                    return matching_record

                except Exception as e:
                    traceback.print_exc()
                    print("\n Exception has occured in Dynamic-Nursery advisory class.\n",e)
                    sys.exit()

            if self.database['advisory_class'].values[0] == "Irrigation":
                try:
                    # read weather file
                    wth_folder = self.advisory_date.strftime("%Y%m%d")
                    wth_file_path = os.path.join(self.config['weather_dir'],os.path.join(f"{wth_folder}",f"{wth_folder}_daily.nc"))
                    # wth_file_url = f"http://nc.niruthi.in/ncfiles/{wth_folder}/{wth_folder}_daily.nc"          
                    # Weather Probability
                    forecast_data = weather(file_url=wth_file_path,latitude=self.lat,longitude=self.long).get_data()
                    # If forecast_data is empty (file missing), handle gracefully
                    if not forecast_data:
                        print(f"No weather data available for {wth_folder}, skipping advisory.")
                        return []  # or return some default advisory record

                    advisory_probability = probability(df=self.database,weather_dict=forecast_data,config=self.config).get_probability()
                    matching_record = self.database.iloc[np.where(advisory_probability==advisory_probability.max())[0],:].to_dict(orient="records")
                    return matching_record
                except Exception as e:
                    traceback.print_exc()
                    print("\n Exception occured in Dynamic- Irrigation adviosry class. \n",e)
                    sys.exit()

            if self.database['advisory_class'].values[0] == "Pest":
                print("checking in pest...")
                if len(self.database)==1:
                    infestsnap_info = infestsnap(state_name=self.database['state'].values[0],
                                                crop_name=self.database['crop_name'].values[0],
                                                lat=self.lat,lon=self.long,
                                                sowing_date=self.sowing_date,season=self.database['season'].values[0],config=self.config).request()
                    adv_content = format_text(pest_filepath=self.config['pest_info'],season=self.database['season'].values[0],crop=self.database['crop_name'].values[0],infestsnap_result=infestsnap_info)
                    self.database['advisory_content'] = adv_content
                    return self.database.to_dict(orient="records")
                else:
                    raise IndexError("\n Expecting one row in the database passed, received more than one row.")
                    # sys.exit()

class infestsnap:
    def __init__(self,state_name:str,crop_name:str,lat:float,lon:float,sowing_date:datetime,season:str,config:dict):
        self.state = state_name
        self.crop = crop_name
        self.lat = lat
        self.lon = lon
        self.sowing_date = sowing_date.strftime("%d-%m-%Y")
        self.season =  season
        self.config = config

    def request(self):
        import subprocess, shlex
        print("sowing date: ",self.sowing_date)
        # infestsnap_request_url = f'https://infsnewapi.niruthiapptesting.com/gfs/pest_info?lat={"%.2f"%self.lat}&lon={"%.2f"%self.lon}'
        # Manish - GFS
        infestsnap_request_url = f'https://ksh-gfs.niruthiapptesting.com/pest-info-new/?lat={self.lat}&lon={self.lon}&is_next_week=true'
        # infestsnap_request_url = f'http://150.241.244.210:8070/gfs/pest_info?lat={"%.2f"%self.lat}&lon={"%.2f"%self.lon}'
        _headers =  'Content-Type : application/json'
        _data = {"state_name": self.state, "sowing_date": self.sowing_date,"crop_name": self.crop}
        # Reference
        # data_ = { "state_name": "Mizoram", "sowing_date": "30-04-2025","crop_name": "Paddy"}
        curl_req = f"curl --resolve --location {infestsnap_request_url} --header 'Content-Type: application/json' --data '{json.dumps(_data)}'"
        print("InfestSnap Request Command: ",curl_req)
        infest_data = subprocess.Popen(shlex.split(curl_req),stdin=subprocess.PIPE,stdout=subprocess.PIPE).communicate()
        # print("printing infestsanp data: *****:::",infest_data[0])
        i_out,i_err = infest_data     
        result = json.loads(i_out.decode("utf-8"))
        # print("InfestSnap Response: ",result)
        if len(result.keys())>0:
            return result
        else:
            raise Exception("No data to return...",json.loads(i_err.decode("utf-8")))
    
def format_text(pest_filepath,season,crop,infestsnap_result): #type:ignore
    try:
        pest_info = read_csv(pest_filepath,encoding="utf-8-sig")
    except UnicodeDecodeError:
        pest_info = read_csv(pest_filepath,encoding='windows-1252')
    pest_info = pest_info[(pest_info['season']==season)&(pest_info['crop']==crop)]
    pest_info['Name of Disease and Insect'] = [i.strip() for i in pest_info['Name of Disease and Insect']]
    try:
        infestation_dict = infestsnap_result['data']
        infest_adv_combined = ""
        for inf in infestation_dict:
            # print("INF: ",inf)
            # if inf['infestation_level'] != "low":
            pest_adviosry = pest_info[pest_info['Name of Disease and Insect']==inf['pest_name'].strip()]['Advisory'].values
            # infest_text = f"Pest:{inf['pest_name']}, Infestation_level:{inf['infestation_level']}, Recommendation:{pest_adviosry}.; \n " ## infestsanp api response
            infest_text = f"Pest:{inf['pest_name']}, Infestation_level:{inf['chances']['current_week']['infestation_level']}, Advisory:{pest_adviosry}.; \n " ## BMGF API Response
            # print("\n",infest_text)

            infest_adv_combined = infest_adv_combined+infest_text
        return infest_adv_combined
    except Exception as e:
        traceback.print_exc()
        print("\n",e,infestsnap_result)
        
class non_growing_stage:
    def __init__(self,latitude:float,longitude:float,adviosry_date:datetime,crop_db:DataFrame,unique_id:int = 0):
        self.lat = latitude
        self.lon = longitude
        self.uid = unique_id
        self.adv_date = adviosry_date
        self.database = crop_db.copy(deep=True)
    
    def generate(self,**kwargs):
        crop_data = get_crop_attributes_bydate(input_date=self.adv_date,crop_db=self.database)
        return crop_data

class probability:
    """
    Provides functionality to calculate probability of weather parameters for the given conditions in crop database to fetch adviosry
    """
    def __init__(self,df:DataFrame,weather_dict:dict,config)->None:
        self.df = df.copy()
        self.weather = weather_dict
        self.config = config
        self.prob_parms = config['prabability_parameters']

    def get_probability(self)->np.ndarray:
        # filter columns by matching params
        col_filtered_df = self.df[self.prob_parms].to_numpy()
        forcasted_weather = list(self.weather.values())  
        abs_difference = np.absolute(np.subtract(col_filtered_df,forcasted_weather))
        # print("absolute_difference: ",abs_difference)
        get_diff_percent = np.divide(abs_difference,col_filtered_df)
        # print("difference_percent: ",get_diff_percent)
        get_diff_percent[np.isnan(get_diff_percent)]=0
        get_diff_percent[np.isinf(get_diff_percent)]=0
        #applying high weightage to rainfall based on crop season
        if self.df['season'].values[0]=="Kharif":
            weight = np.array([1.25,1.1875,1.1875,1.1875,1.1875])
            get_diff_percent = 1- np.divide(np.multiply(get_diff_percent,weight),len(self.prob_parms)+1)
        else:
            get_diff_percent = 1- np.divide(get_diff_percent,len(self.prob_parms))
        probability_percent = np.array(np.sum(1-get_diff_percent,axis=1)).flatten(order="C")
        return probability_percent


# class weather:
#     """
#     Provides functions to do operations related to weather data.
#     """
#     def __init__(self,file_path:str,latitude:float,longitude:float)->None:
#         self.file = file_path
#         self.lat = latitude
#         self.long = longitude

#     def get_data(self)->dict:
#         """
#         Fetchs weekly resampled weather parameter values
#         """
#         print("247weather_file: ",self.file)
#         import xarray as xr
#         self.weather = xr.open_mfdataset(self.file)
#         self.rain_min = self.weather['Rainfall'].sel(Latitude=self.lat,Longitude=self.long,method='nearest').resample(Date_time="W").min().values[0]
#         self.rain_max = self.weather['Rainfall'].sel(Latitude=self.lat,Longitude=self.long,method='nearest').resample(Date_time="W").max().values[0]
#         self.rain_avg = self.weather['Rainfall'].sel(Latitude=self.lat,Longitude=self.long,method='nearest').resample(Date_time="W").mean().values[0]
#         self.rain_sum = self.weather['Rainfall'].sel(Latitude=self.lat,Longitude=self.long,method='nearest').resample(Date_time="W").sum().values[0]
#         self.temp_min = self.weather['Tmin'].sel(Latitude=self.lat,Longitude=self.long,method='nearest').resample(Date_time="W").mean().values[0]
#         self.temp_max = self.weather['Tmax'].sel(Latitude=self.lat,Longitude=self.long,method='nearest').resample(Date_time="W").mean().values[0]
#         self.rh_min = self.weather['RH_min'].sel(Latitude=self.lat,Longitude=self.long,method ='nearest').resample(Date_time="W").mean().values[0]
#         self.rh_max = self.weather['RH_max'].sel(Latitude=self.lat,Longitude=self.long,method='nearest').resample(Date_time="W").mean().values[0]
       
#         self.weather.close()
#         # print(dict(rain_sum=self.rain_sum,temp_min=self.temp_min,
#         #             temp_max=self.temp_max,rh_min=self.rh_min,rh_max=self.rh_max))
#         return dict(rain_sum=self.rain_sum,temp_min=self.temp_min,
#                     temp_max=self.temp_max,rh_min=self.rh_min,rh_max=self.rh_max)

class weather:
    def __init__(self, file_url: str, latitude: float, longitude: float) -> None:

        # self.file = requests.get(file_url) # if it is Hosted NAS Access
        self.file = file_url # if accessing from Local NAS
        self.lat = latitude
        self.long = longitude

    def get_data(self) -> dict:
        print("weather_file in get data: ", self.file)
        try:
            # self.weather = xr.open_dataset(BytesIO(requests.get(self.file,stream=True).content), engine="scipy") # if it is Hosted NAS Access
            self.weather = xr.open_dataset(self.file)  # if accessing from Local NAS
            self.rain_min = self.weather['Rainfall'].sel(Latitude=self.lat, Longitude=self.long, method='nearest').resample(Date_time="W").min().values[0]
            self.rain_max = self.weather['Rainfall'].sel(Latitude=self.lat, Longitude=self.long, method='nearest').resample(Date_time="W").max().values[0]
            self.rain_avg = self.weather['Rainfall'].sel(Latitude=self.lat, Longitude=self.long, method='nearest').resample(Date_time="W").mean().values[0]
            self.rain_sum = self.weather['Rainfall'].sel(Latitude=self.lat, Longitude=self.long, method='nearest').resample(Date_time="W").sum().values[0]
            self.temp_min = self.weather['Tmin'].sel(Latitude=self.lat, Longitude=self.long, method='nearest').resample(Date_time="W").mean().values[0]
            self.temp_max = self.weather['Tmax'].sel(Latitude=self.lat, Longitude=self.long, method='nearest').resample(Date_time="W").mean().values[0]
            self.rh_min = self.weather['RH_min'].sel(Latitude=self.lat, Longitude=self.long, method='nearest').resample(Date_time="W").mean().values[0]
            self.rh_max = self.weather['RH_max'].sel(Latitude=self.lat, Longitude=self.long, method='nearest').resample(Date_time="W").mean().values[0]

            self.weather.close()
            print(dict(rain_sum=self.rain_sum, temp_min=self.temp_min,
                        temp_max=self.temp_max, rh_min=self.rh_min, rh_max=self.rh_max))
            return dict(rain_sum=self.rain_sum, temp_min=self.temp_min,
                        temp_max=self.temp_max, rh_min=self.rh_min, rh_max=self.rh_max)
        except Exception as e:
            print(f"⚠️ Could not open file: {self.file}\n{e}")
            sys.exit()
            return dict(rain_sum=0, temp_min=0, temp_max=0, rh_min=0, rh_max=0)

def get_advisory_index(start_date:datetime,end_date:datetime,crop_db:DataFrame)->int:
    """
    Caluclates the index of the advisory to be sent, by calculating the weeks from swoing date to current date,\n
    then by getting number of adviosries in the filtered crop stage,\n
    then calculates the index of the advisory to desiminated in the filtered dataframe.\n
    Parameters:\n
    i_doy = intial day of the year(sowing_date),\n
    e_day = current day of the year (current_date),\n
    woc = week of crop,\n
    c_stage_adv_len =  crop stage advisory length

    """
    crop_week = (end_date-start_date).days/7
    # print(start_date,end_date)
    # print("crop_week:",crop_week)
    c_stage_len = crop_db['cropstage_week_end'].values[0]-crop_db['cropstage_week_start'].values[0]
    # print("crop_stage len: ", c_stage_len)
    c_stage_adv_len = len(crop_db['advisory_index'].unique())
    # print("advisory length: ",c_stage_adv_len)
    advisory_per_day = (c_stage_len*7)/c_stage_adv_len
    # print("advisories per day: ", advisory_per_day)
    # print("Crop week: ",crop_week)
    remaining_days_in_c_stage = (crop_db['cropstage_week_end'].values[0] - crop_week)*7
    # print("remaining days in the crop stage: ",remaining_days_in_c_stage)
    current_timeline_of_c_stage = (c_stage_len*7)-remaining_days_in_c_stage
    index = int(current_timeline_of_c_stage//advisory_per_day)# if indexing issue ther do +1
    print("index: ",index)
    try:
        # print("advisory index",crop_db['advisory_index'].unique()[index])
        return crop_db['advisory_index'].unique()[index]
    except IndexError as e:
        # print("advisory index",crop_db['advisory_index'].unique()[index-1])
        return crop_db['advisory_index'].unique()[index-1]
class sowing_date:
    def __init__(self,config:dict,season:str,crop_name:str,village_uid:int|str):
        self.config = config
        self.season = season
        self.crop = crop_name
        self.UID = village_uid

    def get_predicted_date(self) -> dict|None:
        if self.season == "Kharif":
            if self.crop == "Paddy":
                sowing_dates = read_csv(self.config['Sowing_files']['Paddy_Kharif'])
                crop_sowing_dates = sowing_dates[sowing_dates['Crop_name']==self.crop]
            else:
                sowing_dates = read_csv(self.config['Sowing_files']['Other_Kharif'])
                crop_sowing_dates = sowing_dates[sowing_dates['Crop_name']==self.crop]
        elif self.season == "Rabi":
            sowing_dates = read_csv(self.config['Sowing_files']['All_Rabi'])
            crop_sowing_dates = sowing_dates[sowing_dates['Crop_name']==self.crop]
        del sowing_dates

        # Get Sowing_date of the crop using 
        try:
            village_sowing_date = crop_sowing_dates[crop_sowing_dates['Unq']==self.UID]['Sowing_date'].values[0]
            crop_sowing_date = village_sowing_date
        except Exception as e:
            print(Exception)
            crop_sowing_date = None
        return crop_sowing_date
def days_in_year(year: int) -> int:
    """
    Returns the total number of days in the given year.
    Handles leap years automatically.
    """
    if not isinstance(year, int) or year <= 0:
        raise ValueError("Year must be a positive integer.")

    # Jan 1 of the given year
    start = date(year, 1, 1)
    # Jan 1 of the next year
    end = date(year + 1, 1, 1)
    # Difference in days
    return (end - start).days
class weekly_adviosry:
    # def __init__(self,season,crop_name,sowing_date,latitude,longitude,elevation:int|None,current_date=datetime.today()+timedelta(days=-1),realtime=0):
    def __init__(self,season,crop_name,sowing_date,latitude,longitude,elevation:int|None,weather_input:str,weather_dict:List[Dict[str, Any]] ,current_date,realtime=0):

        self.season = season
        self.crop_name=crop_name
        self.sowing_date = datetime.strptime(sowing_date,"%Y-%m-%d")
        self.current_date = current_date
        self.lat = latitude
        self.lon = longitude
        self.elevation = elevation
        self.realtime = realtime
        self.weather_input = weather_input
        self.weather_dict = weather_dict
        self.config = json.load(open("input_config.json"))
    def generate(self):
        try:
            crop_calendar = read_csv(self.config['crop_calendar'],encoding="utf-8-sig")
        except UnicodeDecodeError:
            crop_calendar = read_csv(self.config['crop_calendar'],encoding="windows-1252")
        crop_calendar = crop_calendar[(crop_calendar['season']==self.season)&(crop_calendar['crop_name']==self.crop_name)]
        # pest_data = read_csv(self.config['pest_info'])
        advisory_dictionary=[]
        # calculate week of the crop from sowing date till current date 
        # Handling year difference causing issue

        if self.current_date.year == self.sowing_date.year:
            crop_week =  (self.current_date.timetuple().tm_yday - self.sowing_date.timetuple().tm_yday)/7
        elif self.current_date.year < self.sowing_date.year:
            crop_week = (-(abs((self.current_date.timetuple().tm_yday - days_in_year(self.current_date.year))) + self.sowing_date.timetuple().tm_yday))/7
        elif self.current_date.year > self.sowing_date.year:
            crop_week = (self.current_date.timetuple().tm_yday + (days_in_year(self.sowing_date.year) - self.sowing_date.timetuple().tm_yday))/7
            
        if isinstance(self.sowing_date,datetime) and crop_week>0:
                       
            print("current crop week: ", crop_week,f" \n Adviosry for week {crop_week+1} will be generated")

            
            #filter crop_calendar db by start and end week
            if IS_WEEKLY:
                week_calendar = crop_calendar.dropna(subset=['cropstage_week_start','cropstage_week_end'])[(crop_calendar['cropstage_week_start']<(crop_week))&(crop_calendar['cropstage_week_end']>=(crop_week))]           
                for i in week_calendar['advisory_index'].unique():
                    adv_df = week_calendar[week_calendar['advisory_index']==i]
                    if adv_df['advisory_type'].values[0]=="Dynamic":
                        advisory_data = dynamic_classes(index_filtered_db=adv_df,
                                                        elevation=100,  #type:ignore
                                                        sowing_date=self.sowing_date,
                                                        advisory_date=self.current_date,
                                                        latitude=self.lat,
                                                        longitude=self.lon,
                                                        config=self.config,
                                                        weather_dict=self.weather_dict,weather_input=self.weather_input
                                                        ).generate()
                        if isinstance(advisory_data,list):
                            advisory_dictionary.append(advisory_data[0])
                        elif isinstance(advisory_data,dict):
                            advisory_dictionary.append(advisory_data)
                        else:
                            # print(type(advisory_data))
                            advisory_dictionary.append(advisory_data.to_dict(orient="records")) #type:ignore

                    elif adv_df['advisory_type'].values[0]=="Standard":
                        advisory_data = adv_df
                        advisory_dictionary.append(advisory_data.to_dict(orient='records')[0])

                    else:
                        advisory_data = non_growing_stage(latitude=self.lat,longitude=self.lon,adviosry_date=self.current_date,crop_db=crop_calendar).generate()
                        advisory_dictionary.append(advisory_data.to_dict(orient="records")[0])
            else:
                week_calendar = crop_calendar.dropna(subset=['cropstage_week_start','cropstage_week_end'])[(crop_calendar['cropstage_week_start']<(crop_week))&(crop_calendar['cropstage_week_end']>=(crop_week))]
                advisory_index = get_advisory_index(start_date=self.sowing_date,end_date=self.current_date,crop_db=week_calendar)
                adv_df = week_calendar[week_calendar['advisory_index']==advisory_index]
                if adv_df['advisory_type'].values[0]=="Dynamic":
                        advisory_data = dynamic_classes(index_filtered_db=adv_df,
                                                        elevation=100,  #type:ignore
                                                        sowing_date=self.sowing_date,
                                                        advisory_date=self.current_date,
                                                        latitude=self.lat,
                                                        longitude=self.lon,
                                                        config=self.config,
                                                        weather_dict=self.weather_dict,weather_input=self.weather_input
                                                        ).generate()
                        if isinstance(advisory_data,list):
                            advisory_dictionary.append(advisory_data[0])
                        elif isinstance(advisory_data,dict):
                            advisory_dictionary.append(advisory_data)
                        else:
                            # print(type(advisory_data))
                            advisory_dictionary.append(advisory_data.to_dict(orient="records")) #type:ignore

                elif adv_df['advisory_type'].values[0]=="Standard":
                    advisory_data = adv_df
                    advisory_dictionary.append(advisory_data.to_dict(orient='records')[0])

                else:
                    advisory_data = non_growing_stage(latitude=self.lat,longitude=self.lon,adviosry_date=self.current_date,crop_db=crop_calendar).generate()
                    advisory_dictionary.append(advisory_data.to_dict(orient="records")[0])
                
        else:
            advisory_data = non_growing_stage(latitude=self.lat,longitude=self.lon,adviosry_date=self.current_date,crop_db=crop_calendar).generate()
            advisory_dictionary.append(advisory_data.to_dict(orient="records")[0])

        # DataFrame(advisory_dictionary).to_csv("test1.csv",index=False)
        return DataFrame(advisory_dictionary)


def main():
    input_config = json.load(open("input_config.json"))
    # print(input_config)
    print("in main weekly advisory")
    weekly_adviosry(season="Kharif",crop_name="Paddy",sowing_date=datetime(2025,7,1),latitude=24.56,longitude=82.45,elevation=100).generate()
    print("in main after....weekly advisory")
    # pass

if __name__ == "__main__":
    # main()
    import geopandas as gpd
    import fsspec
    from concurrent.futures import ThreadPoolExecutor

    block_shp = gpd.read_file(r"shapefile\Odisha_block_shapefile.shp")
    # block_shp = block_shp[block_shp['district_n']=="Kendrapada"]
    district_filter = ['Ganjam','Kalahandi','Cuttack','Kendrapada','Baleswar','Keonjhar','Koraput','Sundargarh','Mayurbhanja','Bargarh','Bolangir']

    block_shp = block_shp.loc[block_shp['district_n'].isin(district_filter)]
    block_shp['latitude'] = block_shp.centroid.y
    block_shp['longitude'] = block_shp.centroid.x
    crops = ['Blackgram','Paddy','Greengram','Potato','Mustard']
    season = "Rabi"
    sowing_date_file = read_csv(r"data\Predicted_Sowing_Date\All_Crops_Rabi_block_level.csv")
    empty_df = []
    for crop in crops:
        print("Crop_name: ",crop)
        for i,block in block_shp.iterrows():
            block_data = block.to_dict()
            print("Block_Data: ",block_data)
            sowing_date_ = sowing_date_file[(sowing_date_file['Crop_name']==crop)&(sowing_date_file['Unq']==block_data['Unq'])]
            sowing_dt = datetime.strptime(sowing_date_['Sowing_date'].values[0],"%Y-%m-%d").date()
            crop_adviosory_data = weekly_adviosry(season=season,crop_name=crop,sowing_date=sowing_date_['Sowing_date'].values[0],latitude=block_data['latitude'],longitude=block_data['longitude'],elevation=100,realtime=0,current_date=datetime.today()+timedelta(days=-1)).generate()
            crop_adviosory_data['District'] = block_data['district_n']
            crop_adviosory_data['Block'] = block_data['block_name']
            crop_adviosory_data['Adviosry_Date']= (datetime.today()+timedelta(days=-1)).strftime("%Y-%m-%d")
            crop_adviosory_data['Sowing_Date']= sowing_dt.strftime("%Y-%m-%d")
            crop_adviosory_data['Latitude'] = block_data['latitude']
            crop_adviosory_data['Longitude'] = block_data['longitude']
            empty_df.append(crop_adviosory_data)
    # for t in empty_df:
    #     print(t,type(t))
    df = concat(empty_df,ignore_index=False)
=======
## Weekly = True for weekly advisory
## Weekly = False for daily adviosry

IS_WEEKLY = True

import os
from pandas import *
import json
from datetime import datetime,timedelta,date
import numpy as np
import sys
import traceback
import warnings
import tqdm
import requests
import fsspec
from io import BytesIO

warnings.filterwarnings("ignore")

from fastapi import FastAPI
from fastapi.responses import JSONResponse
import math
import xarray as xr

app = FastAPI()

from pydantic import BaseModel
from typing import List, Dict, Any

class WeeklyAdvisoryRequest(BaseModel):
    season: str
    crop_name: str
    sowing_date: str
    current_date: str
    weather_json: List[Dict]
    weather_input: str = "manual"
    lat: float = 21.44
    lon: float = 85.15
    elevation: int = 100

@app.post("/validate/weekly_advisory/")
async def crop_advisory(request: WeeklyAdvisoryRequest):

    if request.elevation is None:
        request.elevation = 100

    if request.current_date is None:
        request.current_date = datetime.now().strftime("%Y-%m-%d")

    print(
        "current date, sowing date:",
        request.season,
        request.crop_name,
        request.sowing_date,
        request.lat,
        request.lon,
        request.current_date,
        request.elevation
    )

    get_response = weekly_adviosry(
        season=request.season,
        crop_name=request.crop_name,
        sowing_date=request.sowing_date,
        latitude=request.lat,
        longitude=request.lon,
        elevation=request.elevation,
        current_date=datetime.strptime(request.current_date, "%Y-%m-%d"),
        weather_input=request.weather_input,
        weather_dict=request.weather_json,
    )

    raw_data = get_response.generate()
    cleaned_data = clean_nan(raw_data)
    print("cleaned data: ",type(cleaned_data))

    return cleaned_data.to_json()


# @app.post("/validate/weekly_advisory/")
# async def crop_advisory(
#     season: str,
#     crop_name: str,
#     sowing_date: str,
#     current_date: str,
#     weather_json: dict,
#     weather_input = "manual",
#     lat: float=21.44,
#     lon: float=85.15,
#     elevation: int=100, #| None = None,
#      #| None = None
# ):
#     """
#     season: str,\n
#     crop_name: str,\n
#     sowing_date: "YYYY-MM-DD",\n
#     current_date: "YYYY-MM-DD"\n
#     """
#     # Default fallbacks
#     if elevation is None:
#         elevation = 100
#     if current_date is None:
#         current_date = datetime.now().strftime("%Y-%m-%d")

#     print("current date, sowing date:", season, crop_name, sowing_date, lat, lon, current_date,elevation)

#     # Call advisory function
#     get_response = weekly_adviosry(
#         season=season,
#         crop_name=crop_name,
#         sowing_date=sowing_date,
#         latitude=lat,
#         longitude=lon,
#         elevation=elevation,
#         current_date=datetime.strptime(current_date,"%Y-%m-%d"),
#         weather_input=weather_input,
#         weather_dict=weather_json,
#     )

#     raw_data = get_response.generate()
#     print(raw_data)
#     cleaned_data = clean_nan(raw_data)

#     # Convert DataFrame → dict for valid JSON
#     data_dict = json.loads(cleaned_data.to_json())#type:ignore

#     return JSONResponse(content=data_dict, status_code=200)

def clean_nan(obj):
    if isinstance(obj, float) and math.isnan(obj):
        return None
    elif isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan(i) for i in obj]
    return obj

def get_config():
    return json.load(open("scripts/setup/input_config.json"))

def get_crop_attributes_bydate(input_date:datetime,crop_db:DataFrame)->DataFrame:
    """
    To get attributes in non-growing season of crop, \ntakes date as input to fetch the adviosry content
    """
    # day_of_month = input_date.timetuple().tm_mday
    # month_of_year = input_date.timetuple().tm_mon
    current_year = input_date.year
    fixed_df = crop_db[crop_db['advisory_type']=='Fixed']
    date_time_values = list(zip(fixed_df.day_of_month,fixed_df.month_of_year))
    fixed_df['Date_col'] = [datetime(year=current_year,month=int(i[1]),day=int(i[0])) for i in date_time_values]
    fixed_df['date_difference'] = abs((fixed_df["Date_col"] - input_date)).apply(lambda x: x.days) # type: ignore
    # print(fixed_df[['advisory_title','date_difference','day_of_month','month_of_year']])
    # return crop_db[(crop_db['day_of_month']==day_of_month)&(crop_db['month_of_year']==month_of_year)]
    return fixed_df.loc[fixed_df['date_difference']==min(fixed_df['date_difference'])]

def get_crop_attributes_byweek(start_date:datetime,end_date:datetime,crop_db:DataFrame)->DataFrame:
    """
    Takes week (float) as input,\n filters andr returns crop_db dataframe by its crop week
    """
    crop_week = (end_date.timetuple().tm_yday-start_date.timetuple().tm_yday)/7
    return crop_week,crop_db.loc[(crop_db['cropstage_week_start']<crop_week)&(crop_db['cropstage_week_end']>=crop_week)].copy(deep=True) # type: ignore

class dynamic_classes:
    #advisory_classes = ["Generic","Variety_selection","Nursery","Pest","Irrigation"]
    def __init__(self,index_filtered_db:DataFrame,elevation:str|None,sowing_date:datetime,advisory_date:datetime,
                 latitude:float,longitude:float,config,weather_dict:List[Dict[str, Any]] ,weather_input='Forecast')->None:
        """
        weather_input = 'forecast' or 'manual', default value = ['forecast'], if 'manual' is selected, user is expected to provide weather input
        """
        self.database = index_filtered_db.copy(deep=True)
        self.elevation = elevation
        self.advisory_date = advisory_date
        self.sowing_date = sowing_date
        self.lat = latitude
        self.long = longitude
        self.config = config
        self.weather_input = weather_input
        self.weather_dict = weather_dict
    def manual_weather(self):
        df = DataFrame(self.weather_dict)
        # print(df)
        rain_sum = df["Rainfall (mm)"].sum()
        tmin = df["Tmin (°C)"].mean()
        tmax = df["Tmax (°C)"].mean()
        rh_min = df["RH_min (%)"].mean()
        rh_max = df["RH_max (%)"].mean()
        return dict(rain_sum=rain_sum, temp_min=tmin, temp_max=tmax, rh_min=rh_min, rh_max=rh_max)

    def generate(self):
        # BASED ON MANUAL INPUT
        if self.weather_input == "Manual":
            if self.database['advisory_class'].values[0] == "Pest":
                print("checking in pest...")
                if len(self.database)==1:
                    infestsnap_info = infestsnap(state_name=self.database['state'].values[0],
                                                crop_name=self.database['crop_name'].values[0],
                                                lat=self.lat,lon=self.long,
                                                sowing_date=self.sowing_date,season=self.database['season'].values[0],config=self.config).request()
                    adv_content = format_text(pest_filepath=self.config['pest_info'],season=self.database['season'].values[0],crop=self.database['crop_name'].values[0],infestsnap_result=infestsnap_info)
                    self.database['advisory_content'] = adv_content
                    return self.database.to_dict(orient="records")
                else:
                    raise IndexError("\n Expecting one row in the database passed, received more than one row.")
            if self.database['advisory_class'].values[0] == "Variety_selection":
                if isinstance(self.elevation,int):
                    if self.elevation <= 200:
                        land_type = "low land"
                    elif self.elevation >200 and self.elevation<= 800:
                        land_type = "medium land"
                    else:
                        land_type = "up land"
                return self.database.loc[self.database['landtype']==land_type].to_dict(orient='records')
            if self.database['advisory_class'].values[0] == "Nursery":
                try:
                    forecast_data = forecast_data = dynamic_classes.manual_weather(self)
                    # Weather Probability
                    advisory_probability = probability(df=self.database,weather_dict=forecast_data,config=self.config).get_probability()
                    matching_record = self.database.iloc[np.where(advisory_probability==advisory_probability.max())[0],:].to_dict(orient='records')
                    return matching_record

                except Exception as e:
                    traceback.print_exc()
                    print("\n Exception has occured in Dynamic-Nursery advisory class.\n",e)
                    sys.exit()

            if self.database['advisory_class'].values[0] == "Irrigation":
                try:
                    forecast_data = dynamic_classes.manual_weather(self)
                    # If forecast_data is empty (file missing), handle gracefully
                    if not forecast_data:
                        return []  # or return some default advisory record

                    advisory_probability = probability(df=self.database,weather_dict=forecast_data,config=self.config).get_probability()
                    matching_record = self.database.iloc[np.where(advisory_probability==advisory_probability.max())[0],:].to_dict(orient="records")
                    return matching_record
                except Exception as e:
                    traceback.print_exc()
                    print("\n Exception occured in Dynamic- Irrigation adviosry class. \n",e)
                    sys.exit()
            else:    
                try:
                    forecast_data = dynamic_classes.manual_weather(self)
                    print("Manual Input Data: \n",forecast_data,"\n")
                    # If forecast_data is empty (file missing), handle gracefully
                    if not forecast_data:
                        return []  # or return some default advisory record

                    advisory_probability = probability(df=self.database,weather_dict=forecast_data,config=self.config).get_probability()
                    matching_record = self.database.iloc[np.where(advisory_probability==advisory_probability.max())[0],:].to_dict(orient="records")
                    return matching_record
                except Exception as e:
                    traceback.print_exc()
                    print(dynamic_classes.manual_weather(self))
                    print("\n Exception occured in Dynamic- Manual weather input section. \n",e)
                    sys.exit()
        
        # Based on FORECASTED DATA
        if self.weather_input == "Forecast":
            if self.database['advisory_class'].values[0] == "Variety_selection":
                if isinstance(self.elevation,int):
                    if self.elevation <= 200:
                        land_type = "low land"
                    elif self.elevation >200 and self.elevation<= 800:
                        land_type = "medium land"
                    else:
                        land_type = "up land"
                return self.database.loc[self.database['landtype']==land_type].to_dict(orient='records')
            if self.database['advisory_class'].values[0] == "Nursery":
                try:
                    # read file
                    wth_folder = self.advisory_date.strftime("%Y%m%d")
                    wth_file_path = os.path.join(self.config['weather_dir'],os.path.join(f"{wth_folder}",f"{wth_folder}_daily.nc"))
                    # wth_file_url = f"https://nc.niruthi.in/ncfiles/{wth_folder}/{wth_folder}_daily.nc"  
                    forecast_data = weather(file_url=wth_file_path,latitude=self.lat,longitude=self.long).get_data()
                    # Weather Probability
                    advisory_probability = probability(df=self.database,weather_dict=forecast_data,config=self.config).get_probability()
                    matching_record = self.database.iloc[np.where(advisory_probability==advisory_probability.max())[0],:].to_dict(orient='records')
                    return matching_record

                except Exception as e:
                    traceback.print_exc()
                    print("\n Exception has occured in Dynamic-Nursery advisory class.\n",e)
                    sys.exit()

            if self.database['advisory_class'].values[0] == "Irrigation":
                try:
                    # read weather file
                    wth_folder = self.advisory_date.strftime("%Y%m%d")
                    wth_file_path = os.path.join(self.config['weather_dir'],os.path.join(f"{wth_folder}",f"{wth_folder}_daily.nc"))
                    # wth_file_url = f"http://nc.niruthi.in/ncfiles/{wth_folder}/{wth_folder}_daily.nc"          
                    # Weather Probability
                    forecast_data = weather(file_url=wth_file_path,latitude=self.lat,longitude=self.long).get_data()
                    # If forecast_data is empty (file missing), handle gracefully
                    if not forecast_data:
                        print(f"No weather data available for {wth_folder}, skipping advisory.")
                        return []  # or return some default advisory record

                    advisory_probability = probability(df=self.database,weather_dict=forecast_data,config=self.config).get_probability()
                    matching_record = self.database.iloc[np.where(advisory_probability==advisory_probability.max())[0],:].to_dict(orient="records")
                    return matching_record
                except Exception as e:
                    traceback.print_exc()
                    print("\n Exception occured in Dynamic- Irrigation adviosry class. \n",e)
                    sys.exit()

            if self.database['advisory_class'].values[0] == "Pest":
                print("checking in pest...")
                if len(self.database)==1:
                    infestsnap_info = infestsnap(state_name=self.database['state'].values[0],
                                                crop_name=self.database['crop_name'].values[0],
                                                lat=self.lat,lon=self.long,
                                                sowing_date=self.sowing_date,season=self.database['season'].values[0],config=self.config).request()
                    adv_content = format_text(pest_filepath=self.config['pest_info'],season=self.database['season'].values[0],crop=self.database['crop_name'].values[0],infestsnap_result=infestsnap_info)
                    self.database['advisory_content'] = adv_content
                    return self.database.to_dict(orient="records")
                else:
                    raise IndexError("\n Expecting one row in the database passed, received more than one row.")
                    # sys.exit()

class infestsnap:
    def __init__(self,state_name:str,crop_name:str,lat:float,lon:float,sowing_date:datetime,season:str,config:dict):
        self.state = state_name
        self.crop = crop_name
        self.lat = lat
        self.lon = lon
        self.sowing_date = sowing_date.strftime("%d-%m-%Y")
        self.season =  season
        self.config = config

    def request(self):
        import subprocess, shlex
        print("sowing date: ",self.sowing_date)
        # infestsnap_request_url = f'https://infsnewapi.niruthiapptesting.com/gfs/pest_info?lat={"%.2f"%self.lat}&lon={"%.2f"%self.lon}'
        # Manish - GFS
        infestsnap_request_url = f'https://ksh-gfs.niruthiapptesting.com/pest-info-new/?lat={self.lat}&lon={self.lon}&is_next_week=true'
        # infestsnap_request_url = f'http://150.241.244.210:8070/gfs/pest_info?lat={"%.2f"%self.lat}&lon={"%.2f"%self.lon}'
        _headers =  'Content-Type : application/json'
        _data = {"state_name": self.state, "sowing_date": self.sowing_date,"crop_name": self.crop}
        # Reference
        # data_ = { "state_name": "Mizoram", "sowing_date": "30-04-2025","crop_name": "Paddy"}
        curl_req = f"curl --resolve --location {infestsnap_request_url} --header 'Content-Type: application/json' --data '{json.dumps(_data)}'"
        print("InfestSnap Request Command: ",curl_req)
        infest_data = subprocess.Popen(shlex.split(curl_req),stdin=subprocess.PIPE,stdout=subprocess.PIPE).communicate()
        # print("printing infestsanp data: *****:::",infest_data[0])
        i_out,i_err = infest_data     
        result = json.loads(i_out.decode("utf-8"))
        # print("InfestSnap Response: ",result)
        if len(result.keys())>0:
            return result
        else:
            raise Exception("No data to return...",json.loads(i_err.decode("utf-8")))
    
def format_text(pest_filepath,season,crop,infestsnap_result): #type:ignore
    try:
        pest_info = read_csv(pest_filepath,encoding="utf-8-sig")
    except UnicodeDecodeError:
        pest_info = read_csv(pest_filepath,encoding='windows-1252')
    pest_info = pest_info[(pest_info['season']==season)&(pest_info['crop']==crop)]
    pest_info['Name of Disease and Insect'] = [i.strip() for i in pest_info['Name of Disease and Insect']]
    try:
        infestation_dict = infestsnap_result['data']
        infest_adv_combined = ""
        for inf in infestation_dict:
            # print("INF: ",inf)
            # if inf['infestation_level'] != "low":
            pest_adviosry = pest_info[pest_info['Name of Disease and Insect']==inf['pest_name'].strip()]['Advisory'].values
            # infest_text = f"Pest:{inf['pest_name']}, Infestation_level:{inf['infestation_level']}, Recommendation:{pest_adviosry}.; \n " ## infestsanp api response
            infest_text = f"Pest:{inf['pest_name']}, Infestation_level:{inf['chances']['current_week']['infestation_level']}, Advisory:{pest_adviosry}.; \n " ## BMGF API Response
            # print("\n",infest_text)

            infest_adv_combined = infest_adv_combined+infest_text
        return infest_adv_combined
    except Exception as e:
        traceback.print_exc()
        print("\n",e,infestsnap_result)
        
class non_growing_stage:
    def __init__(self,latitude:float,longitude:float,adviosry_date:datetime,crop_db:DataFrame,unique_id:int = 0):
        self.lat = latitude
        self.lon = longitude
        self.uid = unique_id
        self.adv_date = adviosry_date
        self.database = crop_db.copy(deep=True)
    
    def generate(self,**kwargs):
        crop_data = get_crop_attributes_bydate(input_date=self.adv_date,crop_db=self.database)
        return crop_data

class probability:
    """
    Provides functionality to calculate probability of weather parameters for the given conditions in crop database to fetch adviosry
    """
    def __init__(self,df:DataFrame,weather_dict:dict,config)->None:
        self.df = df.copy()
        self.weather = weather_dict
        self.config = config
        self.prob_parms = config['prabability_parameters']

    def get_probability(self)->np.ndarray:
        # filter columns by matching params
        col_filtered_df = self.df[self.prob_parms].to_numpy()
        forcasted_weather = list(self.weather.values())  
        abs_difference = np.absolute(np.subtract(col_filtered_df,forcasted_weather))
        # print("absolute_difference: ",abs_difference)
        get_diff_percent = np.divide(abs_difference,col_filtered_df)
        # print("difference_percent: ",get_diff_percent)
        get_diff_percent[np.isnan(get_diff_percent)]=0
        get_diff_percent[np.isinf(get_diff_percent)]=0
        #applying high weightage to rainfall based on crop season
        if self.df['season'].values[0]=="Kharif":
            weight = np.array([1.25,1.1875,1.1875,1.1875,1.1875])
            get_diff_percent = 1- np.divide(np.multiply(get_diff_percent,weight),len(self.prob_parms)+1)
        else:
            get_diff_percent = 1- np.divide(get_diff_percent,len(self.prob_parms))
        probability_percent = np.array(np.sum(1-get_diff_percent,axis=1)).flatten(order="C")
        return probability_percent


# class weather:
#     """
#     Provides functions to do operations related to weather data.
#     """
#     def __init__(self,file_path:str,latitude:float,longitude:float)->None:
#         self.file = file_path
#         self.lat = latitude
#         self.long = longitude

#     def get_data(self)->dict:
#         """
#         Fetchs weekly resampled weather parameter values
#         """
#         print("247weather_file: ",self.file)
#         import xarray as xr
#         self.weather = xr.open_mfdataset(self.file)
#         self.rain_min = self.weather['Rainfall'].sel(Latitude=self.lat,Longitude=self.long,method='nearest').resample(Date_time="W").min().values[0]
#         self.rain_max = self.weather['Rainfall'].sel(Latitude=self.lat,Longitude=self.long,method='nearest').resample(Date_time="W").max().values[0]
#         self.rain_avg = self.weather['Rainfall'].sel(Latitude=self.lat,Longitude=self.long,method='nearest').resample(Date_time="W").mean().values[0]
#         self.rain_sum = self.weather['Rainfall'].sel(Latitude=self.lat,Longitude=self.long,method='nearest').resample(Date_time="W").sum().values[0]
#         self.temp_min = self.weather['Tmin'].sel(Latitude=self.lat,Longitude=self.long,method='nearest').resample(Date_time="W").mean().values[0]
#         self.temp_max = self.weather['Tmax'].sel(Latitude=self.lat,Longitude=self.long,method='nearest').resample(Date_time="W").mean().values[0]
#         self.rh_min = self.weather['RH_min'].sel(Latitude=self.lat,Longitude=self.long,method ='nearest').resample(Date_time="W").mean().values[0]
#         self.rh_max = self.weather['RH_max'].sel(Latitude=self.lat,Longitude=self.long,method='nearest').resample(Date_time="W").mean().values[0]
       
#         self.weather.close()
#         # print(dict(rain_sum=self.rain_sum,temp_min=self.temp_min,
#         #             temp_max=self.temp_max,rh_min=self.rh_min,rh_max=self.rh_max))
#         return dict(rain_sum=self.rain_sum,temp_min=self.temp_min,
#                     temp_max=self.temp_max,rh_min=self.rh_min,rh_max=self.rh_max)

class weather:
    def __init__(self, file_url: str, latitude: float, longitude: float) -> None:

        # self.file = requests.get(file_url) # if it is Hosted NAS Access
        self.file = file_url # if accessing from Local NAS
        self.lat = latitude
        self.long = longitude

    def get_data(self) -> dict:
        print("weather_file in get data: ", self.file)
        try:
            # self.weather = xr.open_dataset(BytesIO(requests.get(self.file,stream=True).content), engine="scipy") # if it is Hosted NAS Access
            self.weather = xr.open_dataset(self.file)  # if accessing from Local NAS
            self.rain_min = self.weather['Rainfall'].sel(Latitude=self.lat, Longitude=self.long, method='nearest').resample(Date_time="W").min().values[0]
            self.rain_max = self.weather['Rainfall'].sel(Latitude=self.lat, Longitude=self.long, method='nearest').resample(Date_time="W").max().values[0]
            self.rain_avg = self.weather['Rainfall'].sel(Latitude=self.lat, Longitude=self.long, method='nearest').resample(Date_time="W").mean().values[0]
            self.rain_sum = self.weather['Rainfall'].sel(Latitude=self.lat, Longitude=self.long, method='nearest').resample(Date_time="W").sum().values[0]
            self.temp_min = self.weather['Tmin'].sel(Latitude=self.lat, Longitude=self.long, method='nearest').resample(Date_time="W").mean().values[0]
            self.temp_max = self.weather['Tmax'].sel(Latitude=self.lat, Longitude=self.long, method='nearest').resample(Date_time="W").mean().values[0]
            self.rh_min = self.weather['RH_min'].sel(Latitude=self.lat, Longitude=self.long, method='nearest').resample(Date_time="W").mean().values[0]
            self.rh_max = self.weather['RH_max'].sel(Latitude=self.lat, Longitude=self.long, method='nearest').resample(Date_time="W").mean().values[0]

            self.weather.close()
            print(dict(rain_sum=self.rain_sum, temp_min=self.temp_min,
                        temp_max=self.temp_max, rh_min=self.rh_min, rh_max=self.rh_max))
            return dict(rain_sum=self.rain_sum, temp_min=self.temp_min,
                        temp_max=self.temp_max, rh_min=self.rh_min, rh_max=self.rh_max)
        except Exception as e:
            print(f"⚠️ Could not open file: {self.file}\n{e}")
            sys.exit()
            return dict(rain_sum=0, temp_min=0, temp_max=0, rh_min=0, rh_max=0)

def get_advisory_index(start_date:datetime,end_date:datetime,crop_db:DataFrame)->int:
    """
    Caluclates the index of the advisory to be sent, by calculating the weeks from swoing date to current date,\n
    then by getting number of adviosries in the filtered crop stage,\n
    then calculates the index of the advisory to desiminated in the filtered dataframe.\n
    Parameters:\n
    i_doy = intial day of the year(sowing_date),\n
    e_day = current day of the year (current_date),\n
    woc = week of crop,\n
    c_stage_adv_len =  crop stage advisory length

    """
    crop_week = (end_date-start_date).days/7
    # print(start_date,end_date)
    # print("crop_week:",crop_week)
    c_stage_len = crop_db['cropstage_week_end'].values[0]-crop_db['cropstage_week_start'].values[0]
    # print("crop_stage len: ", c_stage_len)
    c_stage_adv_len = len(crop_db['advisory_index'].unique())
    # print("advisory length: ",c_stage_adv_len)
    advisory_per_day = (c_stage_len*7)/c_stage_adv_len
    # print("advisories per day: ", advisory_per_day)
    # print("Crop week: ",crop_week)
    remaining_days_in_c_stage = (crop_db['cropstage_week_end'].values[0] - crop_week)*7
    # print("remaining days in the crop stage: ",remaining_days_in_c_stage)
    current_timeline_of_c_stage = (c_stage_len*7)-remaining_days_in_c_stage
    index = int(current_timeline_of_c_stage//advisory_per_day)# if indexing issue ther do +1
    print("index: ",index)
    try:
        # print("advisory index",crop_db['advisory_index'].unique()[index])
        return crop_db['advisory_index'].unique()[index]
    except IndexError as e:
        # print("advisory index",crop_db['advisory_index'].unique()[index-1])
        return crop_db['advisory_index'].unique()[index-1]
class sowing_date:
    def __init__(self,config:dict,season:str,crop_name:str,village_uid:int|str):
        self.config = config
        self.season = season
        self.crop = crop_name
        self.UID = village_uid

    def get_predicted_date(self) -> dict|None:
        if self.season == "Kharif":
            if self.crop == "Paddy":
                sowing_dates = read_csv(self.config['Sowing_files']['Paddy_Kharif'])
                crop_sowing_dates = sowing_dates[sowing_dates['Crop_name']==self.crop]
            else:
                sowing_dates = read_csv(self.config['Sowing_files']['Other_Kharif'])
                crop_sowing_dates = sowing_dates[sowing_dates['Crop_name']==self.crop]
        elif self.season == "Rabi":
            sowing_dates = read_csv(self.config['Sowing_files']['All_Rabi'])
            crop_sowing_dates = sowing_dates[sowing_dates['Crop_name']==self.crop]
        del sowing_dates

        # Get Sowing_date of the crop using 
        try:
            village_sowing_date = crop_sowing_dates[crop_sowing_dates['Unq']==self.UID]['Sowing_date'].values[0]
            crop_sowing_date = village_sowing_date
        except Exception as e:
            print(Exception)
            crop_sowing_date = None
        return crop_sowing_date
def days_in_year(year: int) -> int:
    """
    Returns the total number of days in the given year.
    Handles leap years automatically.
    """
    if not isinstance(year, int) or year <= 0:
        raise ValueError("Year must be a positive integer.")

    # Jan 1 of the given year
    start = date(year, 1, 1)
    # Jan 1 of the next year
    end = date(year + 1, 1, 1)
    # Difference in days
    return (end - start).days
class weekly_adviosry:
    # def __init__(self,season,crop_name,sowing_date,latitude,longitude,elevation:int|None,current_date=datetime.today()+timedelta(days=-1),realtime=0):
    def __init__(self,season,crop_name,sowing_date,latitude,longitude,elevation:int|None,weather_input:str,weather_dict:List[Dict[str, Any]] ,current_date,realtime=0):

        self.season = season
        self.crop_name=crop_name
        self.sowing_date = datetime.strptime(sowing_date,"%Y-%m-%d")
        self.current_date = current_date
        self.lat = latitude
        self.lon = longitude
        self.elevation = elevation
        self.realtime = realtime
        self.weather_input = weather_input
        self.weather_dict = weather_dict
        self.config = json.load(open("input_config.json"))
    def generate(self):
        try:
            crop_calendar = read_csv(self.config['crop_calendar'],encoding="utf-8-sig")
        except UnicodeDecodeError:
            crop_calendar = read_csv(self.config['crop_calendar'],encoding="windows-1252")
        crop_calendar = crop_calendar[(crop_calendar['season']==self.season)&(crop_calendar['crop_name']==self.crop_name)]
        # pest_data = read_csv(self.config['pest_info'])
        advisory_dictionary=[]
        # calculate week of the crop from sowing date till current date 
        # Handling year difference causing issue

        if self.current_date.year == self.sowing_date.year:
            crop_week =  (self.current_date.timetuple().tm_yday - self.sowing_date.timetuple().tm_yday)/7
        elif self.current_date.year < self.sowing_date.year:
            crop_week = (-(abs((self.current_date.timetuple().tm_yday - days_in_year(self.current_date.year))) + self.sowing_date.timetuple().tm_yday))/7
        elif self.current_date.year > self.sowing_date.year:
            crop_week = (self.current_date.timetuple().tm_yday + (days_in_year(self.sowing_date.year) - self.sowing_date.timetuple().tm_yday))/7
            
        if isinstance(self.sowing_date,datetime) and crop_week>0:
                       
            print("current crop week: ", crop_week,f" \n Adviosry for week {crop_week+1} will be generated")

            
            #filter crop_calendar db by start and end week
            if IS_WEEKLY:
                week_calendar = crop_calendar.dropna(subset=['cropstage_week_start','cropstage_week_end'])[(crop_calendar['cropstage_week_start']<(crop_week))&(crop_calendar['cropstage_week_end']>=(crop_week))]           
                for i in week_calendar['advisory_index'].unique():
                    adv_df = week_calendar[week_calendar['advisory_index']==i]
                    if adv_df['advisory_type'].values[0]=="Dynamic":
                        advisory_data = dynamic_classes(index_filtered_db=adv_df,
                                                        elevation=100,  #type:ignore
                                                        sowing_date=self.sowing_date,
                                                        advisory_date=self.current_date,
                                                        latitude=self.lat,
                                                        longitude=self.lon,
                                                        config=self.config,
                                                        weather_dict=self.weather_dict,weather_input=self.weather_input
                                                        ).generate()
                        if isinstance(advisory_data,list):
                            advisory_dictionary.append(advisory_data[0])
                        elif isinstance(advisory_data,dict):
                            advisory_dictionary.append(advisory_data)
                        else:
                            # print(type(advisory_data))
                            advisory_dictionary.append(advisory_data.to_dict(orient="records")) #type:ignore

                    elif adv_df['advisory_type'].values[0]=="Standard":
                        advisory_data = adv_df
                        advisory_dictionary.append(advisory_data.to_dict(orient='records')[0])

                    else:
                        advisory_data = non_growing_stage(latitude=self.lat,longitude=self.lon,adviosry_date=self.current_date,crop_db=crop_calendar).generate()
                        advisory_dictionary.append(advisory_data.to_dict(orient="records")[0])
            else:
                week_calendar = crop_calendar.dropna(subset=['cropstage_week_start','cropstage_week_end'])[(crop_calendar['cropstage_week_start']<(crop_week))&(crop_calendar['cropstage_week_end']>=(crop_week))]
                advisory_index = get_advisory_index(start_date=self.sowing_date,end_date=self.current_date,crop_db=week_calendar)
                adv_df = week_calendar[week_calendar['advisory_index']==advisory_index]
                if adv_df['advisory_type'].values[0]=="Dynamic":
                        advisory_data = dynamic_classes(index_filtered_db=adv_df,
                                                        elevation=100,  #type:ignore
                                                        sowing_date=self.sowing_date,
                                                        advisory_date=self.current_date,
                                                        latitude=self.lat,
                                                        longitude=self.lon,
                                                        config=self.config,
                                                        weather_dict=self.weather_dict,weather_input=self.weather_input
                                                        ).generate()
                        if isinstance(advisory_data,list):
                            advisory_dictionary.append(advisory_data[0])
                        elif isinstance(advisory_data,dict):
                            advisory_dictionary.append(advisory_data)
                        else:
                            # print(type(advisory_data))
                            advisory_dictionary.append(advisory_data.to_dict(orient="records")) #type:ignore

                elif adv_df['advisory_type'].values[0]=="Standard":
                    advisory_data = adv_df
                    advisory_dictionary.append(advisory_data.to_dict(orient='records')[0])

                else:
                    advisory_data = non_growing_stage(latitude=self.lat,longitude=self.lon,adviosry_date=self.current_date,crop_db=crop_calendar).generate()
                    advisory_dictionary.append(advisory_data.to_dict(orient="records")[0])
                
        else:
            advisory_data = non_growing_stage(latitude=self.lat,longitude=self.lon,adviosry_date=self.current_date,crop_db=crop_calendar).generate()
            advisory_dictionary.append(advisory_data.to_dict(orient="records")[0])

        # DataFrame(advisory_dictionary).to_csv("test1.csv",index=False)
        return DataFrame(advisory_dictionary)


def main():
    input_config = json.load(open("input_config.json"))
    # print(input_config)
    print("in main weekly advisory")
    weekly_adviosry(season="Kharif",crop_name="Paddy",sowing_date=datetime(2025,7,1),latitude=24.56,longitude=82.45,elevation=100).generate()
    print("in main after....weekly advisory")
    # pass

if __name__ == "__main__":
    # main()
    import geopandas as gpd
    import fsspec
    from concurrent.futures import ThreadPoolExecutor

    block_shp = gpd.read_file(r"shapefile\Odisha_block_shapefile.shp")
    # block_shp = block_shp[block_shp['district_n']=="Kendrapada"]
    district_filter = ['Ganjam','Kalahandi','Cuttack','Kendrapada','Baleswar','Keonjhar','Koraput','Sundargarh','Mayurbhanja','Bargarh','Bolangir']

    block_shp = block_shp.loc[block_shp['district_n'].isin(district_filter)]
    block_shp['latitude'] = block_shp.centroid.y
    block_shp['longitude'] = block_shp.centroid.x
    crops = ['Blackgram','Paddy','Greengram','Potato','Mustard']
    season = "Rabi"
    sowing_date_file = read_csv(r"data\Predicted_Sowing_Date\All_Crops_Rabi_block_level.csv")
    empty_df = []
    for crop in crops:
        print("Crop_name: ",crop)
        for i,block in block_shp.iterrows():
            block_data = block.to_dict()
            print("Block_Data: ",block_data)
            sowing_date_ = sowing_date_file[(sowing_date_file['Crop_name']==crop)&(sowing_date_file['Unq']==block_data['Unq'])]
            sowing_dt = datetime.strptime(sowing_date_['Sowing_date'].values[0],"%Y-%m-%d").date()
            crop_adviosory_data = weekly_adviosry(season=season,crop_name=crop,sowing_date=sowing_date_['Sowing_date'].values[0],latitude=block_data['latitude'],longitude=block_data['longitude'],elevation=100,realtime=0,current_date=datetime.today()+timedelta(days=-1)).generate()
            crop_adviosory_data['District'] = block_data['district_n']
            crop_adviosory_data['Block'] = block_data['block_name']
            crop_adviosory_data['Adviosry_Date']= (datetime.today()+timedelta(days=-1)).strftime("%Y-%m-%d")
            crop_adviosory_data['Sowing_Date']= sowing_dt.strftime("%Y-%m-%d")
            crop_adviosory_data['Latitude'] = block_data['latitude']
            crop_adviosory_data['Longitude'] = block_data['longitude']
            empty_df.append(crop_adviosory_data)
    # for t in empty_df:
    #     print(t,type(t))
    df = concat(empty_df,ignore_index=False)
>>>>>>> d4d0faf2b9d906823c928f52b65bbcce412a3539
    df.to_csv("Rabi_advisory_odisha_27012026_weekly.csv",index=False,encoding="utf-8-sig")