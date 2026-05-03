import os
from scripts.weather import weather
import pandas as pd
from datetime import datetime, timedelta
import json
import numpy as np
import traceback

class probability:
    def __init__(self,weather_arr,pest_arr):
        self.weather_param = weather_arr
        self.pest_arr = pest_arr
    def predict(self):
        abs_difference = np.absolute(np.subtract(self.pest_arr,self.weather_param))
        # print("absolute_difference: ",abs_difference)
        get_diff_percent = np.divide(abs_difference,self.pest_arr)
        # print("difference_percent: ",get_diff_percent)
        get_diff_percent[np.isnan(get_diff_percent)]=0
        get_diff_percent[np.isinf(get_diff_percent)]=0
        get_diff_percent = 1- np.divide(get_diff_percent,len(self.weather_param))
        probability_percent = np.array(np.sum(1-get_diff_percent,axis=1)).flatten(order="C")
        return probability_percent

class pest:
    def __init__(self,crop_name,season,state,district,
                 latitude,longitude,
                 sowing_date:datetime,current_date:datetime)->None:
        self.crop = crop_name
        self.season = season
        self.state = state
        self.district = district
        self.latitude = latitude
        self.longitude = longitude
        self.sowing_date = sowing_date
        self.current_date = current_date+timedelta(days=-1)
        self.config = json.load(open("input_config.json"))

    def predict(self)->pd.DataFrame:
        pest_data = pd.read_csv(self.config['pest_climatic_conditions'])
        pest_data = pest_data.loc[(pest_data['state']==self.state) & (pest_data['district']==self.district)].copy()
        crop_week = (self.current_date.timetuple().tm_yday - self.sowing_date.timetuple().tm_yday)/7
        print("crop week: ", crop_week)
        try:
            pest_db = pest_data.loc[(pest_data['crop_name']==self.crop) & (pest_data['season']==self.season)&
                                    (pest_data['week1']<crop_week)&(pest_data['week2']>=crop_week)]
            del pest_data
        except Exception as e:            
            raise ValueError('Value error has occured...')
        pest_db['Rain_mean'] = (pest_db['Rain1']+pest_db['Rain2'])/2
        pest_db['Tmin_mean'] = (pest_db['Tmin1']+pest_db['Tmin2'])/2
        pest_db['Tmax_mean'] = (pest_db['Tmax1']+pest_db['Tmax2'])/2
        pest_db['RH_mean'] = (pest_db['RHmin1']+pest_db['RHmax2'])/2
        pest_data = pest_db[['Rain_mean','Tmin_mean','Tmax_mean','RH_mean']].to_numpy()
        wth_filename = self.current_date.strftime("%Y%m%d")+"/"+f"{self.current_date.strftime('%Y%m%d')}_daily.nc"
        weather_file_path = self.config['weather_dir_public']+"/"+wth_filename
        fetch_weather = weather(latitude=self.latitude,longitude=self.longitude,file_url=weather_file_path).get_data()
        rh_mean = (fetch_weather['rh_min']+fetch_weather['rh_max'])/2
        weather_data = [fetch_weather['rain_sum'],fetch_weather['temp_min'],fetch_weather['temp_max'],rh_mean]
        # print(weather_data)
        prob_func = probability(weather_arr=weather_data,pest_arr=pest_data).predict()#.reshape((len(pest_data),1))
        pest_db['Lat'] = self.latitude
        pest_db['Lon'] = self.longitude
        pest_db['Probability'] = prob_func
        def classification_function(i):
            if i >= 0.70:
                return "High"
            if (i >= 0.45) & (i<0.70):
                return "Moderate"
            else:
                return "Low"

        pest_db['infestation_level'] = pest_db['Probability'].apply(classification_function)
        
        return pest_db[['state','district','season','crop_name','Lat','Lon','pest&disease_name','Probability',"infestation_level"]]

def main():
    crop_name = 'Paddy'
    season = "Rabi"
    state, district = 'Odisha', 'Cuttack'
    lat, lon = 21.98, 79.34
    sowing_date, current_date = datetime(2025,7,1), datetime(2025,8,2)

    data = pest(crop_name,season,state,district,
                 lat,lon,
                 sowing_date,current_date).predict()
    print(data)

if __name__ == "__main__":
    main()