import pandas as pd
import urllib.request
import json
from pytz import timezone
import datetime

def time_converter(time):
    d = datetime.datetime.fromtimestamp(int(time),tz=timezone('US/Pacific'))
    converted_time = d.strftime('%H:%M:%S')
    return converted_time

def datetime_converter(time):
    d = datetime.datetime.fromtimestamp(int(time),tz=timezone('US/Pacific'))
    converted_time = d.strftime('%d-%m-%Y %H:%M:%S')
    return converted_time

def file_name(time):
    d = datetime.datetime.fromtimestamp(int(time),tz=timezone('US/Pacific'))
    converted_time = d.strftime('%Y%m%d')
    return converted_time

url = 'http://api.openweathermap.org/data/2.5/weather?zip=95132,us&mode=json&units=metric&APPID=1332d1509ad061c8381ddfd835341138'

url = urllib.request.urlopen(url)
output = url.read().decode('utf-8')
raw_api_dict = json.loads(output)
url.close()
# df = pd.DataFrame()

print(dict(
        zip_code=95132,
        city=raw_api_dict.get('name'),
        country=raw_api_dict.get('sys').get('country'),
        temp=raw_api_dict.get('main').get('temp'),
        temp_max=raw_api_dict.get('main').get('temp_max'),
        temp_min=raw_api_dict.get('main').get('temp_min'),
        humidity=raw_api_dict.get('main').get('humidity'),
        pressure=raw_api_dict.get('main').get('pressure'),
        sky=raw_api_dict['weather'][0]['main'],
        sunrise=time_converter(raw_api_dict.get('sys').get('sunrise')),
        sunset=time_converter(raw_api_dict.get('sys').get('sunset')),
        wind=raw_api_dict.get('wind').get('speed'),
        wind_deg=raw_api_dict.get('deg'),
        dt=datetime_converter(raw_api_dict.get('dt')),
        cloudiness=raw_api_dict.get('clouds').get('all'),
        day=file_name(raw_api_dict.get('dt'))
    ))
