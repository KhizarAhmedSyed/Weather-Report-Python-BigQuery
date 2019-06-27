import requests
from pprint import pprint
import json
import pytz
import datetime
from suntime import Sun, SunTimeException
from datetime import datetime
from timezonefinder import TimezoneFinder
from pytz import timezone
from datetime import datetime, timedelta
import threading
from google.cloud import bigquery
from google.oauth2 import service_account
import google.auth
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="D:/PY/file.json" 
from gcloud import storage
from oauth2client.service_account import ServiceAccountCredentials


url='https://api.openweathermap.org/data/2.5/weather?q=Paris&appid=4d260396d5b19dc31ea21f597ac164ef&units=metric'
res=requests.get(url)
data = res.json()

latitude= data['coord']['lat']
longitude=data['coord']['lon']
country=data['sys']['country']

tt_rise = data['sys']['sunrise']
sunrise = (datetime.fromtimestamp(tt_rise) - timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S')
tt_set = data['sys']['sunset'] 
sunset = (datetime.fromtimestamp(tt_set) - timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S') 
city=data['name']
visibility=data['visibility']
tz = pytz.timezone('Europe/Paris')
paris_now = datetime.now(tz)
temperature=data['main']['temp']
temp_min=data['main']['temp_min']
temp_max=  data['main']['temp_max']
pressure=  data['main']['pressure'] 
humidity=  data['main']['humidity']    
main=data['weather'][0]['main']            
description=data['weather'][0]['description']
wind_speed=data['wind']['speed']

in_json =   {
                "latitude":"{}".format(latitude),
                "longitude":"{}".format(longitude),
                "country":"{}".format(country),
                "city":"{}".format(city),
                "paris_now":"{}".format(paris_now),
                "sunrise":"{}".format(sunrise),
                "sunset":"{}".format(sunset),
                "temperature":"{}".format(temperature),
                "temp_min":"{}".format(temp_min),
                "temp_max":"{}".format(temp_max),
                "pressure":"{}".format(pressure),
                "humidity":"{}".format(humidity),
                "visibility":"{}".format(visibility),
                "main":"{}".format(main),
                "description":"{}".format(description),
                "wind_speed":"{}".format(wind_speed)
            }

with open('data_file.json', 'w') as outfile:  
    json.dump(in_json, outfile)

credentials_dict = {
    'type': 'service_account',
    'client_id': 'gcp client key',
    'client_email':'email',
    'private_key_id': 'gcp key id',
    'private_key':'-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----\n'
}
credentials = ServiceAccountCredentials.from_json_keyfile_dict(
    credentials_dict
)

client = storage.Client(credentials=credentials, project='my-test-project-244405')
bucket = client.get_bucket('khizweatherbucket')
blob = bucket.blob('data_file.json')

blob.upload_from_filename('D:/PY/data_file.json')
client = bigquery.Client()
dataset_id = 'weatherDS'
dataset_ref = client.dataset(dataset_id)
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
#uri = "gs://cloud-samples-data/bigquery/us-states/us-states.json"
#uri = "gs://khizweatherbucket/data_file.json"
uri = "gs://khizweatherbucket/data_file.json"

load_job = client.load_table_from_uri(
    uri,
    dataset_ref.table("weatherTBL"),
    location="US",  # Location must match that of the destination dataset.
    job_config=job_config,
)  # API request
print("Starting job {}".format(load_job.job_id))

load_job.result()  # Waits for table load to complete.
print("Job finished.")
destination_table = client.get_table(dataset_ref.table("weatherTBL"))
print("Loaded {} rows.".format(destination_table.num_rows))

#print(in_json)

