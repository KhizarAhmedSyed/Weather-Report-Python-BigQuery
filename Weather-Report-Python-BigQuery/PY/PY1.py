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
    'client_id': '103886913107157812738',
    'client_email': 'khiz-apiuser@my-test-project-244405.iam.gserviceaccount.com',
    'private_key_id': '076cfa931cd59d80446af4d9d6952ed4d62b34a6',
    'private_key':'-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDG/cTn+fkWDltL\nCN4JfiUTK0LzfHR8BB9a3bkRESpyGJQJOCuAOTy8mNryuUf26/6z47aS51gXb8kJ\nZZjtn+/sDh+mTFXk1wheaGNN+D4l4M8ba8Btvh4teCgGb5/660gc2Bzen3Q5R6XQ\n226uT5V8RPWKzqAmJNQi9nNshJPJv6cbkkV7bz3/RWrqpa9omPMZlwymL1R5QDXj\n9kvVhXIRwPiGU9jtgQuFFkWRe7tvWlj8YLgGu0FO0qunYzhzG0ExHbuIMuJsOr0h\n5UsaVSJg7y41YIakXD5XRuvPuQTnNTr05QZuMfWLf42Timg0HzR3zQADzqNy2J6T\nzVE7JFf7AgMBAAECggEATuIyuLNDpxax4iD1xFWZZOv1coiXtMH9nAvhXX6skOZl\naPI3bHFEPo9p3GbOvHf5VF6k9b94EHJkA7Ge2jRY6/79VPNV6Y3E54gCNdp+6I0i\nDQC+G8MOO4AyfbqqdNHomGajqy0S6dIZTZ5vVfJ+k/DSVeBrZDOdVO7V+uLjo0n/\nX8BD+UFTRv3RX3WQDKygN1PhvWbeZKUDZo7PUnrTgQHq5qnCPrOrmBruw/RM1aY7\ntnUoiuyc8RUb44gDKbb/XtaMVBeXB1tCU2ueJ5BvOzmzS1DqwotgEp/dZiTi5i1I\nSNs/P4DCFEgTGm/vbwIUG+51rfi1J7wS+1HqGEtgwQKBgQDrJDyKZLN6DI6E6+OY\n8/yq4+jnhitQB8748is8fpKEik9ZCN8180ng2xWE851i8Ob1NJiq73TaBskgwhBy\n7dqUzKlW4ksMBHdkwiXnbluBfg+P4EK0gPPzsWC6l9V7M4uxjZBiJGs2KJNEzxiE\nBlAIuPNgjbpDJcGBsuESVslsQwKBgQDYpJtC7+WebAXXyw5mXfCzOMDvd/vcSLuI\n5rw4rFcFsSuVZOTDXzeT3WAHiPzibVB+fzXAtDbBzQDIaov5unEGW83LYHUR5JFP\nowyWcj8Ct6piLN8riWRnNEQ8bojppaD1nbA9tBQQacyj3rRnwaPIovVa6hf4tt4r\nvnchRQaF6QKBgQCJQYKxSezV7mR2xHb7PlibrCO0mcXIlnZDLKD+U+fUxCNjFmGs\nzVGvllLuY4HYUkSOl2AST2qHJfTbUUxmud6ggwLJ+5fQ8P58azPS7sEtSldtJXvq\nf2dnNYAAC0cK/mCpiLfiAd4vI+oq/TQTRNgM72DpWHGEZpKY0cC9grlanwKBgFrc\n1x3A/j5ushiq2rRGjDCvCgSl/yWJ/9XQaHsglTMW9t+mvGfQ5L+IpsEiTGYvUQZt\nFj9nllu8PqrQPTsVXlg7Ytn877z8b5HGIf5rlk/udnDMvyFEc67xdfkepx/Pzu2V\nIJkQkSW9Kg5E1sd5qNb6ugtSiZFQKWiRbueuSqkZAoGBAJBE7JCVqWTkcnbP9rLE\nmU+g9tS3Ri3Fgp9fhOU+uYS4zxmcjYkgVOvQO6McabdnsDKaD6NzGV/s4mp2h+SF\nu740rJJbriew2tpE1wOtTGxc7v2ZXFAYNLxOKl1pgv1oWTbC3K4ep8CKArehY0rH\nDauA1t06QK5Bhqw0DTA6E+sw\n-----END PRIVATE KEY-----\n'
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

