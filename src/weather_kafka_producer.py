from kafka import KafkaProducer
import requests
import json 
import time 
import random


def get_weather():
    api_key = "27f9804e88e7692be0da4accf5b7d240"
    random_lat_long = generate_lat_long()

    url = f"http://api.openweathermap.org/data/2.5/weather?" \
      f"lat={random_lat_long[0]}&lon={random_lat_long[1]}" \
      f"&appid={api_key}"
    
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    
    else:
        print(f"Failed to fetch data : {response.status_code}")
        return None
    

def generate_lat_long():

    latitude = random.uniform(-90 , 90)

    longitude = random.uniform(-180 , 180)
    return latitude, longitude

def producer_weather():

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer = lambda v: json.dumps(v).encode('utf-8')
    )

    while True:
        weather_data = get_weather()
        if weather_data:
            producer.send('weather_topic', weather_data)
            print(f"sent: {weather_data}")
        time.sleep(1) #fetch data every 10 seconds

if __name__ == "__main__":
    producer_weather()

    