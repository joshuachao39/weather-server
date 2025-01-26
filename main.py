from fastapi import FastAPI, HTTPException
import requests
import sqlite3
import base64
import time
import json
import os
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()
API_KEY = os.getenv('API_KEY')

def get_geodata(city_name: str, state_name: str = "", country_code: str = ""):
    try:
        url = f"http://api.openweathermap.org/geo/1.0/direct?q={city_name}{',' + state_name if state_name else ''}{',' + country_code if country_code else ''}&limit=1&appid={API_KEY}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()[0]
        else:
            return None
    except requests.exceptions.RequestException as e:
        print("Error calling geolocation service: ", e)
        return None

def fetch_weather(lat, long):
    try:
        url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={long}&exclude=minutely,hourly,daily,alerts&appid={API_KEY}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print('Error calling weather service:', response.status_code, response.reason)
            return None
    except requests.exceptions.RequestException as e:
        print('Error calling weather service:', e)
        return None




@app.get("/")
async def root():
    return {"message": get_geodata("Fremont")}

@app.get("/get-weather")
async def get_weather(city_name: str, state_name: str = "", country_code: str = ""):
    city_name, state_name, country_code = city_name.upper(), state_name.upper(), country_code.lower()
    lat, long = None, None
    if not state_name or country_code:
        geodata = get_geodata(city_name, state_name, country_code)
        if not geodata:
            raise HTTPException(status_code=500, detail="Failed to reach geolocation server/Could not find geolocation data")
        state_name = geodata["state"]
        country_code = geodata["country"]
        lat = geodata["lat"]
        long = geodata["lon"]
    
    # First check to see if city/state/country is in the DB:
    try:
        with sqlite3.connect("weather.db") as conn:
            cursor = conn.cursor()
            res = cursor.execute(f"SELECT * FROM weather_data WHERE city='{city_name}' AND state='{state_name}' AND country='{country_code}'")
            row = res.fetchone()
            if row:
                # If current timestamp is after an hour of the previous one, make another call and update the DB
                saved_ts = row[6]
                now = int(time.time())
                if now - saved_ts > 3600 or not row[5]:
                    curr_weather = fetch_weather(lat, long)
                    if not curr_weather:
                        raise HTTPException(status_code=500, detail="Failed to reach weather server/Could not lookup weather for requested city/state/country")
                    temp = json.dumps(curr_weather["current"])  # Turns your json dict into a str
                    encodedWeather = str(base64.urlsafe_b64encode(temp.encode('utf-8')), "utf-8")
                    cursor.execute(f"""
                                   UPDATE weather_data
                                   SET weather = '{encodedWeather}', timestamp = {now}
                                   WHERE city = {city_name}, state = {state_name}, country = {country_code}
                                   """)
                    conn.commit()
                    print("Successfully retrieved weather from DB and triggered update")
                    return curr_weather["current"]
                else:
                    decodedWeather = base64.urlsafe_b64decode(row[5])
                    weatherJson = json.loads(decodedWeather)
                    print("Successfully retrieved weather from DB without updating API")
                    return weatherJson
            else:
                # Call the geodata API, then weather API, save into DB
                if not lat or not long:
                    geodata = get_geodata(city_name, state_name, country_code)
                    if not geodata:
                        raise HTTPException(status_code=500, detail="Failed to reach geolocation server/Could not find geolocation data")
                    lat = geodata["lat"]
                    long = geodata["lon"]
                
                print(lat, long)    
    
                curr_weather = fetch_weather(lat, long)
                if not curr_weather:
                    raise HTTPException(status_code=500, detail="Failed to reach weather server/Could not lookup weather for requested city/state/country")
                
                temp = json.dumps(curr_weather["current"])  # Turns your json dict into a str
                encodedWeather = str(base64.urlsafe_b64encode(temp.encode('utf-8')), "utf-8")
                cursor.execute(f"""
                                INSERT INTO weather_data (city, state, country, lat, long, weather, timestamp)
                                VALUES (
                                    '{city_name}',
                                    '{state_name}',
                                    '{country_code}',
                                    {lat},
                                    {long},   
                                    '{encodedWeather}',
                                    {int(time.time())}
                                )
                               """)
                conn.commit()
                return curr_weather["current"]
                
                
    except sqlite3.OperationalError as e:
        raise HTTPException(status_code=500, detail=f"Failed to connect to database: {e}")
    
    
    
    return {"city_name": city_name, "state_name": state_name, "country_code": country_code}

@app.get("/remove-weather-history")
async def remove_weather_history(city_name: str, state_name: str = "", country_code: str = ""):
    city_name, state_name, country_code = city_name.upper(), state_name.upper(), country_code.lower()
    if not state_name or country_code:
        geodata = get_geodata(city_name, state_name, country_code)
        if not geodata:
            raise HTTPException(status_code=500, detail="Failed to reach geolocation server/Could not find geolocation data")
        state_name = geodata["state"]
        country_code = geodata["country"]
    
    with sqlite3.connect("weather.db") as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(f"""
                            UPDATE weather_data
                            SET weather = '', timestamp = {int(time.time())}
                            WHERE city = {city_name}, state = {state_name}, country = {country_code}
                            """)
            conn.commit()
            return {"result": True}
        except sqlite3.OperationalError as e:
            raise HTTPException(status_code=500, detail=f"Failed to remove from db: incorrect input maybe?")

@app.get("/retrieve-all-weather")
async def retrieve_all_weather():
    ret = {}
    with sqlite3.connect("weather.db") as conn:
        cursor = conn.cursor()
        try:
            res = cursor.execute("SELECT * from weather_data")
            for row in res.fetchall():
                decodedWeather = base64.urlsafe_b64decode(row[5])
                ret[f"{row[0]}.{row[1]}.{row[2]}"] = json.loads(decodedWeather)
            return ret
        except sqlite3.OperationalError as e:
            raise HTTPException(status_code=500, detail=f"Failed to remove from db: incorrect input maybe?")
        

