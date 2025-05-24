import requests
import json
import pandas as pd
import uuid
from datetime import datetime
import base64
import paho.mqtt.client as mqtt
import time
from io import StringIO
import streamlit as st
import os
import pydeck as pdk
import msgpack
import logging

import base64
import time
import uuid
import requests
import pandas as pd
from datetime import datetime
from typing import Tuple


logger = logging.getLogger(__name__)

class FuelDataClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_url: str = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken",
        fuel_url: str = "https://api.onegov.nsw.gov.au/FuelPriceCheck/v1/fuel/prices",
        retries: int = 5,
        backoff: int = 5,
        timeout: int = 10,
        retry_on_status: Tuple[int, ...] = (429, 500, 502, 503, 504),
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self.fuel_url = fuel_url
        self.retries = retries
        self.backoff = backoff
        self.timeout = timeout
        self.retry_on_status = retry_on_status
        
    def access_token(self) -> requests.Response:
        b64_credentials = base64.b64encode(f'{self.client_id}:{self.client_secret}'.encode()).decode()
        headers = {
        'Authorization': f'Basic {b64_credentials}',
        'Content-Type': 'application/json'
        }
        querystring = {"grant_type": "client_credentials"}
        
        for attempt in range(1, self.retries + 1):
            try:
                response = requests.get(url=self.token_url, headers=headers, params=querystring, timeout=self.timeout)
                if response.status_code == 200:
                    return response.json()['access_token']
                
                if response.status_code in self.retry_on_status:
                    wait = self.backoff * attempt
                    logger.warning(f"Normal access error {response.status_code}, try{attempt}/{self.retries} time reconnect...")
                    time.sleep(wait)
                    continue
             
                elif response.status_code in (401, 403, 400, 404):
                    
                    logger.error(f"Access denied or bad request (HTTP {response.status_code}), please check token or other parameters.")
                    response.raise_for_status()

                response.raise_for_status()

            except requests.exceptions.RequestException as e:
                logger.error(f"Access error: {e}, please check token or other parameters.")
                break  

        raise TimeoutError(f"Retry {self.retries} times failed: cannot connect to {self.token_url}")

    def get_fuel_data(self)-> Tuple[pd.DataFrame,pd.DataFrame]:
      
        access_token = self.access_token()
        if not access_token:
            raise ValueError("Doesn't have access_token")

        fuel_headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json; charset=utf-8",
            "apikey": self.client_id,
            "transactionid": str(uuid.uuid4()),
            "requesttimestamp": datetime.utcnow().strftime("%d/%m/%Y %I:%M:%S %p"),
        }
            
        for attempt in range(1, self.retries + 1):
            try:
                response = requests.get(self.fuel_url, headers=fuel_headers, timeout=self.timeout)
                if response.status_code == 200:
                    data = response.json()
                    stations = pd.json_normalize(data.get("stations", []))
                    prices   = pd.json_normalize(data.get("prices", []))
                    return stations, prices
                if response.status_code in self.retry_on_status:
                    wait = self.backoff * attempt
                    logger.warning(f"HTTP {response.status_code}, retry {attempt}/{self.retries}...")
                    time.sleep(wait)
                    continue
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                if attempt == self.retries:
                    raise
                wait = self.backoff * attempt
                logger.error(f"Request error: {e}, retrying {attempt+1}/{self.retries}...")
                time.sleep(wait)
        raise TimeoutError(f"Failed to GET {self.fuel_url} after {self.retries} retries")
  

        


def publish_mqtt(client, topic, loaddata, batch_size=200, delay=0.1):
    

    if isinstance(loaddata, list):
        if len(loaddata) > batch_size:
            total = len(loaddata)
            for start_idx in range(0, total, batch_size):
                batch = loaddata[start_idx:start_idx + batch_size]
                start = time.time()
                try:
                    info = client.publish(topic, json.dumps(batch), qos=1)
                    info.wait_for_publish()
                except Exception as e:
                    logger.error(f"MQTT publish {topic}({len(batch)} items) error: {e}")
                end = time.time()
                logger.info(f"Published list batch to {topic} ({len(batch)} items) in {end - start:.4f} seconds")
                if delay > 0:
                    time.sleep(delay)
        else:
            start = time.time()
            try:
                info = client.publish(topic, json.dumps(loaddata), qos=1)
                info.wait_for_publish()
            except Exception as e:
                logger.error(f"MQTT publish {topic} error: {e}")
            end = time.time()
            logger.info(f"Published entire list to {topic} ({len(loaddata)} items) in {end - start:.4f} seconds")

    elif isinstance(loaddata, pd.DataFrame):
        total_rows = len(loaddata)
        if total_rows > batch_size and topic != "fuel/new_prices":
            start = time.time()
            for start_idx in range(0, total_rows, batch_size):
                batch_df = loaddata.iloc[start_idx:start_idx + batch_size]
                csv_str = batch_df.to_csv(index=False)
                
                info = client.publish(topic, csv_str, qos=1)
                info.wait_for_publish()
                
                if delay > 0:
                    time.sleep(delay)
            end = time.time()
            logger.info(f"Published CSV batch to {topic} ({total_rows} rows) in {end - start:.4f} seconds")

        elif topic == "fuel/new_prices":
            for record in loaddata:
                payload = msgpack.packb(record)  
                # print(len(payload))
                client.publish("fuel/new_prices", payload, qos=0)
                time.sleep(0.1)
            logger.info(f"Published {topic} ({len(loaddata)} rows)")
        else:
            csv_str = loaddata.to_csv(index=False)
            start = time.time()
            info = client.publish(topic, csv_str, qos=1)
            info.wait_for_publish()
            end = time.time()
            logger.info(f"Published full CSV to {topic} ({total_rows} rows) in {end - start:.4f} seconds")

    else:
        logger.warning("Unsupported data type for MQTT publish.")


def clean_data(stations, prices,csv_path="fresh_prices.csv"):
    stations = stations.rename(columns={
        'code': 'stationcode',
        'name': 'station_name',
        'location.latitude': 'lat',
        'location.longitude': 'lon'
    }).dropna(subset=['lat', 'lon'])

    stations['lat'] = pd.to_numeric(stations['lat'], errors='coerce')
    stations['lon'] = pd.to_numeric(stations['lon'], errors='coerce')

    prices['price'] = pd.to_numeric(prices['price'], errors='coerce')
    prices = prices[prices['price'] > 0]

    latest_price = prices.sort_values("lastupdated").groupby(['stationcode', 'fueltype']).last().reset_index()
    latest_price['record_id'] = (
        latest_price['stationcode'].astype(str) + '_' +
        latest_price['fueltype'].astype(str) + '_' +
        latest_price['lastupdated'].astype(str)
    )
    fuel_options = sorted(latest_price['fueltype'].unique().tolist())

    existing_data = pd.DataFrame() 
    if os.path.exists(csv_path):
        existing_data = pd.read_csv(csv_path)
        existing_data['record_id'] = (
            existing_data['stationcode'].astype(str) + '_' +
            existing_data['fueltype'].astype(str) + '_' +
            existing_data['lastupdated'].astype(str)
        )
        existing_ids = set(existing_data['record_id'])
    else:
        existing_ids = set()
   
    new_rows = latest_price[~latest_price['record_id'].isin(existing_ids)].copy()
    new_rows.drop(columns=['record_id'], inplace=True)

    if not new_rows.empty:
        new_rows.to_csv(csv_path, mode='a', header=not os.path.exists(csv_path), index=False)
        logger.info(f"✅ update {len(new_rows)} new prices to {csv_path}")
    else:
        logger.info("ℹ️ no update price")

    if len(existing_data):
        return stations, new_rows, fuel_options, existing_data
    else:
        return stations, new_rows, fuel_options



if __name__=="__main__":
    # fuel_data=FuelDataClient(client_id='CvRQC1qC8akmwp9Qfy5owgzWk8izoa9Q',
    #                              client_secret='SNmuC7nzn3IISVcG')
    # stations_raw, prince_raw= fuel_data.get_fuel_data()
    # print(len(stations_raw))
    # print(len(prince_raw))
    # stations,new_prices, fuel_options, price_records = clean_data(stations_raw, prices_raw)

    def push_data():
        fuel_data=FuelDataClient(client_id='CvRQC1qC8akmwp9Qfy5owgzWk8izoa9Q',
                                 client_secret='SNmuC7nzn3IISVcG')
        stations_raw, prices_raw = fuel_data.get_fuel_data()
        stations,new_prices, fuel_options, price_records = clean_data(stations_raw, prices_raw)

        # print(len(new_prices)) 
        # print(type(new_prices))

        client = mqtt.Client()
        client.connect("broker.hivemq.com", 1883, 60)
        client.loop_start()
        publish_mqtt(client,"fuel/new_prices", new_prices)
        publish_mqtt(client,"fuel/stations", stations)
        publish_mqtt(client,"fuel/fuel_options", fuel_options)
        publish_mqtt(client,"fuel/price_records", price_records)
        logger.info("All topic data are published")
        
        client.loop_stop()
        client.disconnect()

    while True:
        try:
            push_data()
        except Exception as e:
            logger.error(f"Loop error: {e}")
        time.sleep(60)

