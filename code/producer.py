import requests
import json
import time
import sys
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['10.157.60.165:9092'],value_serializer=lambda K:json.dumps(K).encode('utf-8'))
fuelType = None

def addition(n):
    return {
        "fuelType": sys.argv[1],
        **n
    }

def main():
    if len(sys.argv) < 2:
        print('Missing argument')
        return
        
    fuelType = sys.argv[1]
    authToken = input("Enter auth token: ")
    yearStart = 2021
    yearEnd = 2023

    res = []
    for y in range(yearStart, yearEnd):
        response = requests.get(
            f"https://markets.tradingeconomics.com/chart?s=ng1:com&d1={y}-01-01&d2={y+1}-01-01&interval=1d&securify=new&url=/commodity/{fuelType}&AUTH={authToken}&ohlc=0"
        )
        if response.json() != None:
            print('fuelType: %s', fuelType)
            data = response.json().get("series", [{}])[0].get("data", [])
            # res.extend(data)    
            print('Length:', len(data) or 0)
            tmp = map(addition, data)
            res.extend(list(tmp))
            sendValue = json.loads(json.dumps(res))
            producer.send('GasData', value=sendValue)
            time.sleep(60)
            res = []
        else:
            print('No Data')
            time.sleep(30)
            continue


main()