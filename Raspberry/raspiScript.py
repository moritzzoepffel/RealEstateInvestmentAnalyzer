# Script that gets data from an Api and saves it to MongoDB Atlas Database
from datetime import datetime

import requests
import pymongo
import Credentials.credentials as credentials


# Get data from Api
def get_data(url):
    payload = {}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36',
        "Upgrade-Insecure-Requests": "1", "DNT": "1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5", "Accept-Encoding": "gzip, deflate"}

    response = requests.request("GET", url, headers=headers, data=payload)
    return response.json()


# Save data to MongoDB Atlas Database
def save_data(data, dbname):
    client = pymongo.MongoClient(
        f"mongodb+srv://moritzzoepffel:{credentials.MONGO_DB_PW}@{credentials.MONGO_DB_DB}/?retryWrites=true&w=majority")
    db = client.test
    collection = db[dbname]
    collection.insert_many(data)


if __name__ == '__main__':
    urls = ["https://www.hamburg-airport.de/service/flightdata/arrivals",
            "https://www.hamburg-airport.de/service/flightdata/departures"]

    now = str(datetime.now()).split('.')[0].replace(' ', '_')
    for url in urls:
        flight_data = get_data(url)
        save_data(flight_data, f"{now}_flights_{url.split('/')[-1]}")
