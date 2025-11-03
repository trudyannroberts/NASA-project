import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
URL = f"https://api.nasa.gov/planetary/apod?api_key={API_KEY}"

def fetch_picture_data():
    response = requests.get(URL)
    response.raise_for_status()
    data = response.json()

    date = data["date"]
    description = data["explanation"]
    copyright = data["copyright"]
    url = data["url"]

    return date, description, copyright, url

