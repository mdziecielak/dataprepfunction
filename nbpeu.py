import requests
import json
api_url = "https://api.nbp.pl/api/exchangerates/rates/a/eur"
response = requests.get(api_url)
response.json()
print(response.json())