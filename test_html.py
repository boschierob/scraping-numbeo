import requests
from bs4 import BeautifulSoup

url = "https://www.numbeo.com/quality-of-life/"
headers = {"User-Agent": "Mozilla/5.0"}

response = requests.get(url, headers=headers)
print(f"Statut HTTP : {response.status_code}")
print(f"Taille HTML : {len(response.text)}")
