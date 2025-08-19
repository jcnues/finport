import requests
import pandas as pd

# URL TO ACCESS TAGS EXPLANATIONS: https://wiki.openstreetmap.org/wiki/Map_features#Addresses

# Overpass API endpoint
OVERPASS_URL = "http://overpass-api.de/api/interpreter"

# Query: find all nodes/ways/relations tagged as "tourism=aquarium"
query = """
[out:json][timeout:300];
(
  node["tourism"="aquarium"];
  way["tourism"="aquarium"];
  relation["tourism"="aquarium"];
);
out center;
"""

# Send request
response = requests.get(OVERPASS_URL, params={'data': query})
data = response.json()


