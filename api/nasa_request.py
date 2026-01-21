import requests
def nasa_request():
    try:
        f = open(r"C:\perso\nasa_api_key\api_keys.txt")
        api_key = f.read()
    except : 
        api_key = 'your_nasa_api_keys'
    url = "https://api.nasa.gov/neo/rest/v1/neo/browse?api_key="+api_key
    result = requests.get("https://api.nasa.gov/neo/rest/v1/neo/browse?api_key=CIM9MbRo4JP9Cge440Z5YVBsSqgacL1hNkLInc3c")
    data = result.json()['near_earth_objects']
    return data
for asteroid in nasa_request():
    print(asteroid)