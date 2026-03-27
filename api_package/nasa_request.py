import requests
import json
import time
from  datetime import datetime, timezone
def nasa_request():
    sorted_data = []
    try:
        f = open(r"C:\perso\nasa_api_key\api_keys.txt")
        api_key = f.read()
    except : 
        api_key = 'your_nasa_api_keys'
    url = "https://api.nasa.gov/neo/rest/v1/neo/browse?api_key="+api_key
    result = requests.get("https://api.nasa.gov/neo/rest/v1/neo/browse?api_key=CIM9MbRo4JP9Cge440Z5YVBsSqgacL1hNkLInc3c")
    data = result.json()['near_earth_objects']
    for asteroid in data: # static data
        asteroid_data = {}
        #general asteroid data
        asteroid_data["name_limited"] = asteroid["name_limited"]
        asteroid_data["designation"] = asteroid["designation"]
        asteroid_data["asteroid_full_name"] = asteroid_data["name_limited"] + '_' + asteroid_data["designation"]
        asteroid_data["neo_reference_id"] = asteroid['neo_reference_id']
        asteroid_data["hazardous_flag"] = asteroid['is_potentially_hazardous_asteroid']
        asteroid_data["sentry_flag"] = asteroid['is_sentry_object']

        #asteroid size
        asteroid_data["diameter_min_km"] = round(asteroid['estimated_diameter']['kilometers']['estimated_diameter_min'],3)
        asteroid_data["diameter_max_km"] = round(asteroid['estimated_diameter']['kilometers']['estimated_diameter_max'],3)
        asteroid_data["diameter_avg_km"] = round((asteroid_data["diameter_min_km"] + asteroid_data["diameter_max_km"]) / 2,3)



        #orbit class
        asteroid_data["orbit_class_type"] = asteroid['orbital_data']['orbit_class']['orbit_class_type']
        asteroid_data["orbit_class_description"] = asteroid['orbital_data']['orbit_class']['orbit_class_description']
        asteroid_data["eccentricity"] = round(float(asteroid['orbital_data']['eccentricity']),3)
        asteroid_data["semi_major_axis_au"] = round(float(asteroid['orbital_data']['semi_major_axis']),3)
        asteroid_data["inclination_deg"] =round(float(asteroid['orbital_data']['inclination']),3)
        asteroid_data["orbital_period_days"] = round(float(asteroid['orbital_data']['orbital_period']),3)
        asteroid_data["perihelion_distance_au"] = round(float(asteroid['orbital_data']['perihelion_distance']),3)
        asteroid_data["orbit_uncertainty"] = asteroid['orbital_data']['orbit_uncertainty']


        for i in range(len(asteroid['close_approach_data'])) :
            asteroid_data["event_id"] = asteroid_data["asteroid_full_name"] + '_' + asteroid['close_approach_data'][i]['close_approach_date_full']
            asteroid_data["approach_date"] = asteroid['close_approach_data'][i]['close_approach_date']
            asteroid_data["approach_date_full"] = asteroid['close_approach_data'][i]['close_approach_date_full']
            asteroid_data["orbiting_body"] = asteroid['close_approach_data'][i]['orbiting_body']
            asteroid_data["velocity_km_s"] = round(float(asteroid['close_approach_data'][i]['relative_velocity']['kilometers_per_second']),3)
            asteroid_data["velocity_km_h"] = round(float(asteroid['close_approach_data'][i]['relative_velocity']['kilometers_per_hour']),3)
            asteroid_data["miss_distance_km"] = round(float(asteroid['close_approach_data'][i]['miss_distance']['kilometers']),3)
            asteroid_data["miss_distance_lunar"] = round(float(asteroid['close_approach_data'][i]['miss_distance']['lunar']),3)
            asteroid_data["miss_distance_au"] = round(float(asteroid['close_approach_data'][i]['miss_distance']['astronomical']),3)
            asteroid_data["timestamp_event"] = datetime.now(timezone.utc).isoformat()


            asteroid_data = json.dumps(asteroid_data)
            asteroid_data = json.loads(asteroid_data)
            sorted_data.append(asteroid_data)

    return sorted_data
nasa_request()