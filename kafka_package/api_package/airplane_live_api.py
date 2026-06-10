import requests
import time
from  datetime import datetime, timezone
def airplane_request():
    list_data = []

    url = "https://api.airplanes.live/v2/point/48.8566/2.3522/250"
    result = requests.get(url)
    data = result.json()
    try:
        if data["ac"]:
            for airplane in data["ac"]:
                airplane_data = {}
                airplane_data["hex"] = airplane.get("hex")
                airplane_data["type"] = airplane.get("type")
                airplane_data["flight"] = airplane.get("flight")
                airplane_data["r"] = airplane.get("r")
                airplane_data["t"] = airplane.get("t")
                airplane_data["desc"] = airplane.get("desc")
                airplane_data["lat"] = airplane.get("lat")
                airplane_data["lon"] = airplane.get("lon")
                airplane_data["alt_geom"] = airplane.get("alt_geom")
                airplane_data["alt_baro"] = airplane.get("alt_baro")
                airplane_data["gs"] = airplane.get("gs")
                airplane_data["track"] = airplane.get("track")
                airplane_data["geom_rate"] = airplane.get("geom_rate")
                airplane_data["mach"] = airplane.get("mach")
                airplane_data["squawk"] = airplane.get("squawk")
                airplane_data["emergency"] = airplane.get("emergency")
                airplane_data["category"] = airplane.get("category")
                airplane_data["messages"] = airplane.get("messages")
                airplane_data["alert"] = airplane.get("alert")
                airplane_data["seen"] = airplane.get("seen")
                airplane_data["seen_pos"] = airplane.get("seen_pos")
                
                airplane_data["timestamp_ingest"] = datetime.now(timezone.utc).isoformat()
                list_data.append(airplane_data)
    except: raise "erreur pas d'avion"
    
    return list_data
airplane_request()
