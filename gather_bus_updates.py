import json
import time
import os
from typing import Dict, Tuple, List

import requests
import pandas as pd
from tqdm import tqdm

BUSTIME_API_KEY = os.getenv('BUSTIME_API_KEY')

# Keep track of the number of requests we make as there is a 10k daily limit
CALLS_COUNT = 0

BASE_URL = 'http://realtime.portauthority.org/bustime/api/v3'

# DATAFEED_NAME can be obtained by querying the '/getrtpidatafeeds' endpoint
# see Appendix for more information
DATAFEED_NAME = 'Port Authority Bus'

VEHICLES_ENDPOINT = '/getvehicles'
ROUTES_ENDPOINT = '/getroutes'
PATTERNS_ENDPOINT = '/getpatterns'
DIRECTIONS_ENDPOINT = '/getdirections'
STOPS_ENDPOINT = '/getstops'
PREDICTIONS_ENDPOINT = '/getpredictions'


def query_api(endpoint: str, params: Dict[str, str] = {}) -> Dict:
    """
    Query the BusTime API on the provided endpoint with the provided parameters.
    The key, format and rpidatafeed parameters will automatically be added
    to the parameters.

    'endpoint' has to be one of the globally defined endpoints
    """

    params['key'] = BUSTIME_API_KEY
    params['format'] = 'json'
    params['rtpidatafeed'] = DATAFEED_NAME

    response = requests.get(BASE_URL + endpoint,
                            params=params)
    global CALLS_COUNT
    CALLS_COUNT += 1

    if response.status_code == 200:
        # Responses are always contained in the 'bustime-response' object
        return json.loads(response.text)['bustime-response']
    else:
        print(response.status_code, response.text)
        return None


def get_vehicles(route_ids: str) -> List[Dict]:
    """
    Retrieve all the running buses on the specified route ids.

    route_ids is a comma-separated list of up to 10 route ids.
    Example: '61A,61B,71D'

    Return a list of vehicles as a list of dicts.
    """

    params = {}
    params['rt'] = route_ids
    params['tmres'] = 's'

    response = query_api(VEHICLES_ENDPOINT, params)

    if response is None or 'vehicle' not in response:
        print('Couldn\'t retrieve vehicles', response)
        return []

    raw_vehicles = response['vehicle']

    # Refer to the documentation for more information
    vehicles = [{'id': v['vid'],
                 'timestamp': v['tmstmp'],
                 'lat': v['lat'],
                 'lon': v['lon'],
                 'destination': v['des'],
                 'pattern_id': v['pid'],
                 'pattern_distance': v['pdist'],
                 'trip_id': v['tatripid'],
                 'speed': v['spd'],
                 'passenger_load': v['psgld']} for v in raw_vehicles]

    return vehicles


ROUTE_ID = '61A,61B,61C,61D,71A,71B,71C'

number_iterations = 3600  # 3600 * 3sec -> 3h
vehicles = []

for i in tqdm(range(number_iterations)):
    vehicles_now = get_vehicles(ROUTE_ID)
    vehicles += vehicles_now

    if (i + 1) % 600 == 0:
        print('Saving...')
        from_date = vehicles[0]['timestamp'].replace(' ', '@')
        to_date = vehicles[-1]['timestamp'].replace(' ', '@')
        df_vehicles = pd.DataFrame(vehicles).drop_duplicates()
        df_vehicles.to_csv(f'vehicles{from_date}-{to_date}.csv')

    time.sleep(3)

print(f'Called the API {CALLS_COUNT} times.')
