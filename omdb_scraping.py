import requests

API_URL = "http://www.omdbapi.com"


def ping_api(movie_name, year=None, api_key="76ca14dd"):

    request_params = {'apikey': api_key, 'type': 'movie', 't': movie_name, 'plot': 'full'}

    if year:
        request_params['y'] = year

    r = requests.get(API_URL, params=request_params)

    return r.json()
