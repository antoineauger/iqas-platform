import json

from httplib2 import Http

def post_dict_to_url(url, dictionary):
    '''
    Pass the whole dictionary as a json body to the url.
    Make sure to use a new Http object each time for thread safety.
    '''
    http_obj = Http()
    resp, content = http_obj.request(
        uri=url,
        method='POST',
        headers={'Content-Type': 'application/json; charset=UTF-8', 'Connection': 'Close'},
        body=json.dumps(dictionary),
    )