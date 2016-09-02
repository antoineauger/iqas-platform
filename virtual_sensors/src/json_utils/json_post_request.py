import json

from httplib2 import Http

def post_dict_to_url(url, dictionary):
    """
        Method to post a dict object (transformed in a JSON payload) to the specified url
        :param url: str
    	:param dictionary: the dictionnary to send (dict)
    """
    http_obj = Http()
    resp, content = http_obj.request(
        uri=url,
        method='POST',
        headers={'Content-Type': 'application/json; charset=UTF-8',
                 'Connection': 'Close'},
        body=json.dumps(dictionary),
    )
