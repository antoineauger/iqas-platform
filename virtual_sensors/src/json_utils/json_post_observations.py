import json

from httplib2 import Http


def post_obs_to_rest_endpoint(url, dictionary):
    """
        Method to POST a dict object (transformed in a JSON payload) to a REST endpoint
        :param url: str
    	:param dictionary: the dictionnary to send (dict)
    """
    http_obj = Http()
    http_obj.request(
        uri=url,
        method='POST',
        headers={'Content-Type': 'application/json; charset=UTF-8',
                 'Connection': 'Close'},
        body=json.dumps(dictionary)
    )


def post_obs_to_kafka_topic(kafka_producer, topic, dictionary):
    kafka_producer.send(topic, bytes(json.dumps(dictionary), 'utf8'))
