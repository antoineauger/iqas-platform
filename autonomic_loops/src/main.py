import json

from bottle import run, request, response, Bottle

from information.information_core import InformationCore
from utils import fake_db

app = Bottle()
bottle_host = '127.0.0.1'
bottle_port = 8080

core = InformationCore()
core.start()
#TODO also start Raw Data core

# Only for local testing
fake_db_filename = '../data/sensors.json'


# REST APIs

@app.route('/', method="GET")
def root():
	return "request initialized successfully!\n"


@app.route('/places', method="GET")
def get_places():
	response.content_type = 'application/json'
	return json.dumps(fake_db.get_places_from_file(fake_db_filename))


@app.route('/places/<place>/topics', method="GET")
def get_topics(place):
	response.content_type = 'application/json'
	return json.dumps(fake_db.get_topics_from_file(fake_db_filename, place))


@app.route('/places/<place>/topics/<topic>/sensors', method="GET")
def get_sensors(place, topic):
	response.content_type = 'application/json'
	return json.dumps(fake_db.get_sensors_from_file(fake_db_filename, place, topic))


@app.route('/places/<place>/topics/<topic>', method='POST')
def create_request(place, topic):
	response.content_type = 'application/json'
	if request.content_length > 0:
		params = json.loads(request.body)
		ticket = core.process_request(place, topic, params)
		return "{'ticket_id' : %d}" % ticket
	else:
		return "{'error': 'wrong params'}"


@app.route('/request/<id>', method="GET")
def get_request(id):
	pass


@app.route('/request/<id>', method="DELETE")
def delete_request(id):
	pass


run(app, host=bottle_host, port=bottle_port, debug=True)
