import json


def get_places_from_file(filename):
	obj_to_return = dict()
	places = set()
	with open(filename, 'r') as fake_db:
		json_file = json.load(fake_db)
		for sensor in json_file['sensors']:
			places.add(sensor['location_name'])
	obj_to_return['places'] = list(places)
	return obj_to_return


def get_topics_from_file(filename, place):
	obj_to_return = dict()
	topics = set()
	with open(filename, 'r') as fake_db:
		json_file = json.load(fake_db)
		for sensor in json_file['sensors']:
			if place == sensor['location_name']:
				for topic in sensor['topics']:
					topics.add(topic['name'])
	obj_to_return['place'] = place
	obj_to_return['topics'] = list(topics)
	return obj_to_return


def get_sensors_from_file(filename, place, topic):
	obj_to_return = dict()
	sensors = list()
	with open(filename, 'r') as fake_db:
		json_file = json.load(fake_db)
		for sensor in json_file['sensors']:
			if place == sensor['location_name']:
				for json_topic in sensor['topics']:
					if topic == json_topic['name']:
						temp_sensor = dict()
						temp_sensor['sensor_id'] = sensor['sensor_id']
						temp_sensor['endpoint'] = json_topic['endpoint']
						print(temp_sensor)
						sensors.append(temp_sensor)
	obj_to_return['place'] = place
	obj_to_return['topic'] = topic
	obj_to_return['sensors'] = list(sensors)
	return obj_to_return
