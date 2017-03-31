import json


def generate_sensor_representation(response, sensor):
	"""
		Return a JSON representation of a virtual sensor
		:param response: a bottle response object (only used to modify its Content-Type)
		:param sensor: a VirtualSensor object
		:returns A str representation of a sensor
		:rtype str
	"""
	response.set_header('Content-Type', 'application/json; charset=UTF-8')
	dict_response = dict()
	dict_response['sensor_id'] = sensor.sensor_id
	dict_response['enabled'] = sensor.enabled
	dict_response['sensing'] = sensor.sensing
	dict_response['capabilities'] = sensor.capabilities
	dict_response['config'] = sensor.config
	return json.dumps(dict_response)


def generate_sensor_capabilities(response, sensor):
	"""
		Return a JSON representation of the sensor's capabilities
		:param response: a bottle response object (only used to modify its Content-Type)
		:param sensor: a VirtualSensor object
		:returns A str representation of sensor capabilities
		:rtype str
	"""
	response.set_header('Content-Type', 'application/json; charset=UTF-8')
	dict_response = dict()
	dict_response['sensor_id'] = sensor.sensor_id
	dict_response['capabilities'] = sensor.capabilities
	return json.dumps(dict_response)


def generate_api_response(response, result, details, capability='', old_value='', value=''):
	"""
		Useful to generate a response after modifying a sensor capability.
		:param response: a bottle response object (only used to modify its Content-Type)
		:param result: a str that can be either "OK" or "NOK"
		:param details: a str to describe errors, if any
		:param capability: a str to explicit what was the topic of the initial request
		:param old_value: the old value for the specified capability (str, int, bool or float)
		:param value: the current value for the specified capability (str, int, bool or float)
		:returns The response to the sensor's API request
		:rtype str
	"""
	response.set_header('Content-Type', 'application/json; charset=UTF-8')
	dict_response = dict()
	dict_response['result'] = result
	dict_response['additional_details'] = details
	dict_response['capability'] = capability
	dict_response['old_value'] = old_value
	dict_response['value'] = value
	return json.dumps(dict_response)
