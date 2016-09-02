import json

def generate_sensor_representation(response, sensor):
	""" Return a JSON representation of a virtual sensor """
	response.set_header('Content-Type', 'application/json; charset=UTF-8')
	dict_response = dict()
	dict_response['sensor_id'] = sensor.sensor_id
	dict_response['enabled'] = sensor.enabled
	dict_response['sensing'] = sensor.sensing
	dict_response['endpoint'] = sensor.endpoint
	dict_response['capabilities'] = sensor.capabilities
	dict_response['config'] = sensor.config
	return json.dumps(dict_response)

def generate_sensor_capabilities(response, sensor):
	""" Return a JSON representation of the sensor's capabilities """
	response.set_header('Content-Type', 'application/json; charset=UTF-8')
	dict_response = dict()
	dict_response['sensor_id'] = sensor.sensor_id
	dict_response['capabilities'] = sensor.capabilities
	return json.dumps(dict_response)

def generate_API_response(response, result, details, capability='', old_value='', value=''):
	""" Useful to generate a response after modifying a sensor capability.
		Result can be either 'OK' or 'NOK'. """
	response.set_header('Content-Type', 'application/json; charset=UTF-8')
	dict_response = dict()
	dict_response['result'] = result
	dict_response['additional_details'] = details
	dict_response['capability'] = capability
	dict_response['old_value'] = old_value
	dict_response['value'] = value
	return json.dumps(dict_response)
