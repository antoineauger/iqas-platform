import json

from bottle import run, request, response, Bottle
from virtual_sensor import VirtualSensor

# Bottle parameters

app = Bottle()
bottle_host = '127.0.0.1'
bottle_port = 8080

# Sensor identification

sensorID = 's01'
sensorEndpoint = 'http://' + bottle_host + ':' + str(bottle_port) + '/' + sensorID
sensor = None

# REST APIs for the virtual sensor (see also the corresponding class virtual_sensor.py)

@app.route('/' + sensorID + '/init', method='GET')
def init_virtual_sensor():
    """ Dummy method for testing purpose """
    response.set_header('Content-Type', 'application/json; charset=UTF-8')
    dict_response = dict()
    dict_response['status'] = "OK"
    dict_response['error_message'] = ""
    dict_response['additional_details'] = "Sensor {} initialized successfully!".format(sensorID)
    return json.dumps(dict_response)

@app.route('/' + sensorID + '/enabled', method='GET')
def get_sensor_enabled():
    response.set_header('Content-Type', 'application/json; charset=UTF-8')
    dict_response = dict()
    dict_response['status'] = "OK"
    dict_response['error_message'] = ""
    dict_response['parameter'] = "enabled"
    dict_response['value'] = sensor.enabled
    return json.dumps(dict_response)

@app.route('/' + sensorID + '/enabled', method='POST')
def set_sensor_enabled():
    request_json_body = json.loads(request.body.read().decode('UTF-8'))
    old_value = sensor.enabled
    new_value = request_json_body['new_value']
    response.set_header('Content-Type', 'application/json; charset=UTF-8')
    dict_response = dict()
    if type(new_value) == bool:
        sensor.enable_sensor(new_value)
        dict_response['status'] = "OK"
        dict_response['error_message'] = ""
        dict_response['parameter'] = 'enabled'
        dict_response['old_value'] = old_value
        dict_response['new_value'] = new_value
    else:
        dict_response['status'] = "NOK"
        dict_response['error_message'] = "Only one boolean is accepted for this POST request. E.g.: {'new_value': true}"
    return json.dumps(dict_response)

@app.route('/' + sensorID + '/sensing', method='GET')
def get_sensor_enabled():
    response.set_header('Content-Type', 'application/json; charset=UTF-8')
    dict_response = dict()
    dict_response['status'] = "OK"
    dict_response['error_message'] = ""
    dict_response['parameter'] = "sensing"
    dict_response['value'] = sensor.sensing
    return json.dumps(dict_response)

@app.route('/' + sensorID + '/sensing', method='POST')
def set_sensor_enabled():
    request_json_body = json.loads(request.body.read().decode('UTF-8'))
    old_value = sensor.enabled
    new_value = request_json_body['new_value']
    response.set_header('Content-Type', 'application/json; charset=UTF-8')
    dict_response = dict()
    if type(new_value) == bool:
        sensor.enable_sensing_process(new_value)
        dict_response['status'] = "OK"
        dict_response['error_message'] = ""
        dict_response['parameter'] = 'sensing'
        dict_response['old_value'] = old_value
        dict_response['new_value'] = new_value
    else:
        dict_response['status'] = "NOK"
        dict_response['error_message'] = "Only one boolean is accepted for this POST request. E.g.: {'new_value': true}"
    return json.dumps(dict_response)

@app.route('/' + sensorID + '/parameters', method='GET')
def get_sensor_details():
    """ Return an overview of the different parameters and settings for the specified virtual sensor """
    if sensor is None:
        response.set_header('Content-Type', 'application/json; charset=UTF-8')
        dict_response = dict()
        dict_response['status'] = "NOK"
        dict_response['error_message'] = "Error: sensor {} is not initialized yet...".format(sensorID)
        return json.dumps(dict_response)
    else:
        return sensor.__repr__()

@app.route('/' + sensorID + '/parameters/<parameter>', method='GET')
def get_sensor_capability(parameter):
    """ Method to get a sensor parameter (capability) """
    response.set_header('Content-Type', 'application/json; charset=UTF-8')
    dict_response = dict()
    if sensor is None:
        dict_response['status'] = "NOK"
        dict_response['error_message'] = "Error: sensor {} is not initialized yet...".format(sensorID)
    elif parameter in sensor.capabilities.keys():
        dict_response['status'] = "OK"
        dict_response['error_message'] = ""
        dict_response['parameter'] = parameter
        dict_response['value'] = sensor.capabilities[parameter]
    else:
        dict_response['status'] = "NOK"
        dict_response['error_message'] = "Unknown parameter '{}' for sensor {}".format(parameter, sensorID)
    return json.dumps(dict_response)

@app.route('/' + sensorID + '/parameters/<parameter>', method='POST')
def set_sensor_capability(parameter):
    """ Method to modify a sensor parameter (capability) """
    response.set_header('Content-Type', 'application/json; charset=UTF-8')
    dict_response = dict()
    if parameter in sensor.capabilities.keys():
        request_json_body = json.loads(request.body.read().decode('UTF-8'))
        old_value = sensor.get_parameter(parameter)
        new_value = request_json_body['new_value']
        sensor.set_parameter(parameter, new_value)
        dict_response['status'] = "OK"
        dict_response['error_message'] = ""
        dict_response['parameter'] = parameter
        dict_response['old_value'] = old_value
        dict_response['new_value'] = new_value
    else:
        dict_response['status'] = "NOK"
        dict_response['error_message'] = "Unknown parameter '{}' for sensor {}".format(parameter, sensorID)
    return json.dumps(dict_response)

with open('../etc/capabilities.json') as capabilities_file:
    capabilities = json.load(capabilities_file)

# Virtual sensor creation and start of the sensor's main thread

sensor = VirtualSensor(sensorID, True, sensorEndpoint, capabilities)

# Start of a bottle server to handle calls to the sensor API

run(app, host=bottle_host, port=bottle_port, debug=True)
