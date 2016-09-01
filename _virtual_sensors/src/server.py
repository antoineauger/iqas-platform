import json
import sys
import logging

from bottle import run, request, response, Bottle

from json_response import JSONResponse
from virtual_sensor import VirtualSensor

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Bottle parameters

def usage():
    """Print short help """
    print("iQAS: an integration platform for QoI Assessment as a Service")
    print("Module: Virtual Sensor")
    print("(C) 2016 ISAE-SUPAERO\n")

if len(sys.argv) != 4:
    print('ERROR: Wrong number of parameters')
    usage()
    exit()

app = Bottle()
bottle_host = str(sys.argv[1])
bottle_port = int(sys.argv[2])
sensor_id = str(sys.argv[3])
sensor = None
sensor_endpoint = 'http://' + bottle_host + ':' + str(bottle_port) + '/' + sensor_id
logger.warning("Virtual sensor '{}' listening on {}".format(sensor_id, sensor_endpoint))

# REST APIs for the virtual sensor (see also the corresponding class virtual_sensor.py)

@app.route('/' + sensor_id, method='GET')
def init_virtual_sensor():
    """ Dummy method for testing purpose """
    json_response = JSONResponse.generate_ok_response(response, "Sensor {} initialized successfully!".format(sensor_id), "", "", "")
    return json_response

@app.route('/' + sensor_id + '/enabled', method='GET')
def get_sensor_enabled():
    json_response = JSONResponse.generate_ok_response(response, "", "enabled", "", sensor.enabled)
    return json_response

@app.route('/' + sensor_id + '/enabled', method='POST')
def set_sensor_enabled():
    request_json_body = json.loads(request.body.read().decode('UTF-8'))
    old_value = sensor.enabled
    value = request_json_body['value']
    if type(value) == bool:
        sensor.enable_sensor(value)
        json_response = JSONResponse.generate_ok_response(response, "", "enabled", old_value, value)
    else:
        json_response = JSONResponse.generate_nok_response(response, "Only one boolean is accepted for this POST request. E.g.: {'value': true}")
    return json_response

@app.route('/' + sensor_id + '/sensing', method='GET')
def get_sensor_enabled():
    json_response = JSONResponse.generate_ok_response(response, "", "sensing", "", sensor.sensing)
    return json_response

@app.route('/' + sensor_id + '/sensing', method='POST')
def set_sensor_enabled():
    request_json_body = json.loads(request.body.read().decode('UTF-8'))
    old_value = sensor.enabled
    value = request_json_body['value']
    if type(value) == bool:
        sensor.enable_sensing_process(value)
        json_response = JSONResponse.generate_ok_response(response, "", "sensing", old_value, value)
    else:
        json_response = JSONResponse.generate_nok_response(response, "Only one boolean is accepted for this POST request. E.g.: {'value': true}")
    return json_response

@app.route('/' + sensor_id + '/parameters', method='GET')
def get_sensor_details():
    """ Return an overview of the different parameters and settings for the specified virtual sensor """
    if sensor is None:
        json_response = JSONResponse.generate_nok_response(response, "Error: sensor {} is not initialized yet...".format(sensor_id))
        return json_response
    else:
        return sensor.__repr__()

@app.route('/' + sensor_id + '/parameters/<parameter>', method='GET')
def get_sensor_capability(parameter):
    """ Method to get a sensor parameter (capability) """
    if sensor is None:
        json_response = JSONResponse.generate_nok_response(response, "Error: sensor {} is not initialized yet...".format(sensor_id))
    elif parameter in sensor.capabilities.keys():
        json_response = JSONResponse.generate_ok_response(response, "", parameter, "", sensor.capabilities[parameter])
    else:
        json_response = JSONResponse.generate_nok_response(response, "Unknown parameter '{}' for sensor {}".format(parameter, sensor_id))
    return json_response

@app.route('/' + sensor_id + '/parameters/<parameter>', method='POST')
def set_sensor_capability(parameter):
    """ Method to modify a sensor parameter (capability) """
    if parameter in sensor.capabilities.keys():
        request_json_body = json.loads(request.body.read().decode('UTF-8'))
        old_value = sensor.get_parameter(parameter)
        value = request_json_body['value']
        sensor.set_parameter(parameter, value)
        json_response = JSONResponse.generate_ok_response(response, "", parameter, old_value, value)
    else:
        json_response = JSONResponse.generate_nok_response(response, "Unknown parameter '{}' for sensor {}".format(parameter, sensor_id))
    return json_response

with open('../etc/capabilities.json') as capabilities_file:
    capabilities = json.load(capabilities_file)

# Virtual sensor creation
sensor = VirtualSensor(sensor_id, True, sensor_endpoint, capabilities)

# Start of a bottle server to handle calls to the sensor API
run(app, host=bottle_host, port=bottle_port, debug=True)
