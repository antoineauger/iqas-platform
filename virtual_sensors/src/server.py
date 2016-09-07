import json
import logging
import sys

from bottle import run, request, response, Bottle

from json_utils.json_http_response import generate_sensor_representation, generate_sensor_capabilities, generate_API_response
from virtual_sensor import VirtualSensor

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Bottle parameters


def usage():
    """ Print short help """
    print("iQAS: an integration platform for QoI Assessment as a Service")
    print("Module: Virtual Sensor")
    print("(C) 2016 ISAE-SUPAERO\n")

if len(sys.argv) != 3:
    print('ERROR: Wrong number of parameters')
    usage()
    exit()

app = Bottle()
bottle_host = "0.0.0.0" # To listen on all interfaces
bottle_port = 8080
sensor_id = str(sys.argv[1])
url_publish_obs = str(sys.argv[2])
sensor = None
sensor_endpoint = 'http://' + bottle_host + ':' + str(bottle_port) + '/' + sensor_id
logger.warning("Virtual sensor '{}' listening on {}".format(sensor_id, sensor_endpoint))

# REST APIs for the virtual sensor (see also the corresponding class virtual_sensor.py)


@app.route('/' + sensor_id, method='GET')
def init_virtual_sensor():
    """ Return an overview of the specified virtual sensor """
    return generate_sensor_representation(response, sensor)


@app.route('/' + sensor_id + '/enabled', method='GET')
def get_sensor_enabled():
    json_response = generate_API_response(response,
                                          result="OK",
                                          details="",
                                          capability="enabled",
                                          old_value="",
                                          value=sensor.enabled)
    return json_response


@app.route('/' + sensor_id + '/enabled', method='POST')
def set_sensor_enabled():
    request_json_body = json.loads(request.body.read().decode('UTF-8'))
    old_value = sensor.enabled
    value = request_json_body['value']
    if type(value) == bool:
        sensor.enable_sensor(value)
        json_response = generate_API_response(response,
                                              result="OK",
                                              details="",
                                              capability="enabled",
                                              old_value=old_value,
                                              value=value)
    else:
        json_response = generate_API_response(response,
                                              result="NOK",
                                              details="Only one boolean is accepted for this POST request. "
                                                      "E.g.: {'value': true}")
    return json_response


@app.route('/' + sensor_id + '/sensing', method='GET')
def get_sensor_sensing():
    json_response = generate_API_response(response,
                                          result="OK",
                                          details="",
                                          capability="sensing",
                                          old_value="",
                                          value=sensor.sensing)
    return json_response


@app.route('/' + sensor_id + '/sensing', method='POST')
def set_sensor_sensing():
    request_json_body = json.loads(request.body.read().decode('UTF-8'))
    old_value = sensor.enabled
    value = request_json_body['value']
    if type(value) == bool:
        result, details = sensor.enable_sensing_process(value)
        json_response = generate_API_response(response,
                                              result=result,
                                              details=details,
                                              capability="sensing",
                                              old_value=old_value,
                                              value=value)
    else:
        json_response = generate_API_response(response,
                                              result="NOK",
                                              details="Only one boolean is accepted for this POST request. "
                                                            "E.g.: {'value': true}")
    return json_response


@app.route('/' + sensor_id + '/capabilities', method='GET')
def get_sensor_details():
    """ Return the different capabilities of the specified virtual sensor """
    return generate_sensor_capabilities(response, sensor)


@app.route('/' + sensor_id + '/capabilities/<capability>', method='GET')
def get_sensor_capability(capability):
    """ Method to get a sensor parameter (capability) """
    result, details, value = sensor.get_capability(capability)
    json_response = generate_API_response(response,
                                          result=result,
                                          details=details,
                                          capability=capability,
                                          old_value="",
                                          value=value)
    return json_response


@app.route('/' + sensor_id + '/capabilities/<capability>', method='POST')
def set_sensor_capability(capability):
    """ Method to modify a sensor parameter (capability) """
    request_json_body = json.loads(request.body.read().decode('UTF-8'))
    result, details, old_value = sensor.get_capability(capability)
    value = request_json_body['value']
    if result == "OK":
        result, details = sensor.set_capability(capability, value)
    json_response = generate_API_response(response,
                                          result=result,
                                          details=details,
                                          capability=capability,
                                          old_value=old_value,
                                          value=value)
    return json_response

with open('../etc/sensor.config') as config_file:
    config = json.load(config_file)
    config['url_publish_obs'] = url_publish_obs

with open('../etc/capabilities.config') as capabilities_file:
    capabilities = json.load(capabilities_file)

# TODO check that supplied parameters are correct

# Virtual sensor creation
sensor = VirtualSensor(sensor_id=sensor_id,
                       enabled=True,
                       endpoint=sensor_endpoint,
                       config=config,
                       capabilities=capabilities)

# Start of a bottle server to handle calls to the sensor API
run(app, host=bottle_host, port=bottle_port, quiet=True)
