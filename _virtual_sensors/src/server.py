from bottle import run, Bottle

from virtual_sensor import VirtualSensor

app = Bottle()
bottle_host = '127.0.0.1'
bottle_port = 8080

sensorID = 's01'
sensor = None

@app.route('/' + sensorID + '/init', method='GET')
def initSensor():
    return "Sensor {} initialized successfully!\n".format(sensorID)

@app.route('/' + sensorID, method='GET')
def getSensorDetails():
    if sensor is None:
        return "NOK\n"
    else:
        return sensor.__repr__()

@app.route('/' + sensorID + '/get/<parameter>', method='GET')
def getSensorDetail(parameter):
    if sensor is None:
        return "Error: sensor not initialized yet...\n"
    elif parameter == 'battery':
        return str(sensor.battery_level)
    else:
        return sensor.__repr__()

sensorEndpoint = 'http://' + bottle_host + ':' + str(bottle_port) + '/' + sensorID
sensor = VirtualSensor(sensorID, True, sensorEndpoint,
                       [{'type': 'temperature', 'min_value': -100, 'max_value': +100, 'resolution': 0.5, 'format': float}])

run(app, host=bottle_host, port=bottle_port, debug=True)
