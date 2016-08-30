class VirtualSensor(object):
	"""
		Class for representing a virtual sensor, i.e. a sensor which produce observations from an optional input file.
		Output format for observations: JSON
		Available APIs to interact with the sensor: TODO
	"""

	def __init__(self, sensorID, enabled, endpoint, capabilities):
		self.sensorID = sensorID
		self.enabled = enabled
		self.endpoint = endpoint
		self.capabilities = capabilities # list of capabilities e.g.: [{'type': 'temperature', 'min_value': -100, 'max_value': +100, 'resolution': 0.5, 'format': float}]
		self.battery_level = 100 # in percentage

	def __repr__(self):
		return "Sensor ID: {}\n" \
               "Enabled: {}\n" \
               "Endpoint: {}\n" \
               "Capabilities: {}\n" \
               "Battery level: {}%\n".format(self.sensorID, self.enabled, self.endpoint, self.capabilities, self.battery_level)



