import logging
import threading

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class VirtualSensor(threading.Thread):
	"""
		Class for representing a virtual sensor, i.e. a sensor which produce observations from an optional input file.
		Output format for observations: JSON
		Available APIs to interact with the sensor: TODO
	"""

	def __init__(self, sensorID, enabled, endpoint, capabilities):
		threading.Thread.__init__(self)
		self.setDaemon(True)
		self._stopevent = threading.Event() # to stop the main thread
		self.sensorID = sensorID
		self.enabled = enabled
		self.endpoint = endpoint
		self.sensing = False

		self.capabilities = capabilities # dict of capabilities e.g.: {'type': 'temperature', 'min_value': -100, 'max_value': 100, 'resolution': 0.5, 'format': float}
		self.battery_level = self.capabilities['battery_level'] # in percentage
		self.obs_consumption = self.capabilities['obs_consumption'] # how much battery is used when sensing one observation
		self.infinite_battery = self.capabilities['infinite_battery'] # if set to True, all battery considerations are ignored

		self.start() # We start the sensor's main thread

	def __repr__(self):
		""" Return a string representation for a virtual sensor object """
		return "Sensor ID: {}\n" \
               "Enabled: {}\n" \
		       "Currently sensing observations: {}\n" \
               "Endpoint: {}\n" \
               "Capabilities: {}\n" \
               "Battery level: {}%\n".format(self.sensorID, self.enabled, self.sensing, self.endpoint, self.capabilities, self.battery_level)

	def __del__(self):
		self._stopevent.set()

	def run(self):
		""" Sensor's main thread. We should never stop this thread, except when destroying the sensor object """
		while not self._stopevent.isSet():
			if self.sensing:
				logger.error("In sensor {} thread (freq={}s)".format(self.sensorID, self.capabilities['frequency']))

				# TODO measure and send data
				if not self.infinite_battery:
					self.battery_level -= self.obs_consumption

			self._stopevent.wait(self.capabilities['frequency']) # We pause based on sensor's frequency

	# The following methods represent the API of the virtual sensor
	# Sensor state (connection and observations measurement)

	def enable_sensor(self, value):
		""" Method to activate/deactivate a sensor, i.e. connect or disconnect it to the network """
		if value:
			if self.infinite_battery or self.battery_level > 0.0:
				self.enabled = True
		else:
			self.enabled = False
			self.enable_sensing_process(False)

	def enable_sensing_process(self, value):
		""" Method to start the observation acquisition process """
		if value:
			if 'frequency' in self.capabilities.keys() and self.capabilities['frequency'] > 0.0:
				if self.enabled and not self.sensing:
					self.sensing = True
				else:
					logger.error("Unable to start the observation acquisition process for sensor {}. The sensor is disabled.".format(self.sensorID))
			else:
				logger.error("Unable to retrieve 'frequency' capability for sensor {}. The acquisition process has not been started.".format(self.sensorID))
		else:
			self.sensing = False

	# Sensor capabilities

	def get_parameter(self, param_name):
		return self.capabilities[param_name]

	def set_parameter(self, param_name, value):
		self.capabilities[param_name] = value

	def recharge_battery(self):
		self.battery_level = 100.0

	# Events to randomly affect sensor or sensor measurement process
	# Useful to introduce biased data or simulate sensor failures

	# TODO

