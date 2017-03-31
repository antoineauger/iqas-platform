import logging
import threading

from json_utils.json_post_observations import post_obs_to_rest_endpoint, post_obs_to_kafka_topic
from obs_utils.obs_generator import ObsGenerator

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class VirtualSensor(threading.Thread):
    """
        Class for representing a virtual sensor, i.e. a sensor which produce observations from an optional input file.
        The output format for each observation is a JSON formatted as follows:
            {"observation": RAW_DATA_OBSERVATION}
        Available APIs to interact with the sensor: TODO
    """
    def __init__(self, sensor_id):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self._stop_event = threading.Event()  # to stop the main thread
        self.sensor_id = sensor_id
        self.sensing = True  # TODO
        self.no_more_obs = False

        # Following attributes will be set later
        self.enabled = None
        self.kafka_producer = None
        self.config = None
        self.obs_generator = None
        self.mode = None
        self.publish_to = None
        self.capabilities = None
        self.obs_consumption = None
        self.infinite_battery = None

    def __del__(self):
        self._stop_event.set()

    def set_config(self, enabled, config, kafka_producer, capabilities):
        self.enabled = enabled
        self.kafka_producer = kafka_producer

        self.config = config
        self.obs_generator = ObsGenerator(self.config)
        self.mode = self.config['mode']  # KAFKA or REST
        self.publish_to = self.config['publish_to']  # where to send observations

        # dict of capabilities
        # e.g.: {'infinite_battery': false, 'frequency': 5.0, 'battery_level': 100, 'obs_consumption': 0.01}
        self.capabilities = capabilities

        # how much battery is used when sensing one observation
        self.obs_consumption = self.capabilities['obs_consumption']

        # if set to True, all battery considerations are ignored
        self.infinite_battery = self.capabilities['infinite_battery']

        self.start()  # We start the sensor's main thread

    # TODO: replace this main thread by a Python scheduler?
    def run(self):
        """
            Sensor's main thread. We should never stop this thread, except when destroying the sensor object
        """
        # TODO load observations from 1) file or 2) generate according input
        while not self._stop_event.isSet():
            # TODO : only for testing, remove!
            if self.sensing:
                logger.error("In sensor {} thread (freq={}s)".format(self.sensor_id, self.capabilities['frequency']))
                obs_dict = self.obs_generator.generate_one_observation(sensor_id=self.sensor_id)
                if obs_dict is not None:

                    if (not self.infinite_battery and float(
                                self.capabilities['battery_level'] - self.obs_consumption) > 0.0) \
                            or self.infinite_battery:
                        self.capabilities['battery_level'] -= self.obs_consumption

                        if self.mode == "KAFKA":
                            post_obs_to_kafka_topic(kafka_producer=self.kafka_producer,
                                                    topic=self.publish_to,
                                                    dictionary=obs_dict)
                        elif self.mode == "REST":
                            try:
                                post_obs_to_rest_endpoint(url=self.publish_to, dictionary=obs_dict)
                            except ConnectionRefusedError:
                                logger.error("ConnectionRefusedError: [Errno 61] Connection refused.")

                    self._stop_event.wait(self.capabilities['frequency'])  # We pause based on sensor's frequency
                else:
                    self.sensing = False
                    self.no_more_obs = True
                    self._stop_event.set()

    # The following methods represent the API of the virtual sensor
    # Sensor state (connection and observations measurement)

    def enable_sensor(self, value):
        """
            Method to activate/deactivate a sensor, i.e. connect or disconnect it to the network
            :param value: bool (True/False)
        """
        if value:
            if self.infinite_battery or self.capabilities['battery_level'] > 0.0:
                self.enabled = True
        else:
            self.enabled = False
            self.enable_sensing_process(False)

    def enable_sensing_process(self, value):
        """
            Method to start the observation acquisition process
            :param value: bool (True/False)
            :returns: result ("OK"/"NOK") + details (message error if any)
            :rtype boolean and str
        """
        if value:
            if 'frequency' in self.capabilities.keys() and self.capabilities['frequency'] > 0.0:
                if self.no_more_obs:
                    error_message = "Unable to retrieve more observations for sensor {}.".format(self.sensor_id)
                    logger.error(error_message)
                    return "NOK", error_message
                elif not self.enabled:
                    error_message = "Unable to start the observation acquisition process for sensor {}. " \
                                    "The sensor is disabled.".format(self.sensor_id)
                    logger.error(error_message)
                    return "NOK", error_message
                elif self.sensing:
                    error_message = "Sensor {} is already sensing. " \
                                    "Check its connectivity with the server if you do not " \
                                    "receive any observation.".format(self.sensor_id)
                    logger.error(error_message)
                    return "NOK", error_message
                else:
                    self.sensing = True
                    return "OK", ""
            else:
                error_message = "Unable to retrieve 'frequency' capability for sensor {}. " \
                                "The acquisition process has not been started.".format(self.sensor_id)
                logger.error(error_message)
                return "NOK", error_message
        else:
            self.sensing = False
            return "OK", ""

    # Sensor capabilities

    def get_capability(self, capability):
        """
            Method to get a specific sensor capability
            :param capability: str
            :returns: result ("OK"/"NOK") + details (message error if any) + current value
            :rtype bool, str and str
        """
        if capability in self.capabilities.keys():
            return "OK", "", self.capabilities[capability]
        else:
            error_message = "Unknown parameter '{}' for sensor {}".format(capability, self.sensor_id)
            logger.error(error_message)
            return "NOK", error_message, ""

    def set_capability(self, capability, value):
        """
            Method to set a specific sensor capability
            :param capability: str
            :param value: the new value for the capability (str, int, float or bool)
            :returns: result ("OK"/"NOK") + details (message error if any)
            :rtype bool and str
        """
        if capability in self.capabilities.keys():
            self.capabilities[capability] = value
            return "OK", ""
        else:
            error_message = "Unknown parameter '{}' for sensor {}".format(capability, self.sensor_id)
            logger.error(error_message)
            return "NOK", error_message

    def set_url_to_publish(self, new_url):
        # TODO check well formed URL
        """
            Method to dynamically set the endpoint where the sensor send its observations
            :param new_url: the new well-formed URL to publish to
            :return: bool and str
        """
        if True:
            self.publish_to = new_url
            return "OK", ""

    def recharge_battery(self):
        self.capabilities['battery_level'] = 100.0

    # Events to randomly affect sensor or sensor measurement process
    # Useful to introduce biased data or simulate sensor failures

    # TODO
