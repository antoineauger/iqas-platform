import threading
import uuid
from information.monitor import Monitor


class InformationCore(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self._stopevent = threading.Event()  # to stop the main thread

        # Monitor process
        self.monitor_process = Monitor()
        self.monitor_process.loop()

        # load knowledge DB
        # start other processes (Monitor, Analyze, Plan and Execute)

    def __del__(self):
        self._stopevent.set()

    def process_request(self, place, topic, params):
        """
        Creates the rigth Request object given the place and topic
        Stores the request in the waiting list (db?)
        returns a ticket to retrieve the request answer
        """
        return uuid.uuid4()

    def run(self):
        """
        MAPE-K autonomic loop
        """
        while not self._stopevent.isSet():
            print("in the thread {0}...".format(self.getName()))
            self._stopevent.wait(1)


