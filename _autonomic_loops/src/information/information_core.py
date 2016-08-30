import threading
import time

class InformationCore(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        # load knowledge DB

    def process_request(self, place, topic, params):
        """
        Creates the rigth Request object given the place and topic
        Stores the request in the waiting list (db?)
        returns a ticket to retrieve the request answer
        """
        return 42

    def run(self):
        """
        MAPE autonomic loop
        """
        while True:
            print("in the thread {0}...".format(self.getName()))
            time.sleep(1)


