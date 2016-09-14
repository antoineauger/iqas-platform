import sched, time


class Monitor(object):

	def __init__(self):
		self.scheduler = sched.scheduler(self.get_time, time.sleep)
		self.run = True
		self.time_ref = 0  # Temporary value -- will be changed in loop()

		self.resources_to_watch = None

	def loop(self):
		"""Event loop"""
		print("event loop started at {0}".format(time.time()))
		# Set the time reference
		self.time_ref = time.time()

		while self.run:
			print("event loop running at {0}".format(time.time()))
			self.scheduler.run(blocking=True)

	def stop(self):
		"""Used to stop emulation from outside"""
		self.run = False

	def get_time(self):
		"""Returns time elapsed since the start of the experiment"""
		return time.time() - self.time_ref
