from enum import Enum


class RequestStatus(Enum):
	"""
		Indicate the status of a request
		The typical workflow is:
		created > submitted > (not_possible) > (aborted) > completed
	"""
	created = 1
	submitted = 2
	not_possible = 3
	aborted = 4
	completed = 5


class Request(object):

	def __init__(self):
		self.state = RequestStatus.created

	def eval(self):
		pass
