import json

class JSONResponse(object):
	""" """

	@staticmethod
	def generate_ok_response(response, additional_details, parameter, old_value, value):
		response.set_header('Content-Type', 'application/json; charset=UTF-8')
		dict_response = dict()
		dict_response['status'] = "OK"
		dict_response['additional_details'] = additional_details
		dict_response['parameter'] = parameter
		dict_response['old_value'] = old_value
		dict_response['value'] = value
		dict_response['error_message'] = ""
		return json.dumps(dict_response)

	@staticmethod
	def generate_nok_response(response, error_message):
		response.set_header('Content-Type', 'application/json; charset=UTF-8')
		dict_response = dict()
		dict_response['status'] = "NOK"
		dict_response['error_message'] = error_message
		return json.dumps(dict_response)