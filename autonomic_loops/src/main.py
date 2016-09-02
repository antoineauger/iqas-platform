import json
from bottle import run, request, response, Bottle
from information.information_core import InformationCore

app = Bottle()
bottle_host = '127.0.0.1'
bottle_port = 8080

core = InformationCore()
core.start()

@app.route('/')
def root():
    return "request initialized successfully!\n"

@app.route('/places')
def get_places():
    return "places"

@app.route('/places/<place>/topics')
def get_topics(place):
    return "topics for {0}: ...".format(place)

@app.route('/places/<place>/topics/<topic>', method='POST')
def create_request(place, topic):
    response.content_type = 'application/json'
    if request.content_length > 0:
        params = json.loads(request.body)
        ticket = core.process_request(place, topic, params)
        return "{'ticket_id' : %d}" % (ticket)
    else:
        return "{'error': 'wrong params'}"

@app.route('/ticket/<id>', method="GET")
def get_ticket(id):
    pass

@app.route('/ticket/<id>', method="DELETE")
def delete_ticket(id):
    pass


run(app, host=bottle_host, port=bottle_port, debug=True)
