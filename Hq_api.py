import os, json, configparser, logging, psutil
from flask import Flask, jsonify, request, make_response
from gevent.pywsgi import WSGIServer
from gevent import monkey
monkey.patch_all()
from flask_cors import CORS
from model import TaskModel
from controller import TaskController
# import gevent

# kill process when double run the program
process_to_kill = "Hq_api.exe"
# get PID of the current process
my_pid = os.getpid()
# iterate through all running processes
for p in psutil.process_iter():
    # if it's process we're looking for...
    if p.name() == process_to_kill:
        # and if the process has a different PID than the current process, kill it
        if not p.pid == my_pid:
            p.terminate()

# Create and configure logger
logging.basicConfig(filename="Logs/unoLog/logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

model = TaskModel()
controller = TaskController(model)
config = configparser.ConfigParser()
config.read(r'settings/config.txt') 
ip =  config.get('hq_config', 'HQ_IP')
port =  config.get('hq_config', 'Port')
worker =  config.get('hq_config', 'worker')
token ='eyJhbGciOiJIUzI1NiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFtZSI6IkphdmFJblVzZSIsImV4cCI6MTY1NDc1NDg1NCwiaWF0IjoxNjU0NzU0ODU0fQ.p6WAfLuC39cMk3XEF4LcU5iZy1rzbL0VTKVpTY7mRGQ'
if worker == "":
    worker=5
else:
   worker = int(worker)

app = Flask(__name__)
CORS(app)

@app.route('/api/post-sales-integration', methods=['POST'])
def handle_sales():
    bearer = request.headers.get('Authorization') 
    bearer_token = bearer.split()[1]
    try:
        if token == bearer_token:
            response_data = request.data
            if is_json(response_data):
                response = controller.post_data(response_data)
                return make_response(jsonify(response), 200)
            else:
                response_obj = { 'status' : 1, 'message': 'Data is not in json format' }
                logger.exception("Data is not in json format: %s", response_obj)
                return make_response(jsonify(response_obj), 400)
    except Exception as e:
        response_obj = { 'status' : 1, 'message': e }
        logger.exception("00 - Exception occurred: %s", response_obj)
        return make_response(jsonify(response_obj), 500)

@app.route('/api/post-maintenance', methods=['POST'])
def handle_maintenance():
    bearer = request.headers.get('Authorization') 
    bearer_token = bearer.split()[1]
    try:
        if token == bearer_token:
            datas = json.loads(request.data)
            response = controller.get_data(datas['mallcode'])
            return make_response(jsonify(response), 200)
    except Exception as e:
        response_obj = { 'status' : 1, 'message': e }
        logger.exception("01 - Exception occurred: %s", response_obj)
        return make_response(jsonify(response_obj), 500)
    
def is_json(myjson):
    try:
        json.loads(myjson)
    except ValueError as e:
        return False
    return True

# Run the app
http_server = WSGIServer((ip, int(port)), app)
# http_server.spawn = worker #Create 5 Workers
http_server.serve_forever()