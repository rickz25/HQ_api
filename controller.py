import json, logging, dataclasses, os.path, configparser, glob
from datetime import date, datetime
from decimal import Decimal
from itertools import islice

# Create and configure logger
logging.basicConfig(filename="Logs/unoLog/logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

config = configparser.ConfigParser()
config.read(r'settings/config.txt') 
filepath =  config.get('hq_config', 'filepath')

# convertion of datetime and decimal
def default(obj):
    if isinstance(obj, (datetime, date)):
        return obj.strftime("%Y-%m-%d %H:%M:%S") 
    if isinstance(obj, Decimal):
        return str(obj)
    if dataclasses.is_dataclass(obj):
        return dataclasses.asdict(obj)
    raise TypeError ("Type %s not serializable" % type(obj))

class TaskController:
    def __init__(self, model):
        self.model = model
    
    def post_data(self, datas):
        try:
            data = json.loads(datas)
            #header_sales
            if data['header_sales']:
                self.model.QueryStatementDelete(data['header_sales']['delete'])
                self.model.QueryStatementInsert(data['header_sales']['insert'])
            #hourly_sales
            if data['hourly_sales']:
                hourly = data['hourly_sales']
                for i in hourly:
                    self.model.QueryStatementDelete(i['delete'])
                    self.model.QueryStatementInsert(i['insert'])
            #eod_sales
            if data['eod_sales']:
                self.model.QueryStatementDelete(data['eod_sales']['delete'])
                self.model.QueryStatementInsert(data['eod_sales']['insert'])
            #logs
            if data['logs']:
                self.model.QueryStatementDelete(data['logs']['delete'])
                self.model.QueryStatementInsert(data['logs']['insert'])
            return { 'status' : 0, 'message': 'Success' }
        except Exception as e:
            return { 'status' : 1, 'message': 'Error' }
            # logger.exception("Exception occurred: %s", str(e))
        
    def get_data(self, mallcode):
        datas=[]
        response={}
        errorMessage = {}
        str_error = None
        try:
            if os.path.exists(filepath):
                filename = filepath+'/'+str(mallcode)+'.json'
                for files in glob.glob(filename):
                    with open(files) as f:
                        datas = json.load(f)
                if os.path.isfile(filename):
                    os.remove(filename)
           
        except Exception as e:
            str_error = e
            errorMessage['status']=1
            errorMessage['message'] = str_error
            logger.exception("Exception occurred: %s", str_error)
        if str_error is None:
            response['status']=0
            response['message']='success'
            response['data']=datas
            return json.dumps(response, default=default)
        else:
            return json.dumps(errorMessage)
       
def batched(iterable, n):
    # batched('ABCDEFG', 3) → ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        yield batch
    
def parsing_date(text):
    for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError('no valid date format found')
def rmv_space(string):
    return string.replace(" ", "")         