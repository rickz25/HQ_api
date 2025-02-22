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

bulklimit =  config.get('hq_config', 'bulk_limit')
if bulklimit == "":
    bulklimit=100
else:
   bulklimit = int(bulklimit)

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
            if datas:
                for batch in batched(datas, bulklimit):
                        sql_per_batch = ''
                        for sql in batch:
                                sql_per_batch +=sql
                        self.model.QueryStatementInsert(sql_per_batch)
            return { 'status' : 0, 'message': 'Success' }
        except Exception as e:
            logger.exception("Exception occurred: %s", str(e))
            return { 'status' : 1, 'message': 'Error' }
        
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