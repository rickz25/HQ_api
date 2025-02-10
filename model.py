from db import SQLServer
from datetime import datetime
import logging, configparser
from itertools import islice
db = SQLServer()

# Create and configure logger
logging.basicConfig(filename="Logs/unoLog/logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

config = configparser.ConfigParser()
config.read(r'settings/config.txt') 

bulklimit =  config.get('hq_config', 'bulk_limit')
if bulklimit == "":
    bulklimit=100
else:
   bulklimit = int(bulklimit)

class TaskModel:

    def QueryStatementDelete(self,sqlQuery):
        try:
            db.remove(sqlQuery)
        except Exception as e:
            logger.exception("Exception occurred when Deletion: %s", e)
            
    def QueryStatementInsert(self,sqlQuery):
        try:
            db.insert(sqlQuery)
        except Exception as e:
            logger.exception("Exception occurred when Insertion: %s", e)

def parsing_date(text):
    for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError('no valid date format found')   
 
def str2(words):
    return str(words).replace("'", '"')  

def batched(iterable, n):
    # batched('ABCDEFG', 3) → ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        yield batch
            

        