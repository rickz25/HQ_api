from model import TaskModel
from view import TaskView
import json
from datetime import date, datetime
from decimal import Decimal
from aiohttp import web
import logging

# Create and configure logger
logging.basicConfig(filename="Logs/unoLog/logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# convertion of datetime and decimal
def default(obj):
    if isinstance(obj, (datetime, date)):
        return obj.strftime("%Y-%m-%d %H:%M:%S") 
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError ("Type %s not serializable" % type(obj))

class TaskController:
    def __init__(self, model):
        self.model = model
    def insert_data(self, data):
        header=[]
        log=[]
        tag={}
        daily_tag=[]
        response={}
        errorMessage = {}
        try:
            # Mapping header
            MappingHeader = data['MappingHeader']
            if MappingHeader:
                for i in MappingHeader:
                    header.append(i['HEADER_ID'])
                self.model.insertMappingHeader(MappingHeader)

                # Transaction mapping
                TransactionMapping = data['TransactionMapping']
                if TransactionMapping:
                    self.model.insertTransaction(TransactionMapping)

            # Daily mapping
            DailyMapping = data['DailyMapping']
            if DailyMapping:
                for i in DailyMapping:
                    # return i['TRN_DATE']
                    # d = parsing_date(i['TRN_DATE'])
                    trn_date = (i['TRN_DATE']).strip()
                    ter_no = (i['TER_NO']).strip()
                    cccode = (i['CCCODE']).strip()
                    tag_id =f"{cccode}_{trn_date}_{ter_no}"
                    daily_tag.append(tag_id)
                self.model.insertDaily(DailyMapping)
                
            # Mapping log
            MappingLogs = data['MappingLogs']
            if MappingLogs:
                for l in MappingLogs:
                    d = parsing_date(l['TRN_DATE'])
                    trn_date = datetime.date(d)
                    merchant_code = (l['MERCHANT_CODE']).strip()
                    ter_no = (l['TERMINAL_NO']).strip()
                    template = l['TEMPLATE']
                    unique_id =f"{merchant_code}_{trn_date}_{ter_no}_{template}"
                    log.append(unique_id)
                self.model.insertMappingLogs(MappingLogs)
            str_error = None
        except Exception as e:
            str_error = str(e)
            logger.exception("Exception occurred: %s", str_error)
            errorMessage['status']=1
            errorMessage['message'] = str_error
        
        if str_error is None:
            tag['header']=header
            tag['logs']=log
            tag['daily']=daily_tag
            response['status']=0
            response['message']='success'
            response['data']=tag
            return response
        else:
            return errorMessage
        
    def get_data(self, mallcode):
        datas={}
        response={}
        errorMessage = {}
        str_error = None
        try:
            
            datas["merchant"] = self.model.getMerchant(mallcode)
            datas["merchant_contacts"] = self.model.getMerchantContacts(mallcode)
            datas["merchant_office_contacts"] = self.model.getMerchantOfficeContacts(mallcode)
            datas["merchant_reason"] = self.model.getMerchantReason(mallcode)
            datas["reason"] = self.model.getReason(mallcode)
            datas["company"] = self.model.getCompany(mallcode)
            datas["contract"] = self.model.getContract(mallcode)
            datas["merchant_class"] = self.model.getMerchantClass(mallcode)
            datas["mall"] = self.model.getMall(mallcode)
            datas["mall_contacts"] = self.model.getMallContacts(mallcode)
            datas["mother_brand"] = self.model.getMotherBrand(mallcode)
            datas["merchant_brand"] = self.model.getMerchantBrand(mallcode)
            datas["category"] = self.model.getCategory(mallcode)
            datas["category_sub"] = self.model.getSCategory(mallcode)
            datas["contract_type"] = self.model.getContractType(mallcode)
            datas["building"] = self.model.getBuilding(mallcode)
            datas["mall_floor"] = self.model.getMallFloor(mallcode)
            datas["mall_area"] = self.model.getMallArea(mallcode)
            datas["pos_vendor"] = self.model.getPosVendor(mallcode)
            datas["pos_vendor_contacts"] = self.model.getPosVendorContacts(mallcode)
            datas["pos_vendor_category"] = self.model.getPosVendorCategory(mallcode)
            datas["user_mall"] = self.model.getUserMall(mallcode)
            datas["users"] = self.model.getUsers(mallcode)
           
        except Exception as e:
            str_error = str(e)
            errorMessage['status']=1
            errorMessage['message'] = str_error
        if str_error is None:
            response['status']=0
            response['message']='success'
            response['data']=datas
            return json.dumps(response, default=default, indent=4)
        else:
            return json.dumps(errorMessage)
       
        
 
    
def parsing_date(text):
    for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError('no valid date format found')
def rmv_space(string):
    return string.replace(" ", "")         