from db import SQLServer
from datetime import datetime
db = SQLServer()
import logging
import configparser
from itertools import islice

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

    # Insert Mapping Header
    def insertMappingHeader(self, jsondata):
        try:
            header_tag=[]
            deleteSql = ''
            insertSql = ''
            TABLE_NAME = "dbo.header_sales"
            for i in jsondata:
                # append tag
                header_tag.append(i['HEADER_ID'])
                d = parsing_date(i['TRN_DATE'])
                trn_date = d.strftime('%Y-%m-%d')
                merchant_code = (i['MERCHANT_CODE']).strip()
                mall_code = i['MALL_CODE']
                template = i['TEMPLATE']
                # For deletion statement
                deleteSql += f"DELETE from {TABLE_NAME} WHERE TRN_DATE='{trn_date}' AND MERCHANT_CODE='{merchant_code}' AND MALL_CODE= {mall_code} AND TEMPLATE= {template}; \n"
                    
                keylist = "("
                valuelist = "("
                firstPair = True
                for key, value in i.items():
                    if value==None:
                        continue
                    if not firstPair:
                        keylist += ", "
                        valuelist += ", "
                    firstPair = False
                    keylist += key
                    if isinstance(value, str):
                        value=str2(value)
                        valuelist += "'" + value + "'"
                    else:
                        valuelist += str(value)
                keylist += ")"
                valuelist += ")"
                insertSql += "INSERT INTO " + TABLE_NAME + " " + keylist + " VALUES " + valuelist + "\n"
            db.remove(deleteSql)
            db.insert(insertSql)
            return header_tag
        except Exception as e:
            logger.exception("Exception occurred when Insert Mapping_header: %s", str(e))
    
    # Insert Transaction
    def insertTransaction(self, jsondata):
        try:
            TABLE_NAME = "dbo.hourly_sales"
            for batch in batched(jsondata, bulklimit):
                deleteSql = ''
                insertSql = ''
                for i in batch:
                    d = parsing_date(i['TRN_DATE'])
                    trn_date = d.strftime('%Y-%m-%d')
                    CCCODE = (i['CCCODE']).strip()
                    TER_NO = (i['TER_NO']).strip()
                    TRANSACTION_NO = (i['TRANSACTION_NO']).strip()
                    # For deletion statement
                    deleteSql += f"DELETE from {TABLE_NAME} WHERE CAST(TRN_DATE AS DATE)='{trn_date}' AND CCCODE='{CCCODE}' AND TER_NO= '{TER_NO}' AND TRANSACTION_NO='{TRANSACTION_NO}'; \n"
                       
                    keylist = "("
                    valuelist = "("
                    firstPair = True
                    for key, value in i.items():
                        if value==None:
                            continue
                        if not firstPair:
                            keylist += ", "
                            valuelist += ", "
                        firstPair = False
                        keylist += key
                        if isinstance(value, str):
                            value=str2(value)
                            valuelist += "'" + value + "'"
                        else:
                            valuelist += str(value)
                    keylist += ")"
                    valuelist += ")"
                    insertSql += "INSERT INTO " + TABLE_NAME + " " + keylist + " VALUES " + valuelist + "\n"
                db.remove(deleteSql)
                db.insert(insertSql)
        except Exception as e:
            logger.exception("Exception occurred when Insert Transaction: %s", str(e))
    
    # Insert Daily
    def insertDaily(self, jsondata):
        try:
            TABLE_NAME = "dbo.eod_sales"
            daily_tag=[]
            for batch in batched(jsondata, bulklimit):
                deleteSql = ''
                insertSql = ''
                for i in batch:
                    # d = parsing_date(i['TRN_DATE'])
                    # trn_date = d.strftime('%Y-%m-%d')
                    trn_date = (i['TRN_DATE']).strip()
                    ter_no = (i['TER_NO']).strip()
                    cccode = (i['CCCODE']).strip()
                    tag_id =f"{cccode}_{trn_date}_{ter_no}"
                    # append tag
                    daily_tag.append(tag_id)
                    # For deletion statement
                    deleteSql += f"DELETE from {TABLE_NAME} WHERE CAST(TRN_DATE AS DATE)='{trn_date}' AND CCCODE='{cccode}' AND TER_NO= '{ter_no}'; \n"
                        
                    keylist = "("
                    valuelist = "("
                    firstPair = True
                    for key, value in i.items():
                        if value==None:
                            continue
                        if not firstPair:
                            keylist += ", "
                            valuelist += ", "
                        firstPair = False
                        keylist += key
                        if isinstance(value, str):
                            value=str2(value)
                            valuelist += "'" + value + "'"
                        else:
                            valuelist += str(value)
                    keylist += ")"
                    valuelist += ")"
                    insertSql += "INSERT INTO " + TABLE_NAME + " " + keylist + " VALUES " + valuelist + " \n"
                db.remove(deleteSql)
                db.insert(insertSql)
            return daily_tag
        except Exception as e:
            logger.exception("Exception occurred when Insert Daily: %s", str(e))
    
    # Inser MappingLogs
    def insertMappingLogs(self, jsondata):
        try:
            log_tag=[]
            deleteSql = ''
            insertSql = ''
            TABLE_NAME = "dbo.mapping_log"
            for i in jsondata:
                d = parsing_date(i['TRN_DATE'])
                trn_date = d.strftime('%Y-%m-%d')
                mall_code = i['MALL_CODE']
                merchant_code = (i['MERCHANT_CODE']).strip()
                ter_no = (i['TERMINAL_NO']).strip()
                template = i['TEMPLATE']
                unique_id =f"{merchant_code}_{trn_date}_{ter_no}_{template}"
                # append tag
                log_tag.append(unique_id)
                # For deletion statement
                deleteSql += f"DELETE from {TABLE_NAME} WHERE CAST(TRN_DATE AS DATE)='{trn_date}' AND MERCHANT_CODE='{merchant_code}' AND MALL_CODE= {mall_code} AND TEMPLATE= {template} AND TERMINAL_NO= '{ter_no}'; \n"
                    
                keylist = "("
                valuelist = "("
                firstPair = True
                for key, value in i.items():
                    if value==None:
                        continue
                    if not firstPair:
                        keylist += ", "
                        valuelist += ", "
                    firstPair = False
                    keylist += key
                    if isinstance(value, str):
                        value=str2(value)
                        valuelist += "'" + value + "'"
                    else:
                        valuelist += str(value)
                keylist += ")"
                valuelist += ")"
                insertSql += "INSERT INTO " + TABLE_NAME + " " + keylist + " VALUES " + valuelist + " \n"
            db.remove(deleteSql)
            db.insert(insertSql)
            return log_tag
        except Exception as e:
            logger.exception("Exception occurred when Insert Mapping_log: %s", str(e))
    def getMallServer(self):
        sql=f"SELECT MALL_CODE,IP_ADDRESS,PORT FROM dbo.mall_server ORDER BY MALL_CODE ASC;"
        return db.fetchAll(sql)
    def getMerchant(self, mallcode):
        sql=f"SELECT * FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode};"
        return db.fetchAll(sql)
    def getMerchantContacts(self, mallcode):
        sql=f"SELECT * FROM dbo.merchant_contacts WHERE MERCHANT_CODE IN (SELECT MERCHANT_CODE FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode});"
        return db.fetchAll(sql)
    def getMerchantOfficeContacts(self, mallcode):
        sql=f"SELECT * FROM dbo.merchant_office_contacts WHERE MERCHANT_CODE IN (SELECT MERCHANT_CODE FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode});"
        return db.fetchAll(sql)
    def getMerchantReason(self, mallcode):
        sql=f"SELECT MERCHANT_CODE,REASON_CODE,FROM_DATE,TO_DATE FROM dbo.merchant_reason WHERE MERCHANT_CODE IN (SELECT MERCHANT_CODE FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode});"
        return db.fetchAll(sql)
    def getReason(self, mallcode):
        sql=f"SELECT * FROM dbo.reason WHERE REASON_CODE IN (SELECT REASON_CODE FROM dbo.merchant_reason WHERE MERCHANT_CODE IN (SELECT MERCHANT_CODE FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode}));"
        return db.fetchAll(sql)
    def getCompany(self, mallcode):
        sql=f"SELECT * FROM dbo.company WHERE COMPANY_CODE IN (SELECT COMPANY_CODE FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode});"
        return db.fetchAll(sql)
    def getContract(self, mallcode):
        sql=f"SELECT * FROM dbo.contract WHERE COMPANY_CODE IN (SELECT COMPANY_CODE FROM dbo.company WHERE COMPANY_CODE IN (SELECT COMPANY_CODE FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode}));"
        return db.fetchAll(sql)
    def getMerchantClass(self, mallcode):
        sql=f"SELECT * FROM dbo.merchant_class WHERE MERCHANT_CLASS_CODE IN (SELECT MERCHANT_CLASS_CODE FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode});"
        return db.fetchAll(sql)
    def getMall(self, mallcode):
        sql=f"SELECT * FROM dbo.mall WHERE BRN_CODE IN (SELECT MALL_CODE FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode});"
        return db.fetchAll(sql)
    def getMallContacts(self, mallcode):
        sql=f"SELECT * FROM dbo.mall_contacts WHERE CAST(BRN_CODE AS NVARCHAR(5)) IN (SELECT CAST(BRN_CODE AS NVARCHAR(5)) FROM dbo.mall WHERE CAST(BRN_CODE AS NVARCHAR(5)) IN (SELECT MALL_CODE FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode}));"
        return db.fetchAll(sql)
    def getMotherBrand(self, mallcode):
        sql=f"SELECT * FROM dbo.mother_brand WHERE MOTHER_BRAND_CODE IN (SELECT MOTHER_BRAND_CODE FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode});"
        return db.fetchAll(sql)
    def getMerchantBrand(self, mallcode):
        sql=f"SELECT * FROM dbo.merchant_brand WHERE MERCHANT_BRAND_CODE IN (SELECT MERCHANT_BRAND_CODE FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode});"
        return db.fetchAll(sql)
    def getCategory(self, mallcode):
        sql=f"SELECT * FROM dbo.category WHERE CATEGORY_CODE IN (SELECT CATEGORY_CODE FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode});"
        return db.fetchAll(sql)
    def getSCategory(self, mallcode):
        sql=f"SELECT * FROM dbo.category_sub WHERE SCATEGORY_CODE IN (SELECT SCATEGORY_CODE FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode});"
        return db.fetchAll(sql)
    def getContractType(self, mallcode):
        sql=f"SELECT * FROM dbo.contract_type WHERE CONTRACT_CODE IN (SELECT CONTRACT_TYPE_CODE FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode});"
        return db.fetchAll(sql)
    def getBuilding(self, mallcode):
        sql=f"SELECT * FROM dbo.building WHERE BUILDING_CODE IN (SELECT BUILDING_CODE FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode});"
        return db.fetchAll(sql)
    def getMallFloor(self, mallcode):
        sql=f"SELECT * FROM dbo.mall_floor WHERE MF_CODE IN (SELECT MF_CODE FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode});"
        return db.fetchAll(sql)
    def getMallArea(self, mallcode):
        sql=f"SELECT * FROM dbo.mall_area WHERE MALL_AREA_CODE IN (SELECT MALL_AREA_CODE FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode});"
        return db.fetchAll(sql)
    def getPosVendor(self, mallcode):
        sql=f"SELECT * FROM dbo.pos_vendor WHERE POS_VENDOR_CODE IN (SELECT POS_VENDOR_CODE FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode});"
        return db.fetchAll(sql)
    def getPosVendorContacts(self, mallcode):
        sql=f"SELECT * FROM dbo.pos_vendor_contacts WHERE POS_VENDOR_CODE IN (SELECT POS_VENDOR_CODE FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode});"
        return db.fetchAll(sql)
    def getPosVendorCategory(self, mallcode):
        sql=f"SELECT * FROM dbo.pos_vendor_category WHERE CATEGORY_CODE IN (SELECT CATEGORY_CODE FROM dbo.merchant WHERE CAST(DATE_EDITED AS DATE)=CAST(GETDATE() AS DATE) AND MALL_CODE = {mallcode});"
        return db.fetchAll(sql)
    def getUserMall(self, mallcode):
        sql=f"SELECT * FROM user_mall WHERE MALL_CODE = {mallcode};"
        return db.fetchAll(sql)
    def getUsers(self, mallcode):
        sql=f"SELECT * FROM users WHERE username IN (SELECT username FROM user_mall WHERE MALL_CODE={mallcode});"
        return db.fetchAll(sql)

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
            

        