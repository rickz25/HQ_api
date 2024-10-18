from db import SQLServer
from datetime import datetime
db = SQLServer() 


class TaskModel:

    # Insert Mapping Header
    def insertMappingHeader(self, jsondata):
        sqlstatement = ''
        TABLE_NAME = "dbo.mapping_header"
        for i in jsondata:
            d = parsing_date(i['TRN_DATE'])
            trn_date = d.strftime('%Y-%m-%d')
            merchant_code = i['MERCHANT_CODE']
            mall_code = i['MALL_CODE']
            template = i['TEMPLATE']
            sql =f"SELECT count(1) from {TABLE_NAME} WHERE TRN_DATE='{trn_date}' AND MERCHANT_CODE='{merchant_code}' AND MALL_CODE= {mall_code} AND TEMPLATE= {template}"
            exist = db.fetchOne(sql)
            if exist:
                sql =f"DELETE from {TABLE_NAME} WHERE TRN_DATE='{trn_date}' AND MERCHANT_CODE='{merchant_code}' AND MALL_CODE= {mall_code} AND TEMPLATE= {template}"
                db.remove(sql)
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
                    valuelist += "'" + value + "'"
                else:
                  valuelist += str(value)
            keylist += ")"
            valuelist += ")"
            sqlstatement += "INSERT INTO " + TABLE_NAME + " " + keylist + " VALUES " + valuelist + "\n"
        return db.insert(sqlstatement)
    
    # Insert Transaction
    def insertTransaction(self, jsondata):
        sqlstatement = ''
        TABLE_NAME = "dbo.transaction_mapping"
        for i in jsondata:
            d = parsing_date(i['TRN_DATE'])
            trn_date = d.strftime('%Y-%m-%d')
            CCCODE = i['CCCODE']
            mall_code = i['MALL_CODE']
            TER_NO = i['TER_NO']
            TRANSACTION_NO= i['TRANSACTION_NO']
            sql =f"SELECT count(1) from {TABLE_NAME} WHERE CAST(TRN_DATE AS DATE)='{trn_date}' AND CCCODE='{CCCODE}' AND MALL_CODE= {mall_code} AND TER_NO= '{TER_NO}' AND TRANSACTION_NO='{TRANSACTION_NO}'"
            exist = db.fetchOne(sql)
            if exist:
                sql =f"DELETE from {TABLE_NAME} WHERE CAST(TRN_DATE AS DATE)='{trn_date}' AND CCCODE='{CCCODE}' AND MALL_CODE= {mall_code} AND TER_NO= '{TER_NO}' AND TRANSACTION_NO='{TRANSACTION_NO}'"
                db.remove(sql)
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
                    valuelist += "'" + value + "'"
                else:
                    valuelist += str(value)
            keylist += ")"
            valuelist += ")"
            sqlstatement += "INSERT INTO " + TABLE_NAME + " " + keylist + " VALUES " + valuelist + "\n"
        return db.insert(sqlstatement)
    
    # Insert Daily
    def insertDaily(self, jsondata):
        sqlstatement = ''
        TABLE_NAME = "dbo.daily_mapping"
        for i in jsondata:
            d = parsing_date(i['TRN_DATE'])
            trn_date = d.strftime('%Y-%m-%d')
            CCCODE = i['CCCODE']
            mall_code = i['MALL_CODE']
            TER_NO = i['TER_NO']
            sql =f"SELECT count(1) from {TABLE_NAME} WHERE CAST(TRN_DATE AS DATE)='{trn_date}' AND CCCODE='{CCCODE}' AND MALL_CODE= {mall_code} AND TER_NO= '{TER_NO}'"
            exist = db.fetchOne(sql)
            if exist:
                sql =f"DELETE from {TABLE_NAME} WHERE CAST(TRN_DATE AS DATE)='{trn_date}' AND CCCODE='{CCCODE}' AND MALL_CODE= {mall_code} AND TER_NO= '{TER_NO}'"
                db.remove(sql)
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
                    valuelist += "'" + value + "'"
                else:
                    valuelist += str(value)
            keylist += ")"
            valuelist += ")"
            sqlstatement += "INSERT INTO " + TABLE_NAME + " " + keylist + " VALUES " + valuelist + "\n"
        return db.insert(sqlstatement)
    
    # Inser MappingLogs
    def insertMappingLogs(self, jsondata):
        sqlstatement = ''
        TABLE_NAME = "dbo.mapping_log"
        for i in jsondata:
            d = parsing_date(i['TRN_DATE'])
            trn_date = d.strftime('%Y-%m-%d')
            MERCHANT_CODE = i['MERCHANT_CODE']
            mall_code = i['MALL_CODE']
            TEMPLATE = i['TEMPLATE']
            TERMINAL_NO = i['TERMINAL_NO']
            sql =f"SELECT count(1) from {TABLE_NAME} WHERE CAST(TRN_DATE AS DATE)='{trn_date}' AND MERCHANT_CODE='{MERCHANT_CODE}' AND MALL_CODE= {mall_code} AND TEMPLATE= {TEMPLATE} AND TERMINAL_NO= '{TERMINAL_NO}'"
            exist = db.fetchOne(sql)
            if exist:
                sql =f"DELETE from {TABLE_NAME} WHERE CAST(TRN_DATE AS DATE)='{trn_date}' AND MERCHANT_CODE='{MERCHANT_CODE}' AND MALL_CODE= {mall_code} AND TEMPLATE= {TEMPLATE} AND TERMINAL_NO= '{TERMINAL_NO}'"
                db.remove(sql)
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
                    valuelist += "'" + value + "'"
                else:
                    valuelist += str(value)
            keylist += ")"
            valuelist += ")"
            sqlstatement += "INSERT INTO " + TABLE_NAME + " " + keylist + " VALUES " + valuelist + "\n"
        return db.insert(sqlstatement)
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
            

        