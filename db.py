import pyodbc, configparser, time, logging
config = configparser.ConfigParser()
config.read(r'settings/config.txt') 

# Create and configure logger
logging.basicConfig(filename="Logs/unoLog/logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
class SQLServer:
    def __init__(self):
        # db = LiteDB('app/db/db.sqlite')
        # record = db.fetchSetting()
        # server =record[1]
        # database = record[2]
        # user = record[3]
        # pd = record[4]

        driver=config.get('hq_config', 'db_driver')
        server =config.get('hq_config', 'db_host')
        database = config.get('hq_config', 'db_name')
        user = config.get('hq_config', 'db_username')
        pd = config.get('hq_config', 'db_password')
        add_details = config.get('hq_config', 'db_add_details')
        self.conn = pyodbc.connect(f"Driver={driver};"
                    rf"Server={server};"
                    f"Database={database};"
                    f"{add_details}"
                    f"uid={user};"
                    f"pwd={pd};")
        self.cursor = self.conn.cursor()
    def rowCount(self,sql):
        self.cursor.execute(sql)
        return self.cursor.rowcount

    def fetchOne(self,sql):
        self.cursor.execute(sql)
        return self.cursor.fetchone()
    def fetchAll(self,sql):
        retry_flag = 3
        retry_count = 0
        while retry_count < retry_flag:
            try:
                self.cursor.execute(sql)
                columns = [column[0] for column in  self.cursor.description]
                results = []
                for row in  self.cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                return results
            except Exception as e:
                retry_count = retry_count + 1
                logger.exception("Exception occurred: %s", e)
                time.sleep(1)
    def insert(self, statement):
        retry_flag = 3
        retry_count = 0
        while retry_count < retry_flag:
            try:
                self.cursor.execute(statement)
                self.conn.commit()
                break
            except Exception as e:
                retry_count = retry_count + 1
                logger.exception("Exception occurred: %s", e)
                time.sleep(1)
    def remove(self, statement):
        retry_flag = 3
        retry_count = 0
        while retry_count < retry_flag:
            try:
                self.cursor.execute(statement)
                self.conn.commit()
                break
            except Exception as e:
                retry_count = retry_count + 1
                time.sleep(1)
    def rows(self):
        return self.cursor.rowcount

    # def __del__(self):
    #     self.conn.close()