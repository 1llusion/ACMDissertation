from collector import Collector
import datetime
from pymongo.errors import WriteError
import pandas as pd


class PopularityMigrate(Collector):
    ins_table = 'artists'   # Name of collection. Used to insert data
    threads = 0
    cons_amount = 50    # Batch size to be processed by the consumer

    first_run = True

    def __init__(self, client_id, client_secret, inserter_size=1e+8):
        self.ins_size = inserter_size
        super(PopularityMigrate, self).__init__(client_id=client_id, client_secret=client_secret)

        '''
        self.mysql_db = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="",
            database="audio_analysis"
        )
        '''

    def start_collection(self, threads=1):
        self.threads = threads
        self.start()

    def condition(self):
        '''
        mysql_cursor = self.mysql_db.cursor()
        mysql_cursor.execute("SELECT COUNT(*) as 'Amount' FROM `features` WHERE processed = 0")
        val = mysql_cursor.fetchone()
        val = val[0]
        return val
        '''
        # Making sure it runs only once
        if self.first_run is True:
            self.first_run = False
            ret = True
        else:
            ret = False

        return ret

    def producer(self):
        '''
        mysql_cursor = self.mysql_db.cursor()
        mysql_cursor.execute("SELECT `spotify_id` FROM `features` WHERE processed = 0 LIMIT 500000")
        ids = mysql_cursor.fetchall()

        ret = []
        for i in ids:
            ret.append(i[0])

        return ret
        '''
        df = pd.read_csv('B:\Home\Downloads\\popularity (1).csv')
        ret = df.values.tolist()

        ret_lst = []

        for data in ret:
            ret_lst.append(data)

        return ret_lst

    def consumer(self, data):
        return [data]

    def inserter(self, data):
        inserts = 0
        for track in data:
            f = '%Y-%m-%d %H:%M:%S'
            add = {'popularity': track[2], 'time': datetime.datetime.strptime(track[3], f)}
            try:
                result = self.db.songs.update_one({'_id': track[1]}, {'$push': {'popularity': add}}, upsert=True)
                #self.db.songs.update({'$set': {'popularity': add}})
                if result.modified_count:
                    print("Inserted!")
                inserts += result.modified_count
            except WriteError:
                continue

        return inserts
