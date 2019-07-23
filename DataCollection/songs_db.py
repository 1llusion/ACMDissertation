from db import Db
import datetime


class SongsDb(Db):

    def __init__(self, **kwargs):
        super(SongsDb, self).__init__(**kwargs)
        self.songs = self.db.songs

    def songs_for_update(self, count=False):
        ins_date = datetime.datetime.today() - datetime.timedelta(days=1)

        if count:
            num = self.songs.count_documents({'last_popularity_insert': {'$lt': ins_date}})
            return num

        ret = self.songs.find({'last_popularity_insert': {'$lt': ins_date}}).sort('last_popularity_insert')
        return ret

    def set_insert_time(self, ids):
        self.songs.update({'_id': {'$in': ids}},
                          {'$set': {'last_popularity_insert': datetime.datetime.now()}},
                          multi=True)
