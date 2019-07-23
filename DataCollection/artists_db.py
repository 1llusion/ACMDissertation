from db import Db
import datetime


class ArtistsDb(Db):

    def __init__(self, **kwargs):
        super(ArtistsDb, self).__init__(**kwargs)
        #self.artists = self.db.artists
        self.songs = self.db.songs

    def artists_for_update(self, count=False):
        ins_date = datetime.datetime.today() - datetime.timedelta(days=1)

        # TODO Change to $lt
        if count:
            num = self.songs.count_documents({'artists.last_popularity_insert': {'$gte': ins_date}})

            return num

        ret = self.songs.find({'artists.last_popularity_insert': {'$gte': ins_date}})\
            .sort('artists.last_popularity_insert')
        return ret

    def set_insert_time(self, ids):
        self.songs.update({'_id': {'$in': ids}},
                          {'$set': {'artists.last_popularity_insert': datetime.datetime.now()}},
                          multi=True)

    def get_artist_ids(self, count=False):
        if count:
            num = self.songs.count_documents({'artists': {'$exists': False}})
            return num
        ret = self.songs.find({'artists': {'$exists': False}}, {'spotify_id': 1})
        return ret

    def insert_data(self, table, data):
        for i in data:
            self.songs.update_one({'spotify_id': i['spotify_id']}, {'$set': {'artists': i['artists']}}, upsert=True)
