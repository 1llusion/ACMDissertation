from songs_db import SongsDb
from collector import Collector
import datetime


class SongPopularity(SongsDb, Collector):
    ins_table = 'song_popularity'
    threads = 0
    cons_amount = 50

    def __init__(self, client_id, client_secret, inserter_size=1e+8):
        self.ins_size = inserter_size
        super(SongPopularity, self).__init__(client_id=client_id, client_secret=client_secret)

    def start_collection(self, threads=1):
        self.threads = threads
        self.start()

    def condition(self):
        return self.songs_for_update(count=True)

    def producer(self):
        return self.songs_for_update()

    def consumer(self, data):
        sp = self.get_spotify()

        # Transforming values into a list from list of dicts
        data_list = self.data_to_list('spotify_id', data)
        tracks = sp.tracks(data_list)

        id_list = self.data_to_list('_id', data)
        self.set_insert_time(id_list)

        return tracks['tracks']

    def inserter(self, data):
        return {'spotify_id': data['id'], 'popularity': data['popularity'], 'time': datetime.datetime.now()}
