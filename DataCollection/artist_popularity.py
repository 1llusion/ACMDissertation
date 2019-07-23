from artists_db import ArtistsDb
from collector import Collector
import datetime


class ArtistPopularity(ArtistsDb, Collector):
    ins_table = 'artist_popularity'
    threads = 0
    cons_amount = 50

    def __init__(self, client_id, client_secret, inserter_size=1e+8):
        self.ins_size = inserter_size
        super(ArtistPopularity, self).__init__(client_id=client_id, client_secret=client_secret)

    def start_collection(self, threads):
        self.threads = threads
        self.start()

    def condition(self):
        count = self.artists_for_update(count=True)
        return count

    def producer(self):
        tracks = self.artists_for_update()

        out = []
        for artists in tracks:
            for artist in artists['artists']:
                out.append(artist['artist_id'])

        return out

    def consumer(self, data):
        sp = self.get_spotify()

        artists = sp.artists(data)

        return artists['artists']

    def inserter(self, data):
        add = {'popularity': data['popularity'], 'time': datetime.datetime.now()}
        self.songs.update_one({'artists.artist_id': data['id']}, {'$push': {'artists.$.popularity': add}}, upsert=True)
        return data
