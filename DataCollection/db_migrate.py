from collector import Collector
import mysql.connector
import datetime
import requests
from pymongo.errors import DuplicateKeyError
import time
import pandas as pd


class DbMigrate(Collector):
    ins_table = 'artists'   # Name of collection. Used to insert data
    threads = 0
    cons_amount = 50    # Batch size to be processed by the consumer

    first_run = True

    def __init__(self, client_id, client_secret, inserter_size=1e+8):
        self.ins_size = inserter_size
        super(DbMigrate, self).__init__(client_id=client_id, client_secret=client_secret)

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
        df = pd.read_csv('B:\Home\Downloads\\features (10).csv', usecols=[13], nrows=50000)
        ret = df.values.tolist()

        ret_lst = []

        for data in ret:
            ret_lst.append(data[0])

        return ret_lst

    def consumer(self, data):
        sp = self.get_spotify()

        # Storage for all documents
        doc_data = {'songs': [], 'albums': [], 'artists': []}

        tracks = sp.tracks(data)
        audio_features = sp.audio_features(data)

        # Getting extra features for tracks
        track_ins = []
        for track in tracks['tracks']:

            for artist in track['album']['artists']:
                del artist['external_urls']
                del artist['href']
                del artist['uri']

            del track['album']['external_urls']
            del track['album']['href']
            del track['album']['uri']

            del track['external_urls']
            del track['href']
            del track['is_local']
            del track['uri']
            del track['popularity']   # Popularity is added later throug popularity_migrate

            try:
                audio_analysis = sp.audio_analysis(str(track['id']))

                if 'meta' in audio_analysis:
                    del audio_analysis['meta']
                del audio_analysis['bars']
                del audio_analysis['beats']
                del audio_analysis['tatums']

                track['end_of_fade_in'] = audio_analysis['track']['end_of_fade_in']
                track['start_of_fade_out'] = audio_analysis['track']['start_of_fade_out']

                del audio_analysis['track']

                for key, val in audio_analysis.items():
                    track[key] = val
            except (requests.exceptions.ChunkedEncodingError, requests.exceptions.SSLError, TypeError) as e:
                print("[ERROR][-]", e)

            for feature in audio_features:
                try:
                    if feature['id'] == track['id']:
                        del feature['type']
                        del feature['id']
                        del feature['uri']
                        del feature['track_href']
                        del feature['analysis_url']

                        for key, val in feature.items():
                            track[key] = val
                        break
                except KeyError:
                    continue

            track_ins.append(track)

        doc_data['songs'] = track_ins

        #Getting album ids and artist ids
        album_ids = []
        artist_ids = []
        for track in tracks['tracks']:
            album_ids.append(track['album']['id'])
            for artist in track['artists']:
                artist_ids.append(artist['id'])

        # Making artist batches
        album_search = []
        for i in range(0, len(album_ids)):
            if i % 20 and i is not len(album_ids) - 1 or i is 0:
                album_search.append(album_ids[i])
            else:
                albums = sp.albums(album_search)
                album_search.clear()

                album_ins = []
                for album in albums['albums']:
                    del album['external_urls']
                    del album['href']
                    del album['uri']
                    del album['tracks']['href']

                    for artist in album['artists']:
                        del artist['external_urls']
                        del artist['href']
                        del artist['uri']

                    for item in album['tracks']['items']:
                        del item['external_urls']
                        del item['href']
                        del item['uri']

                        for artist in item['artists']:
                            del artist['external_urls']
                            del artist['href']
                            del artist['uri']

                    album_ins.append(album)

                doc_data['albums'] = album_ins

        # Making artist batches
        artists_search = []
        for i in range(0, len(artist_ids)):
            if i % 50 and i is not len(artist_ids) - 1 or i is 0:
                artists_search.append(artist_ids[i])
            else:
                if len(artists_search):
                    artists = sp.artists(artists_search)
                    artists_search.clear()

                    artist_ins = []
                    for artist in artists['artists']:
                        del artist['external_urls']
                        del artist['href']
                        del artist['uri']

                        artist['total_followers'] = artist['followers']['total']
                        del artist['followers']

                        artist_ins.append(artist)

                    doc_data['artists'] = artist_ins

        return [doc_data]

    def inserter(self, data):
        # Do insert on songs, albums, artists
        inserts = 0

        for song in data['songs']:
            try:
                song['_id'] = song.pop('id')
                self.db['songs'].insert_one(song)

                # Updating the processed variable
                #url = "http://127.0.0.1/dataapi/index.php"
                #params = "key=JykqSFeSJVGsBzeHdEN5GZuQ&do=process&id=" + song['_id']
                #requests.get(url=url, params=params)

                inserts += 1
            except DuplicateKeyError:
                continue

        for album in data['albums']:
            try:
                album['_id'] = album.pop('id')
                self.db['albums'].insert_one(album)

                inserts += 1
            except DuplicateKeyError:
                continue

        for artist in data['artists']:
            try:
                artist['_id'] = artist.pop('id')
                self.db['artists'].insert_one(artist)

                inserts += 1
            except DuplicateKeyError:
                continue

        time.sleep(3)
        return inserts
