from crawler import SpotifyCrawler
import json


class Features(SpotifyCrawler):

    def get_features(self, songs):
        song_ids = []

        # Getting rid of empty arrays
        if not len(songs):
            return

        for song in songs:
            song_ids.append(song['id'])

        sp = self.get_spotify()
        # bar = ChargingBar("Inserting data to database", max=len(songs), suffix='%(percent)d%%')
        # Max 50
        songs_features = sp.audio_features(tracks=song_ids)
        self.insert_data(songs_features)
        # bar.next()
        # bar.finish()

        # Insert a song to db
        # TODO Check for duplicates

    def insert_data(self, songs):
        for song in songs:
            json_data = json.dumps(song)

            url = self.api_url + '?key=' + self.api_key + '&do=insert'

            self.request(url,
                         'POST',
                         {'data': json_data})
