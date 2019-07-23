from collector import Collector
from songs_db import SongsDb


class SongMaintenance(Collector, SongsDb):
    ins_columns = ['track_name', 'spotify_id']
    threads = 0
    cons_amount = 50

    first_run = False  # The script has been run only once

    def __init__(self):
        super(SongMaintenance, self).__init__()

    def start_collection(self, threads=1):
        while self.names_for_update():
            self.threads = threads
            path = self.start()

            # Enable next line only on debug
            path = path.replace("\\", "/")
            self.load_file(path, 'features', self.ins_columns, replace=True)

    def condition(self):
        if self.first_run:
            return False
        self.first_run = True
        return True

    def producer(self):
        spot_id = self.get_names_for_update()
        return spot_id

    def consumer(self, data):
        sp = self.get_spotify()
        tracks = sp.tracks(data)
        return tracks['tracks']

    def inserter(self, data):
        return {'track_name': data['name'], 'spotify_id': data['id']}
