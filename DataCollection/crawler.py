import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Crawler:
    # Storing the songs to check for duplicates
    songs = 0

    def __init__(self, client_id, client_secret):
        self.client_credentials_manager = SpotifyClientCredentials(client_id=client_id,
                                                                   client_secret=client_secret)

    def get_spotify(self):
        return spotipy.Spotify(client_credentials_manager=self.client_credentials_manager)

    @staticmethod
    def data_to_list(key, data):
        return [d.get(key) for d in data if d.get(key) is not None]
