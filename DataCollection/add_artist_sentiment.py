from collector import Collector
import nltk
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


class AddArtistSentiment(Collector):
    ins_table = 'artists'   # Name of collection. Used to insert data
    threads = 0
    cons_amount = 50    # Batch size to be processed by the consumer

    def __init__(self, client_id, client_secret, inserter_size=1e+8):
        self.ins_size = inserter_size
        nltk.download('vader_lexicon')
        super(AddArtistSentiment, self).__init__(client_id=client_id, client_secret=client_secret)

    def start_collection(self, threads=1):
        self.threads = threads
        self.start()

    def condition(self):
        songs = self.db.songs.count_documents({'artist_sentiment_compound': {'$exists': False}})

        print(songs, "Artists do not have sentiment analysis yet.")

        return songs

    def producer(self):
        songs = self.db.songs.find({'artist_sentiment_compound': {'$exists': False}}).limit(10000)
        return songs

    def consumer(self, data):
        ret_data = []
        for song in data:
            sid = SentimentIntensityAnalyzer()

            artist_name = []
            for artist in song['artists']:
                artist_name.append(artist['name'])

            if len(artist_name) <= 0:
                continue

            try:
                text = " ".join(artist_name)
                ss = sid.polarity_scores(text)

                ret_data.append({'id': song['_id'],
                                 'artist_sentiment_compound': ss['compound'],
                                 'artist_sentiment_positive': ss['pos'],
                                 'artist_sentiment_negative': ss['neg'],
                                 'artist_sentiment_neutral': ss['neu']})

            except ValueError as er:
                continue

        return ret_data

    def inserter(self, data):
        self.db.songs.update_one({'_id': data['id']}, {'$set':
                                                       {'artist_sentiment_compound': data['artist_sentiment_compound'],
                                                        'artist_sentiment_positive': data['artist_sentiment_positive'],
                                                        'artist_sentiment_negative': data['artist_sentiment_negative'],
                                                        'artist_sentiment_neutral': data['artist_sentiment_neutral']}},
                                 upsert=True)
