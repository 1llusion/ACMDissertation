from collector import Collector
from PyLyrics import *
import nltk
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


class AddLyricSentiment(Collector):
    ins_table = 'artists'   # Name of collection. Used to insert data
    threads = 0
    cons_amount = 50    # Batch size to be processed by the consumer

    def __init__(self, client_id, client_secret, inserter_size=1e+8):
        self.ins_size = inserter_size
        nltk.download('vader_lexicon')
        super(AddLyricSentiment, self).__init__(client_id=client_id, client_secret=client_secret)

    def start_collection(self, threads=1):
        self.threads = threads
        self.start()

    def condition(self):
        songs = self.db.songs.count_documents({'lyric_sentiment_compound': {'$exists': False}})

        print(songs, "Songs do not have sentiment analysis yet.")

        return songs

    def producer(self):
        songs = self.db.songs.find({'lyric_sentiment_compound': {'$exists': False}}).limit(1000)
        return songs

    def consumer(self, data):
        ret_data = []
        for song in data:
            sid = SentimentIntensityAnalyzer()

            artist_name = song['artists'][0]['name']
            song_name = song['name']

            try:
                text = PyLyrics.getLyrics(artist_name, song_name)
                ss = sid.polarity_scores(text)

                ret_data.append({'id': song['_id'],
                                 'lyric_sentiment_compound': ss['compound'],
                                 'lyric_sentiment_positive': ss['pos'],
                                 'lyric_sentiment_negative': ss['neg'],
                                 'lyric_sentiment_neutral': ss['neu']})

            except ValueError as er:
                ret_data.append({'id': song['_id'],
                                 'lyric_sentiment_compound': -1,
                                 'lyric_sentiment_positive': -1,
                                 'lyric_sentiment_negative': -1,
                                 'lyric_sentiment_neutral': -1})

        return ret_data

    def inserter(self, data):
        self.db.songs.update_one({'_id': data['id']}, {'$set':
                                                       {'lyric_sentiment_compound': data['lyric_sentiment_compound'],
                                                        'lyric_sentiment_positive': data['lyric_sentiment_positive'],
                                                        'lyric_sentiment_negative': data['lyric_sentiment_negative'],
                                                        'lyric_sentiment_neutral': data['lyric_sentiment_neutral']}},
                                 upsert=True)