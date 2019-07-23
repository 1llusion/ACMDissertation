from collector import Collector
from PyLyrics import *
import nltk
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


class AddNameSentiment(Collector):
    ins_table = 'artists'   # Name of collection. Used to insert data
    threads = 0
    cons_amount = 50    # Batch size to be processed by the consumer

    def __init__(self, client_id, client_secret, inserter_size=1e+8):
        self.ins_size = inserter_size
        nltk.download('vader_lexicon')
        super(AddNameSentiment, self).__init__(client_id=client_id, client_secret=client_secret)

    def start_collection(self, threads=1):
        self.threads = threads
        self.start()

    def condition(self):
        songs = self.db.songs.count_documents({'name_sentiment_compound': {'$exists': False}})

        print(songs, "Song names do not have sentiment analysis yet.")

        return songs

    def producer(self):
        songs = self.db.songs.find({'name_sentiment_compound': {'$exists': False}}).limit(10000)
        return songs

    def consumer(self, data):
        ret_data = []
        for song in data:
            sid = SentimentIntensityAnalyzer()

            try:
                ss = sid.polarity_scores(song['name'])

                ret_data.append({'id': song['_id'],
                                 'name_sentiment_compound': ss['compound'],
                                 'name_sentiment_positive': ss['pos'],
                                 'name_sentiment_negative': ss['neg'],
                                 'name_sentiment_neutral': ss['neu']})

            except ValueError as er:
                continue

        return ret_data

    def inserter(self, data):
        self.db.songs.update_one({'_id': data['id']}, {'$set':
                                                       {'name_sentiment_compound': data['name_sentiment_compound'],
                                                        'name_sentiment_positive': data['name_sentiment_positive'],
                                                        'name_sentiment_negative': data['name_sentiment_negative'],
                                                        'name_sentiment_neutral': data['name_sentiment_neutral']}},
                                 upsert=True)
