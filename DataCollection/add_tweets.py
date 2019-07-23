from collector import Collector
from tweep import Twitter

class AddTweets(Collector):
    threads = 0
    cons_amount = 50
    ins_table = 'artists'  # Name of collection. Used to insert data

    first_run = True

    def __init__(self, client_id, client_secret, consumer_key, consumer_secret, access_token, access_token_secret, inserter_size=1e+8,):
        self.ins_size = inserter_size
        self.tweep = Twitter(consumer_key, consumer_secret, access_token, access_token_secret)
        super(AddTweets, self).__init__(client_id=client_id, client_secret=client_secret)

    def start_collection(self, threads=1):
        self.threads = threads
        self.start()

    def condition(self):
        songs = self.db.songs.count_documents({'$or': [
            {'tweets': -1},
            {'tweets':
                 {'$exists': False}
             }
        ]
        })

        print(songs, "Tweets have not been updated yet.")

        return songs

    def producer(self):
        songs = self.db.songs.find({'$or': [
                                            {'tweets': -1},
                                            {'tweets':
                                                {'$exists': False}
                                            }
                                          ]
                                  }).limit(10000)
        return songs

    def consumer(self, data):
        ret_data = []
        for song in data:
            artist_name = []
            # Putting together artist names
            for artist in song['artists']:
                artist_name.append(artist['name'])

            final_name = " ,".join(artist_name) + " - " + song['name']
            tweet_amount, compound, positive, negative, neutral = self.tweep.search(final_name)

            ret_data.append({'id': song['_id'],
                             'tweets': tweet_amount,
                             'tweets_compound': compound,
                             'tweets_positive': positive,
                             'tweets_negative': negative,
                             'tweets_neutral': neutral})

        return ret_data

    def inserter(self, data):
        self.db.songs.update_one({'_id': data['id']}, {'$set': {'tweets': data['tweets'],
                                                                 'tweets_compound': data['tweets_compound'],
                                                                 'tweets_positive': data['tweets_positive'],
                                                                 'tweets_negative': data['tweets_negative'],
                                                                 'tweets_neutral': data['tweets_neutral']}}, upsert=True)