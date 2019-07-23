import tweepy
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd
import nltk
import time

'''https://www.youtube.com/watch?v=3Pzni2yfGUQ
   https://github.com/cjhutto/vaderSentiment'''


class Twitter:
    # TODO Start only one model for all threads.
    # TODO OOOOORRRR re-generate client secret for each thread
    error = 0
    remaining_time = 0

    def __init__(self, consumer_key, consumer_secret, access_token, access_token_secret, lexicon='vader_lexicon'):
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        nltk.download(lexicon)

        self.api = tweepy.API(auth)

    def search(self, s_string):
        if time.time() < self.remaining_time:
            return -1, -1, -1, -1, -1

        tweet_amount = 0
        compound = 0
        positive = 0
        negative = 0
        neutral = 0
        try:
            # rates = self.api.rate_limit_status()
            # self.remaining = rates['resources']['search']['/search/tweets']['remaining']
            tweets = self.api.search(s_string)
            # TODO Place stuff that can be a global variable out of the function
            tweet_amount = len(tweets)

            data = pd.DataFrame(data=[tweet.text for tweet in tweets], columns=['Tweets'])
            sid = SentimentIntensityAnalyzer()
            listy = []

            for index, row in data.iterrows():
                ss = sid.polarity_scores(row["Tweets"])
                listy.append(ss)

            se = pd.Series(listy)
            data['polarity'] = se.values

            if len(data['polarity']):
                for row in data['polarity']:
                    compound += row['compound']
                    positive += row['pos']
                    negative += row['neg']
                    neutral += row['neu']

                compound /= len(data['polarity'])
                positive /= len(data['polarity'])
                negative /= len(data['polarity'])
                neutral /= len(data['polarity'])

                '''
                print('Tweet Amount:', tweet_amount)
                print('Compound:', compound)
                print('Positive:', positive)
                print('Negative', negative)
                print('Neutral', neutral)
                '''
                #print("[Tweepy][+] Tweets added!")
        except tweepy.RateLimitError:
            # TODO Find a way to print the error onlY every so often
            resources = self.api.rate_limit_status()
            self.remaining_time = resources['resources']['search']['/search/tweets']['reset']

        return tweet_amount, compound, positive, negative, neutral