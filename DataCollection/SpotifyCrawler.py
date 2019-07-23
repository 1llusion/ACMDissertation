#!/usr/bin/env python
# coding: utf-8
from recommendations import Recommendations
from song_popularity import SongPopularity
from db_migrate import DbMigrate
import datetime
from artist_popularity import ArtistPopularity
from popularity_migrate import PopularityMigrate
from add_tweets import AddTweets
from add_topics import AddTopics
from add_lyric_sentiment import AddLyricSentiment
from add_name_sentiment import AddNameSentiment
from add_artist_sentiment import AddArtistSentiment

''' SPOTIFY DATA '''
client_id = ""
client_secret = ""

''' TWITTER DATA '''
consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''


if __name__ == '__main__':
    # Timing the script. Don't need precision.
    time_start = datetime.datetime.now()

    #crawler = Recommendations(client_id, client_secret, api_url, api_key)
    popularity = SongPopularity(client_id=client_id,
                                client_secret=client_secret,
                                inserter_size=1e+8
                                )

    db_migrate = DbMigrate(client_id=client_id,
                           client_secret=client_secret,
                           inserter_size=1e+8,
                           )

    popularity_migrate = PopularityMigrate(client_id=client_id,
                                           client_secret=client_secret,
                                           inserter_size=1e+8,
                                           )

    artist_popularity = ArtistPopularity(client_id=client_id,
                                         client_secret=client_secret,
                                         inserter_size=1e+8
                                         )

    add_tweets = AddTweets(client_id=client_id,
                           client_secret=client_secret,
                           inserter_size=1e+8,
                           consumer_key=consumer_key,
                           consumer_secret=consumer_secret,
                           access_token=access_token,
                           access_token_secret=access_token_secret)
    add_topics = AddTopics(client_id=client_id,
                           client_secret=client_secret,
                           inserter_size=1e+8
                           )

    add_lyric_sentiment = AddLyricSentiment(client_id=client_id,
                                            client_secret=client_secret,
                                            inserter_size=1e+8
                                            )

    add_name_sentiment = AddNameSentiment(client_id=client_id,
                                          client_secret=client_secret,
                                          inserter_size=1e+8
                                          )
    add_artist_sentiment = AddArtistSentiment(client_id=client_id,
                                          client_secret=client_secret,
                                          inserter_size=1e+8
                                          )
    '''
    feature_maintenance = SongMaintenance(client_id,
                                          client_secret,
                                          api_url,
                                          api_key,
                                          inserter_size=1e+8,
                                          inserter_folder='song_name_csv',
                                          inserter_name='song_name',
                                          final_output_path=Path(Path.cwd(), 'song_name_combined'),
                                          final_output_name='songNameCombined',
                                          clean_files=True
                                          )
    '''
    #results = crawler.random_recommendations(min_popularity=0, amount=10000, download=False, spectrogram=False, insert=True, track_seed=True, threads=6)
    #popularity.start_collection(threads=6)
    #db_migrate.start_collection(threads=10)
    #popularity_migrate.start_collection(threads=10)
    #add_topics.start_collection(threads=0)
    #add_name_sentiment.start_collection(threads=6)
    #add_artist_sentiment.start_collection(threads=6)
    #add_lyric_sentiment.start_collection(threads=6)
    add_tweets.start_collection(threads=6)
    #artist_popularity.start_collection(threads=6)
    #feature_maintenance.start_collection(threads=6)

    time_end = datetime.datetime.now()

    exec_time = time_end - time_start

    print("Done!", "Execution time: " + str(exec_time))

