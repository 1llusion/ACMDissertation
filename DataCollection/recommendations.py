from songs_db import SongsDb
from threading import Thread, get_ident
import datetime
from random import shuffle
from queue import Queue
from crawler import Crawler


class Recommendations(SongsDb, Crawler):

    def random_recommendations(self, amount=1, download=False, spectrogram=False, insert=False, threads=6, track_seed=False, **kwargs):
        q = Queue(maxsize=amount)
        sp = self.get_spotify()

        seed_genres = sp.recommendation_genre_seeds()
        # bar = ChargingBar('Fetching recommendations', max=amount, suffix='%(percent)d%%')

        threads = threads if amount >= threads else amount
        threads = threads if threads > 2 else 2

        for t in range(threads):
            if t % 2:
                # Getting tracks
                t = Thread(target=self.recommendation_producer, args=(q, amount, seed_genres,), kwargs={'track_seed': track_seed,
                                                                                                        **kwargs})
                t.start()
            else:
                # Processing tracks
                t = Thread(target=self.recommendation_consumer, args=(q,), kwargs={'download': download,
                                                                                   'insert': insert,
                                                                                   'spectrogram': spectrogram})
                t.daemon = True
                t.start()

        # Joining everything
        q.join()
        # bar.next()
        # bar.finish()

        # Track seed will use a random id from the database

    def recommendation_producer(self, q, amount, seed_genres, track_seed=False, **kwargs):
        # Looping while the amount is larger than the amount of songs we need
        sp = self.get_spotify()
        shuffle(seed_genres["genres"])
        seed_genres = dict.fromkeys(seed_genres["genres"], 100)
        unique_value = 0

        # We are saving who produces the most tracks per second.
        # Only the most profitable method is used
        genres_time = 0
        tracks_time = 0

        # Every 100 loops we get both tracks and genres just to see if we find a better way to get tracks
        swith_counter = 0

        while amount > self.songs:
            # Trying to maximize the limit
            if amount > 100:
                limit = amount % 100 if amount % 100 else 100
            else:
                limit = amount

            if genres_time >= tracks_time or swith_counter % 100 is 0:
                time_start = datetime.datetime.now()
                # seed_genre = random.sample(seed_genres["genres"], 5)
                best_genre = max(seed_genres.keys(), key=(lambda k: seed_genres[k]))

                print("Getting genre -", best_genre)
                recommendations = sp.recommendations(seed_genres=[best_genre], limit=limit, **kwargs)
                unique_value = self.add_recommendation(q, recommendations['tracks'])
                time_end = datetime.datetime.now()

                timedelta = time_end - time_start
                genres_time += unique_value / (timedelta.seconds * 1000000 + timedelta.microseconds)
                genres_time /= 2

                seed_genres[best_genre] = unique_value if unique_value > 0 else 1
                print("Finished", best_genre, "- with", seed_genres[best_genre], "unique songs on thread", get_ident())

            # Getting tracks
            track_seed_list = []
            if track_seed and tracks_time >= genres_time or swith_counter % 100 is 0:
                time_start = datetime.datetime.now()

                track_id = self.get_random_track_id()
                if not track_id:
                    continue
                print("Getting track -", track_id)
                seed_id = 'spotify:track:' + track_id
                track_seed_list.append(seed_id)

                # recommendations = sp.recommendations(limit=limit, seed_tracks=track_seed_list, **kwargs)
                recommendations = sp.recommendations(limit=limit, seed_tracks=track_seed_list, **kwargs)
                track_value = self.add_recommendation(q, recommendations['tracks'])
                time_end = datetime.datetime.now()

                timedelta = time_end - time_start
                tracks_time += track_value / (timedelta.seconds * 1000000 + timedelta.microseconds)
                tracks_time /= 2

                print("Finished track with", track_value, "songs on thread", get_ident())
                unique_value += track_value

            # Preventing from an infinite loop
            print("Genres efficiency -", genres_time * 1000000, "tracks per second on thread", get_ident())
            print("Tracks efficiency -", tracks_time * 1000000, "tracks per second on thread", get_ident())
            amount -= unique_value
            swith_counter += 1

    '''
            Right now I need to collect song data, therefore I am not checking for preview_URL as it seems to be quite rare
            Add it back for sepctrogram creation, or create a whole new class for it anyway
    '''

    def add_recommendation(self, q, recommendations):
        unique_value = 0

        for song in recommendations:
            # Checking if the track has a preview url as well as if it is a unique value
            unique = self.is_unique(song['id'])
            # if song["preview_url"] and unique is True:
            if unique is True:
                q.put(song)
                self.songs += 1
                unique_value += 1
        return unique_value

    def recommendation_consumer(self, q, download=0, insert=0, spectrogram=0):
        threads = []
        while True:
            # Getting a maximum batch of 50 tracks as this is the limit for recomendations
            tracks = []
            queue_size = q.qsize()
            track_range = 50 if queue_size > 50 else queue_size
            if queue_size:
                for i in range(track_range):
                    song = q.get()
                    tracks.append(song)
                    q.task_done()

            # Multi-threading downloads and adding to database
            if download:
                t1 = Thread(target=self.download, args=(tracks,), kwargs={'spectrogram': spectrogram})
                threads.append(t1)
                t1.start()

            if insert:
                t2 = Thread(target=self.get_features, args=(tracks,))
                threads.append(t2)
                t2.start()

            for t in threads:
                t.join()