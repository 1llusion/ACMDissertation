from collector import Collector
from PyLyrics import *
import gensim
import nltk
from nltk.stem import WordNetLemmatizer, SnowballStemmer


class AddTopics(Collector):
    ins_table = 'artists'   # Name of collection. Used to insert data
    threads = 0
    cons_amount = 50    # Batch size to be processed by the consumer

    first_run = True

    def __init__(self, client_id, client_secret, inserter_size=1e+8):
        nltk.download('wordnet')
        self.ins_size = inserter_size
        super(AddTopics, self).__init__(client_id=client_id, client_secret=client_secret)

    # https://github.com/priya-dwivedi/Deep-Learning/blob/master/topic_modeling/LDA_Newsgroup.ipynb

    def start_collection(self, threads=1):
        self.threads = threads
        self.start(multi=False)

    def condition(self):
        songs = self.db.songs.count_documents({'lyric_topics': {'$exists': False}})

        print(songs, "Lyric Topics have not been updated yet.")

        return songs

    def producer(self):
        songs = self.db.songs.find({'lyric_topics': {'$exists': False}}).limit(1000)

        return songs

    def consumer(self, data):
        ret_data = []
        for song in data:

            artist_name = song['artists'][0]['name']
            song_name = song['name']

            try:
                text = PyLyrics.getLyrics(artist_name, song_name)  # Print the lyrics directly
                topics = self.preprocess(text)

                ret = []
                for topic in topics[0][0]:
                    ret.append(topic[1])

            except ValueError as er:
                ret = []
                pass

            ret_data.append({'id': song['_id'],
                             'lyric_topics': ret})

        return ret_data

    def inserter(self, data):
        self.db.songs.update_one({'_id': data['id']}, {'$set': {'lyric_topics': data['lyric_topics']}}, upsert=True)

    @staticmethod
    def lemmatize_stemming(text):
        stemmer = SnowballStemmer("english")
        return stemmer.stem(WordNetLemmatizer().lemmatize(text, pos='v'))

    # Tokenize and lemmatize
    def preprocess_(self, text):
        result = []
        for token in gensim.utils.simple_preprocess(text):
            if token not in gensim.parsing.preprocessing.STOPWORDS and len(token) > 3:
                result.append(self.lemmatize_stemming(token))

        return result

    def preprocess(self, text):
        text_processed = [self.preprocess_(text)]
        dictionary = gensim.corpora.Dictionary(text_processed)
        # dictionary.filter_extremes(no_below=1)
        bow_corpus = [dictionary.doc2bow(doc) for doc in text_processed]

        lda_model = gensim.models.LdaMulticore(bow_corpus,
                                               num_topics=1,
                                               id2word=dictionary,
                                               passes=10,
                                               workers=2)

        topics = lda_model.top_topics(bow_corpus, topn=5)
        return topics

