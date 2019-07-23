import gensim
import nltk
nltk.download('wordnet')
from nltk.stem import WordNetLemmatizer, SnowballStemmer


class TextPreprocess:
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