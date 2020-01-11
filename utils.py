import nltk
import string
from nltk.corpus import stopwords


class Utils:
    @staticmethod
    def tokenize_words(title):
        token_title = nltk.word_tokenize(title)
        tagged = nltk.pos_tag(token_title)
        stop_words_english = list(stopwords.words('english'))
        stop_words_french = list(stopwords.words('french'))
        stop_words_spanish = list(stopwords.words('spanish'))
        stop_words_general = stop_words_french + stop_words_english + stop_words_spanish
        aux2 = list(filter(
            lambda x: (x[0].lower() not in stop_words_general) and (x[0].lower() not in string.punctuation) and (
                        x[0].lower() not in ('\'s', 'primarytitle')), tagged))
        return aux2
