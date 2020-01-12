import nltk
import string
from nltk.corpus import stopwords


class Utils:
    @staticmethod
    def tokenize_words(sentence):
        """
        Funtions which cleans the string and returns a list without stopwords in different languages (new languages
        can be added), it uses a lambda funtion in order to get the list
        :param sentence: sentence to be cleaned
        :return: clean list of lower case words, without stopwords and punctuation
        """
        sentence = sentence.replace('\n', ' ')  # Replace all the endlines with a blank space
        token_sentence = nltk.word_tokenize(sentence)
        tagged = nltk.pos_tag(token_sentence)
        # we get all the stopwords in English, French and Spanish
        stop_words_english = list(stopwords.words('english'))
        stop_words_french = list(stopwords.words('french'))
        stop_words_spanish = list(stopwords.words('spanish'))
        stop_words_general = stop_words_french + stop_words_english + stop_words_spanish  # List containing all the stopwords
        aux2 = list(filter(
            lambda x: (x[0].lower() not in stop_words_general) and (x[0].lower() not in string.punctuation) and (
                    x[0].lower() not in ('\'s', 'primarytitle')), tagged))
        return aux2



