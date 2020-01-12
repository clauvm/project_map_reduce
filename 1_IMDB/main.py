from mrjob.job import MRJob
from mrjob.step import MRStep
import nltk

from utils import Utils

nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('stopwords')


class MRCounterCommonKeysMoviesShorts(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reduce_sort_counts)
        ]

    def mapper_get_words(self, _, record):
        """
        Mapper which receives the document with all the data and processes every row, splitting it into an array
        in order to be able to work only with titleType and primaryTitle. We concatenate the type of title and the word
        for every iteration
        :param _:
        :param record: document we work on
        """
        aux1 = record.split("\t")
        aux2 = Utils.tokenize_words(aux1[2])  # Since we want to work with the words in the primaryTitle, we have to clean the data first
        for word in aux2:
            yield (aux1[1]+"+"+word[0].lower(), 1)

    def reducer_count_words(self, word, counts):
        """
        Reducer which splits the param word into titleType and primaryTitle and sum the occurrence of every word
        :param word: string separated by "+" which contains the titleType and primaryTitle
        :param counts: 1
        """
        aux3 = word.split("+")  # Split the string
        yield aux3[0], (sum(counts), aux3[1])

    def reduce_sort_counts(self, type, word_counts):
        """
        Reducer which finds the 50 most common keywords for all movies and shorts
        :param type: titleType (movie or short)
        :param word_counts: amount of times the keyword is repeated in the document, keyword
        """
        aux = 0
        for count, word in sorted(word_counts, reverse=True):
            if aux < 50:  # Controls that we get only the 50 most common keywords
                aux = aux+1
                yield type, (int(count), word)
#
#
class MRCounterCommonKeysMovieGenre(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reduce_sort_counts)
        ]

    def mapper_get_words(self, _, record):
        """
        Mapper which receives the document with all the data and processes every row, splitting it into an array
        in order to be able to work only with titleType, primaryTitle and genre. According to the instruction, only movies
        will be processed. We concatenate the genre and the word
        for every iteration
        :param _:
        :param record: document we work on
        """
        aux1 = record.split("\t")
        if aux1[1] == "movie":
            genres = aux1[8].split(",")
            aux2 = Utils.tokenize_words(aux1[2])  # Since we want to work with the words in the primaryTitle, we have to clean the data first
            for genre in genres:
                for word in aux2:
                    if genre == "\\N":  # Some movies doesn't have a genre, so we change this to "N/A"
                        genre = "N/A"
                    yield (genre+"+"+word[0].lower(), 1)


    def reducer_count_words(self, word, counts):
        """
        Reducer which splits the param word into titleType and primaryTitle and sum the occurrence of every word
        :param word: string separated by "+" which contains the genre and primaryTitle
        :param counts: 1
        """
        aux3 = word.split("+")
        yield aux3[0], (sum(counts), aux3[1])

    def reduce_sort_counts(self, type, word_counts):
        """
        Reducer which finds the 15 most common keywords for every genre
        :param type: genre
        :param word_counts: amount of times the keyword is repeated in the document, keyword
        """
        aux = 0
        for count, word in sorted(word_counts, reverse=True):
            if aux < 15: # Controls that we get only the 15 most common keywords
                aux = aux+1
                yield type, (int(count), word)


if __name__ == '__main__':
    MRCounterCommonKeysMoviesShorts.run()
    MRCounterCommonKeysMovieGenre.run()