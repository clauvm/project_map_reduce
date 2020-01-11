# from mrjob.job import MRJob
# from mrjob.step import MRStep
# import re
# import nltk
#
# from utils import Utils
#
# nltk.download('punkt')
# nltk.download('averaged_perceptron_tagger')
# nltk.download('stopwords')
#
# WORD_RE = re.compile(r"[\w']+")
#
# class MRCounterCommonKeysMoviesShorts(MRJob):
#
#     def steps(self):
#         return [
#             MRStep(mapper=self.mapper_get_words,
#                    combiner=self.combiner_count_words,
#                    reducer=self.reducer_count_words),
#             MRStep(reducer=self.reduce_sort_counts)
#         ]
#
#     def mapper_get_words(self, _, record):
#         aux1 = record.split("\t")
#         aux2 = Utils.tokenize_words(aux1[2])
#         for word in aux2:
#             yield (aux1[1]+"+"+word[0].lower(), 1)
#
#     def combiner_count_words(self, word, counts):
#         yield (word, sum(counts))
#
#     def reducer_count_words(self, word, counts):
#         aux3 = word.split("+")
#         yield aux3[0], (sum(counts), aux3[1])
#
#     def reduce_sort_counts(self, type, word_counts):
#         aux = 0
#         for count, word in sorted(word_counts, reverse=True):
#             if aux < 10:
#                 aux = aux+1
#                 yield type, (int(count), word)
#
#
# class MRCounterCommonKeysMovieGenre(MRJob):
#     def steps(self):
#         return [
#             MRStep(mapper=self.mapper_get_words,
#                    combiner=self.combiner_count_words,
#                    reducer=self.reducer_count_words),
#             MRStep(reducer=self.reduce_sort_counts)
#         ]
#
#     def mapper_get_words(self, _, record):
#         aux1 = record.split("\t")
#         if aux1[1] == "movie":
#             genres = aux1[8].split(",")
#             aux2 = Utils.tokenize_words(aux1[2])
#             for genre in genres:
#                 for word in aux2:
#                     if genre == "\\N":
#                         genre = "N/A"
#                     yield (genre+"+"+word[0].lower(), 1)
#
#     def combiner_count_words(self, word, counts):
#         yield (word, sum(counts))
#
#     def reducer_count_words(self, word, counts):
#         aux3 = word.split("+")
#         yield aux3[0], (sum(counts), aux3[1])
#
#     def reduce_sort_counts(self, type, word_counts):
#         aux = 0
#         for count, word in sorted(word_counts, reverse=True):
#             if aux < 10:
#                 aux = aux+1
#                 yield type, (int(count), word)
#
#
# if __name__ == '__main__':
#     MRCounterCommonKeysMoviesShorts.run()
#     MRCounterCommonKeysMovieGenre.run()

from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import nltk

from utils import Utils

nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('stopwords')

WORD_RE = re.compile(r"[\w']+")

class MRCounterCommonKeysMoviesShorts(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   # combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reduce_sort_counts)
        ]

    def mapper_get_words(self, _, record):
        aux1 = record.split("\t")
        aux2 = Utils.tokenize_words(aux1[2])
        for word in aux2:
            yield aux1[1], (word[0].lower(), 1)

    # def combiner_count_words(self, word, counts):
    #     yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        # aux3 = word.split("+")
        for value in counts:
            yield word, (sum(value[1]), value[0])

    def reduce_sort_counts(self, type, word_counts):
        aux = 0
        for count, word in sorted(word_counts, reverse=True):
            if aux < 10:
                aux = aux+1
                yield type, (int(count), word)


# class MRCounterCommonKeysMovieGenre(MRJob):
#     def steps(self):
#         return [
#             MRStep(mapper=self.mapper_get_words,
#                    combiner=self.combiner_count_words,
#                    reducer=self.reducer_count_words),
#             MRStep(reducer=self.reduce_sort_counts)
#         ]
#
#     def mapper_get_words(self, _, record):
#         aux1 = record.split("\t")
#         if aux1[1] == "movie":
#             genres = aux1[8].split(",")
#             aux2 = Utils.tokenize_words(aux1[2])
#             for genre in genres:
#                 for word in aux2:
#                     if genre == "\\N":
#                         genre = "N/A"
#                     yield (genre+"+"+word[0].lower(), 1)
#
#     def combiner_count_words(self, word, counts):
#         yield (word, sum(counts))
#
#     def reducer_count_words(self, word, counts):
#         aux3 = word.split("+")
#         yield aux3[0], (sum(counts), aux3[1])
#
#     def reduce_sort_counts(self, type, word_counts):
#         aux = 0
#         for count, word in sorted(word_counts, reverse=True):
#             if aux < 10:
#                 aux = aux+1
#                 yield type, (int(count), word)


if __name__ == '__main__':
    MRCounterCommonKeysMoviesShorts.run()
    # MRCounterCommonKeysMovieGenre.run()