import os

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol, JSONProtocol, RapidJSONProtocol, RapidJSONValueProtocol, \
    UltraJSONValueProtocol, SimpleJSONValueProtocol
from mrjob.step import MRStep
from random import randrange
import re
import nltk
import json

# from mrjob.protocol import JSONProtocol

from utils import Utils

WORD_RE = re.compile(r"[\w']+")


class MRJaccardCoefficient(MRJob):
    def steps(self):
        return [
            MRStep(mapper_raw=self.extract_entities)
        ]
    # sentimentfile = '/Users/clau/Documents/master/Distributed Computing and storage/map_reduce_project/3_TEXT-SIMILARITY/test.json'
    # copy_document = sentimentfile[:]
    # stemmer = nltk.PorterStemmer()
    # stems = json.load(open(copy_document))
    # copy_document = stems[:]
    # number_file = randrange(len(stems))
    # file_to_compare = stems[number_file]
    # # del copy_document[number_file]
    # del copy_document[1]
    # INPUT_PROTOCOL = JSONProtocol

    # def mapper_init(self):
    #     self.chordProgressions = {}
    #
    # def mapper(self, _, line):
    #     key, value = line.strip().split('\t')
    #     self.chordProgressions[key] = {}
    #     self.chordProgressions[key] = json.loads(value)
    #     # find the total count of chord progressions in the diectionary
    #     for genre in self.chordProgressions[key].keys():
    #         normalizationCount = 0

    # def mapper(self, _, record):
    #     for word in WORD_RE.findall(record['summary']):
    #         yield [word.lower(), record['id']]


    def extract_entities(self, json_path, json_uri):
        js = json.load(open(json_path))
        for entity in js:
            yield

if __name__ == '__main__':
    MRJaccardCoefficient.run()
