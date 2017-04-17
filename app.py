
class WordCount(object):
    def __init__(self):
        pass

    def map(self, word):
        return word, 1

    def reduce(self, record1, record2):
        return record1[0], int(record1[1]) + int(record2[1])



