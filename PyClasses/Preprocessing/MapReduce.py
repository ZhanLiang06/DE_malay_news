from mrjob.job import MRJob
from mrjob.step import MRStep

class WordCounter(MRJob):
    
    def mapper(self, _, line):
        word = line.strip()
        if word:  
            yield word, 1

    def combiner(self, word, counts):
        yield word, sum(counts)

    def reducer(self, word, counts):
        yield word, sum(counts)

    # Map -> Combine -> Reduce
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer
            )
        ]

if __name__ == '__main__':
    WordCounter.run()





#=================================================================================
#                IF THE STEMMED DATA IS STORED IN CSV FILE
#=================================================================================


# from mrjob.job import MRJob
# from mrjob.step import MRStep
# import csv
# import ast

# class WordCounter(MRJob):
#     def mapper(self, _, line):
#         reader = csv.reader([line])
#         row = next(reader, None)  
#         if row and len(row) > 3: 
#             stemmed_tokens = row[3] 
#             if stemmed_tokens:
#                 try:
#                     tokens = ast.literal_eval(stemmed_tokens)
#                     if isinstance(tokens, list):  
#                         for token in tokens:
#                             if isinstance(token, str):  
#                                 yield token, 1
#                 except (ValueError, SyntaxError):
#                     self.increment_counter('Errors', 'Malformed Tokens', 1)

#     def combiner(self, word, counts):
#         yield word, sum(counts)

#     def reducer(self, word, counts):
#         yield word, sum(counts)

#     def steps(self):
#         return [
#             MRStep(
#                 mapper=self.mapper,
#                 combiner=self.combiner,
#                 reducer=self.reducer
#             )
#         ]

# if __name__ == '__main__':
#     WordCounter.run()
