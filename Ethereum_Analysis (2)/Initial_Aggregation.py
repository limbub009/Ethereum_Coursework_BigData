from mrjob.job import MRJob
import re
import time


WORD_REGEX = re.compile(r"\b\w+\b")



class Initial_Aggregation(MRJob):
    def mapper(self, _ , line):
        try:
            fields = line.split(",")
            if (len(fields) == 7):
                to_address = str(fields[2])
                value = int(fields[3])
            yield(to_address, value)
        except:
            pass

    def reducer(self, key, x):
        total = sum(x)
        if (total > 0):
            yield(key,total)







if __name__ == '__main__':
    Initial_Aggregation.JOBCONF = { 'mapreduce.job.reduces': '10'}
    Initial_Aggregation.run()
