from mrjob.job import MRJob
import re
import time


WORD_REGEX = re.compile(r"\b\w+\b")



class popularScams(MRJob):
    def mapper(self, _ , line):
        fields = line.split(",")
        if (len(fields) >= 5):
            id = str(fields[0])
            addr = str(fields[4])
            extraAddr = len(fields) - 4
            if (len(fields) == 5):
                yield(1, (id, addr))
            elif (len(fields) > 4):
                for i in range(4,len(fields)):
                    yield(1,(id,fields[i]))

    def reducer(self, key, values):
        for value in values:
            id = value[0]
            addy = value[1]
            x = ","+addy
            yield(id, x)
        #if block_number != None and len(to_addresses)!=0:










if __name__ == '__main__':
    popularScams.JOBCONF = { 'mapreduce.job.reduces': '10'}
    popularScams.run()
