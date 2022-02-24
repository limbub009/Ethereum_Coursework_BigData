from mrjob.job import MRJob
import re
import time

class gasUsed(MRJob):

        def mapper(self, _, line):
                try:
                        fields = line.split(',')
                        if len(fields) == 7 :
                                timestamp = int(fields[6])
                                date = time.strftime("%m %Y", time.gmtime(timestamp))
                                GasUsed = float(fields[4])
                                yield (date, (GasUsed, 1))
                except:
                        pass

        def combiner(self, date, values):
                gasTotal = 0
                count = 0
                for value in values:
                        gasTotal += value[0]
                        count += value[1]

                yield (date, (gasTotal, count))

        def reducer(self, date, values):
                gasTotal = 0
                count = 0
                for value in values:
                        gasTotal += value[0]
                        count += value[1]

                yield(date, (gasTotal/count))

if __name__ == '__main__':
    gasUsed.JOBCONF = { 'mapreduce.job.reduces': '10'}
    gasUsed.run()
