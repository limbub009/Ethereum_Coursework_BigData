from mrjob.job import MRJob
import re
import time


WORD_REGEX = re.compile(r"\b\w+\b")


class Time_AnalysisAVG(MRJob):
    def mapper(self, _ ,line):
        try:
            fields = line.split(",")
            if (len(fields) == 7):
                month = time.strftime("%m", time.gmtime(int(fields[6])))
                year = time.strftime("%Y", time.gmtime(int(fields[6])))
                date = month + " " + year
                value = float(fields[3])
                yield(date, (value,1))
        except:
            pass


    def reducer(self, key, x):
        TotalFeature = 0
        Count = 0
        for a in x:
            TotalFeature+= a[0]
            Count += a[1]
        avg = float(TotalFeature/Count)
        yield(key, avg)

if __name__ == '__main__':
    Time_AnalysisAVG.JOBCONF = { 'mapreduce.job.reduces': '10'}
    Time_AnalysisAVG.run()
