from mrjob.job import MRJob
import re
import time


WORD_REGEX = re.compile(r"\b\w+\b")


class Time_Analysis(MRJob):
    def mapper(self, _ ,line):
        try:
            fields = line.split(",")
            if (len(fields) == 7):
                month = time.strftime("%m", time.gmtime(int(fields[6])))
                year = time.strftime("%y", time.gmtime(int(fields[6])))
                date = month + " " + year
                yield(date, 1)
        except:
            pass

    def reducer(self, key, x):
        total = sum(x)
        yield(key ,total)


if __name__ == '__main__':
    Time_Analysis.JOBCONF = { 'mapreduce.job.reduces': '10'}
    Time_Analysis.run()
