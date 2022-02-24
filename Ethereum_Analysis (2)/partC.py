from mrjob.job import MRJob
from mrjob.step import MRStep

class partCAggregate(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 9:
                miner = fields[2]
                size = int(fields[4])
                yield(miner, size)
        except:
            pass
            
    def reducer(self, key, size):
        total = sum(size)
        yield(key, total)

if __name__ == '__main__':
    partCAggregate.JOBCONF = { 'mapreduce.job.reduces': '10'}
    partCAggregate.run()
