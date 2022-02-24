from mrjob.job import MRJob
import re
import operator



class lab4(MRJob):
    def mapper(self, _ , line):
        try:
            #one mapper, we need to first differentiate among both types
            fields = line.split('\t')
            if len(fields) == 2:
                #this should be a company sector line
                value = int(fields[1])
                address = fields[0]
                yield (None, (address, value))
        except :
            pass

    def reducer(self, _, values):
        top10 = []
        rank = 1
        for x in values:
            top10.append(x)
        sortedtop10 = sorted(top10, key=lambda ex: ex[1], reverse = True)
        sortedtop10 = sortedtop10[:10]
        for z in range(len(sortedtop10)):
            output = '{address} || {amount:.0f}'
            current = sortedtop10[z]
            yield(rank, output.format(address=current[0], amount =current[1]))
            rank+=1



if __name__ == '__main__':
    lab4.JOBCONF = { 'mapreduce.job.reduces': '3'}
    lab4.run()
