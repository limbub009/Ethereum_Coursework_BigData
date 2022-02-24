from mrjob.job import MRJob
import re
import operator



class lab4(MRJob):
    def mapper(self, _ , line):
        try:
            fields = line.split(",")
            if len(fields) == 3:
                id = str(fields[1])
                address = fields[0].lstrip("\"").rstrip("\"\t\"")
                value = fields[2].rstrip("\"")
                yield(None,(id,address,value))
        except:
             pass

    def reducer(self, key, values):
        top10 = []
        rank = 1
        for x in values:
            top10.append(x)
        sortedtop10 = sorted(top10, key=lambda ex: ex[2], reverse = True)
        sortedtop10 = sortedtop10[:10]
        for z in range(len(sortedtop10)):
            output = '||{address}||{value}'
            current = sortedtop10[z]
            yield(current[0], output.format(address =current[1], value = current[2]))
            rank+=1




if __name__ == '__main__':
    lab4.JOBCONF = { 'mapreduce.job.reduces': '3'}
    lab4.run()
