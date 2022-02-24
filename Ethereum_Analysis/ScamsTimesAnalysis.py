from mrjob.job import MRJob
import time
import re

WORD_REGEX = re.compile(r"\b\w+\b")


class joining_transactions_contracts(MRJob):
    def mapper(self, _ , line):
        try:
#trasnactions
            if (len(line.split(',')) == 7):
                fields=line.split(',')
                join_key = fields[2]
                the_value = float(fields[3])
                timestamp = time.strftime("%d%m%y", time.gmtime(int(fields[6])))
                x = the_value / 10 ** 14
                yield(join_key, (x,1,timestamp))
#
# #top 10
            elif(len(line.split('||'))==3):
                fields=line.split('||')
                id = fields[0]
                join_key = fields[1]
                yield(join_key,(id,2))
        except:
            pass

    def reducer(self, key, values):
        in_contract = False
        x = 0
        timestamp = None
        prev = []
        for value in values:
            if value[1] == 1:
                x = value[0]
                timestamp = str(value[2])
                prev.append(timestamp)
            if value[1] == 2:
                in_contract = True
                id = value[0]
        if (in_contract == True):
            output = str(id).lstrip("\"").rstrip("\"\t\"") + "," +str(prev).strip("[]")
            yield(key, output)



if __name__ == '__main__':
    joining_transactions_contracts.JOBCONF = { 'mapreduce.job.reduces': '10'}
    joining_transactions_contracts.run()
