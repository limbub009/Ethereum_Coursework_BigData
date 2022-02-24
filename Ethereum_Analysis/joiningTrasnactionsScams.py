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
                join_key = fields[2].lstrip('"')
                join_value = float(fields[3])
                timestamp = fields[6]
                x = join_value / 10 ** 14
                yield(join_key, (x,1,timestamp))

#scams aggregation
            elif(len(line.split(','))==2):
                fields=line.split(',')
                id = fields[0].strip("\"").rstrip("\"\t ")
                join_key = fields[1].rstrip("\"").lstrip(" ")
                yield(join_key,(id,2))
        except:
            pass

    def reducer(self, key, values):
        in_contract = False
        total = 0
        for value in values:
            if value[1]==1:
                total += float(value[0])
                timestamp = value[2]

            if value[1]==2:
                id = value[0]
                in_contract = True

        if (in_contract==True and total > 0):
            x = ","+ str(id).rstrip("\"").lstrip(" ") +","+ str(total)
            x.strip()
            yield(key, x)

        #if block_number != None and len(to_addresses)!=0:








if __name__ == '__main__':
    joining_transactions_contracts.JOBCONF = { 'mapreduce.job.reduces': '10'}
    joining_transactions_contracts.run()
