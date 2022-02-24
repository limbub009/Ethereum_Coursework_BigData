from mrjob.job import MRJob
import re
import time


WORD_REGEX = re.compile(r"\b\w+\b")



class joining_transactions_contracts(MRJob):
    def mapper(self, _ , line):
        try:
            #find from data set 1, indentifier
            if (len(line.split('\t')) == 2):
                fields=line.split('\t')
                join_key = str(fields[0])             #to_address
                join_value = float(fields[1])
                join_value / (10**14)        #value
                yield(join_key, (join_value,1))


            #find from data set 2, identifier
        elif(len(line.split(','))==5):
                fields=line.split(',')
                join_key = fields[0]                   #to_addresses         #block number
                yield(join_key,(0,2))
        except:
            pass

    def reducer(self, key, values):
        in_contract = False
        total = 0
        for value in values:
            if value[1]==1:
                total += value[0]

            elif value[1]==2:
                in_contract = True
        if (in_contract and total > 0):
            power = 10**14
            total = total/power
            yield(key, total)


        #if block_number != None and len(to_addresses)!=0:










if __name__ == '__main__':
    joining_transactions_contracts.JOBCONF = { 'mapreduce.job.reduces': '10'}
    joining_transactions_contracts.run()
