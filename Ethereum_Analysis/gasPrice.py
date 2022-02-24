from mrjob.job import MRJob
import time

class gasPrice(MRJob):
    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7 :
                time_epoch = int(fields[6])
                month = time.strftime("%b %Y", time.gmtime(time_epoch))
                gas_price = float(feilds[5])
                yield (month, (gas_price, 1))
        except:
            pass

    def combiner(self, month, values):
        gas_price_total = 0
        transaction_count = 0
        for value in values:
            gas_price_total += value[0]
            transaction_count += value[1]

        yield (month, (gas_price_total, transaction_count))#

    def reducer(self, month, values):
        gas_price_total = 0
        transaction_count = 0
        for value in values:
            gas_price_total += value[0]
            transaction_count += value[1]

        yield(month, (gas_price_total/transaction_count))

if __name__ == '__main__':
    gasPrice.JOBCONF = { 'mapreduce.job.reduces': '10'}
    gasPrice.run()
