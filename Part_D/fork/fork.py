from mrjob.job import MRJob
import time

class fork(MRJob):

    def mapper(self,_,line):
        try:
            fields = line.split(',')
            gas_price = float(fields[5])
            block_timestamp = time.gmtime(float(fields[6]))
            if len(fields) == 7:
                if (block_timestamp.tm_year== 2020 and block_timestamp.tm_mon== 12):
                    yield ((block_timestamp.tm_mday), (1, gas_price))

        except:
            pass

    def combiner(self,block_timestamp,count_price):
        totalCount = 0
        totalPrice = 0
        for count,price in count_price:
            totalCount += count
            totalPrice = price

        yield (block_timestamp,(totalCount,totalPrice))

    def reducer(self,block_timestamp,count_price):
        totalCount = 0
        totalPrice = 0
        for count,price in count_price:
            totalCount += count
            totalPrice = price

        yield (block_timestamp, (totalCount,totalPrice))

if __name__=='__main__':
    fork.run()
