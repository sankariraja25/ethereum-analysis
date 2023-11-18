import pyspark
import time

sc = pyspark.SparkContext()


def checkTransactions(line):
        try:
                fields = line.split(',')
                if len(fields)!= 7:
                        return False
                float(fields[5])
                float(fields[6])
                return True
        except:
                return False

def checkContracts(line):
        try:
                fields = line.split(',')
                if len(fields) != 5:
                        return False
                float(fields[3])
                return True

        except:
                False

def checkBlocks(line):
        try:
                fields = line.split(',')
                if len(fields)!=9:
                        return False

                float(fields[0])
                float(fields[3])
                float(fields[7])
                return True

        except:
                return False


transactionsInput = sc.textFile('/data/ethereum/transactions')
contractsInput = sc.textFile('/data/ethereum/contracts')
blocksInput = sc.textFile('/data/ethereum/blocks')

transactions = transactionsInput.filter(checkTransactions)
contracts = contractsInput.filter(checkContracts)
blocks = blocksInput.filter(checkBlocks)

time_t = transactions.map(lambda i: (float(i.split(',')[6]), float(i.split(',')[5])))
date_d = time_t.map(lambda (a,b): (time.strftime("%y.%m", time.gmtime(a)), (b,1)))
t_time = date_d.reduceByKey(lambda (a1, b1), (a2, b2): (a1+a2, b1+b2)).map(lambda j: (j[0], (j[1][0]/j[1][1])))

fin = t_time.sortByKey(ascending=True)
fin.saveAsTextFile('AverageGas')

blocks = contracts.map(lambda k: (k.split(',')[3], 1))

blockdifference = blocks.map(lambda b: (b.split(',')[0], (int(b.split(',')[3]), int(b.split(',')[6]), time.strftime("%y.%m", time.gmtime(float(b.split(',')[7]))))))
results = blockdifference.join(blocks).map(lambda (id, ((a, b, c), d)): (c, ((a,b), d)))
final = results.reduceByKey(lambda ((a1,b1), c1) , ((a2, b2), c2): ((a1 + a2, b1 + b2), c1+c2)).map(lambda z: (z[0], (float(z[1][0][0]/z[1][1]), z[1][0][1]/ z[1][1]))).sortByKey(ascending=True)
final.saveAsTextFile('TimeDifference')

print("App ID: {0}".format(sc.applicationId))
