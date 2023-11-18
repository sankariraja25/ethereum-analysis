import pyspark

def checkTransactions(line):
        try:
                fields = line.split(',')
                if len(fields)!=7:
                        return False
                return True

        except:
                return False

def checkContracts(line):
        try:
                fields = line.split(',')
                if len(fields)!=5:
                        return False
                return True
        except:
                return False

sc = pyspark.SparkContext()

transactionsInput = sc.textFile("/data/ethereum/transactions")
transactions = transactionInput.filter(checkTransactions)
maptransaction = transactions.map(lambda i : (i.split(',')[2], int(i.split(',')[3])))
aggregatetransaction = maptransaction.reduceByKey(lambda c,d : c+d)
contractsInput = sc.textFile("/data/ethereum/contracts")
contracts = contractsInput.filter(checkContracts)
mapcontracts = contracts.map(lambda j: (j.split(',')[0], None))
joined = aggregatetransaction.join(mapcontracts)

topContracts = joined.takeOrdered(10, key = lambda l: -l[1][0])


with open('comparativeEvaluationSparkOut.txt', 'w') as f:
        for value in topContracts:
                f.write("{}:{}\n".format(value[0],value[1][0]))
