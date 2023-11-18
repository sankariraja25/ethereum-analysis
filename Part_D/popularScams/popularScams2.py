from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class PartDscams2(MRJob):
    def mapper1(self, _, lines):
        try:
            fields = lines.split(",")
            if len(fields) == 7:
                to_address = fields[2]
                yield to_address, ('T',0)

            else:
                line = json.loads(lines)
                scams = line["scams"]

                for scam in scams:
                    record = line["scam"][scam]
                    category = record["category"]
                    addresses = record["addresses"]
                    status = record["status"]

                    for address in addresses:
                        yield address, ('S', category,status)

        except:
            pass

    def reducer1(self, address, values):
        totalValue=0
        category=None
        status = None
        for value in values:
            if value[0] == 'T':
                totalValue = totalValue + totalValue[0]
            else:
                category = value[1]
                status = value[2]
        if category is not None and status is not None:
            yield (status,category), totalValue

    def mapper2(self,status_category,totalValue):
        yield(status_category,totalValue)
    def reducer2(self, status_category, totalValue):
        yield(status_category,sum(totalValue))

    def steps(self):
        return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, reducer = self.reducer2)]

if __name__ == '__main__':
	PartDscams2.run()
