from mrjob.job import MRJob
from mrjob.step import MRStep

class PartB(MRJob):
	def mapper1(self, _, line):
		fields = line.split(',')
		try:
			if len(fields) == 7:
				to_address = fields[2]
				value = int(fields[3])
				yield to_address, ('T',value)
			elif len(fields) == 5:
				address = fields[0]
				yield address, ('C',1)
		except:
			pass
	def reducer1(self, address, values):
		contractAddressExist = False
		transactionValues = []
		for value in values:
			if value[0]=='T':
				transactionValues.append(i[1])
			elif value[0] == 'C':
				contractAddressExist = True
		if contractAddressExist == True:
			yield address, sum(transactionValues)

	def mapper2(self, address,values):
		yield None, (address,values)

	def reducer2(self, _, values):
		sortedvalues = sorted(values, reverse = True, key = lambda x: x[1])
		for address,values in sortedvalues[:10]:
			yield address, values

	def steps(self):
		return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, reducer = self.reducer2)]

if __name__ == '__main__':
	PartB.run()
