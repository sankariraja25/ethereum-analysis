from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class PartDscams1(MRJob):
	def mapper1(self, _, lines):
		try:
			fields = lines.split(",")
			if len(fields) == 7:
				to_address = fields[2]
				value = float(fields[3])
				yield to_address, (value,'T')

			else:
				line = json.loads(lines)
				scams = line["scams"]

				for scam in scams:
					record = line["scams"][scam]
					category = record["category"]
					addresses = record["addresses"]

					for address in addresses:
						yield address, (category,'S')

		except:
			pass

	def reducer1(self, address, values):
		totalValue=0
		category=None

		for value_category,flag_value in values:
			if flag_value == 'T':
				totalValue = totalValue + value_category
			else:
				category = value_category
		if category is not None:
			yield category, totalValue

	def mapper2(self,category,totalValue):
		yield(category,totalValue)
	def reducer2(self, category, totalValue):
		yield(category,sum(totalValue))

	def steps(self):
		return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, reducer = self.reducer2)]

if __name__ == '__main__':
	PartDscams1.run()
