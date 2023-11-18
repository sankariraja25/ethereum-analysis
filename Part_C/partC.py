from mrjob.job import MRJob
from mrjob.step import MRStep

class PARTC(MRJob):
	def mapper1(self, _, line):
		fields = line.split(',')
		try:
			if len(fields) == 9:
				miner = fields[2]
				size = fields[4]
				yield (miner, int(size))

		except:
			pass

	def reducer1(self, miner, size):
		try:
			yield(miner, sum(size))

		except:
			pass

	def mapper2(self, miner, totalSize):
		try:
			yield(None, (miner,totalSize))
		except:
			pass

	def reducer2(self, _, miner_totalSize):
		try:
			sortedMiner = sorted(miner_totalSize, reverse = True, key = lambda x:x[1])
			for miner,totalSize in sortedMiner[:10]:
				yield(miner,totalSize)
		except:
			pass


	def steps(self):
		return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, reducer = self.reducer2)]

if __name__ == '__main__':
	PARTC.run()
