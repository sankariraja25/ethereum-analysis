from mrjob.job import MRJob
import time

class PartA1(MRJob):

	def mapper(self, _,line):
		fields = line.split(',')
		try:
			if len(fields) == 7:
				block_timestamp = int(fields[6])
				month = time.strftime("%m", time.gmtime(block_timestamp))
				year = time.strftime("%y", time.gmtime(block_timestamp))
				yield ((month,year), 1)
		except:
			pass

	def reducer(self,month_year,count):
		yield(month_year,sum(count))

if __name__ == '__main__':
	PartA1.run()
