from mrjob.job import MRJob
import time

class PartA2(MRJob):
	def mapper(self,_, line):
		fields = line.split(',')
		try:
			if len(fields)==7:
				block_timestamp = int(fields[6])
				value = float(fields[3])
				month = time.strftime("%m", time.gmtime(block_timestamp))
				year = time.strftime("%y", time.gmtime(block_timestamp))
				yield((month,year),(value,1))
		except:
			pass
	def combiner(self,month_year,value_count):
		total_value = 0
		total_count = 0
		for value,count in value_count:
			total_value += value
			total_count += count
		yield(month_year,(total_value,total_count))

	def reducer(self,month_year,value_count):
		total_value = 0
		total_count = 0
		for value,count in value_count:
			total_value += value
			total_count += count
		average_value = total_value/total_count

		yield(month_year,average_value)

if __name__ == '__main__':
	PartA2.run()
