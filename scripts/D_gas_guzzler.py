""" 
	PART D: Gas Guzzlers
	
	yields avg gas price and avg required gas for each transaction

"""

from mrjob.job import MRJob
from datetime import datetime

class GasGuzzler(MRJob):

	def mapper(self, _, row):
		try:
			fields = row.split(',')
			if len(fields) == 7:
				gas = int(fields[4])
				gas_price = int(fields[5]) # in Wei
				timestamp = int(fields[6])
				date = datetime.fromtimestamp(timestamp).strftime("%d/%b/%y")
				yield(date, ("gas", gas))
				yield(date, ("price", gas_price))
		except:
			pass
	
	# date, [("gas", 234234), ("price", 2131414),...]
	def combiner(self, date, values):
		gas_total = 0
		gas_count = 0
		price_total = 0
		price_count = 0

		for value in values:
			if value[0] == "gas":
				gas_total += value[1]
				gas_count += 1
			elif value[0] == "price":
				price_total += value[1]
				price_count += 1
		
		yield(date, ("gas", gas_total, gas_count))	
		yield(date, ("price", price_total, price_count))	
				

	def reducer(self, date, values):
		gas_total = 0
		gas_count = 0
		price_total = 0
		price_count = 0

		for value in values:
			if value[0] == "gas":
				gas_total += value[1]
				gas_count += value[2]
			elif value[0] == "price":
				price_total += value[1]
				price_count += value[2]
		
		#yield("date, avg_gas, avg_gas_price", None)
		yield("{},{},{}".format(date, gas_total/gas_count, price_total/price_count), None)	
if __name__ == '__main__':
	GasGuzzler.JOBCONF = {'mapreduce.job.reduces': '4'}
	GasGuzzler.run()
