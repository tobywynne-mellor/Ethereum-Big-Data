""" 
	PART B
"""

from mrjob.job import MRJob
from mrjob.step import MRStep

class B(MRJob):

	def mapper(self, _, row):
		try:
			fields = row.split(',')
			if len(fields) < 6:
				# contact (address, (id, ecr20, ecr721))
				yield(fields[0], ("contract", fields[2], fields[3])) 
			else:
				# transaction (to_address, value)
				yield(fields[2], ("transaction", int(fields[3])))
		except:
			pass	

	def combiner(self, address, values):
		total_recieved = 0
		smartcon = None
		present_in_contracts = False

		for value in values:
			if value[0] == "contract":
				if not smartcon:
					present_in_contracts = True	
					ecr20 = True if value[1] == "true" else False
					ecr721 = True if value[2] == "true" else False
					if ecr20:
						smartcon = "ecr20"
					elif ecr721:
						smartcon = "ecr721"
			else:
				total_recieved += value[1]
		if present_in_contracts:
			yield(address, (total_recieved, smartcon))	

	def reducer(self, address, values):
		try:
			total_sum = 0
			smartcon = None 
			for val in values:
				if smartcon is not None and val[1] is not None:
					smartcon = val[1]
				if val[0] is not None:
					total_sum += val[0]
					
			yield(address, (total_sum, smartcon))	
		except:
			pass

	def mapper2(self, address, total_recieved_and_smartcon):
		yield(None, (address, total_recieved_and_smartcon))

	def combiner2(self, _, values):
		top_ten_totals = sorted(values, key=lambda val: val[1][0], reverse=True)[10:]
		yield(None, top_ten_totals)

	def reducer2(self, _, values):
		try:
			top_ten_totals = sorted(values, key=lambda val: val[1][0], reverse=True)[10:]
			
			rank = 1	
			for row in top_ten_totals:
				yield("{} {} {} {}".format(rank, row[0], row[1][0], row[1][1]))
				rank += 1
		except:
			pass
		
	def steps(self):
	    return [MRStep(mapper=self.mapper,
	                   combiner=self.combiner,
			   reducer=self.reducer),
	            MRStep(mapper=self.mapper2,
	                   combiner=self.combiner2,
	                   reducer=self.reducer2)]

if __name__ == '__main__':
	B.run()
