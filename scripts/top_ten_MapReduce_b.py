""" 
	PART B
"""

from mrjob.job import MRJob
from mrjob.step import MRStep

class B(MRJob):

	def mapper_repartition_aggregate(self, _, row):
		try:
			fields = row.split(',')
			if len(fields) == 5:
				# contact (address, ("contract", 1))
				yield(fields[0], ("contract", 1)) 
			elif len(fields) == 7:
				# transaction (address, ("transaction", value))
				value = int(fields[3])
				if value > 0:
					yield(fields[2], ("transaction", value))
			else:
				pass
		except:
			pass	

	# recieve: address, [("contract", 1), ("transaction", 1234567), ...]
	def combiner_repartition_init(self, address, values):
		total_recieved = 0

		# {"contract": count, "transaction": total_value}
		contracts_and_transactions = {}

		for value in values:
			if value[0] in contracts_and_transactions:
				contracts_and_transactions[value[0]] += value[1]
			else:
				contracts_and_transactions[value[0]] = value[1]

		for con_or_tran in contracts_and_transactions:
			yield(address, (con_or_tran, contracts_and_transactions[con_or_tran]))	

	# recieve: address, [("contract", 521), ("transaction", 12341231), ...]
	def reducer_repartition_join(self, address, values):
		# {"contract": count, "transaction": total_value}
		contracts_and_transactions = {}

		for value in values:
			if not value[1] > 0:
				continue

			if value[0] in contracts_and_transactions:
				contracts_and_transactions[value[0]] += value[1]
			else:
				contracts_and_transactions[value[0]] = value[1]

		# check that both transactions and contracts are present in value array
		if "transaction" in contracts_and_transactions:
			if "contract" in contracts_and_transactions:
				yield(address, contracts_and_transactions["transaction"])

	# recieve: address, [("contract", 521), ("transaction", 12341231), ...]
	#def reducer_repartition_join(self, address, values):
	#	in_contracts = False
	#	total_transacted = 0
	#	
	#	for value in values:
	#		if value[1] > 0:
	#			if value[0] == "transaction":
	#				total_transacted += value[1]
	#			elif value[0] == "contract" and in_contracts is False:
	#				in_contracts = True
	#	
	#	if in_contracts is True:
	#		yield(address, total_transacted)
						
	
	#def mapper_top_ten_init(self, address, total_recieved):
	def mapper_top_ten_init(self, _, row):
		fields = row.split('\t')	
		yield(None, (fields[0], fields[1]))

	# recieve: None, [(address, total_recieved), ...]
	def combiner_top_ten(self, _, values):
		# sort by total recieved
		top_ten_totals = sorted(values, key=lambda val: val[1], reverse=True)

		i = 0
		for value in top_ten_totals:
			yield(None, value) 
			i += 1
			if i >= 10:
				break
		

	# recieve: None, [(address, total_recieved), ...]
	def reducer_top_ten(self, _, values):
		top_ten_totals = sorted(values, key=lambda val: val[1], reverse=True)
	
		rank = 0
		
		yield("{}, {}, {}".format("rank", "address", "total transacted"))
		for row in top_ten_totals:
			rank += 1
			yield("{}, {}, {}".format(rank, row[0], row[1]))
			if rank >= 10:
				break

	def steps(self):
		#return [MRStep(mapper=self.mapper_repartition_aggregate, combiner=self.combiner_repartition_init, reducer=self.reducer_repartition_join), MRStep(mapper=self.mapper_top_ten_init, combiner=self.combiner_top_ten, reducer=self.reducer_top_ten)]
		return [MRStep(mapper=self.mapper_top_ten_init, combiner=self.combiner_top_ten, reducer=self.reducer_top_ten)]

if __name__ == '__main__':
	B.JOBCONF = {'mapreduce.job.reduces': '4'}
	B.run()
