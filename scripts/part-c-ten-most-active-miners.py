""" 
	PART C

	Evaluate the top 10 miners by the size of the blocks mined. This is simpler as it does not require a join. You will first have to aggregate blocks to see how much each miner has been involved in. You will want to aggregate size for addresses in the miner field. This will be similar to the wordcount that we saw in Lab 1 and Lab 2. You can add each value from the reducer to a list and then sort the list to obtain the most active miners.

"""

from mrjob.job import MRJob
from mrjob.step import MRStep

class C(MRJob):

	def mapper(self, _, row):
		try:
			fields = row.split(',')
			miner = fields[2]
			size = int(fields[4])
			yield(miner, size)
		except:
			pass

	# recieve: (miner, [size, ...])
	def combiner(self, miner, sizes):
		total_size = sum(sizes)
		yield(miner, total_size)

	# recieve: (miner, [size, ...])
	def reducer(self, miner, sizes):
		total_size = sum(sizes)
		yield(miner, total_size)

	def mapper2(self, miner, total_size):
		yield(None, (miner, total_size))

	# recieve: None, [(miner, total_size), ...]
	def combiner2(self, _, miner_size):
		top_miners = sorted(miner_size, reverse=True, key=lambda tup: tup[1])
		i = 0
		for miner in top_miners:
			yield(None, miner)
			i += 1
			if i >= 10:
				break

	# recieve: None, [(miner, total_size), ...]
	def reducer2(self, _, miner_size):
		top_miners = sorted(miner_size, reverse=True, key=lambda tup: tup[1])
		i = 0
		yield("rank,miner, size", None)
		for miner in top_miners:
			i += 1
			yield("{},{},{}".format(i, miner[0], miner[1]), None)
			if i >= 10:
				break

	def steps(self):
		return [MRStep(mapper=self.mapper, 
									combiner=self.combiner, 
									reducer=self.reducer),
					 MRStep(mapper=self.mapper2, 
					 				combiner=self.combiner2, 
									reducer=self.reducer2)]

if __name__ == '__main__':
	C.JOBCONF = {'mapreduce.job.reduces': '4'}
	C.run()
