""" 
	PART D: Popular Scams
	yield date, category, value, status, scam_id, url	
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.util import log_to_stream, log_to_null
import sys
import logging
import json
from datetime import datetime

log = logging.getLogger(__name__)


class PopularScams(MRJob):
	scams = {}
	addresses = {} 
	categories = [] 

	def set_up_logging(self, quiet=False, verbose=False, stream=None):
        	log_to_stream(name='mrjob', debug=verbose, stream=stream)
        	log_to_stream(name='__main__', debug=verbose, stream=stream)

	def mapper_init(self):
		with open('scams.json') as scam_file:
			scam_json = json.loads(scam_file.readline())	
			scams_data = scam_json["result"]
			self.scams = scams_data
		scam_file.close()
		
		testaddr = ""
		# populate self.addresses with address -> scam id mapping
		for scam_id in self.scams:
			for address in self.scams[scam_id]["addresses"]:
				if address in self.addresses:
					self.addresses[address].append(scam_id)
				else:
					self.addresses[address] = [scam_id]
				testaddr = address

			if self.scams[scam_id]["category"] not in self.categories:
				self.categories.append(self.scams[scam_id]["category"])

		#log.info("categories")
		#log.info(self.categories)

		#for addr in self.addresses:
		#	log.info(self.addresses[addr])
		#	log.info("addr: {}, associated_scams: {}".format(addr, len(self.addresses[addr])))
		#log.info(testaddr)
		#log.info(self.addresses[testaddr][0])
		#log.info(self.scams[self.addresses[testaddr][0]])	

		#log.info("addresses")
		#log.info(self.addresses)
		
		
								
	def mapper(self, _, row):
		try:
			fields = row.split(',')
			address = fields[2]
			value = int(fields[3])
			timestamp = int(fields[6])
			date = datetime.fromtimestamp(timestamp).strftime("%b/%y")

			if address in self.addresses:
				scam_ids = self.addresses[address]
				for scam_id in scam_ids:
					scam = self.scams[scam_id]
					category = scam['category']
					status = scam['status']	
					url = scam['url']
					yield(date, (category, value))
					#log.info("found {} in addresses".format(address))
		except Exception as e:
			log.info("failing the mapper")
			log.info(e)

	# recieve: date, [(category, value),...]
	def combiner(self, date, values):
		try:

			#values = [x for x in values]
			#log.info("combiner {} - value_count: {}, values: {}".format(date, len(values), values))
			categories = {}
			
			for scam in values:
				if scam[0] in categories:
					categories[scam[0]] += scam[1]
				else:
					categories[scam[0]] = scam[1]

			#log.info(categories)

			for category, summed_value in categories.items():
				#log.info("yielding {}, ({},{})".format(date, category, summed_value))
				yield(date, (category, summed_value))

		except Exception as e:
			log.info("Failed combiner")
			log.info(e)

	# recieve: date, [(category, value),...]
	def reducer(self, date, values):
		try:
			#log.info("reducer {} - values: {}".format(date, list(values)))

			categories = {}
			
			for scam in values:
				#log.info("scam: {}".format(scam))
				if scam[1] in categories:
					categories[scam[0]] += scam[1]
				else:
					categories[scam[0]] = scam[1]

			#log.info("categories: {}".format(categories))
			
			phishing = categories["Phishing"] if "Phishing" in categories else 0
			fake_ico = categories["Fake ICO"] if "Fake ICO" in categories else 0
			scamming = categories["Scamming"] if "Scamming" in categories else 0

			#yield("date, Phishing, Fake ICO, Scam, Scamming", None)
			yield("{},{},{},{}".format(date, phishing, fake_ico, scamming), None)		
		except Exception as e:
			log.info("failed reducer")
			log.info(e)


if __name__ == '__main__':
	PopularScams.JOBCONF = {'mapreduce.job.reduces': '4'}
	PopularScams.run()
