import pyspark
from datetime import datetime

'''
PART B

Evaluate the top 10 smart contracts by total Ether received. An outline of the subtasks required to extract this information is provided below, focusing on a MRJob based approach. This is, however, only one possibility, with several other viable ways of completing this assignment.

JOB 1 - INITIAL AGGREGATION
To workout which services are the most popular, you will first have to aggregate transactions to see how much each address within the user space has been involved in. You will want to aggregate value for addresses in the to_address field. This will be similar to the wordcount that we saw in Lab 1 and Lab 2.

JOB 2 - JOINING TRANSACTIONS/CONTRACTS AND FILTERING
Once you have obtained this aggregate of the transactions, the next step is to perform a repartition join between this aggregate and contracts (example here). You will want to join the to_address field from the output of Job 1 with the address field of contracts

Secondly, in the reducer, if the address for a given aggregate from Job 1 was not present within contracts this should be filtered out as it is a user address and not a smart contract.

JOB 3 - TOP TEN
Finally, the third job will take as input the now filtered address aggregates and sort these via a top ten reducer, utilising what you have learned from lab 4.

TRANSACTIONS
+------------+--------------------+--------------------+-------------------+------+-----------+---------------+
|block_number|        from_address|          to_address|              value|   gas|  gas_price|block_timestamp|
+------------+--------------------+--------------------+-------------------+------+-----------+---------------+
|     6638809|0x0b6081d38878616...|0x412270b1f0f3884...| 240648550000000000| 21000| 5000000000|     1541290680|


2413528,0x12962485aa2ff2829e5683f05d1dfe98093a6687,0xe94b04a0fed112f3664e45adb2b8915693dd5ff3,2125017790000000000,132650,23187348167,1476088707
'''

sc = pyspark.SparkContext()

def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields)!=7:
            return False
	str(fields[2])
        return True
    except:
        return False

def mapper(line):
    [_, _, to_address, _, _, _, _] = line.split(',')
    return (to_address, 1)

lines = sc.textFile("/data/ethereum/transactions/")

clean_lines = lines.filter(is_good_line)

# How many transactions per user (to_address)
# (to_address, 1)
addresses = clean_lines.map(mapper) 

# (to_address, (sum(addresses)))
addresses_count = addresses.reduceByKey(lambda a, b: a + b) 

result = addresses_count
#result = date_sum_count.map(lambda l: (l[0], str(l[1][0]/l[1][1]), str(l[1][1]))) 

print("ApplicationId: ", sc.applicationId)

print("User, Count")
for row in result.collect():
    print("{}, {}".format(row[0], row[1]))
