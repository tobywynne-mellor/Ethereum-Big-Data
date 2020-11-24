import pyspark
import re
from datetime import datetime

'''
1. Create a bar plot showing the number of transactions occurring every month between the start and end of the dataset.

2. Create a bar plot showing the average value of transaction in each month between the start and end of the dataset.

Note: As the dataset spans multiple years and you are aggregating together all transactions in the same month, make sure to include the year in your analysis.

TRANSACTIONS
+------------+--------------------+--------------------+-------------------+------+-----------+---------------+
|block_number|        from_address|          to_address|              value|   gas|  gas_price|block_timestamp|
+------------+--------------------+--------------------+-------------------+------+-----------+---------------+
|     6638809|0x0b6081d38878616...|0x412270b1f0f3884...| 240648550000000000| 21000| 5000000000|     1541290680|


2413528,0x12962485aa2ff2829e5683f05d1dfe98093a6687,0xe94b04a0fed112f3664e45adb2b8915693dd5ff3,2125017790000000000,132650,23187348167,1476088707
'''

sc = pyspark.SparkContext()

#we will use this function later in our filter transformation
def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields)!=7:
            return False
	float(fields[3])
	float(fields[6])	
        return True
    except:
        return False

def mapper(line):
    [_, _, _, value, _, _, block_timestamp] = line.split(',')
    date = datetime.fromtimestamp(float(block_timestamp)).strftime('%m/%Y')
    val = float(value)
    return (date, (val, 1))

lines = sc.textFile("/data/ethereum/transactions/")

clean_lines = lines.filter(is_good_line)

# (date, (val, 1))
dates_and_values = clean_lines.map(mapper)

# reducebykey (date, (sum(vals), count(vals)))
# map (date, avg, count)
transactions_per_month = dates_and_values\
.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))\
.map(lambda l: (l[0], l[1][0]/l[1][1], l[1][1]))

result = transactions_per_month.persist()

print("ApplicationId: ", sc.applicationId)

print("Date, Avg, Count")
for row in result.collect():
    print("{}, {}, {}".format(row[0], row[1], row[2]))
