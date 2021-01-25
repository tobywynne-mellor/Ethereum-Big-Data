import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

print("ApplicationId: {}".format(sc.applicationId))

# load data 
upto_june_data = spark.read.csv('/user/twm31/ml/Ethereum_prices_upto_june19.csv', header=True, inferSchema=True)
all_data = spark.read.csv('/user/twm31/ml/ethereum_prices.csv', header=True, inferSchema=True)
daily_value_transaction_data = spark.read.csv('ml/D_part_A_daily_transactions.csv', header=True, inferSchema=True)
gas_data = spark.read.csv('ml/D_gas_guzzler_result.csv', header=True, inferSchema=True)

# set the datatypes for non strings

upto_june_data = upto_june_data.withColumn("Timestamp", upto_june_data.Timestamp.cast("bigint"))
upto_june_data = upto_june_data.withColumn("Open", upto_june_data.Open.cast("float"))
upto_june_data = upto_june_data.withColumn("High", upto_june_data.High.cast("float"))
upto_june_data = upto_june_data.withColumn("Low", upto_june_data.Low.cast("float"))
upto_june_data = upto_june_data.withColumn("Volume", upto_june_data.Volume.cast("float"))
upto_june_data = upto_june_data.withColumn("Price", upto_june_data.Price.cast("float"))

all_data = all_data.withColumn("Timestamp", all_data.Timestamp.cast("bigint"))
all_data = all_data.withColumn("Open", all_data.Open.cast("float"))
all_data = all_data.withColumn("High", all_data.High.cast("float"))
all_data = all_data.withColumn("Low", all_data.Low.cast("float"))
all_data = all_data.withColumn("Vol", all_data.Vol.cast("float"))
all_data = all_data.withColumn("Price", all_data.Price.cast("float"))

daily_value_transaction_data = daily_value_transaction_data.withColumn("Avg", daily_value_transaction_data.Avg.cast("float"))
daily_value_transaction_data = daily_value_transaction_data.withColumn("Count", daily_value_transaction_data.Count.cast("bigint"))

gas_data = gas_data.withColumn("avg_gas", gas_data.avg_gas.cast("float"))
gas_data = gas_data.withColumn("avg_gas_price", gas_data.avg_gas_price.cast("float"))

# join the daily average transaction value and count with the price data
upto_june_data = upto_june_data.join(daily_value_transaction_data, on="Date", how="leftouter")

print("upto_june_data before join")
gas_data.show()

# join with gas data
upto_june_data = upto_june_data.join(gas_data, on="Date", how="leftouter")

print("upto_june_data after join with gas and transactions")
upto_june_data.show()

feature_columns = ['Timestamp', 'Volume', 'avg_gas_price', 'avg_gas', 'Avg', 'Count'] # omit price and date columns 

#print("feature columns: {}".format(feature_columns))

assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

# for after jun 2019 as don't have gas and transaction data
assembler2 = VectorAssembler(inputCols=["Timestamp", "Vol"], outputCol="features")

new_upto_june_data = assembler.setHandleInvalid("skip").transform(upto_june_data.na.drop())
new_all_data = assembler2.setHandleInvalid("skip").transform(all_data.na.drop())
new_upto_june_data_low = assembler2.setHandleInvalid("skip").transform(all_data.na.drop()) 
#print("before remove nulls - before_jun19 rows: {}, after_jun19 rows:{}".format(new_upto_june_data.count(), new_after_june_data.count()))
#new_upto_june_data.na.drop()
#new_after_june_data.na.drop()
#print("after remove nulls - before_jun19 rows: {}, after_jun19 rows:{}".format(new_upto_june_data.count(), new_after_june_data.count()))

print("add feature column from {}".format(str(feature_columns)))
new_upto_june_data.show()

# train on the upto june 2019 dataset
train, test = new_upto_june_data.randomSplit([0.7, 0.3])
train2, test2 = new_all_data.randomSplit([0.7, 0.3])

algo = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, featuresCol="features", labelCol="Price")

high_feature_model = algo.fit(train)
low_feature_model = algo.fit(train2)

print("Predictions From Test1 Sample")

predictions = high_feature_model.transform(test)
predictions.select("Date", "Volume", "Avg", "Count", "avg_gas", "avg_gas_price", "Price", "prediction").show()
predictions = predictions.drop("features", "Timestamp", "Open", "High", "Low")

evaluation_summary = high_feature_model.evaluate(test)
print("Mean Absolute Error: {}".format(evaluation_summary.meanAbsoluteError))
print("Root Mean Squared Error: {}".format(evaluation_summary.rootMeanSquaredError))
print("R Squared: {}".format(evaluation_summary.r2))


print("Predictions From Test2 Sample")

predictions = low_feature_model.transform(test2)
predictions.select("Date", "Vol", "Price", "prediction").show()
predictions = predictions.drop("features", "Timestamp", "Open", "High", "Low")

evaluation_summary = low_feature_model.evaluate(test2)
print("Mean Absolute Error: {}".format(evaluation_summary.meanAbsoluteError))
print("Root Mean Squared Error: {}".format(evaluation_summary.rootMeanSquaredError))
print("R Squared: {}".format(evaluation_summary.r2))



print("Predictions From All Data Low Features")

predictions2 = low_feature_model.transform(new_all_data)
predictions2.select("Date", "Vol", "Price", "prediction").show()
predictions2 = predictions2.drop("features", "Timestamp", "Open", "High", "Low")
outputdir = "/user/twm31/ml/out/all_data_low_feature_prediction"
predictions2.write.format("csv").save(outputdir, header= 'true')
print("SAVED: Predictions From All Data low feature to {}".format(outputdir))

evaluation_summary2 = low_feature_model.evaluate(new_all_data)
print("Mean Absolute Error: {}".format(evaluation_summary2.meanAbsoluteError))
print("Root Mean Squared Error: {}".format(evaluation_summary2.rootMeanSquaredError))
print("R Squared: {}".format(evaluation_summary2.r2))

print("Predictions From Upto June 19 High Features")

predictions2 = high_feature_model.transform(new_upto_june_data)
predictions2.select("Date", "Volume", "Price", "prediction").show()
predictions2 = predictions2.drop("features", "Timestamp", "Open", "High", "Low")
outputdir = "/user/twm31/ml/out/upto_june_19_high_feature_prediction"
predictions2.write.format("csv").save(outputdir, header= 'true')
print("SAVED: Predictions From Upto to june 19 data high feature to {}".format(outputdir))

evaluation_summary2 = high_feature_model.evaluate(new_upto_june_data)
print("Mean Absolute Error: {}".format(evaluation_summary2.meanAbsoluteError))
print("Root Mean Squared Error: {}".format(evaluation_summary2.rootMeanSquaredError))
print("R Squared: {}".format(evaluation_summary2.r2))

print("Predictions From Upto June 19 Low Features")

predictions2 = low_feature_model.transform(new_upto_june_data_low)
predictions2.select("Date", "Vol", "Price", "prediction").show()
#predictions2 = predictions2.drop("features", "Timestamp", "Open", "High", "Low")
#outputdir = "/user/twm31/ml/out/upto_june_19_high_feature_prediction"
#predictions2.write.format("csv").save(outputdir, header= 'true')
#print("SAVED: Predictions From Upto to june 19 data high feature to {}".format(outputdir))

evaluation_summary2 = low_feature_model.evaluate(new_upto_june_data_low)
print("Mean Absolute Error: {}".format(evaluation_summary2.meanAbsoluteError))
print("Root Mean Squared Error: {}".format(evaluation_summary2.rootMeanSquaredError))
print("R Squared: {}".format(evaluation_summary2.r2))

