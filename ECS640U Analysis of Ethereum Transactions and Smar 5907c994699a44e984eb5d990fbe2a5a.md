# ECS640U Analysis of Ethereum Transactions and Smart Contracts Coursework

## Toby Wynne-Mellor

### **1702471810**

Grade 40/40 100%

# Part A. Time Analysis (20%)

## Job ID

[http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_2379/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_2379/)

## Explaination

I completed this job using Spark. To start, I  instantiated an RDD called 'lines' with the transaction dataset from HDFS. I filtered the dataset for malformed entries and mapped the data to get the month and year of each transaction as well as the value. I then used the `reduceByKey()` spark function to sum the values and number of values before using these to calculate the average and total number of transactions per month using the spark `map()` function. At the end of the script I printed the `applicationId` and the RDD results in CSV format via the `collect()` function. I took the result CSV file and imported it into to excel where I was able to create a plot in order to make sense of the data.

From the plots we can see that the number of transactions per month since Ethereum's inception gradually increased for the first two years upto around 2.5 million before mid 2017 where the number increased dramatically to reach its peak in Janurary 2018 at nearly 35 million transactions in one month. From there it has decreased slightly but seems to be tending upwards since Feburary 2019. 

The peak seems to be around the time where the price also peaked which would explain the high traffic.

![ECS640U%20Analysis%20of%20Ethereum%20Transactions%20and%20Smar%205907c994699a44e984eb5d990fbe2a5a/Chart_A_transactions_per_month.png](ECS640U%20Analysis%20of%20Ethereum%20Transactions%20and%20Smar%205907c994699a44e984eb5d990fbe2a5a/Chart_A_transactions_per_month.png)

The average transaction amount per month started very high at nearly 500 million Wei in August 2015 before dramatically declining to less than 100 million the month after. Now the transaction amount is consistently far lower than the first 2 years of trading. I would guess that this is because people are using the currency more on a day to day basis as the currency matures and the number of decentralised applications increases as well as the stigma of using crypto currency is broken down.

![ECS640U%20Analysis%20of%20Ethereum%20Transactions%20and%20Smar%205907c994699a44e984eb5d990fbe2a5a/Chart_A_Average_Transaction_Amount_per_month.png](ECS640U%20Analysis%20of%20Ethereum%20Transactions%20and%20Smar%205907c994699a44e984eb5d990fbe2a5a/Chart_A_Average_Transaction_Amount_per_month.png)

Amount Measured in Ether

# Part B  - Top Ten Most Popular Services (20%)

## Job IDs

### Step 1

- [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_8148/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_8148/)

### Step 2

- [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_8248/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_8248/)

## Explaination

I wrote a 2 step MapReduce job to calculate the top ten most popular smart contracts by Wei received. I needed to join the transaction data with the contracts data to determine the smart contract that receives the most Ether.

### Step 1

The first step does a repartition join to yield the total Wei received for each smart contract address.

I started by writing the mapper that begins the repartition join by receiving lines from the transactions and contracts datasets and labels each line as `"contract"`  and `"transaction"`  according to the number of columns in each line before yielding `(address, (label, value))`. The value is `1` for contracts but the actual transaction value in Wei for transactions.

The combiner receives the `address` as the key and a list of key -value pairs  `(label, value)` and sums the values by label. So the count of contracts and the sum of transaction values are calculated for the subset of received values (as we're in the combiner).

The reducer does the same as the combiner to collect the total value for all transactions to each address and yields `(address, total_received)` if the address is present in the contracts data to complete the join (i.e. if "contract" is found as one of the labels for the address in the list of values.). 

### Step 2

The second step receives the total Wei received for each smart contract address and calculates the top ten smart contracts by Wei received.

The mapper yields `(None, (address, total_received))` so that the combiner has access to all addresses at once (from the split of the data that the mapper and combiner are assigned).

The combiner sorts the addresses in reverse by `total_received` and yields each of the top 10 addresses with their `total_received` value.

The reducer does the same as the combiner but only deals with the 10 key-value pairs from each of the combiners. The top ten contracts are yielded with a rank in CSV format.

## Result

```bash
Rank, Address,                                    Total Transacted
1,    0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444, 84155100809965865822726776
2,    0xfa52274dd61e1643d2205169732f29114bc240b3, 45787484483189352986478805
3,    0x7727e5113d1d161373623e5f49fd568b4f543a9e, 45620624001350712557268573
4,    0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef, 43170356092262468919298969
5,    0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8, 27068921582019542499882877
6,    0xbfc39b6f805a9e40e77291aff27aee3c96915bdd, 21104195138093660050000000
7,    0xe94b04a0fed112f3664e45adb2b8915693dd5ff3, 15562398956802112254719409
8,    0xbb9bc244d798123fde783fcc1c72d3bb8c189413, 11983608729202893846818681
9,    0xabbb6bebfa05aa13e908eaa492bd7a8343760477, 11706457177940895521770404
10,   0x341e790174e3a4d35b65fdc067b6b5634a61caea, 8379000751917755624057500
```

I used [etherscan.io](http://etherscan.io) to determine the smart contract of each of the top ten addresses.  They are predominantly made up of crypto exchanges and payment intermediaries.

```bash
Rank, ContractName
1, ReplaySafeSplit -> split payments for security
2, Kraken -> exchange
3, Bitfinex -> exchange
4, Poloniex -> exchange
5, Gemini -> exchange
6, Poloniex -> exchange
7, ReplaySafeSplit
8, DAO -> venture capital firm
9, ReplaySafeSplit
10, ReplaySafeSplit
```

# Part C - Top 10 Miners By Size of Blocks Mined

## Job IDs

### Step 1

- [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_8284/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_8284/)

### Step 2

- [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_8287/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_8287/)

Evaluate the top 10 miners by the size of the blocks mined. This is simpler as it does not require a join. You will first have to aggregate blocks to see how much each miner has been involved in. You will want to aggregate size for addresses in the miner field.

## Explanation

I wrote a 2 step job to take the blocks data and determine the top 10 miners by size of blocks mined.

### Step 1

The mapper finds the miner and size of block from the CSV row and yields `(miner, size)`. The combiner and reducer are identical, they sum the sizes of all blocks mined for each miner and yield `(miner, total_size)`.

### Step 2

Much like step 2 for part B the mapper yields `(None, (miner, total_size))` so that the combiner has access to all miners at once.

The combiner sorted the miners in reverse order and yields the first 10 miners. The reducer does the same and yields the rank, miner and total_size of blocks mined in CSV format.

## Result

```bash
rank, miner,                                      size
1,    0xea674fdde714fd979de3edf0f56aa9716b898ec8, 23989401188
2,    0x829bd824b016326a401d083b33d092293333a830, 15010222714
3,    0x5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c, 13978859941
4,    0x52bc44d5378309ee2abf1539bf71de1b7d7be3b5, 10998145387
5,    0xb2930b35844a230f00e51431acae96fe543a0347, 7842595276
6,    0x2a65aca4d5fc5b5c859090a6c34d164135398226, 3628875680
7,    0x4bb96091ee9d802ed039c4d1a5f6216f90f81b01, 1221833144
8,    0xf3b9d2c81f2b24b0fa0acaaa865b7d9ced5fc2fb, 1152472379
9,    0x1e9939daaad6924ad004c2560e90804164900341, 1080301927
10,   0x61c808d82a3ac53231750dadc13c777b59310bd9, 692942577
```

# Part D

## Popular Scams (15/50)

### Job  IDs

### Daily

[http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_10200/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_10200/)

### Monthly

[http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_10237/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_10237/)

### Explanation

Utilising the scams dataset, I wrote a MapReduce job to calculate the amount of Wei each type of scam received.

I used a mapper_init function to prepare the scam dataset before the transactions dataset is processed. I read the scam.json file that I included in the Hadoop command via `--file scams.json`. Using the scam data, I populated the three member variables of the class `scams`, `addresses` and `categories`. `scams` was assigned the `["result"]` field of the full json file so that I can reference different parts of the scams later in the program in O(n) time due to it being a dictionary. `addresses` was assigned a dictionary with a key for every mentioned address in each scam with it's value set to the scam_id. This enables me later to find the scam_id for any address and then find the scam information using the scam_id and the `scams` dictionary. `categories` is a list used to store every unique category in the scams dataset.

The mapper function takes each line from the transaction dataset and performs a replication join with the scams dataset in memory. It checks if the address in the transaction exists in the `addresses` dictionary, if it does then it will yield all the scams (with their category and value) associated with the address in the form `(date, (category, value))`.

The combiner sums the values for each category per date and yields for each unique category `(date, (category, summed_value))`.

The reducer does the same as the combiner and yields the amount received by each scam category for each date in CSV format.

I ran this twice, once with monthly dates and once with daily dates.

![ECS640U%20Analysis%20of%20Ethereum%20Transactions%20and%20Smar%205907c994699a44e984eb5d990fbe2a5a/Screenshot_2020-12-16_at_01.47.17.png](ECS640U%20Analysis%20of%20Ethereum%20Transactions%20and%20Smar%205907c994699a44e984eb5d990fbe2a5a/Screenshot_2020-12-16_at_01.47.17.png)

Monthly dates

### Result

Using Excel I was able to create several charts to represent the data. To answer the broad question of which scam was the most lucrative of all time, I generated a bar chart using the total amount of Wei received by each scam category for the entire transaction dataset. Phishing has been the most lucrative scam, generating over 27.5 Thousand Trillion Wei which equates to about 16 million USD, Scamming comes second with almost half as many at 15 Thousand Trillion Wei or 7 million USD. Fake ICO 1 Thousand Trillion Wei or 500k USD.

![ECS640U%20Analysis%20of%20Ethereum%20Transactions%20and%20Smar%205907c994699a44e984eb5d990fbe2a5a/D_scams_barchart.png](ECS640U%20Analysis%20of%20Ethereum%20Transactions%20and%20Smar%205907c994699a44e984eb5d990fbe2a5a/D_scams_barchart.png)

Amount generated by different scams in Wei

Phishing makes up 62% of known scams, Scamming makes up 35% and Fake ICOs make up 3%. I would think Fake ICOs are much harder to pull off which is why they make up a small amount of scams.

![ECS640U%20Analysis%20of%20Ethereum%20Transactions%20and%20Smar%205907c994699a44e984eb5d990fbe2a5a/D_scams_piechart.png](ECS640U%20Analysis%20of%20Ethereum%20Transactions%20and%20Smar%205907c994699a44e984eb5d990fbe2a5a/D_scams_piechart.png)

Pie chart of distribution of revenue from scams for all transactions in Ethereum up to June 2019

The most lucrative scam can be seen to change over time. Looking at the bar chart of value received by different scams per month, it is clear that Fake ICOs was quite a successful scam in the early days of Ethereum but from the end of 2017 Fake ICOs stop generating any revenue. Successful scams can be seen where there are spikes in the chart. In September 2018, general scamming was most lucrative. In September 2017, Fake ICOs were most lucrative. From this chart, Phishing doesn't seem to be so lucrative.

![ECS640U%20Analysis%20of%20Ethereum%20Transactions%20and%20Smar%205907c994699a44e984eb5d990fbe2a5a/scams_monthly.png](ECS640U%20Analysis%20of%20Ethereum%20Transactions%20and%20Smar%205907c994699a44e984eb5d990fbe2a5a/scams_monthly.png)

Monthly amount received by recognised scams in Wei

When you look at the data from a daily perspective you can see new patterns emerge. Firstly, phishing is more prevalent and can be seen to generate more revenue more frequently than any other scam due to its wide and tall cluster of points on the left side. While Fake ICOs made a lot in September 2017 the frequency of these scams is far lower - probably due to the sophistication required to execute such scams. As time progresses, all revenue from scams declines and the most common form of scam is general scamming, Fake ICOs are nearly non-existent.

![ECS640U%20Analysis%20of%20Ethereum%20Transactions%20and%20Smar%205907c994699a44e984eb5d990fbe2a5a/scams_daily.png](ECS640U%20Analysis%20of%20Ethereum%20Transactions%20and%20Smar%205907c994699a44e984eb5d990fbe2a5a/scams_daily.png)

Daily amount received by recognised scams in Wei

## Gas Guzzlers (10/50)

### Job ID

[http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_9003/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1607539937312_9003/)

### Explanation

I wrote a simple mapReduce program to calculate the date, average gas and average gas price for each day in the transaction dataset.

The mapper yields the date, gas requirement and gas price for each transaction in the form `(date, ("gas", gas))` and `(date, ("price", gas_price))`.

The combiner sums each gas value and each price value to get the `gas_total` and `price_total`. It also stores a count of each gas and price value that it receives. The combiner yields the date, `gas_total`, `gas_count`, `price_total` and `price_count` in the form  `(date, ("gas", gas_total, gas_count))` and `(date, ("price", price_total, price_count))`.

The reducer also counts and sums the gas and price values for each date. Using these totals and counts the reducer yields the average gas used and the average gas price each day in CSV format.

`yield("{},{},{}".format(date, gas_total / gas_count, price_total / price_count,None)`

### Result

It can be seen that the gas price has remained relatively consistent at around 20 - 50 Wei. There are however quite a few outliers. For instance, at Ethereum's inception the gas price fluctuated a lot from 100 - 600 Wei. Another outlier is on 8th December 2016 the average Wei price reached its peak of 939.56.

![ECS640U%20Analysis%20of%20Ethereum%20Transactions%20and%20Smar%205907c994699a44e984eb5d990fbe2a5a/avg_daily_gas_price.png](ECS640U%20Analysis%20of%20Ethereum%20Transactions%20and%20Smar%205907c994699a44e984eb5d990fbe2a5a/avg_daily_gas_price.png)

It seems that contracts haven't become more complicated as the average daily gas requirement is consistently fluctuating at around 100,000 - 300,000. There are however some outliers like 28th April 2018 where the gas requirement shot up to 610,761. There is less volatility in gas price today when compared to Ethereum's inception.

![ECS640U%20Analysis%20of%20Ethereum%20Transactions%20and%20Smar%205907c994699a44e984eb5d990fbe2a5a/avg_daily_gas_requirement.png](ECS640U%20Analysis%20of%20Ethereum%20Transactions%20and%20Smar%205907c994699a44e984eb5d990fbe2a5a/avg_daily_gas_requirement.png)

While daily transactions does increase (as seen in part A) the gas requirement does not, indicating that people are generally using the simple contracts so the average gas requirement remains consistently low. This correlates with part B since the most popular smart contracts are mainly exchanges which are typically not gas intensive. 

## Price Forecasting (25/50)

### Job ID

[http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1608030573468_0158/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1608030573468_0158/)

Ethereum Price Data Set: [https://uk.investing.com/crypto/ethereum/historical-data](https://uk.investing.com/crypto/ethereum/historical-data)

### Explanation

I intended to conduct my price forecast in the following two parts:

1. Using the average daily gas price from the gas guzzlers exercise and the daily average value and number of transactions from the time analysis exercise and Ethereum price dataset to June 2019 to build an accurate model up to June 2019. Due to the dataset only going up to June 2019, I can only use these additional features up to that point in time.
2. Using just the price dataset (full dataset - inception to now), see how accurate I can build a model to forecast  past June 2019.

These two steps will allow me to compare the difference between the two models and whether the additional features improves accuracy.

I found a dataset from [uk.investing.com](http://uk.investing.com) for the daily prices of Ethereum from its inception to now. I downloaded it and used excel to make a copy that contains a subset of the rows that goes from inception up to the end of June 2019. I used my previous analysis from the gas guzzlers and time analysis exercises to enrich my price dataset. 

I used Spark with the Spark ML library to complete this task. After I had prepared the data (price data, gas data, transaction amount and count data), I uploaded it to HDFS and loaded all of it into my spark program. To further prepare the data for Spark, I casted the useful fields in the data to the appropriate data type - i.e. price was cast to `float` and timestamp was cast to a `bigint`.

### Creating The High Feature Model

The  high feature model is the price data augmented with the gas and transaction volume data from previous tasks. Since the gas and transaction data only goes up to June 2019, I couldn't use this model to predict events after that time.

I did a left outer join on the price data with transaction volume data using the shared key `Date`. I repeated this for gas data.

In order to do Linear Regression in Pyspark you need to define the features and the label of your data set. One way to do this is to assemble a vector from the feature columns in your data and add it as its own "feature" column. You can do this using a VectorAssembler and passing in the columns you want to use as features. I made sure not to include the price column here so that the model needs to guess it as price is our label.

```python
from pyspark.ml.feature import VectorAssembler

# define features in your dataset for model to use
feature_columns = ['Timestamp', 'Volume', 'avg_gas_price', 'avg_gas', 'Avg', 'Count']

# give the assembler the features column
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

# transform data so that it now includes the features column
new_data = assembler.setHandleInvalid("skip").transform(data.na.drop())
```

I then randomly split the data set 70% for training and 30% for testing. The LinearRegression algorithm from Pyspark is now initialised with the features column and label column as well as some parameters before the `fit` function is called on it.

```python
from pyspark.ml.regression import LinearRegression

train, test = data.randomSplit([0.7, 0.3])

algo = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, featuresCol="features", labelCol="Price")

model = algo.fit(train)
```

The model is now trained and I can get predictions for any dataset with matching schema.

```python
# new DataFrame with additional prediction column
predictions = model.transform(test)
```

### Creating The Low Feature Model

I made another model for the time past June 2019 that only uses the timestamp and volume features from the price data to make predictions.

### Results

The high feature model up to June 2019 is far more accurate than the low feature model. You can see the trend of the predictions roughly follow the actual price around where the graph spikes in Jan 2018. The model clearly isn't well defined at the beginning where it gives negative numbers for the price which is impossible - ideally these could be clamped to 0. It seems that the combination of the trends average gas price, average gas requirement, average transaction value and transaction count in addition to the timestamp make for a decent guess of the Ethereum price. However, when you remove these additional features, the performance falls dramatically. The low feature model barely resembles the Ethereum Price. The low feature model does seem to show some fluctuation around where the price is volatile in the second half of the time range but apart from that the predictions are very poor. The low feature model predicts that the price is consistently around $300 from inception to September 2019 which is hugely misleading of the actual price trend as it should be peaking at $1200 in Jan 2018.

![ECS640U%20Analysis%20of%20Ethereum%20Transactions%20and%20Smar%205907c994699a44e984eb5d990fbe2a5a/ethereum_price_plot.png](ECS640U%20Analysis%20of%20Ethereum%20Transactions%20and%20Smar%205907c994699a44e984eb5d990fbe2a5a/ethereum_price_plot.png)

The high feature model has a mean absolute error of ~62.5 on the prediction compared to the low feature model's ~113 for the same time range, demonstrating that the low feature model is far worse. Unfortunately, I was unable to accurately predict the price of Ethereum past June 2019 due to not having all the features past that date but given the accuracy of the pre June 2019 predictions and the demonstrated benefit of having the additional features, I believe my model to be a good estimator for the price of Ethereum.

### Stats

- Test High Feature up to June 2019 Model

    ```bash
    Mean Absolute Error: 73.1737507258
    Root Mean Squared Error: 108.69651288
    R Squared: 0.733330363456
    ```

- Test All Time Low Feature Model

    ```bash
    Mean Absolute Error: 119.823114214
    Root Mean Squared Error: 163.779536937
    R Squared: 0.0294845711487
    ```

- High Feature Model up to June 2019 Prediction

    ```bash
    Mean Absolute Error: 62.4799538896
    Root Mean Squared Error: 89.2163198779
    R Squared: 0.780634209898
    ```

- Low Feature All Time Prediction

    ```bash
    Mean Absolute Error: 112.377336063
    Root Mean Squared Error: 152.672265817
    R Squared: 0.0419718310326
    ```

- Low Feature Model up to June 2019 Prediction

    ```python
    Mean Absolute Error: 112.797592607
    Root Mean Squared Error: 152.551577215
    R Squared: 0.0434858896574
    ```