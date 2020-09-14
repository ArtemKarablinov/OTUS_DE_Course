# for this exercise you need to download `all_stocks_5yr.csv`
# from https://www.kaggle.com/camnugent/sandp500

from pyspark.sql import SparkSession

def pp(s):
    dec = "-" * 50
    print("\n\n{} {} {}".format(dec, s, dec))

if __name__ == '__main__':
    spark = SparkSession.builder.appName("my_hw_K").getOrCreate()
    sc = spark.sparkContext

    path_to_csv = "/Users/ruagup3/Downloads/all_stocks_5yr.csv"
    df = spark.read.csv(path_to_csv, header=True).rdd

    header = df.first()
    stocks = df.filter(lambda x: x != header)

    print("Header: ", header)


    pp("Max gain")

    # delete nan
    stocks = stocks.filter(lambda x: all(x[i] != '' for i in [1]))
    # convert to float
    stocks = stocks.map(lambda x: [float(y) if i in [1] else y for i, y in enumerate(x)])


    openPrice = stocks.map(lambda x: (x[-1], [x[1], x[0]]))

    minPrice = openPrice.reduceByKey(lambda a, b: a if a[0] < b[0] else b)
    maxPrice = openPrice.reduceByKey(lambda a, b: a if a[0] >= b[0] else b)

    fluctuation = (minPrice.join(maxPrice)
                   .map(lambda x: (x[0], x[1][1][0] - x[1][0][0], x[1][0][1], x[1][1][1]))
                   .sortBy(lambda x: -x[1])
                   )

    [print(s) for s in fluctuation.take(3)]

    pp("Daily gain")

    # delete nan
    stocks = stocks.filter(lambda x: all(x[i] != '' for i in [4]))
    # convert to float
    stocks = stocks.map(lambda x: [float(y) if i in [4] else y for i, y in enumerate(x)])

    closePrice = stocks.map(lambda x: (x[0], (x[-1], x[4])))
    dateIndex = stocks.map(lambda x: x[0]).distinct().sortBy(lambda x: x).zipWithIndex()
    dateIndexShift = dateIndex.map(lambda x: (x[0], x[1] + 1))

    def joinbyIndex(rdd, index):
        return rdd.join(index).map(lambda x: ((x[1][1], x[1][0][0]), [x[1][0][1], x[0]]))


    dailyGain = (joinbyIndex(closePrice, dateIndex)
                 .join(joinbyIndex(closePrice, dateIndexShift))
                 .map(lambda x: ((x[0][1], x[1][0][1]), x[1][0][0] / x[1][1][0] - 1))
                 .sortBy(lambda x: -x[1])
                 )

    def topNtoWrite(rdd, n, prefix):
        return (rdd
                .zipWithIndex().filter(lambda x: x[1] < n)
                .map(lambda x: (prefix, str(x[0])))
                .groupByKey().map(lambda x: x[0] + ','.join(x[1]).replace(' ', ''))
                )

    result = (topNtoWrite(fluctuation.map(lambda x: x[0]), 3, '2a - ')
               .union(topNtoWrite(dailyGain.map(lambda x: x[0][0]), 3, '2b - '))
               )

    result.repartition(1).saveAsTextFile('hw2_Karablinov')
    spark.stop()



