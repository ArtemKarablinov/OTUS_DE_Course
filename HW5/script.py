from pyspark.sql import SparkSession, Row
from pyspark.sql.column import Column, _to_java_column, _to_seq

if __name__ == '__main__':

    # Spark Session
    spark = SparkSession \
        .builder \
        .getOrCreate()
    sc = spark.sparkContext

    # Load Data
    df = sc.parallelize([1484460310, 795880355, 3556987496]).map(Row("Int")).toDF()

    # Create UDF
    def int_to_ip_udf(col):
        return Column(sc._jvm.CustomUDFs.intToIpUDF().apply(_to_seq(sc, [col], _to_java_column)))

    # Result
    df = df.withColumn("Ip", int_to_ip_udf(df["Int"]))

    # Save to CSV
    df.coalesce(1).write.format("com.databricks.spark.csv").save("results_hw5.csv",
                                                                 header="false")
