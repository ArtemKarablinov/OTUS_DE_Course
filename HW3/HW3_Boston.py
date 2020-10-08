import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.window import Window

if __name__ == '__main__':
    path_to_crime_df = 'crime.csv'
    path_offense_codes = 'offense_codes.csv'
    spark = SparkSession.builder.appName('my_hw3').getOrCreate()
    # read data
    crime = spark.read.csv(path_to_crime_df, header=True)
    of = spark.read.csv(path_offense_codes, header=True)
    # of.show()

    # task #1
    print("task 1")
    crimes_total = crime.select(f.col("DISTRICT"),
                                f.col("INCIDENT_NUMBER")).groupBy("DISTRICT").count()

    crimes_total.show()

    # task #2
    print("task 2")
    crime = crime.withColumn("YM", f.date_format(f.col('OCCURRED_ON_DATE'), "YYYY-MM"))
    crime = crime.withColumn("YMD",
                             f.date_format(f.col('OCCURRED_ON_DATE'), "YYYY-MM-dd"))
    # crime.show()

    crime_monthly = crime \
        .groupBy(["DISTRICT", "YM"]) \
        .count() \
        .groupby("DISTRICT") \
        .agg(f.expr('percentile_approx(count, 0.5)').alias('crimes_monthly'))

    crime_monthly.show()

    # task #3
    print("task 3")
    windowSpec = Window.partitionBy('DISTRICT').orderBy(f.col('count').desc())

    frequentCrimes = crime \
        .join(f.broadcast(of), of.CODE == crime.OFFENSE_CODE) \
        .select('DISTRICT',
                f.split(of.NAME, ' - ').getItem(0).alias('crime_type')) \
        .groupBy('DISTRICT', 'crime_type') \
        .count() \
        .orderBy('DISTRICT', 'count', ascending=False) \
        .withColumn('rank', f.rank().over(windowSpec)) \
        .filter(f.col('rank') <= 3) \
        .groupBy('DISTRICT') \
        .agg(
            f.concat_ws(', ',
                        f.collect_list(f.col('crime_type'))) \
                .alias('frequent_crime_types'))

    frequentCrimes.show()

    # task 4
    print("task 4")
    lat = crime \
        .filter(f.col('Lat') != -1.0) \
        .groupBy('DISTRICT') \
        .agg(f.mean('Lat').alias('lat'))

    lat.show()

    # task 5
    print("task 5")

    lng = crime \
        .filter(f.col('Long') != -1.0) \
        .groupBy('DISTRICT') \
        .agg(f.mean('Long').alias('lng'))

    lng.show(truncate=False)

    # Results
    print("Results")
    result = crime \
        .join(crimes_total, on='DISTRICT', how='left') \
        .join(frequentCrimes, on='DISTRICT', how='left') \
        .join(lat, on='DISTRICT', how='left') \
        .join(lng, on='DISTRICT', how='left')

    result.show()