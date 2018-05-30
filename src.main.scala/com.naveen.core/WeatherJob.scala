package com.naveen.core

Object WeatherJob {


  /*
   UseCase:Create meaning out of the NOAA weather data
   Solution:Pick a weather station (Niagara) and see the trend of monthly day time average temparature, find the day of min and max temperatures by year

   output: --Day of max temperature by year
+----+-----+---+---------+--------+
|year|month|day|daily_max|_rankCal|
+----+-----+---+---------+--------+
|1942|    6| 10|     32.9|       1|
|1943|    9|  1|     33.5|       1|
|1944|    1| 26|     13.0|       1|
|1946|    7| 11|     34.4|       1|
|1947|    8|  6|     34.4|       1|
|1948|    8| 27|     36.7|       1|
|1949|    7|  3|     33.9|       1|
|1950|    7|  9|     32.8|       1|
|1951|    8| 30|     32.8|       1|
|1952|    7|  7|     33.3|       1|
|1953|    9|  3|     36.7|       1|
 */

  /* Importing spark libraries*/

  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions.row_number

  // Creating spark context object

  val conf = new SparkConf().setAppName("WheatherSparkApp")
  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)


  import sqlContext.implicits._
  import org.apache.spark.sql._
  import org.apache.spark.sql.expressions.Window

  //Implementing case class for data schema, Improving the performance of the RDD's or DF's used
  case class WeatherDataFrame(
                               val year: Int,
                               val month: Int,
                               val day: Int,
                               val hour: Int,
                               val air_temp: Int,
                               val dewpoint_temp: Int,
                               val sea_level_pressure: Int,
                               val wind_dir: String,
                               val wind_speed_rate: Int,
                               val sky_coverage_code: Int,
                               val liq_precip_onehr: String,
                               val liq_precip_sixhr: String
                             )


  def main (args: Array[String] ): Unit = {


  //read the data and remove the header
  val source = sc.textFile ("file:///home/ngari001c/att/Niagara-pipedelimited.txt")
  val header = source.first
  val finalRdd = source.filter (x => x != header)

  //dataframe conversion
  val sparkComposedRDD = finalRdd.map (_.split ("\\|", - 1) ).map (x => WeatherDataFrame (x (0).trim.toInt, x (1).trim.toInt, x (2).trim.toInt, x (3).trim.toInt, x (4).trim.toInt, x (5).trim.toInt, x (6).toInt, x (7).trim.toString, x (8).trim.toInt, x (9).trim.toInt, x (10).trim, x (11).trim) ).toDF ()

  val w = org.apache.spark.sql.expressions.Window.orderBy ($"year", $"month", $"day", $"hour")

  val res = sparkComposedRDD.withColumn ("_appox", lag ($"air_temp", 1, 0).over (w) ).withColumn ("_appox1", lead ($"air_temp", 1, 0).over (w) )

  //calculate unknown value by averaging pre and post values
  val weatherDF = res.withColumn ("_new_temp", when ($"air_temp" === "-9999", ($"_appox" + $"_appox1") / 2).otherwise ($"air_temp") )




  //Final step in finding the day of max temperature
  val dailyMax = weatherDF.groupBy ($"year", $"month", $"day").agg (max ($"_new_temp") / 10)
  val dailyMax1 = dailyMax.withColumnRenamed ("(max(_new_temp) / 10)", "daily_max")
  val maxTempByYear = dailyMax1.withColumn ("_rankCal", row_number ().over (Window.partitionBy ($"year").orderBy ($"daily_max".desc) ) ).filter ($"_rankCal" === "1").orderBy ($"year".asc)



  //Final step in finding the day of min temperature
  val dailyMin = weatherDF.groupBy ($"year", $"month", $"day").agg (min ($"_new_temp") / 10)
  val dailyMin1 = dailyMin.withColumnRenamed ("(min(_new_temp) / 10)", "daily_min")
  val minTempByYear = dailyMin1.withColumn ("_rankCal", row_number ().over (Window.partitionBy ($"year").orderBy ($"daily_min") ) ).filter ($"_rankCal" === "1").orderBy ($"year".asc)



  //Monthly average day temperature
  val daytemp = sparkComposedRDD.filter ($"hour" <= 18 and $"hour" >= 8)
  val res = daytemp.withColumn ("_appox", lag ($"air_temp", 1, 0).over (w) ).withColumn ("_appox1", lead ($"air_temp", 1, 0).over (w) )
  val finaldaytemp = res.withColumn ("_new_temp", when ($"air_temp" === "-9999", ($"_appox" + $"_appox1") / 20).otherwise ($"air_temp" / 10) )
  val monthlyAvg = finaldaytemp.groupBy ($"year", $"month").agg (avg ($"_new_temp") ).toInt



    sc.stop()
  }

}