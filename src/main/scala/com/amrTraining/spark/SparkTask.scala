package com.amrTraining.spark
import org.apache.derby.iapi.types.Like
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.Like
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SparkSession, functions, hive}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.DataFrameWriter
object SparkTask {



  def main (args : Array[String]) :Unit ={


//    val yourAWSCredentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)
//    val amazonS3Client = new AmazonS3Client(yourAWSCredentials)
//    val bucketName = "modelCountry"          // specifying bucket name
//    val bucketName1 = "topFiveCountries"          // specifying bucket name
//    val bucketName2 = "TopFiveModels"          // specifying bucket name
//    val bucketName3 = "TopFiveStates"          // specifying bucket name


    val spark =
         SparkSession
        .builder
        .appName("Car Theft App")
        .enableHiveSupport()
        .getOrCreate()

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(spark.sparkContext)

    var cars = spark.read.option("header","true").csv("s3://dev-harri-reporting/amr.rommaneh/cars - cars.csv")

    cars = cars.withColumnRenamed("Car Brand","car_brand")
    cars = cars.withColumnRenamed("Country of Origin","country_of_origin")

    cars.cache()
    cars.persist(StorageLevel.MEMORY_AND_DISK)

    cars.show()
    

    var df = spark.read.option("header","true").csv("s3://dev-harri-reporting/amr.rommaneh/2015_State_Top10Report_wTotalThefts.csv")
    df =  df.withColumnRenamed("Make/Model","make_model")
    df =  df.withColumnRenamed("Model Year","model_year")


    df.createOrReplaceTempView("thefts")
    cars.createOrReplaceTempView("cars")
    df.show()

    var joined_ = spark.sql("select * FROM thefts left join cars on thefts.make_model like concat (cars.car_brand,'%')")
    joined_ = joined_.withColumn("model",regexp_replace(col("make_model"), col("car_brand"), lit("")))

    joined_ = joined_.drop("make_model")

    joined_.cache()
    joined_.persist(StorageLevel.MEMORY_AND_DISK)

    joined_.show()


    // TODO: Internal tables VS external tables
//    sqlContext.sql("create database car_theft")
  sqlContext.sql("use car_theft")
  //  sqlContext.sql("CREATE TABLE IF NOT EXISTS car_theft.Cars(car_brand STRING, country_of_origin STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
   // sqlContext.sql("CREATE TABLE IF NOT EXISTS car_theft.Thefts_Record(state STRING,rank int, make_model STRING,model_year int,thefts int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
  //  sqlContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS car_theft.thefts_with_origin(state STRING ,car_brand STRING,model STRING,country_of_origin STRING,model_year int,thefts int) PARTITIONED BY(rank int) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' LOCATION 's3://dev-harri-reporting/amr.rommaneh/sparkTask' ")

//   sqlContext.sql("CREATE TABLE IF NOT EXISTS thefts_with_origin_partioned(state STRING,rank int, car_brand STRING,model STRING,country_of_origin STRING,model_year int,thefts int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")


    cars.write.option("header","true").mode("overwrite").saveAsTable("car_theft.Cars")
    df.write.option("header","true").mode("overwrite").saveAsTable("car_theft.Thefts_Record")


   joined_.write.option("header","true").format("hive").mode("append").saveAsTable("car_theft.thefts_with_origin")
var modelCountry=sqlContext.sql("Select model,country_of_origin from car_theft.thefts_with_origin")
modelCountry.show()


//    joined.write.option("header","true").partitionBy("rank").mode("overwrite").csv("/home/amr.rommaneh/task")

//    sqlContext.sql("select * FROM thefts_with_origin").show(false)


  


   joined_.cache()
    joined_.persist(StorageLevel.MEMORY_AND_DISK)
println("beefooore if ")
println(args.length)

if(args.length >=1){
   sqlContext.sql("DROP TABLE IF EXISTS car_theft.new_thefts")
    var newThefts = spark.read.option("header","true").csv(args(0))
newThefts.show()
println("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")

    newThefts =    newThefts.withColumnRenamed("Make/Model","make_model")
    newThefts =    newThefts.withColumnRenamed("Model Year","model_year")
    newThefts.createOrReplaceTempView("new_thefts")
    newThefts.show()
    var newThefts_ =   sqlContext.sql("select * FROM car_theft.new_thefts left join car_theft.Cars on new_thefts.make_model like concat (Cars.car_brand,'%')")
println("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
    newThefts_ = newThefts_.withColumn("model",regexp_replace(col("make_model"), col("car_brand"), lit("")))
newThefts_.show()    
println("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")

//sqlContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS car_theft.new_thefts(state STRING,rank int, car_brand STRING,model STRING,country_of_origin STRING,model_year int,thefts int) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'LOCATION 's3://dev-harri-reporting/amr.rommaneh/sparkTask'")
newThefts_.write.option("header","true").mode("overwrite").saveAsTable("car_theft.new_thefts")
println("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")

newThefts_.show()

println("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")

newThefts_ = newThefts_.drop("make_model")
    //newThefts_.write.option("header","true").mode("overwrite").saveAsTable("carthreft.new_thefts")
println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
newThefts_.show()

 //   var joinedTable=sqlContext.sql("Update  thefts_with_origin set thefts_with_origin.thefts=thefts_with_origin.thefts+new_thefts.thefts " +
   //   "Join new_thefts On thefts_with_origin.model=new_thefts.model And thefts_with_origin.model_year=new_thefts.model_year And thefts_with_origin.car_brand=new_thefts.car_brand ")

sqlContext.sql("Delete from thefts_with_origin Where Exists (Select a.state , a.rank , a.car_brand , a.model , a.model_year From car_theft.thefts_with_origin a Inner join car_theft.new_thefts b On a.state = b.state Where a.model=b.model And a.model_year=b.model_year And a.car_brand=b.car_brand And a.state = b.state)")
sqlContext.sql("select count(*) from car_theft.thefts_with_origin").show

  newThefts_.write.option("header","true").mode("append").saveAsTable("car_theft.thefts_with_origin")
sqlContext.sql("select count(*) from car_theft.thefts_with_origin").show

    //joinedTable.show()


}
    var topFiveCountries =sqlContext.sql("Select Sum(thefts_with_origin.thefts) as s ,country_of_origin from car_theft.thefts_with_origin Group By country_of_origin SORT BY s DESC LIMIT 5")

    //topFiveCountries.write.csv("topFiveCountries")
    topFiveCountries.show()


    val TopFiveModels =sqlContext.sql("Select model ,  SUM(thefts) as s, car_brand from car_theft.thefts_with_origin t Group By car_brand, model SORT BY s DESC LIMIT 5")

    //TopFiveModels.write.csv("TopFiveModels")
    TopFiveModels.show()


    val TopFiveStates =sqlContext.sql("Select Sum(thefts) as s , state  from car_theft.thefts_with_origin  Group By state SORT BY s DESC LIMIT 5")

    //TopFiveStates.write.csv("TopFiveStates")
    TopFiveStates.show()

val count = sqlContext.sql("select count(*) from car_theft.thefts_with_origin")

println(count)
count.show()
  
}


}
