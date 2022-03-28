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

object SparkTask {
  val spark =
    SparkSession
      .builder
      .appName("Car Theft App")
      .enableHiveSupport()
      .getOrCreate()

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(spark.sparkContext)


  def main (args : Array[String]) :Unit ={

    var cars = spark.read.option("header","true").csv("s3://dev-harri-reporting/amr.rommaneh/cars.csv")

    cars=cars.withColumnRenamed("Car Brand","car_brand")
    cars= cars.withColumnRenamed("Country of Origin","country_of_origin")

    cars.cache()
    cars.persist(StorageLevel.MEMORY_AND_DISK)

    var df = spark.read.option("header","true").csv("s3://dev-harri-reporting/amr.rommaneh/2015_State_Top10Report_wTotalThefts.csv")
    df=  df.withColumnRenamed("Make/Model","make_model")

    df.createOrReplaceTempView("thefts")
    cars.createOrReplaceTempView("cars")


    var joined =spark.sql("select * FROM thefts left join cars on thefts.make_model like concat (cars.car_brand,'%')")
    joined = joined.withColumn("model",regexp_replace(col("make_model"), col("car_brand"), lit("")))

    joined = joined.drop("make_model")

    joined.cache()
    joined.persist(StorageLevel.MEMORY_AND_DISK)


    sqlContext.sql("create database car_theft")
    sqlContext.sql("use car_theft")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS Cars(car_brand STRING, country_of_origin STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS Thefts(state STRING,rank int, make_model STRING,model_year int,thefts int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
    cars.write.saveAsTable("carthreft.Cars")
    df.write.saveAsTable("carthreft.Thefts")


    joined.write.option("header","true").mode("overwrite").saveAsTable("carthreft.thefts_with_origin")

    joined.write.option("header","true").partitionBy("rank").mode("overwrite").saveAsTable("carthreft.rank")



    var modelCountry=sqlContext.sql("Select model,country_of_origin from  thefts_with_origin")
    var joinedTable=sqlContext.sql("Select * from Cars INNER Join Thefts On Cars.car_brand=Thefts.make ")

    joined.cache()
    joined.persist(StorageLevel.MEMORY_AND_DISK)


    println( " enter your choice ")
  println("1) get the model with country of origin")
  println("2) top 5 countries their cars are stolen")
  println("3) update the records")
  println("4) top 5 states their cars are stolen")
  println("5) top 5 models  are stolen")

  var choice = scala.io.StdIn.readInt()

  if(choice == 1){

    modelCountry.show()

  }else if (choice ==2){
    getTopFiveCountries()
  }else if (choice ==3){
    println("enter the path of the csv file of the new records")
    var path = scala.io.StdIn.readLine()

    update(path)
  }else if (choice==4){
    getTopFiveStates()
  }else if (choice==5){
    getTopFiveModels()
  }else{
    println("wrong choice")
  }



}


  def update(path :String): Unit ={
    sqlContext.sql("DROP TABLE new_thefts;")
    var df = spark.read.option("header","true").csv(path)
    df =    df.withColumnRenamed("Make/Model","make_model")

 df.createOrReplaceTempView("new_thefts")

    var newThefts =   sqlContext.sql("select * FROM new_thefts left join Cars on new_thefts.make_model like concat (Cars.car_brand,'%')")
    newThefts = newThefts.withColumn("model",regexp_replace(col("make_model"), col("car_brand"), lit("")))


   // sqlContext.sql("CREATE TABLE IF NOT EXISTS new_thefts(state STRING,rank int, make_model STRING,model_year int,thefts int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")


    var x=sqlContext.sql("Select * from thefts_with_origin ")
    newThefts.write.option("header","true").mode("overwrite").saveAsTable("carthreft.new_thefts")


    var joinedTable=sqlContext.sql("Update  thefts_with_origin set thefts_with_origin.thefts=thefts_with_origin.thefts+new_thefts.thefts " +
      "Join new_thefts On thefts_with_origin.model=new_thefts.model And thefts_with_origin.model_year=new_thefts.model_year And thefts_with_origin.car_brand=new_thefts.car_brand ")


  }

  def getTopFiveCountries ():Unit={
    //Extract a csv file contains the most 5 countries from where Americans buy their thefted cars ?

    var topFiveCountries =sqlContext.sql("SELECT TOP 5 * FROM " +
      " (Select Sum(thefts_with_origin.thefts),thefts_with_origin.country_of_origin " +
      "from thefts_with_origin " +
      "GroupBy country_of_origin " +
      "ORDER BY Sum(thefts_and_origin.thefts) ASC)")

    topFiveCountries.write.csv("topFiveCountries")
    topFiveCountries.show()
  }


  def getTopFiveModels():Unit={

    val TopFiveModels =sqlContext.sql("SELECT TOP 5 * FROM " +
      " (Select Sum(thefts_with_origin.thefts),DISTINCT thefts_with_origin.make,thefts_with_origin.model " +
      "from thefts_with_origin " +
      "GroupBy model " +
      "ORDER BY Sum(thefts_with_origin.thefts) ASC)")

    TopFiveModels.write.csv("TopFiveModels")

  }


  def getTopFiveStates():Unit={

    val TopFiveStates =sqlContext.sql("SELECT TOP 5 * FROM " +
      " (Select Sum(thefts_with_origin.thefts),thefts_with_origin.state " +
      "from thefts_with_origin " +
      "GroupBy state " +
      "ORDER BY Sum(thefts_with_origin.thefts) ASC)")

    TopFiveStates.write.csv("TopFiveStates")

  }


}
