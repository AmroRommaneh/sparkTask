package com.amrTraining.spark
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
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

    var cars = spark.read.option("header","true").csv("s3://dev-harri-reporting/amr.rommaneh/cars.csv")

    cars = cars.withColumnRenamed("Car Brand","car_brand")
    cars = cars.withColumnRenamed("Country of Origin","country_of_origin")

    cars.cache()
    cars.persist(StorageLevel.MEMORY_AND_DISK)

    cars.show()

    var df = spark.read.option("header","true").csv("s3://dev-harri-reporting/amr.rommaneh/2015_State_Top10Report_wTotalThefts.csv")
    df =  df.withColumnRenamed("Make/Model","make_model")



    df.createOrReplaceTempView("thefts")
    cars.createOrReplaceTempView("cars")

    df.show()

    var joined =spark.sql("select * FROM thefts left join cars on thefts.make_model like concat (cars.car_brand,'%')")
    joined = joined.withColumn("model",regexp_replace(col("make_model"), col("car_brand"), lit("")))

    joined = joined.drop("make_model")

    joined.cache()
    joined.persist(StorageLevel.MEMORY_AND_DISK)

    joined.show()


    // TODO: Internal tables VS external tables
    sqlContext.sql("create database car_theft")
    sqlContext.sql("use car_theft")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS Cars(car_brand STRING, country_of_origin STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS Thefts(state STRING,rank int, make_model STRING,model_year int,thefts int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS thefts_with_origin(state STRING,rank int, car_brand STRING,model STRING,country_of_origin STRING,model_year int,thefts int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")


    cars.write.option("header","true").mode("overwrite").saveAsTable("carthreft.Cars")
    df.write.option("header","true").mode("overwrite").saveAsTable("carthreft.Thefts")


    joined.write.option("header","true").mode("overwrite").saveAsTable("carthreft.thefts_with_origin")

    joined.write.option("header","true").partitionBy("rank").mode("overwrite").saveAsTable("carthreft.rank")

    sqlContext.sql("select * FROM thefts_with_origin").show(false)


    var modelCountry=sqlContext.sql("Select model,country_of_origin from  thefts_with_origin")

    modelCountry.show()
    joined.cache()
    joined.persist(StorageLevel.MEMORY_AND_DISK)



    sqlContext.sql("DROP TABLE new_thefts;")
    var newThefts = spark.read.option("header","true").csv(args(0))
    newThefts =    newThefts.withColumnRenamed("Make/Model","make_model")

    newThefts.createOrReplaceTempView("new_thefts")

    var newThefts_ =   sqlContext.sql("select * FROM new_thefts left join Cars on new_thefts.make_model like concat (Cars.car_brand,'%')")
    newThefts_ = newThefts_.withColumn("model",regexp_replace(col("make_model"), col("car_brand"), lit("")))
    sqlContext.sql("CREATE TABLE IF NOT EXISTS new_thefts(state STRING,rank int, car_brand STRING,model STRING,country_of_origin STRING,model_year int,thefts int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")




    newThefts_.write.option("header","true").mode("overwrite").saveAsTable("carthreft.new_thefts")


    var joinedTable=sqlContext.sql("Update  thefts_with_origin set thefts_with_origin.thefts=thefts_with_origin.thefts+new_thefts.thefts " +
      "Join new_thefts On thefts_with_origin.model=new_thefts.model And thefts_with_origin.model_year=new_thefts.model_year And thefts_with_origin.car_brand=new_thefts.car_brand ")
    joinedTable.write.option("header","true").mode("overwrite").saveAsTable("carthreft.thefts_with_origin")

    joinedTable.show()



    var topFiveCountries =sqlContext.sql("SELECT TOP 5 * FROM " +
      " (Select Sum(thefts_with_origin.thefts),thefts_with_origin.country_of_origin " +
      "from thefts_with_origin " +
      "GroupBy country_of_origin " +
      "ORDER BY Sum(thefts_and_origin.thefts) ASC)")

    topFiveCountries.write.csv("topFiveCountries")
    topFiveCountries.show()


    val TopFiveModels =sqlContext.sql("SELECT TOP 5 * FROM " +
      " (Select Sum(thefts_with_origin.thefts),DISTINCT thefts_with_origin.make,thefts_with_origin.model " +
      "from thefts_with_origin " +
      "GroupBy model " +
      "ORDER BY Sum(thefts_with_origin.thefts) ASC)")

    TopFiveModels.write.csv("TopFiveModels")
    TopFiveModels.show()


    val TopFiveStates =sqlContext.sql("SELECT TOP 5 * FROM " +
      " (Select Sum(thefts_with_origin.thefts),thefts_with_origin.state " +
      "from thefts_with_origin " +
      "GroupBy state " +
      "ORDER BY Sum(thefts_with_origin.thefts) ASC)")

    TopFiveStates.write.csv("TopFiveStates")
    TopFiveStates.show()


    //
//
//    println( " enter your choice ")
//  println("1) get the model with country of origin")
//  println("2) top 5 countries their cars are stolen")
//  println("3) update the records")
//  println("4) top 5 states their cars are stolen")
//  println("5) top 5 models  are stolen")
//
//  var choice = scala.io.StdIn.readInt()
//
//  if(choice == 1){
//
//    modelCountry.show()
//
//  }else if (choice ==2){
//    getTopFiveCountries()
//  }else if (choice ==3){
//    println("enter the path of the csv file of the new records")
//    var path = scala.io.StdIn.readLine()
//
//    update(path)
//  }else if (choice==4){
//    getTopFiveStates()
//  }else if (choice==5){
//    getTopFiveModels()
//  }else{
//    println("wrong choice")
//  }
//


}


  def update(path :String): Unit ={
  }

  def getTopFiveCountries ():Unit={
    //Extract a csv file contains the most 5 countries from where Americans buy their thefted cars ?
  }


  def getTopFiveModels():Unit={


  }


  def getTopFiveStates():Unit={
//
//    val TopFiveStates =sqlContext.sql("SELECT TOP 5 * FROM " +
//      " (Select Sum(thefts_with_origin.thefts),thefts_with_origin.state " +
//      "from thefts_with_origin " +
//      "GroupBy state " +
//      "ORDER BY Sum(thefts_with_origin.thefts) ASC)")
//
//    TopFiveStates.write.csv("TopFiveStates")

  }


}
