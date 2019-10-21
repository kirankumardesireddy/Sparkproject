package org.sparklearning

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.SparkSession



object demo  extends App {
  
  val conf = new SparkConf()
   .setMaster("local")
   .setAppName("demo")
    
    val sc = new SparkContext(conf)
  
    val spark = SparkSession
    .builder
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate
    
val sqlContext= new org.apache.spark.sql.SQLContext(sc)
  
import sqlContext.implicits._
    
val input = sc.textFile("/Users/kirand/Documents/Movies.txt")
     
case class MovieLine(Line: String)

val movieline = input.map(line => MovieLine(line))

movieline.toDF().registerTempTable("MovieLine")
 
val test = spark.sql("select *  from MovieLine")
//test.show()

//input.flatMap { line => line.split(";")}.map {word => (word,1)}.reduceByKey(_+_).saveAsTextFile("/Users/kirand/Documents/Movieswordcountfile.txt")
  
  
 
 
  
  val rd1 = sc.range(1,10)
  
  rd1.collect.foreach(println)
  
   sc.stop
  
 
  
}