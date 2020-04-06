/*
 * Youngest Country - Yearly distribution of youngest Countries
 */

package com.worldbank
import org.apache.spark.sql.SparkSession

object youngestCountryYearly extends App {
 
  val session = SparkSession.builder().appName("YoungestCountry").master("local").getOrCreate()
  
  val fileRDD = session.read.csv("F:/DataFlair - Spark and Scala/LMS Downloads/Practical Scala/WorldBankAnalysis/World_Bank_Indicators.csv").rdd
  
  
  val rdd = fileRDD.map(f => {
    val cname = f.getString(0)
    val year = f.getString(1).split("/")
    val youngPop = f.getString(16)

    if(youngPop == null)
      (year(2),(cname,0))
    else
      (year(2),(cname,youngPop.toInt))
    
  })
  .groupByKey()
  
  rdd.foreach(println)
  
  val rdd1 = rdd.map(f => {
    
    val year = f._1
    val arr = f._2.toList.sortBy(f => f._2).last
    (year,arr._1,arr._2)
  }).sortBy(_._1, true)
  
  rdd1.foreach(println)
  
  
}