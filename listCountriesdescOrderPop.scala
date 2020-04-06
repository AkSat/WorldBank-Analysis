
// ------------ Most populous Countries - List of countries in the descending order of their population -----------

package com.worldbank
import org.apache.spark.sql.SparkSession

object listCountriesdescOrderPop extends App{
  
  val session = SparkSession.builder().appName("MostPopulous").master("local").getOrCreate()
  
  val fileRDD = session.read.csv("F:/DataFlair - Spark and Scala/LMS Downloads/Practical Scala/WorldBankAnalysis/World_Bank_Indicators.csv").rdd
  
  val rdd = fileRDD.map(f => {
    val cname = f.getString(0)  
    val pop = f.getString(9)
    
    if(pop == null)
      (cname,0L)
    else
    {
      val getPop = pop.replace(",", "").toLong 
      (cname,getPop)
    }
  }).groupBy(f => f._1).mapValues(f => f.map(f => f._2).max)
  .sortBy(k => k._2, false)
  
  rdd.foreach(println)
}