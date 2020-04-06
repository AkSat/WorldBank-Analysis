
//---------------  Highest GDP growth - List of Countries with highest GDP growth from 2009 to 2010 in descending order  --------

package com.worldbank
import org.apache.spark.sql.SparkSession


object highestGDPGrowth extends App{
  val session = SparkSession.builder().appName("HighestGDPGrowth").master("local").getOrCreate()
  
  val fileRDD = session.read.csv("F:/DataFlair - Spark and Scala/LMS Downloads/Practical Scala/WorldBankAnalysis/World_Bank_Indicators.csv").rdd
  
  val rdd = fileRDD.map(f => {
    val cname = f.getString(0)
    val gdp = f.getString(18)
    val year = f.getString(1).split("/")
    
    if(gdp == null)
      (cname,(0L,year(2)))
    else
      (cname,(gdp.replace(",", "").toLong,year(2)))
  }).filter(f => f._2._2.equals("2009") | f._2._2.equals("2010") )
  .groupByKey().map(f => {
    val cname = f._1
    val arr = f._2.unzip
    val gdp2010 = arr._1.last
    val gdp2009 = arr._1.head
    
    val growth:Double = (gdp2010 - gdp2009) * 100
    
    (cname,growth)
  })
  .sortBy(f => f._2, false).take(1)
  
  rdd.foreach(println)
}