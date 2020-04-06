package com.worldbank
import org.apache.spark.sql.SparkSession


object highGrowthNetUsage extends App{

  val session = SparkSession.builder().appName("HighestNetGrowth").master("local").getOrCreate()
  val fileRDD = session.read.csv("F:/DataFlair - Spark and Scala/LMS Downloads/Practical Scala/WorldBankAnalysis/World_Bank_Indicators.csv").rdd
  
  val rdd = fileRDD.map(f => {
    val cname = f.getString(0)  
    val net = f.getString(5)
    
    if(net == null)
      (cname,0)
    else
    {
      (cname,net.toInt)
    }
  }).groupBy(f => f._1).map(f =>{
    val unzipped = f._2.unzip
    val min = unzipped._2.min
    val max = unzipped._2.max
    val growth = max - min
    (f._1,growth)
  })
  .sortBy(k => k._2, false)
  
  rdd.foreach(println)
}