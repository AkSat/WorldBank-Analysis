//---------------- Highest population growth - Country with highest % population growth in past decade ------------------

package com.worldbank

import org.apache.spark.sql.SparkSession

object highestPopGrowth extends App{

  val session = SparkSession.builder().appName("HighestPopGrowth").master("local").getOrCreate()
  
  val fileRDD = session.read.csv("F:/DataFlair - Spark and Scala/LMS Downloads/Practical Scala/WorldBankAnalysis/World_Bank_Indicators.csv").rdd
  
  val rdd = fileRDD.map(f => {
    val cname = f.getString(0)
    val pop = f.getString(9)
    
    if(pop == null)
      (cname,0L)
    else
      (cname,pop.replace(",", "").toLong)
  }).groupByKey().map(f => {
    
      val min = f._2.min
      val max = f._2.max
      var ratio:Double = ((max - min)/min) * 100
      (f._1,ratio)
  }).sortBy(_._2, false).first()
  
  println("Country : " + rdd._1 + " ---->  % Growth :  " + rdd._2)
}