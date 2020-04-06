
// ------------------ Highest urban population - Country having the highest urban population ---------------

package com.worldbank

import org.apache.spark.sql.SparkSession

object countryHighestUrbanPopulation extends App{
  
    val session = SparkSession.builder.appName("WB-HighestUrbanPopulation").master("local[1]").getOrCreate()
    
    val fileRDD = session.read.csv("F:/DataFlair - Spark and Scala/LMS Downloads/Practical Scala/WorldBankAnalysis/World_Bank_Indicators.csv").rdd
    
    //fileRDD.foreach(println)
    
    val rdd = fileRDD.map(f => {
      val cname = f.getString(0)
      val popNum = f.getString(10)
      //var defaultNum:Long = 0L
      if(popNum == null)
        (cname,0L)
       else{
         val getNum = popNum.replace(",", "")
         (cname,getNum.toLong)
       }
    })
    .reduceByKey(_+_)
    
    
     val rdd1 = fileRDD.map(f => {
      val cname = f.getString(0)
      val popNum = f.getString(10)
      //var defaultNum:Long = 0L
      if(popNum == null)
        (0L,cname)
       else{
         val getNum = popNum.replace(",", "")
         (getNum.toLong,cname)
       }
    }).sortByKey(false).first()
    
    
    val rdd2 = fileRDD.map(f => {
      val cname = f.getString(0)
      val popNum = f.getString(10)
      //var defaultNum:Long = 0L
      if(popNum == null)
        (cname,0L)
       else{
         val getNum = popNum.replace(",", "")
         (cname,getNum.toLong)
       }
    })
    .reduceByKey(_+_).collect().sortWith(_._2 > _._2).toList.head
    
    
    val getTuple = rdd.sortBy(_._2, false).first
    println(s"${getTuple._1} is the country with the highest population")
    //getTuple.foreach(println)
    println("===>  Class : " + rdd.getClass)
    println("=>>>>> Country : " + rdd2._1)
    
}