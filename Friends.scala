// Databricks notebook source
val friendsdata = sc.textFile("/FileStore/tables/FriendsData.csv")

// COMMAND ----------

friendsdata.count()

// COMMAND ----------

val withoutheader = friendsdata.filter( line => !line.contains("Age"))

// COMMAND ----------

withoutheader.count()

// COMMAND ----------

val RDD = withoutheader.map( x => ( x.split(",")(2), (1,x.split(",")(3).toDouble) ) )

RDD.collect()

// COMMAND ----------

val reducerdd = RDD.reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2) )

// COMMAND ----------

val finalrdd = reducerdd.mapValues( data => data._2 / data._1)

// COMMAND ----------

for ( (x,y) <- finalrdd.collect() )
println ("Age: " + x + " , Avg No. of friends: " + y)

// COMMAND ----------


