// Databricks notebook source
val propertydata = sc.textFile("/FileStore/tables/Property_data-6.csv")

// COMMAND ----------

propertydata.count()

// COMMAND ----------

val removedheader = propertydata.filter( line => !line.contains("Price") )

// COMMAND ----------

val roomrdd = removedheader.map( x => ( x.split(",")(3), (1,x.split(",")(2).toDouble) ) )

roomrdd.collect()

// COMMAND ----------

roomrdd.map(x=>x._1).take(2)

// COMMAND ----------

val reducerdd = roomrdd.reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2) )

// COMMAND ----------

val finalrdd = reducerdd.mapValues( data => data._2 / data._1)

// COMMAND ----------

for ( (x,y) <- finalrdd.collect() )
println ("No. of Bedrooms: " + x + " , Avg Price: " + y)

// COMMAND ----------


