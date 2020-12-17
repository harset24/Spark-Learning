// Databricks notebook source
val primerdd = sc.textFile("/FileStore/tables/numberData-1.csv")

// COMMAND ----------

primerdd.count()

// COMMAND ----------

val header = primerdd.first
val withoutheader = primerdd.filter(line=>line!=header)
withoutheader.collect()
val primedata = withoutheader.map(line=>line.toInt)
primedata.collect()

// COMMAND ----------

def prime(a:Int):Boolean={
  var r = true
  var x = 0
  if(a==0 || a==1){
    return false
  }
  else{
    for(x<-2 until a){
      if(a%x==0){
        r = false
      }
    }
    return r
  }
}

// COMMAND ----------

val primenum = primedata.filter(line=>prime(line))
primenum.count()

// COMMAND ----------

val primesum = primenum.reduce( (x,y) => x+y )
primesum
