package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd._
import org.apache.spark.sql.Row

object Preprocessing1_2attributes_approach {
  def run(spark:SparkSession, df1: DataFrame, df2: DataFrame, teacher: DataFrame, threshold:Double, factor:Double, output_path:String, no_of_attributes:Int) = {
    val columns1: Array[String] = df1.columns
    val columns2: Array[String] = df2.columns
    
    val parsed_df1 = df1.distinct().rdd
    val parsed_df2 = df2.distinct().rdd
 
    //since we are working with only 1 or 2 attributes, we collect predicate for them
    val extractedEntity1 = extractedentity_AllPredicates(parsed_df1, columns1, no_of_attributes)
    val extractedEntity2 = extractedentity_AllPredicates(parsed_df2, columns2, no_of_attributes)
    LSHoperation.run(spark, extractedEntity1, extractedEntity2, teacher, threshold, factor, output_path)
  }
  
  def extractedentity_NoPredicates(df:RDD[Row], no_of_attributes:Int) :RDD[(String,String)]= {
   return df.map(f=>{
     val key = f.get(0)+""
     var value :String = ""
     var i = 1
     while(i <= no_of_attributes)
     { if(!f.isNullAt(i))
       value += f.get(i) + " " 
       i += 1
     }
     value = value.replace(",", " ").stripSuffix(" ")
     (key,value.trim())
   })
  }
  
  def extractedentity_AllPredicates(df:RDD[Row],columns:Array[String], no_of_attributes:Int) = {
    df.map(f=>{
      val key = f.get(0)+""
      var value = ""
      var i = 1
      while(i <= no_of_attributes){
        if(!f.isNullAt(i))
        value += columns(i)+ " " + f.get(i) + " "
        i += 1
      }
      value = value.replace(",", " ").stripSuffix(" ")
      (key,value.trim())
    })
  }
  
  def extractedentity_SO(df:RDD[Row], no_of_attributes:Int):RDD[(String,String)] = {
    return extractedentity_NoPredicates(df, no_of_attributes).map(f=>{
     (f._1,f._1.split("/").last + " " + f._2)
   })
  }
  
  def extractedentity_SPO(df:RDD[Row],columns:Array[String], no_of_attributes:Int):RDD[(String,String)] = {
    return extractedentity_AllPredicates(df,columns, no_of_attributes).map(f=>{
      (f._1,f._1.split("/").last + " " + f._2)
    })
  }
}
