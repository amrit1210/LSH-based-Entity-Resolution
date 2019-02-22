package net.sansa_stack.template.spark.rdf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{ Tokenizer, HashingTF }
import org.apache.spark.ml.feature.CountVectorizer
//import org.dmg.pmml.False
import org.apache.spark.ml.feature.IDF
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.feature.MinHashLSHModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.RelationalGroupedDataset
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable

object LSHoperation {
  
  def run(spark: SparkSession, extractedEntity_NoPredicates: RDD[(String, String)], extractedEntity_FilterPredicates: RDD[(String, String)], extractedEntity_AllPredicates: RDD[(String, String)], extractedEntity_SPO: RDD[(String,String)] ) = {
    //extractedEntity_NoPredicates.foreach(println)
    //extractedEntity_FilterPredicates.foreach(println)
    //extractedEntity_AllPredicates.foreach(println)
    //extractedEntity_SPO.foreach(println)
    val featuredData_Df: DataFrame = vectoriseText(spark, extractedEntity_FilterPredicates)
    //featuredData_Df.show(false)
    val (model: MinHashLSHModel, transformedData_Df: DataFrame) = minHashLSH(featuredData_Df)
    //transformedData_Df.show(false)
    val dataset: Dataset[_] = approxSimilarityJoin(model, transformedData_Df)
    //dataset.show(false)
    matchentities(dataset)
    
    //val blocks:RelationalGroupedDataset = dataset.groupBy("datasetA.entities")
    
  }
  def vectoriseText(spark: SparkSession, entities: RDD[(String, String)]): DataFrame = {
    // Using HashingTf for converting text to vector format

    /*
     *
     *Here we create a dataframe on the extracted entities naming the two columns("entities", "attributes") in the entityProfile_Df
     * Then we are tokenizing the column "attributes" i.e. breaking the sentences or phrases to words and we add a new column words to our new dataFrame tokenizedData_Df
     * Now, we vectoise the text by either using HashingTf or CountVectorizer method.
     * So we create featuredData_hashedDf by HashingTf method, setting our setNumFeatures to 20 i.e. this means that it would probably encounter 20 different terms/words in the words column
     * We try to avoid collisions by keeping this value high.
     * Also, we have created the featuredData_cvDf using the CountVectorizer method, setting our setVocabSize to 20. This means that in our dictionary we will be adding approximately terms<=20
     *
     */
    val entityProfile_Df = spark.createDataFrame(entities).toDF("entities", "attributes")
    //entityProfile_Df.show(false)
    val tokenizer = new Tokenizer().setInputCol("attributes").setOutputCol("words")
    val tokenizedData_Df = tokenizer.transform(entityProfile_Df)
    //tokenizedData_Df.show(false)
    val hashingTf = new HashingTF().setInputCol("words").setOutputCol("Features").setNumFeatures(50)
    val featuredData_hashedDf = hashingTf.transform(tokenizedData_Df)
    //featuredData_hashedDf.show(false)
    val cvModel = new CountVectorizer().setInputCol("words").setOutputCol("Features").setVocabSize(100).fit(tokenizedData_Df)
    val featuredData_cvDf = cvModel.transform(tokenizedData_Df)
    //featuredData_cvDf.show(false)

    /*val idf = new IDF().setInputCol("Features").setOutputCol("scaledFeatures")
    val idfModel = idf.fit(featuredData_cvDf) //scaling the features
    val rescaledData = idfModel.transform(featuredData_cvDf) */
    return featuredData_hashedDf
  }

  def minHashLSH(featuredData_Df: DataFrame): (MinHashLSHModel, DataFrame) = {
    val mh = new MinHashLSH().setNumHashTables(3).setInputCol("Features").setOutputCol("HashedValues")
    val model = mh.fit(featuredData_Df)
    val transformedData_Df = model.transform(featuredData_Df)
    //transformedData_Df.show(false)
    return (model, transformedData_Df)
  }

  def approxSimilarityJoin(model: MinHashLSHModel, transformedData_Df: DataFrame): Dataset[_] = {
    return model.approxSimilarityJoin(transformedData_Df, transformedData_Df, 0.75)
  //0.80 - NO Predicates
  //0.75 - FILTER Predicates
  //0.75 - ALL Predicates
  //0.75 - SPO
  }
  
  def matchentities(dataset:Dataset[_]) = {
    val refined_entities_dataset = dataset.filter("datasetA.entities != datasetB.entities")
    .select(col("datasetA.entities").alias("entity1"),col("datasetB.entities").alias("entity2"),col("distCol"))
    
    refined_entities_dataset.show(false)
   // val x = refined_entities_dataset.repartition(400).persist(StorageLevel.MEMORY_AND_DISK) 
     val refined_entities_RDDA = refined_entities_dataset.rdd.map(f => {
      val key = f.getString(0)
      val value = f.getString(1)
      (key, value)
    })/*.reduceByKey(_+","+_)
    .map(f=>{
      if(f._1<f._2)
        (f._1,f._2)
      else
        (f._2,f._1)
    })
    .distinct()
    .foreach(println)
     val initialSet3 = mutable.Set.empty[String]
    //initialSet3.foreach(println)
    val addToset3  = (s: mutable.Set[String], v: String) => s += v
    val mergePartitiononSet3 = (p1: mutable.Set[String], p2: mutable.Set[String]) => p1 ++= p2*/
    val x1 = refined_entities_dataset.repartition(400).persist(StorageLevel.MEMORY_AND_DISK) 
    val x1Map = x1.rdd.map(row => {
      val id = row.getString(0)
      val value = row.getString(1)
      (id, value)
    })
    //x1Map.foreach(println)

    val initialSet3 = mutable.Set.empty[String]
    val addToSet3 = (s: mutable.Set[String], v: String) => s += v
    //println(addToSet3)
    val mergePartitionSets3 = (p1: mutable.Set[String], p2: mutable.Set[String]) => p1 ++= p2
    //println(mergePartitionSets3)
    val uniqueByKey3 = x1Map.aggregateByKey(initialSet3)(addToSet3, mergePartitionSets3)
    //uniqueByKey3.foreach(println)
  

    val k = uniqueByKey3.map(f => ((f._1, (f._2 += (f._1)).toSet)))
    k.foreach(println)
    /*val partitioner = new HashPartitioner(500) //500 working fine
    val mapSubWithTriplesPart = mapSubWithTriples.partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK) 

    val ys = k.partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK) 
    val joinSimSubTriples2 = ys.join(mapSubWithTriplesPart)

    val clusterOfSubjects = joinSimSubTriples2.map({
      case (s, (iter, iter1)) => ((iter).toSet, iter1) 
    })*/
  }
  
}