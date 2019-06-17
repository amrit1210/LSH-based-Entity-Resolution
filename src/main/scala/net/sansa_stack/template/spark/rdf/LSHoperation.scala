package net.sansa_stack.template.spark.rdf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{ Tokenizer, HashingTF }
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.feature._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import java.util.regex._
import org.apache.jena.graph.Triple
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.feature.MinHashLSHModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.RelationalGroupedDataset
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable._
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.functions.struct
import scala.util.control.Exception.Catch
import org.apache.spark.ml.feature.RegexTokenizer


object LSHoperation {
  
  def run(spark: SparkSession, extractedEntity_1:RDD[(String,String)], extractedEntity_2: RDD[(String,String)], teacher: DataFrame, threshold:Double, factor:Double, output_path:String) = {
    val extractedEntity = extractedEntity_1.union(extractedEntity_2).distinct()
    println("Entities in Dataset1: "+extractedEntity_1.count())
    println("Entities in Dataset2: "+extractedEntity_2.count())
   // run_dirty(spark,extractedEntity, teacher, threshold, factor, output_path)
    //val groundTruth:RDD[(String,String)] = getGroundTruth_clean(extractedEntity_1, extractedEntity_2)
    val (featuredData_Df1: DataFrame, featuredData_Df2: DataFrame) = vectoriseText_clean(spark, extractedEntity_1, extractedEntity_2, factor)
    //val featuredData_Df2: DataFrame = vectoriseText(spark, extractedEntity_2,vocab_size_entity2)
    //println(vocab_size_entity1)
    //println(vocab_size_entity2)
    val (model: MinHashLSHModel, transformedData_Df1: DataFrame, transformedData_Df2: DataFrame) = minHashLSH_clean(featuredData_Df1,featuredData_Df2)
    val dataset: Dataset[_] = approxSimilarityJoin(model, transformedData_Df1,transformedData_Df2, threshold)
    //dataset.show(false)
    val ds = dataset.repartition(400).persist(StorageLevel.MEMORY_AND_DISK)
    matchentities(spark,ds,teacher, output_path)
  }
  def run_clean1(spark: SparkSession, extractedEntity_1:RDD[(String,String)], extractedEntity_2: RDD[(String,String)], teacher: RDD[Triple], threshold:Double, factor:Double, output_path:String) = {
    val extractedEntity = extractedEntity_1.union(extractedEntity_2).distinct()
    println("entity1: "+extractedEntity_1.count())
    println("entity2: "+extractedEntity_2.count())
   // run_dirty(spark,extractedEntity, teacher, threshold, factor, output_path)
    //val groundTruth:RDD[(String,String)] = getGroundTruth_clean(extractedEntity_1, extractedEntity_2)
    val (featuredData_Df1: DataFrame, featuredData_Df2: DataFrame) = vectoriseText_clean(spark, extractedEntity_1, extractedEntity_2, factor)
    //val featuredData_Df2: DataFrame = vectoriseText(spark, extractedEntity_2,vocab_size_entity2)
    //println(vocab_size_entity1)
    //println(vocab_size_entity2)
    val (model: MinHashLSHModel, transformedData_Df1: DataFrame, transformedData_Df2: DataFrame) = minHashLSH_clean(featuredData_Df1,featuredData_Df2)
    val dataset: Dataset[_] = approxSimilarityJoin(model, transformedData_Df1,transformedData_Df2, threshold)
    //dataset.show(false)
    val ds = dataset.repartition(400).persist(StorageLevel.MEMORY_AND_DISK)
   // matchentities(spark,ds,teacher, output_path)
  }
  
  def remove_stopwords(tokenizedData_Df: DataFrame): DataFrame = {
    val remover =  new StopWordsRemover().setInputCol("words").setOutputCol("filtered_words")
    val removed_df = remover.transform(tokenizedData_Df)
    //removed_df.withColumn("filtered_words", when(col("filtered_words").contains(", null"), "") otherwise(col("filtered_words")))
    //removed_df.show(false)
    return remover.transform(tokenizedData_Df)
  }
  def vectoriseText(spark: SparkSession, entities: RDD[(String, String)],factor:Double): DataFrame = {
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
    val tokenizer = new RegexTokenizer().setInputCol("attributes").setOutputCol("words").setPattern("""[\w\-_]+""")
    val tokenizedData_Df = tokenizer.transform(entityProfile_Df)
    //tokenizedData_Df.show(false)
    val cleanData_Df = remove_stopwords(tokenizedData_Df)
    //cleanData_Df.select("entities", "filtered_words").show(false)
    //val vocabSize = 1000000
    //cleanData_Df.select("filtered_words").rdd.foreach(println)
    val vocab = calculate_Vocabsize(cleanData_Df.select("filtered_words").distinct.rdd )
    val vocab_size = Math.round(factor*vocab).toInt
    val hashingTf = new HashingTF().setInputCol("filtered_words").setOutputCol("Features").setNumFeatures(vocab_size)
    val featuredData_hashedDf = hashingTf.transform(cleanData_Df)
    //featuredData_hashedDf.show(false)
   /*val idf = new IDF().setInputCol("raw_Features").setOutputCol("Features")
    val idfModel = idf.fit(featuredData_hashedDf)
    val rescaledHashedData = idfModel.transform(featuredData_hashedDf)
    //rescaledHashedData.show(false)
    val cvModel = new CountVectorizer().setInputCol("filtered_words").setOutputCol("Features").setVocabSize(3231).fit(cleanData_Df)
    val featuredData_cvDf = cvModel.transform(cleanData_Df)
    //featuredData_cvDf.select("entities","Features").show(false)

    val idf = new IDF().setInputCol("Features").setOutputCol("scaledFeatures")
    val idfModel = idf.fit(featuredData_cvDf) //scaling the features
    val rescaledData = idfModel.transform(featuredData_cvDf) */
    //return rescaledHashedData
    return featuredData_hashedDf
  }
  
  def getGroundTruth_clean(extractedEntity_1:RDD[(String,String)], extractedEntity_2: RDD[(String,String)]) :RDD[(String,String)] =
  {
    return extractedEntity_1.map(f=>f._1).intersection(extractedEntity_2.map(f=>f._1))
    .map(f=>{(f,f)}).distinct()
  }
  
  def calculate_Vocabsize(cleandfrdd:RDD[Row]): Int = {
    //cleandfrdd.foreach(println)
    val vocab =  cleandfrdd.map(_.mkString).reduce(_+", "+_)
    .split(", ").toSet
    //println(trial)
    //println("trial: "+trial.size)
   // var s = cleandfrdd.map(_.mkString.replace("WrappedArray(", "").replace("(", "").replace(")", ", ")).reduce(_+_).split(", ").toSet  
    //s-=("-","/")
    //s.foreach(println)
    //return s.size
    return (vocab.size)
    
  }
  def vectoriseText_clean(spark: SparkSession, entities1: RDD[(String, String)], entities2: RDD[(String,String)], factor:Double): (DataFrame,DataFrame) = {
    val entityProfile_Df1 = spark.createDataFrame(entities1).toDF("entities", "attributes")
    val entityProfile_Df2 = spark.createDataFrame(entities2).toDF("entities", "attributes")
    //entityProfile_Df.show(false)
    val tokenizer = new RegexTokenizer().setInputCol("attributes").setOutputCol("words").setPattern("""[\w\-_]+""").setGaps(false)
    val tokenizedData_Df1 = tokenizer.transform(entityProfile_Df1)
    val tokenizedData_Df2 =  tokenizer.transform(entityProfile_Df2)
    //tokenizedData_Df1.show(false)
    val cleanData_Df1 = remove_stopwords(tokenizedData_Df1)
    val cleanData_Df2 =  remove_stopwords(tokenizedData_Df2)
    val cleandfrdd = cleanData_Df1.union(cleanData_Df2).select("filtered_words").distinct.rdd    
    val vocab = calculate_Vocabsize(cleandfrdd) //100000000 
    //println("vocab:" + vocab)
    val vocab_size =  Math.round(factor*vocab).toInt
    println("vocab_size: "+vocab_size)
    val cleanData_DF = cleanData_Df1.union(cleanData_Df2).distinct()
    
   /* val hashingTf = new HashingTF().setInputCol("filtered_words").setOutputCol("Features").setNumFeatures(vocab_size)
    val featuredData_hashedDf1 = hashingTf.transform(cleanData_Df1)
    val featuredData_hashedDf2 = hashingTf.transform(cleanData_Df2) */
    /*val idf = new IDF().setInputCol("raw_Features").setOutputCol("Features")
    val idfModel = idf.fit(featuredData_hashedDf2.union(featuredData_hashedDf1))
    val rescaledHashedData1 = idfModel.transform(featuredData_hashedDf2)
    val rescaledHashedData2 = idfModel.transform(featuredData_hashedDf1)*/
    //featuredData_hashedDf.show(false)*/
    val cvModel = new CountVectorizer().setInputCol("filtered_words").setOutputCol("Features").setVocabSize(Math.round(factor*vocab_size).toInt).setMinDF(1).fit(cleanData_DF) //Math.round(0.85*(1368+3231)).toInt
    val featuredData_cvDf1 = cvModel.transform(cleanData_Df1)
    val featuredData_cvDf2 = cvModel.transform(cleanData_Df2)
    //featuredData_cvDf.select("entities","Features").show(false)*/
    //featuredData_cvDf1.show(false)
    /*val idf = new IDF().setInputCol("Features").setOutputCol("scaledFeatures")
    val idfModel = idf.fit(featuredData_cvDf) //scaling the features
    val rescaledData = idfModel.transform(featuredData_cvDf) */
    //return (rescaledHashedData1, rescaledHashedData2)
    return (featuredData_cvDf1, featuredData_cvDf2)
    //return (featuredData_hashedDf1, featuredData_hashedDf2)
  }

  def minHashLSH(featuredData_Df: DataFrame): (MinHashLSHModel, DataFrame) = {
    val mh = new MinHashLSH().setNumHashTables(5).setInputCol("Features").setOutputCol("HashedValues")
    val model = mh.fit(featuredData_Df)
    val transformedData_Df = model.transform(featuredData_Df)
    //transformedData_Df.show(false)
    return (model, transformedData_Df)
  }
  
  def minHashLSH_clean(featuredData_Df1: DataFrame, featuredData_Df2: DataFrame): (MinHashLSHModel, DataFrame, DataFrame) = {
    //featuredData_Df1.show(false)
    val mh = new MinHashLSH().setNumHashTables(3).setInputCol("Features").setOutputCol("HashedValues")
    val featuredData_DF =  featuredData_Df1.union(featuredData_Df2).distinct()
    val model = mh.fit(featuredData_DF)
    //model = mh.fit(featuredData_Df2)
    val transformedData_Df1 = model.transform(featuredData_Df1)
    val transformedData_Df2 = model.transform(featuredData_Df2)
    //println(transformedData_Df1.count())
    //println(transformedData_Df2.count())
    return (model, transformedData_Df1, transformedData_Df2)
  }

  def approxSimilarityJoin(model: MinHashLSHModel, transformedData_Df: DataFrame, threshold:Double): Dataset[_] = {
    return model.approxSimilarityJoin(transformedData_Df, transformedData_Df, threshold)
  //0.80 - NO Predicates
  //0.75 - FILTER Predicates
  //0.75 - ALL Predicates
  //0.75 - SPO
  }
  
  def approxSimilarityJoin(model: MinHashLSHModel, transformedData_Df1: DataFrame, transformedData_Df2: DataFrame, threshold:Double): Dataset[_] = {
   return model.approxSimilarityJoin(transformedData_Df1, transformedData_Df2, threshold)
  }
  
  def matchentities(spark:SparkSession,dataset:Dataset[_], df_of_ground:DataFrame, output_path:String) = {
   // if(!dataset.isEmpty)
    //{
    //println(dataset.count())
    val refined_entities_dataset = dataset
    //.filter("datasetA.entities != datasetB.entities")
    .select(col("datasetA.entities").alias("entity1"),col("datasetB.entities").alias("entity2"),col("distCol"))
    
    
  //  refined_entities_dataset.show(false)
    //df_of_ground.show(false)
    val teacher = df_of_ground.rdd
    val predicted = refined_entities_dataset.drop("distCol").rdd
    //val teacher = df_of_ground.withColumn("compare", struct(df_of_ground("idDBLP"),df_of_ground("idACM")))//.select("compare")//.rdd
    //val predicted = refined_entities_dataset.withColumn("compare", struct(refined_entities_dataset("entity1"),refined_entities_dataset("entity2"))).select("compare").rdd
    //teacher.show(false)
    //teacher.foreach(println)
    //predicted.show(false)
    //predicted.foreach(println)
  
    //val truePositives = predicted.intersection(teacher).collect()
    //val myFile = spark.sparkContext.textFile("out.txt")
    //predicted.coalesce(1).saveAsTextFile(output_path)
    
    evaluation(teacher,predicted)
    println("***************************************************************************************")
    //truePositives.foreach(println)
    //println("TruePositives:"+ truePositives.length)
    /*println("Actual: "+ teacher.count())
    println("Predicted: "+ predicted.count())
    println("True Positives: "+ truePositives.count())
    val precision = (truePositives.count() * 100.00)/(predicted.count())
    println("Precision: "+ precision)
    val recall = (truePositives.count() * 100 )/(teacher.count())
    println("Recall: "+ recall)
    val f1_measure = (2 * precision * recall)/(precision + recall)
    println("F1-measure: "+ f1_measure)*/
   // }
  }
    def evaluation(teacher:RDD[Row], predicted:RDD[Row]) = {
    val truePositives = teacher.intersection(predicted).count
    println("***************************************************************************************")
    println("***************************************************************************************")
    //truePositives.foreach(println)
   // println("TruePositives:"+ truePositives.length)
    val actual = teacher.count()
    val results = predicted.count()
    println("Actual: "+ actual)
    println("Predicted: "+ results)
    println("True Positives: "+ truePositives)
    val precision = (truePositives * 100.00)/(results)
    println("Precision: "+ precision)
    val recall = (truePositives * 100 )/(actual)
    println("Recall: "+ recall)
    val f1_measure = (2 * precision * recall)/(precision + recall)
    println("F1-measure: "+ f1_measure)
    println("***************************************************************************************")
    println("***************************************************************************************")
    }
}
  
