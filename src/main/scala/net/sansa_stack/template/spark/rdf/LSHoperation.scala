package net.sansa_stack.template.spark.rdf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{ RegexTokenizer, Tokenizer, HashingTF }
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


object LSHoperation {
  
  def run(spark: SparkSession, extractedEntity_1:RDD[(String,String)], extractedEntity_2: RDD[(String,String)], teacher: DataFrame, threshold:Double, factor:Double, output_path:String) = {
    val extractedEntity = extractedEntity_1.union(extractedEntity_2).distinct()
    println("Entities in Dataset1: "+extractedEntity_1.count())
    println("Entities in Dataset2: "+extractedEntity_2.count())
    val (featuredData_Df1: DataFrame, featuredData_Df2: DataFrame) = vectoriseText_clean(spark, extractedEntity_1, extractedEntity_2, factor)
    val (model: MinHashLSHModel, transformedData_Df1: DataFrame, transformedData_Df2: DataFrame) = minHashLSH_clean(featuredData_Df1,featuredData_Df2)
    val dataset: Dataset[_] = approxSimilarityJoin(model, transformedData_Df1,transformedData_Df2, threshold)
    val ds = dataset.repartition(400).persist(StorageLevel.MEMORY_AND_DISK)
    matchentities(spark,ds,teacher, output_path)
  }
  def remove_stopwords(tokenizedData_Df: DataFrame): DataFrame = {
    val remover =  new StopWordsRemover().setInputCol("words").setOutputCol("filtered_words")
    val removed_df = remover.transform(tokenizedData_Df)
    return remover.transform(tokenizedData_Df)
  }
  
  def getGroundTruth_clean(extractedEntity_1:RDD[(String,String)], extractedEntity_2: RDD[(String,String)]) :RDD[(String,String)] =
  {
    return extractedEntity_1.map(f=>f._1).intersection(extractedEntity_2.map(f=>f._1))
    .map(f=>{(f,f)}).distinct()
  }
  
  def calculate_Vocabsize(cleandfrdd:RDD[Row]): Int = {
    val vocab =  cleandfrdd.map(_.mkString).reduce(_+", "+_).split(", ").toSet
    return (vocab.size)
    
  }
  def vectoriseText_clean(spark: SparkSession, entities1: RDD[(String, String)], entities2: RDD[(String,String)], factor:Double): (DataFrame,DataFrame) = {
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
    
    val entityProfile_Df1 = spark.createDataFrame(entities1).toDF("entities", "attributes")
    val entityProfile_Df2 = spark.createDataFrame(entities2).toDF("entities", "attributes")
    val tokenizer = new Tokenizer().setInputCol("attributes").setOutputCol("words").setPattern("""[\w\-_]+""").setGaps(false) 
    val tokenizedData_Df1 = tokenizer.transform(entityProfile_Df1)
    val tokenizedData_Df2 =  tokenizer.transform(entityProfile_Df2)
    val cleanData_Df1 = remove_stopwords(tokenizedData_Df1)
    val cleanData_Df2 =  remove_stopwords(tokenizedData_Df2)
    val cleandfrdd = cleanData_Df1.union(cleanData_Df2).select("filtered_words").distinct.rdd    
    val vocab = calculate_Vocabsize(cleandfrdd) 
    val vocab_size =  Math.round(factor*vocab).toInt
    println("vocab_size: "+vocab_size)
    val cleanData_DF = cleanData_Df1.union(cleanData_Df2).distinct()
    
    val cvModel = new CountVectorizer().setInputCol("filtered_words").setOutputCol("Features").setVocabSize(Math.round(factor*vocab_size).toInt).setMinDF(1).fit(cleanData_DF)
    val isNoneZeroVector = udf({v: Vector => v.numNonzeros > 0}, DataTypes.BooleanType)
    val featuredData_cvDf1 = cvModel.transform(cleanData_Df1).filter(isNoneZeroVector(col("Features"))).select(col("entities"), col("Features"))
    val featuredData_cvDf2 = cvModel.transform(cleanData_Df2).filter(isNoneZeroVector(col("Features"))).select(col("entities"), col("Features"))
    return (featuredData_cvDf1, featuredData_cvDf2)
  }
  
  def minHashLSH_clean(featuredData_Df1: DataFrame, featuredData_Df2: DataFrame): (MinHashLSHModel, DataFrame, DataFrame) = {
    val mh = new MinHashLSH().setNumHashTables(3).setInputCol("Features").setOutputCol("HashedValues")
    val featuredData_DF =  featuredData_Df1.union(featuredData_Df2).distinct()
    val model = mh.fit(featuredData_DF)
    val transformedData_Df1 = model.transform(featuredData_Df1)
    val transformedData_Df2 = model.transform(featuredData_Df2)
    return (model, transformedData_Df1, transformedData_Df2)
  }

  def approxSimilarityJoin(model: MinHashLSHModel, transformedData_Df1: DataFrame, transformedData_Df2: DataFrame, threshold:Double): Dataset[_] = {
   return model.approxSimilarityJoin(transformedData_Df1, transformedData_Df2, threshold)
  }
  
  def matchentities(spark:SparkSession,dataset:Dataset[_], df_of_ground:DataFrame, output_path:String) = {
    val refined_entities_dataset = dataset.select(col("datasetA.entities").alias("entity1"),col("datasetB.entities").alias("entity2"),col("distCol"))
    val teacher = df_of_ground.rdd
    val predicted = refined_entities_dataset.drop("distCol").rdd
    evaluation(teacher,predicted)
    println("***************************************************************************************")
  }
    def evaluation(teacher:RDD[Row], predicted:RDD[Row]) = {
    val truePositives = teacher.intersection(predicted).count
    println("***************************************************************************************")
    println("***************************************************************************************")
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
  
