package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Triple
import org.apache.spark.ml.feature.{ Tokenizer, HashingTF }
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.feature.MinHashLSHModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.sql.Row
import org.apache.spark.RangePartitioner
import org.apache.spark.storage.StorageLevel
import org.apache.spark.RangePartitioner
import org.apache.spark.RangePartitioner

object Preprocessing_RDF_Data {
  def run(spark:SparkSession, triplesRDD1:RDD[Triple], triplesRDD2:RDD[Triple], teacher:DataFrame, threshold_subject:Double, jsimilarity_predicate:Double, threshold_object:Double,vocab_size:Long, output_path:String, startTime:Long) {
    val entity_profiles1 =  get_entity_profiles(triplesRDD1)
    val entity_profiles2 =  get_entity_profiles(triplesRDD2)
    val ds_subjects1:RDD[(String, String, String, String)] = lsh_subjects(spark, entity_profiles1, entity_profiles2, threshold_subject,vocab_size)
    val ds_subjects = ds_subjects1.repartition(600).persist(StorageLevel.MEMORY_AND_DISK) 
    val refined_data_pred = get_similar_predicates(ds_subjects,jsimilarity_predicate)
    val ds_predicates = refined_data_pred.repartition(400).persist(StorageLevel.MEMORY_AND_DISK)
    val refined_objects = get_similarity_objects(ds_predicates,threshold_object)
    evaluation(refined_objects, teacher, output_path,startTime)
  }
  
  def get_entity_profiles(triplesRDD:RDD[Triple]): RDD[(String, String)]  = {
     val entity = triplesRDD.distinct() //filteration for dbpedia data common predicates related to wikidata
     .filter(f=>{!f.getPredicate.getURI.contains("owl:sameas") && !f.getPredicate.getURI.contains("wikiPageID")&& !f.getPredicate.getURI.contains("wikiPageRevisionID") && !f.getPredicate.getURI.contains("wikiPageRevisionLink")})
     .filter(f=>{!f.getPredicate.getURI.contains("wikiPageUsesTemplate") && !f.getPredicate.getURI.contains("wikiPageHistoryLink")&& !f.getPredicate.getURI.contains("wikiPageExternalLink") && !f.getPredicate.getURI.contains("wikiPageEditLink")})
     .filter(f=>{!f.getPredicate.getURI.contains("wikiPageExtracted") && !f.getPredicate.getURI.contains("wikiPageLength")&& !f.getPredicate.getURI.contains("wikiPageModified") && !f.getPredicate.getURI.contains("wikiPageOutDegree") && !f.getPredicate.getURI.contains("wikiPageRedirects")})
     .filter(f => (f.getObject().isURI() || f.getObject.getLiteralLanguage=="en" || f.getObject.getLiteralLanguage==""))
     .map(f=>{
      val key = f.getSubject.getURI.split("/").last.trim()
      val pred = f.getPredicate.getURI.split(Array('/', '#')).last.trim()
      if(f.getObject.isURI())
      {
        val obj =  f.getObject.getURI.split("/").last.trim()
        val value = pred + ":" +obj //predicate and object are seperated by ':'
        (key,value)
      }
      else
      {
        val obj =  f.getObject.getLiteral.toString().split(Array('^','@')).head.trim()
        val value = pred + ":" +obj.replace(":", "")
        (key,value)
      }
    }).reduceByKey(_+" , "+_) // triples seperated by ' , '
    return entity
  }
  
  def lsh_subjects(spark:SparkSession, entites_RDD1:RDD[(String, String)], entities_RDD2:RDD[(String, String)], threshold_subject:Double, vocab_size:Long):RDD[(String, String, String, String)] = {
  
    val ent_sub1 = entites_RDD1.map(f=>{(f._1,f._1.split("_"))})
    val part_rdd1 = new RangePartitioner(400, ent_sub1)
    val partitioned_rdd1 = ent_sub1.partitionBy(part_rdd1).persist(StorageLevel.MEMORY_AND_DISK)
    
    val ent_sub2 = entities_RDD2.map(f=>{(f._1,f._1.split("_"))})
    val part_rdd2 = new RangePartitioner(400,ent_sub2)
    val partitioned_rdd2 =  ent_sub2.partitionBy(part_rdd2).persist(StorageLevel.MEMORY_AND_DISK)
    
    val entities_Df1 =  spark.createDataFrame(partitioned_rdd1).toDF("entities","ent_sub")
    val entities_Df2 =  spark.createDataFrame(partitioned_rdd2).toDF("entities","ent_sub")

    val (hashfeatured_entities_Df1:DataFrame, hashfeatured_entities_Df2:DataFrame) = applyHashingTf_sub("ent_sub", "features", 3357323.toInt, entities_Df1, entities_Df2)
    val (model_sub: MinHashLSHModel, transformed_sub_Df1:DataFrame, transformed_sub_Df2:DataFrame) = minHashLSH(hashfeatured_entities_Df1, hashfeatured_entities_Df2)
    val ds_subjects1 = approxsimilarityjoin(model_sub, transformed_sub_Df1, transformed_sub_Df2, threshold_subject)
    val ds_subjects_rdd = ds_subjects1.rdd
    val ds_subjects_data1 = ds_subjects_rdd.map(f=>{(f.get(0).toString(), f.get(1).toString())}).join(entites_RDD1)
    val ds_subjects_data2 = ds_subjects_data1.map(f=>{(f._2._1,(f._1,f._2._2))}).join(entities_RDD2)
    val ds_subjects_data = ds_subjects_data2.map(f=>{(f._2._1._1,f._2._1._2,f._1,f._2._2)})
    return ds_subjects_data
  }
  
  def applyHashingTf_sub(inp_col:String, out_col:String,numFeatures:Int,data1:DataFrame,data2:DataFrame):(DataFrame,DataFrame) = {
    val hashingTf =  new HashingTF().setInputCol(inp_col).setOutputCol(out_col).setNumFeatures(numFeatures)
    /*If we want to use CountVectorizer model
    val data = data1.union(data2).distinct()
    val hashingTf =  new CountVectorizer().setInputCol(inp_col).setOutputCol(out_col).setVocabSize(numFeatures).setMinDF(1).fit(data)*/
    val hashfeatured_entities_Df1 = hashingTf.transform(data1)
    val hashfeatured_entities_Df2 = hashingTf.transform(data2)
    return (hashfeatured_entities_Df1, hashfeatured_entities_Df2)
  }
  
  def minHashLSH(featured_entites_Df1:DataFrame, featured_entites_Df2:DataFrame): (MinHashLSHModel, DataFrame, DataFrame) = {
    val mh = new MinHashLSH().setNumHashTables(3).setInputCol("features").setOutputCol("hashed_values")
    val featured_data = featured_entites_Df1.union(featured_entites_Df2).distinct()
    val model = mh.fit(featured_data)
    val transformed_entities_Df1 = model.transform(featured_entites_Df1)
    val transformed_entities_Df2 = model.transform(featured_entites_Df2)
    return (model, transformed_entities_Df1, transformed_entities_Df2)
  }
  
  def approxsimilarityjoin(model:MinHashLSHModel, df1:DataFrame, df2:DataFrame, threshold:Double):DataFrame = {
  val dataset =  model.approxSimilarityJoin(df1, df2, threshold)
  val refined_ds = dataset.select(col("datasetA.entities").alias("entity1"),col("datasetB.entities").alias("entity2")) //only for lsh1subjects
  return refined_ds
  }
  
  def get_similar_predicates(similar_subj_rdd:RDD[(String, String, String, String)],jSimilartiy:Double):RDD[(String, List[String], String, List[String], List[String], Double)] = {
    val refined_data_sub = similar_subj_rdd.map(f=>{
      val sub1 = f._1
      val s_data1 = f._2
      val sub2 = f._3
      val s_data2 = f._4
      val pred_obj1 = s_data1.split(" , ").toList
      val pred_obj2 = s_data2.split(" , ").toList
      var list_pred1 = List[String]()
      var list_pred2 = List[String]()
      for(x<-pred_obj1) {
        list_pred1 = list_pred1:+x.split(":").head
      }
      for(x<-pred_obj2) {
        list_pred2 = list_pred2:+x.split(":").head
      }
      val intersect_pred = list_pred1.intersect(list_pred2)
      val union_pred = list_pred1.length + list_pred2.length - intersect_pred.length

      val similarity = intersect_pred.length.toDouble/union_pred.toDouble
      
      (sub1,pred_obj1, sub2, pred_obj2, intersect_pred, similarity)
    })
   similar_subj_rdd.unpersist() 
   val refined_data_pred =  refined_data_sub.filter(f=>f._6>=jSimilartiy)
   return refined_data_pred
  }
  
  def get_similarity_objects(ds_pred:RDD[(String, List[String], String, List[String], List[String], Double)],threshold_objects:Double):RDD[(String, String, Double)] = {
      val mapped_objects =  ds_pred.map(f=>{
      val sub1 = f._1
      val pred_obj1 = f._2
      val sub2 = f._3
      val pred_obj2 = f._4
      val common_pred = f._5
      
      var obj1:String = " "
      var obj2:String = " "
      
      for(x<-pred_obj1) {
        val pred = x.split(":").head
        val obj = x.split(":").last
        if(common_pred.contains(pred))
          obj1 = obj1 + " " + obj
      }
      
      for(x<-pred_obj2) {
        val pred = x.split(":").head
        val obj = x.split(":").last
        if(common_pred.contains(pred))
          obj2 = obj2 + " " + obj
      }
      
      val sub_obj1 = obj1.trim().split(" ").toList.distinct
      val sub_obj2 = obj2.trim().split(" ").toList.distinct
      
      val intersect_obj = sub_obj1.intersect(sub_obj2).length
      val union_obj = sub_obj1.length +sub_obj2.length - intersect_obj
 
      val similarity = intersect_obj.toDouble/union_obj.toDouble
      
      (sub1, sub2, similarity)
    })
    ds_pred.unpersist() 
    val results = mapped_objects.filter(f=>f._3>=threshold_objects)
    return results
  }
  
  def evaluation(result:RDD[(String,String,Double)], teacher:DataFrame, output_path:String,startTime:Long) = {
    val predicted_rdd = result.map(f=>{
      (f._1,f._2)
    })
    val teacher_rdd = teacher.rdd
    val actual_rdd = teacher_rdd.map(f=>{
      (f.get(0).toString(),f.get(1).toString())
    })
    val truePositives = actual_rdd.intersection(predicted_rdd).count
    println("***************************************************************************************")
    println("***************************************************************************************")

    val actual = actual_rdd.count()
    val results = predicted_rdd.count()
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
    runTime(System.nanoTime() - startTime)
    predicted_rdd.coalesce(1).saveAsTextFile(output_path)
    println("Output Saved!")
  }

  //calculate run time
  def runTime(processedTime: Long): Unit = {
    import java.util.concurrent.TimeUnit
    val milliseconds = TimeUnit.MILLISECONDS.convert(processedTime, TimeUnit.NANOSECONDS)
    val seconds = Math.floor(milliseconds / 1000d + .5d).toInt
    val minutes = TimeUnit.MINUTES.convert(processedTime, TimeUnit.NANOSECONDS)

    if (milliseconds >= 0) {
      println("Processed Time (MILLISECONDS):",milliseconds)

      if (seconds > 0) {
       println("Processed Time (SECONDS):approx.",seconds)

        if (minutes > 0) {
          println("Processed Time (MINUTES):",minutes)
        }
      }
    }
  }
}
