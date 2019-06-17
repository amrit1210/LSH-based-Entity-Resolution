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
  def run(spark:SparkSession, triplesRDD1:RDD[Triple], triplesRDD2:RDD[Triple], teacher:DataFrame, threshold_subject:Double, jsimilarity_predicate:Double, threshold_object:Double, output_path:String) {
    val entity_profiles1 =  get_entity_profiles(triplesRDD1)
    val entity_profiles2 =  get_entity_profiles(triplesRDD2)
    //println("entity profiles in first dataset:"+entity_profiles1.count())
    //println("entity profiles in second dataset:"+entity_profiles2.count())
    println("LSH_subjects")
    //parseGroundTruth(teacher, output_path)
    //getGroundTruth(entity_profiles1, entity_profiles2, output_path)
    //entity_profiles1.foreach(println)
   val ds_subjects1:RDD[(String, String, String, String)] = lsh_subjects1(spark, entity_profiles1, entity_profiles2, threshold_subject)
   val ds_subjects = ds_subjects1.repartition(400).persist(StorageLevel.MEMORY_AND_DISK) 
    println("subject result")
    //ds_subjects.columns.foreach(println)
    //println(ds_subjects.count())
    //ds_subjects1.show(false)
   // val ds_subjects_rdd = ds_subjects.rdd
  /*  val results_subjects = ds_subjects.map(f=>{
      (f._1,f._3) //only for lsh1subjects
    })*/
  //  results_subjects.take(4).foreach(println)
    println("ds_subjects_received")
    //println("LSH_subjects results:"+ds_subjects1.count())
    val refined_data_pred = get_similar_predicates1(ds_subjects,jsimilarity_predicate)
    val ds_predicates1 = refined_data_pred.repartition(400).persist(StorageLevel.MEMORY_AND_DISK)
    val ds_predicates = ds_predicates1.map(f=>{(f._1,f._3)})
    
    println("Jsimilarity predicates received")
   // println("JaccardSimilarity Predicates REsults:"+ds_predicates.count())
 
    evaluation_predicates(ds_predicates, teacher, output_path) //trial to check subjects and predicates
   // val ds_pred = ds_predicates.repartition(400).persist(StorageLevel.MEMORY_AND_DISK)
    //val refined_data_pred = get_similar_predicates(ds_subjects1,jsimilarity_predicate)
    val refined_objects = get_similarity_objects(ds_predicates1,threshold_object)
  //  val results = get_similarity_objects(refined_data_pred,threshold_object)
   
    //val results = get_similarity_objects_(refined_objects, threshold_object)
    println("Evaluation of objects")
    evaluation(refined_objects, teacher, output_path)
    
    //results.foreach(println)
    //lsh_objects(spark, refined_objects,threshold_object)
  }
  
  def get_entity_profiles(triplesRDD:RDD[Triple]): RDD[(String, String)]  = {
    //triplesRDD.foreach(println)
    //println("Triples in dataset:"+triplesRDD.count())
     val entity = triplesRDD.distinct()
    // .filter(f=>{!f.getSubject.isBlank() && !f.getObject.isBlank()})
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
       // (key,(pred,obj))
        val value = pred + ":" +obj //predicate and object are seperated by ':'
        (key,value)
      }
      else
      {
        val obj =  f.getObject.getLiteral.toString().split(Array('^','@')).head.trim()
        //(key,(pred, obj))
        val value = pred + ":" +obj.replace(":", "")
        (key,value)
      }
     // val value = List(f)
    //  (key,value)
    }).reduceByKey(_+" , "+_) // triples seperated by ' , '
    return entity
  }
  
  def lsh_subjects1(spark:SparkSession, entites_RDD1:RDD[(String, String)], entities_RDD2:RDD[(String, String)], threshold_subject:Double):RDD[(String, String, String, String)] = {
  
    val ent_sub1 = entites_RDD1.map(f=>{(f._1,f._1.split("_"))})
    val part_rdd1 = new RangePartitioner(400, ent_sub1)
    val partitioned_rdd1 = ent_sub1.partitionBy(part_rdd1).persist(StorageLevel.MEMORY_AND_DISK)
    
    val ent_sub2 = entities_RDD2.map(f=>{(f._1,f._1.split("_"))})
    val part_rdd2 = new RangePartitioner(400,ent_sub2)
    val partitioned_rdd2 =  ent_sub2.partitionBy(part_rdd2).persist(StorageLevel.MEMORY_AND_DISK)
    
    //ent_sub1_RDD.unpersist()
    val entities_Df1 =  spark.createDataFrame(partitioned_rdd1).toDF("entities","ent_sub")
    //ent_sub2_RDD.unpersist()
    val entities_Df2 =  spark.createDataFrame(partitioned_rdd2).toDF("entities","ent_sub")

    val (hashfeatured_entities_Df1:DataFrame, hashfeatured_entities_Df2:DataFrame) = applyHashingTf_sub("ent_sub", "features", 3357323.toInt, entities_Df1, entities_Df2)
   /* val hashingTf =  new HashingTF().setInputCol("ent_sub").setOutputCol("features").setNumFeatures(entities_Df1.count().toInt)
    val hashfeatured_entities_Df1 = hashingTf.transform(entities_Df1)
    val hashfeatured_entities_Df2 = hashingTf.transform(entities_Df2)
    //hashfeatured_entities_Df1.show(false)
    val cv_Model =  new CountVectorizer().setInputCol("ent_sub").setOutputCol("features").setVocabSize(800).setMinDF(1).fit(entities_Df1)
    val cvfeatured_entites_Df1 = cv_Model.transform(entities_Df1)
    val cvfeatured_entites_Df2 = cv_Model.transform(entities_Df2)
    cvfeatured_entites_Df1.show(false)*/
    val (model_sub: MinHashLSHModel, transformed_sub_Df1:DataFrame, transformed_sub_Df2:DataFrame) = minHashLSH(hashfeatured_entities_Df1, hashfeatured_entities_Df2)
    val ds_subjects = approxsimilarityjoin(model_sub, transformed_sub_Df1, transformed_sub_Df2, threshold_subject)
    val ds_subjects_rdd = ds_subjects.rdd
    val ds_subjects_data1 = ds_subjects_rdd.map(f=>{(f.get(0).toString(), f.get(1).toString())}).join(entites_RDD1)
    val ds_subjects_data2 = ds_subjects_data1.map(f=>{(f._2._1,(f._1,f._2._2))}).join(entities_RDD2)
    val ds_subjects_data = ds_subjects_data2.map(f=>{(f._2._1._1,f._2._1._2,f._1,f._2._2)})
   // return approxsimilarityjoin(model_sub, transformed_sub_Df1, transformed_sub_Df2, threshold_subject)
    return ds_subjects_data
  }
  
  def lsh_subjects(spark:SparkSession, entites_RDD1:RDD[(String, String)], entities_RDD2:RDD[(String, String)], threshold_subject:Double):DataFrame = {
  
    val part_rdd1 = new RangePartitioner(400, entites_RDD1)
    val partitioned_rdd1 = entites_RDD1.partitionBy(part_rdd1).persist(StorageLevel.MEMORY_AND_DISK)
    val ent_sub1_RDD =  partitioned_rdd1.map(f=>{
      (f._1, f._1.split("_"), f._2)
    })
    
    val part_rdd2 = new RangePartitioner(400,entities_RDD2)
    val partitioned_rdd2 =  entities_RDD2.partitionBy(part_rdd2).persist(StorageLevel.MEMORY_AND_DISK)
    val ent_sub2_RDD =  partitioned_rdd2.map(f=>{
      (f._1, f._1.split("_"), f._2)
    })
    //ent_sub1_RDD.unpersist()
    val entities_Df1 =  spark.createDataFrame(ent_sub1_RDD).toDF("entities","ent_sub","entity_data")
    println("entities df1")
    entities_Df1.show(false)
    //ent_sub2_RDD.unpersist()
    val entities_Df2 =  spark.createDataFrame(ent_sub2_RDD).toDF("entities","ent_sub","entity_data")
    println("entities df2")
    entities_Df2.show(false)
    val (hashfeatured_entities_Df1:DataFrame, hashfeatured_entities_Df2:DataFrame) = applyHashingTf_sub("ent_sub", "features", 3357323.toInt, entities_Df1, entities_Df2)
    println("hashfeatured_entities df1")
    hashfeatured_entities_Df1.show(false)
    println("hashfeatured_entities df2")
    hashfeatured_entities_Df2.show(false)
   /* val hashingTf =  new HashingTF().setInputCol("ent_sub").setOutputCol("features").setNumFeatures(entities_Df1.count().toInt)
    val hashfeatured_entities_Df1 = hashingTf.transform(entities_Df1)
    val hashfeatured_entities_Df2 = hashingTf.transform(entities_Df2)
    //hashfeatured_entities_Df1.show(false)
    val cv_Model =  new CountVectorizer().setInputCol("ent_sub").setOutputCol("features").setVocabSize(800).setMinDF(1).fit(entities_Df1)
    val cvfeatured_entites_Df1 = cv_Model.transform(entities_Df1)
    val cvfeatured_entites_Df2 = cv_Model.transform(entities_Df2)
    cvfeatured_entites_Df1.show(false)*/
    val (model_sub: MinHashLSHModel, transformed_sub_Df1:DataFrame, transformed_sub_Df2:DataFrame) = minHashLSH(hashfeatured_entities_Df1, hashfeatured_entities_Df2)
    println("transformed_sub_df1")
    transformed_sub_Df1.show(false)
    println("transformed_sub_df2")
    transformed_sub_Df2.show(false)
    return approxsimilarityjoin(model_sub, transformed_sub_Df1, transformed_sub_Df2, threshold_subject)
  }
  def applyHashingTf_sub(inp_col:String, out_col:String,numFeatures:Int,data1:DataFrame,data2:DataFrame):(DataFrame,DataFrame) = {
    val hashingTf =  new HashingTF().setInputCol(inp_col).setOutputCol(out_col).setNumFeatures(numFeatures)
   // val data = data1.union(data2).distinct()
   // val hashingTf =  new CountVectorizer().setInputCol(inp_col).setOutputCol(out_col).setVocabSize(numFeatures).setMinDF(1).fit(data)
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
  //val refined_ds = dataset.select(col("datasetA.entities").alias("entity1"),col("datasetA.entity_data").alias("entity1_data"),col("datasetB.entities").alias("entity2"),col("datasetB.entity_data").alias("entity2_data"))
  val refined_ds = dataset.select(col("datasetA.entities").alias("entity1"),col("datasetB.entities").alias("entity2")) //only for lsh1subjects
  return refined_ds
  }
  
  def get_similar_predicates1(similar_subj_rdd:RDD[(String, String, String, String)],jSimilartiy:Double):RDD[(String, List[String], String, List[String], List[String], Double)] = {
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
      //val union_pred =  list_pred1.union(list_pred2).distinct.length.toDouble
      val similarity = intersect_pred.length.toDouble/union_pred.toDouble
      
      //(sub1,pred_obj1, sub2, pred_obj2)
      (sub1,pred_obj1, sub2, pred_obj2, intersect_pred, similarity)
    })
    println("Refined_data_sub received")
   similar_subj_rdd.unpersist() //need to test this
   val refined_data_pred =  refined_data_sub.filter(f=>f._6>=jSimilartiy)
   println("Checked jsimilarity for predicates")
    //refined_data_sub.foreach(println)
   //refined_data_pred.foreach(println)
   return refined_data_pred
  }
  def get_similar_predicates(ds:DataFrame,jSimilartiy:Double):RDD[(String, List[String], String, List[String], List[String], Double)] = {
    val similar_subj_rdd = ds.rdd
    val refined_data_sub = similar_subj_rdd.map(f=>{
      val sub1 = f.get(0).toString()
      val s_data1 = f.get(1).toString()
      val sub2 = f.get(2).toString()
      val s_data2 = f.get(3).toString()
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
      //val union_pred =  list_pred1.union(list_pred2).distinct.length.toDouble
      val similarity = intersect_pred.length.toDouble/union_pred.toDouble
      //(sub1,pred_obj1, sub2, pred_obj2)
      (sub1,pred_obj1, sub2, pred_obj2, intersect_pred, similarity)
    })
   val refined_data_pred =  refined_data_sub.filter(f=>f._6>=jSimilartiy)
    //refined_data_sub.foreach(println)
   //refined_data_pred.foreach(println)
   return refined_data_pred
  }
  
  def get_similarity_objects(ds_pred:RDD[(String, List[String], String, List[String], List[String], Double)],threshold_objects:Double):RDD[(String, String, Double)] = {
     println("Check object similarity")
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
      //val union_obj = obj1.union(obj2).distinct.length.toDouble
      val similarity = intersect_obj.toDouble/union_obj.toDouble
      
      (sub1, sub2, similarity)
    })
    ds_pred.unpersist() //need to test
    val results = mapped_objects.filter(f=>f._3>=threshold_objects)
    return results
  }
  
  def get_similarity_objects_(refined_objects: RDD[(String, String, String, String)], threshold_objects:Double):RDD[(String, String,Double)] = {
    val mapped_objects = refined_objects.map(f=> {
      val entity1 = f._1
      val obj1 = f._2.split(" ").toList.distinct
      val entity2 = f._3
      val obj2 = f._4.split(" ").toList.distinct
      
      val intersect_obj = obj1.intersect(obj2).length
      val union_obj = obj1.length +obj2.length - intersect_obj
      //val union_obj = obj1.union(obj2).distinct.length.toDouble
      val similarity = intersect_obj.toDouble/union_obj.toDouble
      (entity1,entity2,similarity)
    })
    
   val results = mapped_objects.filter(f=>f._3>=threshold_objects)
   return results
  }
  
  def getGroundTruth(ds1:RDD[(String, String)], ds2:RDD[(String, String)], output:String){
   val one = ds1.map(f=>f._1)
   val two = ds2.map(f=>f._1)
   val gt = one.intersection(two)
   val gt_infobox = gt.map(f=>(f+"\t"+f))
   gt_infobox.coalesce(1).saveAsTextFile(output)
  }
  
  def parseGroundTruth(teacher:DataFrame, output_path:String) {
    val gt_rdd = teacher.rdd
    val out = gt_rdd.map(f=>{
      val lwapis1 = f.get(1).toString().split("/").last.replace(">", "").trim()
      val dbpedia = f.get(0).toString().split("/").last.replace(">", "").trim()
      (dbpedia+"\t"+lwapis1)
    })
    out.coalesce(1).saveAsTextFile(output_path)
  }
  
  def evaluation(result:RDD[(String,String,Double)], teacher:DataFrame, output_path:String) = {
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
    //truePositives.foreach(println)
   // println("TruePositives:"+ truePositives.length)
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
    predicted_rdd.coalesce(1).saveAsTextFile(output_path)
    println("Output Saved!")
  }
  
  def evaluation_predicates(result:RDD[(String, String)], teacher:DataFrame, output_path:String) = {
    println("Evaluation phase for predicates")
    val teacher_rdd = teacher.rdd
    val actual_rdd = teacher_rdd.map(f=>{
      (f.get(0).toString(),f.get(1).toString())
    })
   
    println("***************************************************************************************")
    println("***************************************************************************************")
    //truePositives.foreach(println)
   // println("TruePositives:"+ truePositives.length)
    val actual = actual_rdd.count()
    val results = result.count()
    println("Actual: "+ actual)
    println("Predicted: "+ results)
    val truePositives = actual_rdd.intersection(result).count
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
 /* def lsh_objects(spark:SparkSession, entity_ob_RDD:RDD[(String,String, String, String)], threshold_objects:Double) = {
  val entity_ob_df = spark.createDataFrame(entity_ob_RDD).toDF("entity1","entity1_obs_data", "entity2", "entity2_obs_data")
  val ent_tokenized1_df = applyRegexTokenizer_object("entity1_obs_data", "entity1_data", entity_ob_df)
  val ent_tokenized_df = applyRegexTokenizer_object("entity2_obs_data", "entity2_data", ent_tokenized1_df)
  val hashfeatured_entities_Df1 = applyHashingTf_object("entity1_data", "entity1_features", entity_ob_df.count().toInt, ent_tokenized_df)
  val hashfeatured_entities_Df = applyHashingTf_object("entity2_data", "entity2_features", entity_ob_df.count().toInt, hashfeatured_entities_Df1)
  val (model1:MinHashLSHModel,transformed_entities_Df1:DataFrame) = applyMinHashLSH_object("entity1_features", "entity1_hashedValues", hashfeatured_entities_Df)
  val (model:MinHashLSHModel, transformed_entities_Df:DataFrame) = applyMinHashLSH_object("entity2_features", "entity2_hashedValues", transformed_entities_Df1)
  approxsimilarityjoin_object(spark, model, transformed_entities_Df, threshold_objects)
  }
  
  def applyMinHashLSH_object(inp_col:String, out_col:String, data:DataFrame): (MinHashLSHModel, DataFrame) = {
  val mh = new MinHashLSH().setNumHashTables(3).setInputCol(inp_col).setOutputCol(out_col)
  val model = mh.fit(data)
  val transformed_entities_Df = model.transform(data)
  return (model, transformed_entities_Df)
  }
  
  def applyRegexTokenizer_object(inp_col:String, out_col:String, data:DataFrame):DataFrame = {
  val regextokenizer = new RegexTokenizer().setInputCol(inp_col).setOutputCol(out_col).setPattern("""[\w\-_]+""").setGaps(false)
  val ent_tokenized_df = regextokenizer.transform(data)
  return ent_tokenized_df
  }
  
  def applyHashingTf_object(inp_col:String, out_col:String, numfeatures:Int, data:DataFrame):DataFrame = {
  val hashingTf =  new HashingTF().setInputCol(inp_col).setOutputCol(out_col).setNumFeatures(numfeatures)
  val hashfeatured_entities = hashingTf.transform(data)
  return hashfeatured_entities
  }
  
  def approxsimilarityjoin_object(spark:SparkSession,model:MinHashLSHModel, data:DataFrame, threshold:Double) {
    val columns = data.columns
    val data_rdd = data.rdd
    data_rdd.map(f=>{
   
     val entity_df1 = Seq((f.get(0), f.get(4), f.get(6), f.get(8))).toDF("entity1","entity1_objects","entity1_features", "entity1_hashedValues")
     val entity_df2 = Seq((f.get(2), f.get(5), f.get(7), f.get(9))).toDF("entity2", "entity2_objects","entity2_features", "entity2_hashedValues")
     model.approxSimilarityJoin(entity_df1, entity_df2, threshold) 
    }).foreach(println)
    
  }*/
}