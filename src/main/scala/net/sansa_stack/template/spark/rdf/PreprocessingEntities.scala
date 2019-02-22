package net.sansa_stack.template.spark.rdf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PreprocessingEntities {
  def run(spark: SparkSession, triplesRDD: RDD[org.apache.jena.graph.Triple]) = {
    val parsedTriplesRDD: RDD[(String, String, Object)] =  get_parsedTriples(triplesRDD)
    //parsedTriplesRDD.foreach(println)
    val extractedEntity_NoPredicates: RDD[(String, String)] = entityExtraction_NoPredicates(parsedTriplesRDD)
    //extractedEntity_NoPredicates.foreach(println)
    val extractedEntity_FilterPredicates: RDD[(String, String)] =  entityExtraction_FilterPredicates(parsedTriplesRDD)
    //extractedEntity_FilterPredicates.foreach(println)
    val extractedEntity_AllPredicates: RDD[(String, String)] = entityExtraction_AllPredicates(parsedTriplesRDD)
    //extractedEntity_AllPredicates.foreach(println)
    val extractedEntity_SPO: RDD[(String, String)] = entityExtraction_SPO(parsedTriplesRDD)
    //extractedEntity_SPO.foreach(println)
    LSHoperation.run(spark, extractedEntity_NoPredicates, extractedEntity_FilterPredicates, extractedEntity_AllPredicates, extractedEntity_SPO)
  }
  
  def get_parsedTriples(triplesRDD: RDD[org.apache.jena.graph.Triple]): RDD[(String, String, Object)] = {
    //Extracting the end part of Triples
    return triplesRDD.distinct()
    .filter(f => (f.getObject().isURI() || f.getObject.getLiteralLanguage=="en" || f.getObject.getLiteralLanguage==""))
      /* .filter(f=> !f.getPredicate.getURI.contains("#type"))
    .filter(f=> !f.getPredicate.getURI.contains("#subClassOf"))
    .filter(f=> !f.getPredicate.getURI.contains("#wasDerivedFrom"))
    .filter(f=> !f.getPredicate.getURI.contains("owl"))
    .filter(f=> !f.getPredicate.getURI.contains("#domain"))
    .filter(f=> !f.getPredicate.getURI.contains("#range"))
    .filter(f=> !f.getPredicate.getURI.contains("#subPropertyOf"))*/
      .map(f => {
        val s = f.getSubject.getURI.split("/").last
        val p = f.getPredicate.getURI.split("/").last
        if (f.getObject.isURI()) {
          val o = f.getObject.getURI.split("/").last
          (s, p, o)
        } else {
          val o = f.getObject.getLiteralValue
          (s, p, o)
        }
      })
    //parsedTriplesRDD.foreach(println)

    /**
     * val extractedEntities_2 = parsedTriplesRDD.groupBy(f=>{
     * f._1 //grouping based on subject
     * })
     *
     * extractedEntities_2.foreach(println)
     *
     */
  }

  def entityExtraction_NoPredicates(parsedTriplesRDD:RDD[(String, String, Object)]):RDD[(String, String)] = {
    return parsedTriplesRDD.map(f => {
      val key = f._1 + "" // Subject is the key
      // val value = f._2 + "  " + f._3 //Predicate + Object is the value
      val value = f._3 + ""
      (key, value)
    }).reduceByKey(_ + " " + _)
   // extractedEntities.foreach(println)
  }
  
  def entityExtraction_FilterPredicates(parsedTriplesRDD:RDD[(String, String, Object)]):RDD[(String, String)] = {
    val filterPredicates:List[String] = List("22-rdf-syntax-ns#type","rdf-schema#subClassOf","rdf-schema#label","prov#wasDerivedFrom","owl#disjointWith","rdf-schema#domain","rdf-schema#range","rdf-schema#subPropertyOf", "owl#equivalentClass")
    return parsedTriplesRDD.map(f => {
      val key = f._1 + "" // Subject is the key
      var value = f._2 + " " + f._3 //Predicate + Object is the value
      if(filterPredicates.contains(f._2))
        value = f._3+""
      (key, value)
    }).reduceByKey(_ + " " + _)
   // extractedEntities.foreach(println)
  }
  
  def entityExtraction_AllPredicates(parsedTriplesRDD:RDD[(String, String, Object)]):RDD[(String, String)] = {
    return parsedTriplesRDD.map(f => {
      val key = f._1 + "" // Subject is the key
      // val value = f._2 + "  " + f._3 //Predicate + Object is the value
      val value = f._2 + " " + f._3
      (key, value)
    }).reduceByKey(_ + " " + _)
    //extractedEntities.foreach(println)
  }
  
  def entityExtraction_SPO(parsedTriplesRDD:RDD[(String, String, Object)]):RDD[(String, String)] = {
    return parsedTriplesRDD.map(f=>{
      val key = f._1 + ""
      val value = f._2 + " " + f._3
      (key,value)
    }).reduceByKey(_ + " " + _)
    .map(f=>{
      val key = f._1 + ""
      val value  = f._1 + " "+ f._2
      (key,value)
    })
  }
}