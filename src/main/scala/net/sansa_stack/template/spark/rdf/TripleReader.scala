package net.sansa_stack.template.spark.rdf

import java.net.URI

import scala.collection.mutable

import net.sansa_stack.rdf.spark.io._
import org.apache.spark.sql.SparkSession
import org.apache.jena.riot.Lang
import org.apache.spark.sql.SQLContext
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._
import java.io._
import java.util.concurrent.TimeUnit
import org.apache.log4j.Logger

object TripleReader {
  @transient lazy val consoleLog: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in1, config.in2, config.groundTruth, config.threshold1, config.threshold2, config.factor,config.vocab_size, config.out, config.option)
      case None =>
        println(parser.usage)
    }
  }

  def run(input1: String, input2:String, groundTruth:String, threshold1:Double, threshold2:Double, factor: Double,vocab_size:Long, out:String, option:Int): Unit = {

    val spark = SparkSession.builder
      .appName(s"Entity Resolution Model ")
     // .master("local[*]")
     .master("spark://172.18.160.16:3090")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "800")
      .config("spark.sql.autoBroadcastJoinThreshold", "304857600")
      .config("spark.executor.overhead.memory", "2048")
      .config("spark.driver.overhead.memory", "2048")
      .getOrCreate()

    println("======================================")
    println("|        Entity Resolution Model      |")
    println("======================================")

    val lang = Lang.NTRIPLES
    val sqlContext = new SQLContext(spark.sparkContext)

    //val triples = spark.rdf(lang)(input)

    //triples.take(5).foreach(println(_))

    //triplesRDD.saveAsTextFile(output)
    
    if(option == 1 || option == 2) 
    { // Check for the SPARK-ER Approach evaluation
     
    if(input1.endsWith(".csv"))
    {   
      val dataset_df1 = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      //.option("mode", "DROPMALFORMED")
      .load(input1)
      if(!input2.isEmpty() && input2.endsWith("csv"))
      {
        val dataset_df2 = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header","true")
        .option("inferSchema","true")
        //.option("mode", "DROPMALFORMED")
        .load(input2)
        if(!groundTruth.isEmpty() && groundTruth.endsWith(".csv"))
        {
          val teacher = sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .option("mode", "DROPMALFORMED")
          .load(groundTruth)
          val startTime = System.nanoTime()
          //1-attribute approach 
          if(option==1) {
          println("1-attribute approach")
          Preprocessing1_2attributes_approach.run(spark, dataset_df1, dataset_df2, teacher, threshold1, factor, out, option)
          runTime(System.nanoTime() - startTime)
          }
          //2-attribute approach
          if(option == 2) {
          println("2-attribute approach")
          Preprocessing1_2attributes_approach.run(spark, dataset_df1, dataset_df2, teacher, threshold2, factor, out, option)
          runTime(System.nanoTime() - startTime)
          }
        }
      }
     }
   }
   else if(option == 3)
    {
    val triples_entities1 = spark.rdf(lang)(input1)
    val triples_entities2 = spark.rdf(lang)(input2)
    val teacher = sqlContext.read
          .format("com.databricks.spark.csv")
         // .option("header", "false")
          .option("mode", "DROPMALFORMED")
          .option("delimiter", "\t")
          //.option("delimiter", " ")
          .load(groundTruth)
    //println(teacher.count()).master("spark://172.18.160.16:3090")
    println("teacher result")
   // teacher.show(false)
    val startTime = System.nanoTime()
    Preprocessing_RDF_Data.run(spark, triples_entities1,triples_entities2, teacher, threshold1, factor, threshold2,vocab_size, out, startTime)
    runTime(System.nanoTime() - startTime)
    }  
    spark.stop
  }
  
  // remove path files
  def removePathFiles(root: Path): Unit = {
    if (Files.exists(root)) {
      Files.walkFileTree(root, new SimpleFileVisitor[Path] {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      })
    }
  }
//calculate run time
  def runTime(processedTime: Long): Unit = {
    val milliseconds = TimeUnit.MILLISECONDS.convert(processedTime, TimeUnit.NANOSECONDS)
    val seconds = Math.floor(milliseconds / 1000d + .5d).toInt
    val minutes = TimeUnit.MINUTES.convert(processedTime, TimeUnit.NANOSECONDS)

    if (milliseconds >= 0) {
      consoleLog.info(s"Processed Time (MILLISECONDS): $milliseconds")

      if (seconds > 0) {
        consoleLog.info(s"Processed Time (SECONDS): $seconds approx.")

        if (minutes > 0) {
          consoleLog.info(s"Processed Time (MINUTES): $minutes")
        }
      }
    }
  }
  
  case class Config(in1: String = "", in2: String = "", groundTruth:String = "", threshold1:Double=0.0, threshold2:Double = 0.0, factor:Double=0.0,vocab_size:Long=0, out:String = "", option:Int=0)

  val parser = new scopt.OptionParser[Config]("Entity Resolution Model") {

    head(" Entity Resolution Model")

    opt[String]('i', "input1").required().valueName("<path>").
      action((x, c) => c.copy(in1 = x)).
      text("path to file that contains the data (in N-Triples or csv format)")
      
    opt[String]('c', "input2").required().valueName("<path>").
      action((x, c) => c.copy(in2 = x)).
      text("path to file that contains the data for comparison (in N-Triples or csv format)")
    
    opt[String]('g', "groundTruth").required().valueName("<path>").
      action((x,c) => c.copy(groundTruth = x)).
      text("path to Truth data for evaluating our results")
      
    opt[Double]('t', "threshold1").required().
      action((x,c) => c.copy(threshold1 = x)).
      text("threshold for entity profile match by subjects")
    
    opt[Double]('s', "threshold2").required().
      action((x,c) => c.copy(threshold2 = x)).
      text("threshold for entity profile match by objects")
    
    opt[Double]('f', "factor").required().
      action((x,c) => c.copy(factor = x)).
      text("factor for vocabsize or jsimilarity for predicate match")
      
    opt[Int]('v', "vocab_size").required()
      .action((x,c) => c.copy(vocab_size = x)).
      text("option for approach")
      
    opt[String]('o', "output_folder").required().valueName("<directory>")
      .action((x,c) => c.copy(out = x)).
      text("the output directory")
    
    opt[Int]('z', "option").required()
      .action((x,c) => c.copy(option = x)).
      text("option for approach")

    help("help").text("prints this usage text")
  }
}
