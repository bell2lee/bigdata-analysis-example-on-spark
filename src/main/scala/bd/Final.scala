package bd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Final {
  val conf: SparkConf = new SparkConf().setAppName("Final")
  val sc = new SparkContext(conf)

  val bigDataSampleSource: RDD[String] = sc.textFile("hdfs:/final_data/bigdata-input.txt")
  val route: RDD[(String, String)] = bigDataSampleSource.map(line => line.split("\t")).map(line => (line(0), line(1)))

  def getLength(): Unit = { // 1
    println(route.count())
  }

  def getInputLoadCount(): Unit = { // 2
    val countedEnt = route.map(m => (m._1, 1)).reduceByKey((x, y) => x + y).sortBy(m => m._2, false)
    val top1Ent = countedEnt.first()
    println(countedEnt.filter(m => m._2 == top1Ent._2).collect.mkString)
  }

  def getOutputLoadCount(): Unit = { // 3
    val route_unqiue = route.map(line => line._1).distinct()
    println(route_unqiue.count())
  }

  def getMostMoveLoad(): Unit = { // 4
    val countedEnt = route.map(m => (m._1, 1)).reduceByKey((x, y) => x + y).sortBy(m => m._2, false)
    val top1Ent = countedEnt.first()
    println(countedEnt.filter(m => m._2 == top1Ent._2).collect.mkString)

    /*
    val countedRoute = route.countByKey()
    val top1 = countedRoute.maxBy(m => m._2)
    countedRoute.filter(m => m._2 == top1._2)
    */
  }

  def getMostDestinationLoad(): Unit = { // 5
    val countedExit = route.map(m => (m._2, 1)).reduceByKey((x, y) => x + y).sortBy(m => m._2, false)
    val top1Exit = countedExit.first()
    println(countedExit.filter(m => m._2 == top1Exit._2).collect.mkString)
  }

  val FILE_PATH:String = "hdfs:/final_data/bigdata-input.txt"
  val S3_FILE_PATH:String = "s3://leeky-us-east-1-share/bigdata-input.txt"

  def main(args: Array[String]): Unit = {
    val iters = 3
    val lines = sc.textFile("")
    val reads = lines.map(s => {
      val splited = s.split("\t")
      (splited(0), splited(1))
    })
    val distinctReads = reads.distinct()
    val groupedReads = distinctReads.groupByKey()

    var results = reads.mapValues(v => 1.0)

    for (i <- 1 to iters)
    {
      val startTime = System.currentTimeMillis()
      val weights = groupedReads.join(results).values.flatMap{ case(a, b) => a.map(x => (x, b / a.size))}

      results = weights.reduceByKey((a, b) => a + b).mapValues(y => 0.1 + 0.9 * y)

      val interimResult = results.take(10)
      interimResult.foreach(record => println(record._1 + " : " + record._2))
      val endTime = System.currentTimeMillis()
      println(i + " th interation took " + (endTime - startTime) / 1000 + " seconds")
    }

    // results.sortBy(m => m._2, false).take(10)
  }
}
