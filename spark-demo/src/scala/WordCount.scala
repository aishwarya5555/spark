

package scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object ScalaWordCount {

def main(args: Array[String]) = {

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)

    //Read some example file to a test RDD
    val inputFile = sc.textFile("C:\\Users\\aishwarya\\eclipse-workspace\\spark-demo\\input.txt")

    val counts = inputFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    counts.foreach(println)

    //Stop the Spark context
    sc.stop
  }

}