package scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SplitsTheText {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SplitsTheText")
    //  .setMaster("local");
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR")

    val nonEmptyLines = sc.textFile("hdfs:///bigdatapgp/common_folder/assignment4/text_dataset/macbeth.txt")
      .filter(line => !line.isEmpty());
    //val nonEmptyLines = sc.textFile("input.txt").filter(line => !line.isEmpty())

    val words = nonEmptyLines.flatMap(line => line.split(" "))
      .map(w => w.replaceAll("[^a-zA-Z0-9]+", "").toLowerCase())

    val wordsStartWithVowels = words
      .filter(w => w.startsWith("a") || w.startsWith("e") ||
        w.startsWith("i") || w.startsWith("o") || w.startsWith("u"))

    val sorted = wordsStartWithVowels.map(s => (s, 1)).reduceByKey((a, b) => a + b).sortBy(w => w, true, 5)
    sorted.foreach(println)
    sorted.saveAsTextFile(args(0))
  }

}