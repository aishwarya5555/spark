package scala

import org.apache.spark.Partitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SplitsTheText2Partition {

  class FilePartitioner(numberOfPartitioner: Int) extends Partitioner {

    override def numPartitions: Int = numberOfPartitioner

    override def getPartition(key: Any): Int = {
      if(key.toString().charAt(0) == 'a'){
        return 5 % numPartitions;
      } else if(key.toString().charAt(0) == 'e'){
        return 1 % numPartitions;
      } else if(key.toString().charAt(0) == 'i'){
        return 2 % numPartitions;
      }
      else if(key.toString().charAt(0) == '0'){
        return 3 % numPartitions;
      }
      else if(key.toString().charAt(0) == 'u'){
        return 4 % numPartitions;
      }
      0
    }

    override def equals(other: Any): Boolean = other match {
      case partitioner: FilePartitioner =>
        partitioner.numPartitions == numPartitions
      case _ =>
        false
    }
  }

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

    val sorted = wordsStartWithVowels.map(s => (s, 1)).reduceByKey((a, b) => a + b).sortBy(w => w, true)
    sorted.foreach(println)
    sorted.partitionBy(new FilePartitioner(5)).saveAsTextFile(args(0))
  }

}