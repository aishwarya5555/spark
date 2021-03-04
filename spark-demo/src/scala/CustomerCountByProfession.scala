package scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source

object CustomerCountByProfession {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CustomerCountByProfession")
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR")

    val customerDataSet = sc.textFile("hdfs:///bigdatapgp/common_folder/ect2/custs.txt");
    val profWithCustCount = customerDataSet.map(t => (t.split(",")(4), 1)).reduceByKey((a, b) => a + b);

    profWithCustCount.foreach(t => {
      println("Profession :: " + t._1 + ", Customer Count :: " + t._2)
    })

  }

}