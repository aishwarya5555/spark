package scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source

object TopTenProductWithMaxRevenue {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TopTenProductWithMaxRevenue")
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR")

    val transcationDataSet = sc.textFile("hdfs:///bigdatapgp/common_folder/ect2/txns1.txt");
    val productWithRevenue = transcationDataSet.map(t => (t.split(",")(5), t.split(",")(3).toDouble)).reduceByKey((c, l) => c + l);

    val topProducts = productWithRevenue.sortBy(r => -r._2).take(10);

    topProducts.foreach(t => {
      println("Product :: " + t._1 + ", Revenue::" + t._2)
    })

  }

}