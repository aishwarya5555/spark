package scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source

object TopTenCitiesWithMaxRevenue {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TopTenCitiesWithMaxRevenue")
    //  .setMaster("local");
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR")

    val transcationDataSet = sc.textFile("hdfs:///bigdatapgp/common_folder/ect2/txns1.txt");
    val cityWithRevenue = transcationDataSet.map(t => (t.split(",")(6), t.split(",")(3).toDouble)).reduceByKey((c, l) => c + l);

    val topCities = cityWithRevenue.sortBy(r => -r._2).take(10);

    topCities.foreach(t => {
      println("City :: " + t._1 + " Revenue::" + t._2)
    })

  }

}