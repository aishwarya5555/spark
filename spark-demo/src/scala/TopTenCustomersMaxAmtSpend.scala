package scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source

object TopTenCustomersMaxAmtSpend {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TopTenCustomersMaxAmtSpend").setMaster("local")
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR")

    //val customerDataSet = sc.textFile("hdfs:///bigdatapgp/common_folder/ect2/custs.txt");
    val customerDataSet = sc.textFile("input2.txt");
    val custMap = customerDataSet.map(t => (t.split(",")(0).toInt, t.split(",")(1).concat("," + t.split(",")(2)))).collect().toMap;

    //val transcationDataSet = sc.textFile("hdfs:///bigdatapgp/common_folder/ect2/txns1.txt");
    val transcationDataSet = sc.textFile("input.txt");
    val custWithAmt = transcationDataSet.map(t => (t.split(",")(2).toInt, t.split(",")(3).toDouble)).reduceByKey((c, l) => c + l);

    val topCustomers = custWithAmt.sortBy(r => -r._2).take(10);

    topCustomers.foreach(t => {
      println("CustomerId :: " + t._1 + ", Customer :: " + custMap.get(t._1).get + ", Amount::" + t._2)
    })

  }

}