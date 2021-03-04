package scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source

object BRBikeRentalApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("BRBikeRentalApp")
    .setMaster("local");
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR")

    val tripDataSet = sc.textFile("hdfs:///bigdatapgp/common_folder/assignment4/trip_dataset/trip_data.csv")
      .filter(line => !line.isEmpty());
    //val tripDataSet = sc.textFile("trip_data.csv")

    //Max duration of a bike trip.
    println("Max_Duration_Of_Bike_Trip = " + tripDataSet.collect().maxBy(trip => trip.split(",")(1).toLong))

    //find the number of rides for ’Subscriber ’ and ‘Customer’ Using accumulators
    val subscriberaccumulator = sc.longAccumulator("subscriber")
    val customerAccumulator = sc.longAccumulator("customer")
    val nullZipCodeAccumulator = sc.longAccumulator("nullZipCode")

    tripDataSet.foreach(trip => {
      if (trip.split(",")(9).toString().equalsIgnoreCase("Subscriber")) {
        subscriberaccumulator.add(1)
      } else if (trip.split(",")(9).toString().equalsIgnoreCase("Customer")) {
        customerAccumulator.add(1)
      } else if (trip.split(",")(10) == null) {
        nullZipCodeAccumulator.add(1)
      }
    })

    println("Subscriber trip count=" + subscriberaccumulator.value)
    println("Customer trip count=" + customerAccumulator.value)
    println("Null ZipCode count=" + nullZipCodeAccumulator.value)

    //Group By
    // val groupTrip = tripDataSet.groupBy(trip => trip.split(",")(9).toString()).collect()
    //groupTrip.foreach(t => {
    //  println(t._1 + "count::" + t._2.size)
    //})
  }

}