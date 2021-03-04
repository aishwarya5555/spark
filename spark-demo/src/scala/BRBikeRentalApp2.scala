package scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source

object BRBikeRentalApp2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("BRBikeRentalApp2")
      .setMaster("local");
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR")

    val stationDataSet = sc.textFile("hdfs:///bigdatapgp/common_folder/assignment4/trip_dataset/station_data.csv");

    //Read the Station dataset and create an RDD. Now print the number of bike stations in each city.
    val noOfStations = stationDataSet.map(st => (st.split(",")(5), 1)).reduceByKey((c, l) => c + l)
    noOfStations.collect().foreach(println)

    //Read the trip dataset and create an RDD.
    val tripDataSet = sc.textFile("hdfs:///bigdatapgp/common_folder/assignment4/trip_dataset/trip_data.csv");

    //get all station Ids of San Jose
    val sanJoseStationIds = stationDataSet.filter(st => st.split(",")(5).equalsIgnoreCase("San Jose")).map(st => st.split(",")(0).toInt).collect();
    println("San Jose :: " + sanJoseStationIds.mkString(","))

    //Print the number of trips (bike rides) starting from San Jose.
    val noOfTripStartFromJose = tripDataSet.filter(trip => sanJoseStationIds.contains(trip.split(",")(4).toInt)).count();
    println("noOfTripStartFromJose = " + noOfTripStartFromJose)
    
    //map station Id with city
    val stationCityMap = stationDataSet.map(st => (st.split(",")(0).toInt, st.split(",")(5))).collect().toMap;

    val broadcastStCityMap = sc.broadcast(stationCityMap)

    //Find the city with max number of bike rides.
    val result = tripDataSet.map(trip => (broadcastStCityMap.value.get(trip.split(",")(4).toInt), 1)).reduceByKey((a, b) => a + b).collect()

    val max = result.sortBy(r => r._2).maxBy(_._2)

    println("City with max Trips::" + max._1.get + " Trips::" + max._2);

  }

}