package scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object LinesInDataSet {
  def main(args: Array[String]) {
    //Start the Spark context
    val conf = new SparkConf().setAppName("LinesInDataSet")
      .setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val nonEmptyLines = sc.textFile("hdfs:///bigdatapgp/common_folder/assignment4/text_dataset/macbeth.txt")
    .filter(line => !line.isEmpty());
    
    //val nonEmptyLines = sc.textFile("input.txt").filter(line => !line.isEmpty())

    val lineAccumulator = sc.longAccumulator("lineNumber")
    nonEmptyLines.take(500).foreach(line => {
      lineAccumulator.add(1);
      println("<Number of characters>" + line.length + "<line_number>" + lineAccumulator.count + "<line>" + line);
    })

    lineAccumulator.reset();
    val lineNoOfChar = nonEmptyLines.map(line => {
      lineAccumulator.add(1)
      (line.length, (lineAccumulator.count, line))
    }).collect();
    
    println("=======================================================================")
    val maxLine = lineNoOfChar.maxBy(lineDetails => lineDetails._1);
    println("<Number of characters>" + maxLine._1 + "<line_number>" + maxLine._2._1 + "<line_with_maximum_characters>" + maxLine._2._2)

    val minLine = lineNoOfChar.minBy(lineDetails => lineDetails._1);
    println("<Number of characters>" + minLine._1 + "<line_number>" + minLine._2._1 + "<line_with_minimum_characters>" + minLine._2._2)

  }
}