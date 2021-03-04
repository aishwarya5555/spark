package scala
import org.apache.spark.AccumulatorParam
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object FizzBuzz {
  def main(args: Array[String]) = {
    //Start the Spark context
    val conf = new SparkConf().setAppName("FizzBuzz")
    //s  .setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = sc.range(1, 101, 1)
    val accumulator = sc.accumulator("")(StringAccumulator)

    numbers.foreach(num => {
      if (num % 3 == 0 || num % 5 == 0) {
        if (num % 3 == 0) {
          accumulator += "Fizz"
        }
        if (num % 5 == 0) {
          accumulator += "Buzz";
        }
      } else {
        accumulator += num.toString();
      }
    });

    val rdd = sc.parallelize(accumulator.value)
    //rdd.foreach(println) //uncomment when run in local 
    rdd.saveAsTextFile(args(0)) //comment when run in local 
    //Stop the Spark context
    sc.stop
  }

  object StringAccumulator extends AccumulatorParam[String] {
    def zero(s: String): String = s
    def addInPlace(s1: String, s2: String) = s1 + s2
  }
}