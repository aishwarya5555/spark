package scala

object Square {
  
  def values(fun: (Int) => Int, low: Int, high: Int) = {
    val nos = List.range(low, high + 1)
    nos.map(n => (n, fun(n))).foreach(println)
  }
  
  def main(args:Array[String]){
    values((n)=> n * n, -5, 5)
  }
  
}