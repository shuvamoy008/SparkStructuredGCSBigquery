package org.apache.spark.examples
import scala.math.random
import org.apache.spark._

object Pi {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Pi")
    val sc = new SparkContext(conf)

    val slices = if (args.length < 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt
    val xs = 1 until n
    val rdd = sc.parallelize(xs, slices)
      .setName("'Initial rdd'")
    val sample = rdd.map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      (x, y)
    }.setName("'Random points sample'")

    val inside = sample.filter { case (x, y) => (x * x + y * y < 1) }.setName("'Random points inside circle'")

    val count = inside.count()

    println("Pi is roughly " + 4.0 * count / n)

  }
}
