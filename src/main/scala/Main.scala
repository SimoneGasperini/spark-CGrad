import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

import linalg.{Vector, Matrix}


object Main {

  var spark:SparkContext = _

  def main (args: Array[String]): Unit = {
    this.spark = get_sparkContext()
    val size:Int = 10
    val A:Matrix = get_randomMatrixSPD(size=size) * 6 - 3
    val b:Vector = get_randomVector(size=size) * 4 - 2
    val x:Vector = conjugate_gradient(A=A, b=b)
    val check:Vector = (A dot x) - b
    println(check.magnitude())
    this.spark.stop()
  }

  def get_sparkContext (): SparkContext = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("spark-CGrad").setMaster("local[*]")
    new SparkContext(conf)
  }

  def get_randomMatrixSPD (size:Int): Matrix = {
    val array:Array[Array[Double]] =
      (for (_ <- 0 until size) yield
        (for (_ <- 0 until size) yield
          Random.nextDouble).toArray).toArray
    val matrix = new Matrix
    matrix.init(spark=this.spark, array=array)
    matrix dot matrix.transpose()
  }

  def get_randomVector (size:Int): Vector = {
    val array:Array[Double] =
      (for (_ <- 0 until size)
        yield Random.nextDouble).toArray
    val vector = new Vector
    vector.init(spark=this.spark, array=array)
    vector
  }

  def conjugate_gradient (A:Matrix, b:Vector, tol:Double=1E-6, maxiters:Int=100): Vector = {
    var x:Vector = get_randomVector(size=b.size())
    var r:Vector = b - (A dot x)
    var p:Vector = r
    var iter = 0
    while (iter < maxiters) {
      iter = iter + 1
      println(iter)
      val Ap:Vector = A dot p
      val alpha_den:Double = p dot Ap
      val r_squared:Double = r dot r
      val alpha:Double = r_squared / alpha_den
      x = x + p * alpha
      r = r - Ap * alpha
      val beta_num:Double = r dot r
      if (beta_num < tol)
        iter = maxiters
      val beta:Double = beta_num / r_squared
      p = r + p * beta
    }
    x
  }

}
