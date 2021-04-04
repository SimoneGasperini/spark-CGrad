import scala.util.Random
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import StandardCGrad.MatrixOperations._
import DistributedCGrad._


object Main {

  type VectorRDD = RDD[(Int, Double)]

  def main(args:Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("spark-CGrad").setMaster("local[*]")
    val spark = new SparkContext(conf)

    val size:Int = 10
    val A:Array[Array[Double]] = get_randomMatrixSPD(size)
    val b:Array[Double] = get_randomVector(size)
    val cgrad = new DistributedCGrad(matrix_A=A, vector_b=b, spark=spark)

    cgrad.solve()
    spark.stop()
  }

  def get_randomMatrixSPD(size:Int): Array[Array[Double]] = {
    val mat:Array[Array[Double]] = (for (_ <- 0 until size) yield
      (for (_ <- 0 until size) yield
        Random.nextDouble).toArray).toArray
    matmul(mat,mat.transpose)
  }

  def get_randomVector(size:Int): Array[Double] = {
    (for (_ <- 0 until size) yield Random.nextDouble).toArray
  }

}
