package DistributedCGrad

import scala.util.Random
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import DistributedCGrad.MatrixOperations._
import DistributedCGrad.VectorOperations._


class DistributedCGrad(matrix_A:Array[Array[Double]], vector_b:Array[Double], spark:SparkContext) {

  type MatrixRDD = RDD[(Int, (Int,Double))]
  type VectorRDD = RDD[(Int, Double)]

  val size:Int = vector_b.length
  val A:MatrixRDD = get_MatrixRDD(array2D=matrix_A, spark=spark).cache()
  val b:VectorRDD = get_VectorRDD(array=vector_b, spark=spark).cache()
  val x0:VectorRDD = get_VectorRDD(array=get_randomArray(size), spark=spark).cache()

  def get_randomArray (size:Int): Array[Double] = {
    (for (_ <- 0 until size) yield Random.nextDouble).toArray
  }

  def solve(tol:Double=1E-6, maxiters:Int=100): VectorRDD = {
    var x:VectorRDD = x0
    var r:VectorRDD = subtract(b, mat_dot_vec(A,x))
    var p:VectorRDD = r
    var iter = 0
    while (iter < maxiters) {
      iter = iter+1
      println(iter)
      val Ap:VectorRDD = mat_dot_vec(A,p)
      val alpha_den:Double = dot(p,Ap)
      val r_squared:Double = dot(r,r)
      val alpha:Double = r_squared / alpha_den
      x = add(x, multiply(p,alpha))
      r = subtract(r, multiply(Ap,alpha))
      val beta_num:Double = dot(r,r)
      if (beta_num < tol)
        iter = maxiters
      val beta:Double = beta_num / r_squared
      p = add(r, multiply(p,beta))
    }
    x
  }

}
