package DistributedCGrad

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
 * A Matrix is stored as an RDD of (key1, (key2,value)) pairs where key1 is the column id,
 * key2 is the row id, and value is the corresponding matrix element.
 * The Matrix is represented as a dense structure (also zero elements are stored)
*/
object MatrixOperations {

  type MatrixRDD = RDD[(Int, (Int,Double))]
  type VectorRDD = RDD[(Int, Double)]

  def get_MatrixRDD(array2D:Array[Array[Double]], spark:SparkContext): MatrixRDD = {
    spark.parallelize(for {
      i <- array2D.indices
      j <- array2D(i).indices
    } yield (j, (i, array2D(i)(j))) )
  }

  def transpose(matrix:MatrixRDD): MatrixRDD = {
    matrix.map{case (j, (i,x)) => (i, (j,x)) }
  }

  def mat_dot_vec(matrix:MatrixRDD, vector:VectorRDD): VectorRDD = {
    matrix.groupByKey()
      .join(vector)
      .flatMap{case (_,v) =>
        v._1.map(mv => (mv._1, mv._2*v._2))
      }.reduceByKey(_+_)
  }

  def mat_dot_mat(leftMatrixRDD:MatrixRDD, rightMatrixRDD:MatrixRDD): MatrixRDD = {
    leftMatrixRDD.join(transpose(rightMatrixRDD))
      .map{case (_, ((i,x),(k,y))) => ((i,k), x*y) }
      .reduceByKey(_+_)
      .map{case ((i,k), sum) => (k, (i,sum)) }
  }

  def show(matrix:MatrixRDD) {
    val size: Int = math.sqrt(matrix.count()).toInt
    val array = Array.ofDim[Double](size,size)
    for ((j, (i,x)) <- matrix.collect) array(i)(j) = x
    print('[')
    for (i <- array.indices) {
      print("[")
      for (j <- array(i).indices) {
        if (j == size-1) print(array(i)(j))
        else print(array(i)(j) + ", ")
      }
      if (i == size-1) println("]]")
      else println("]")
    }
  }

}
