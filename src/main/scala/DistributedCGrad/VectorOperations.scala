package DistributedCGrad

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
 * A Vector is stored as an RDD of (key,value) pairs where key is the row id and value is the
 * corresponding vector element. The Vector is represented as a dense structure (also zero elements are stored)
*/
object VectorOperations {

  type VectorRDD = RDD[(Int, Double)]

  def get_VectorRDD(array:Array[Double], spark:SparkContext): VectorRDD = {
    spark.parallelize(for (i <- array.indices) yield (i,array(i)) )
  }

  def magnitude(vector:VectorRDD): Double = {
    math.sqrt(dot(vector,vector))
  }

  def elementWise_byScalar(vector:VectorRDD, k:Double, op:(Double,Double)=>Double): VectorRDD = {
    vector.map{case (i,x) => (i, op(x,k)) }
  }

  def multiply(vector:VectorRDD, k:Double): VectorRDD = elementWise_byScalar(vector=vector, k=k, op=_*_)

  def elementWise_byVector(vector1:VectorRDD, vector2:VectorRDD, op:(Double,Double)=>Double): VectorRDD = {
    vector1.join(vector2).map{case (i, (x,y)) => (i, op(x,y)) }
  }

  def add(vector1:VectorRDD, vector2:VectorRDD): VectorRDD = elementWise_byVector(vector1=vector1, vector2=vector2, op=_+_)
  def subtract(vector1:VectorRDD, vector2:VectorRDD): VectorRDD = elementWise_byVector(vector1=vector1, vector2=vector2, op=_-_)
  def multiply(vector1:VectorRDD, vector2:VectorRDD): VectorRDD = elementWise_byVector(vector1=vector1, vector2=vector2, op=_*_)

  def dot(vector1:VectorRDD, vector2:VectorRDD): Double = {
    vector1.join(vector2)
      .map{case (_, (x,y)) => x*y}
      .reduce(_+_)
  }

  def show(vector:VectorRDD) {
    val size:Int = vector.count().toInt
    val array = Array.ofDim[Double](size)
    for ((i,x) <- vector.collect) array(i) = x
    print("[")
    for (i <- array.indices)
      if (i == size-1) print(array(i))
      else print(array(i) + ", ")
    println("]")
  }

}
