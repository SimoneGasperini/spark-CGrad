package linalg

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
 * A Matrix object is stored as an RDD of (key1,(key2,value)) pairs where
 * key1 is the column id, key2 is the row id, and value is the corresponding
 * matrix element.
 * The Matrix is represented as a dense structure (also zero elements are stored)
 *
 * @param rdd: RDD[(col:Int, (row:Int, element:Double))]
 */
class Matrix (var rdd:RDD[(Int, (Int, Double))] = null) {
  
  def init (spark:SparkContext, array:Array[Array[Double]]) {
    rdd = spark.parallelize(for {
      i <- array.indices
      j <- array(i).indices
    } yield (j,(i,array(i)(j))))
  }

  def size (): Int = {
    math.sqrt(rdd.count).toInt
  }

  def transpose (): Matrix = {
    new Matrix(rdd.map{case (j,(i,x)) => (i,(j,x))})
  }

  def elementWise_byScalar (op:(Double,Double) => Double, k:Double): Matrix = {
    new Matrix(rdd.map{case (j,(i,x)) => (j,(i,op(x,k)))})
  }

  def - (k:Double): Matrix = elementWise_byScalar(op=_-_, k=k)
  def * (k:Double): Matrix = elementWise_byScalar(op=_*_, k=k)
  def + (k:Double): Matrix = elementWise_byScalar(op=_+_, k=k)
  def / (k:Double): Matrix = elementWise_byScalar(op=_/_, k=k)

  def dot (vec:Vector): Vector = {
    new Vector(rdd.groupByKey()
      .join(vec.rdd)
      .flatMap{case(_, v) =>
        v._1.map(mv => (mv._1, mv._2 * v._2))}
      .reduceByKey(_+_))
  }

  def show () {
    val s:Int = size()
    val array = Array.ofDim[Double](n1=s, n2=s)
    for ((j,(i,x)) <- rdd.collect) array(i)(j) = x
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
