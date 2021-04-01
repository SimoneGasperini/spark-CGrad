package linalg

import org.apache.spark.rdd.RDD


/**
 * A Matrix object is created from an RDD of (key1,(key2,value)) pairs where
 * key1 is the column id, key2 is the row id, and value is the corresponding
 * matrix element.
 * The Matrix is represented as a dense structure (also zero elements are stored)
 *
 * @param rdd: RDD[(col:Int, (row:Int, element:Double))]
 */
class Matrix (val rdd:RDD[(Int, (Int, Double))]) {

  def size (): Long = {
    math.sqrt(rdd.count).toLong
  }

  def elementWise_byScalar (op:(Double,Double) => Double, k:Double): Matrix = {
    new Matrix(rdd.map{case (j,(i,x)) => (j,(i,op(x,k)))})
  }

  def - (k:Double): Matrix = elementWise_byScalar(op=_-_, k=k)
  def * (k:Double): Matrix = elementWise_byScalar(op=_*_, k=k)
  def + (k:Double): Matrix = elementWise_byScalar(op=_+_, k=k)
  def / (k:Double): Matrix = elementWise_byScalar(op=_/_, k=k)

  def show () {
    var current: Long = -1
    for ((_, (row, el)) <- rdd.collect) {
      if (row == current) print(el + ", ")
      else {
        if (current != -1) println(")")
        print("(" + el + ", ")
        current += 1
      }
    }
    print(")")
  }

}
