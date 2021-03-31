package linalg

import org.apache.spark.rdd.RDD

class Vector (val rdd:RDD[(Int, Double)]) {

  def size (): Long = {
    rdd.count()
  }

  def magnitude (): Double = {
    math.sqrt(this dot this)
  }

  def elementWise_byScalar (op:(Double,Double) => Double, k:Double): Vector = {
    new Vector(rdd.map{case (i,x) => (i,op(x,k))})
  }

  def + (k:Double): Vector = elementWise_byScalar(op=_+_, k=k)
  def - (k:Double): Vector = elementWise_byScalar(op=_-_, k=k)
  def * (k:Double): Vector = elementWise_byScalar(op=_*_, k=k)
  def / (k:Double): Vector = elementWise_byScalar(op=_/_, k=k)

  def elementWise_byVector (op:(Double,Double) => Double, that:Vector): Vector = {
    new Vector(this.rdd.join(that.rdd).map{case (i,(x,y)) => (i,op(x,y))})
  }

  def + (that:Vector): Vector = elementWise_byVector(op=_+_, that=that)
  def - (that:Vector): Vector = elementWise_byVector(op=_-_, that=that)
  def * (that:Vector): Vector = elementWise_byVector(op=_*_, that=that)
  def / (that:Vector): Vector = elementWise_byVector(op=_/_, that=that)

  def dot (that:Vector): Double = {
    this.rdd.join(that.rdd)
      .map{case (_,(x,y)) => x*y}
      .reduce(_+_)
  }

}
