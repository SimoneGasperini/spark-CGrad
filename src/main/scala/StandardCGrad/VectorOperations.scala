package StandardCGrad

object VectorOperations {

  type Matrix = Array[Array[Double]]
  type Vector = Array[Double]

  def elementWise_byVector(vector1:Vector, vector2:Vector, op:(Double,Double)=>Double): Vector = {
    vector1.zip(vector2).map{case (x,y) => op(x,y)}
  }

  def add(vector1:Vector, vector2:Vector): Vector = elementWise_byVector(vector1=vector1, vector2=vector2, op=_+_)
  def subtract(vector1:Vector, vector2:Vector): Vector = elementWise_byVector(vector1=vector1, vector2=vector2, op=_-_)
  def multiply(vector1:Vector, vector2:Vector): Vector = elementWise_byVector(vector1=vector1, vector2=vector2, op=_*_)

}
