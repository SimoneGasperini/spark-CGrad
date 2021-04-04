package StandardCGrad

object MatrixOperations {

  type Matrix = Array[Array[Double]]
  type Vector = Array[Double]

  def matmul(leftMatrix:Matrix, rightMatrix:Matrix): Matrix = {
    for (row <- leftMatrix)
      yield for (col <- rightMatrix.transpose)
        yield row.zip(col).map {case (x,y) => x*y}.sum
  }

}
