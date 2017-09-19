package com.kolovsky.modeler

/**
  * Created by kolovsky on 24.5.17.
  */
trait Assignment {
  /**
    * perform assignment
    * @return traffic edge map
    */
  def run(odm: Constant.ROWODM): Array[Double]
  def getInfo(): String
}
