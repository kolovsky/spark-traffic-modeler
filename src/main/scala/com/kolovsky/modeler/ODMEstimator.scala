package com.kolovsky.modeler

/**
  * Created by kolovsky on 26.8.17.
  */
trait ODMEstimator {

  /**
    * Calibrate ODM using by reference counts
    * @return calibrated ODM
    */
  def estimate(): Types.ROWODM

  /**
    * Return info about computation
    * @return
    */
  def getInfo(): String
}
