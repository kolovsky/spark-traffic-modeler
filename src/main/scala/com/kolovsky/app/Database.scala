package com.kolovsky.app

import com.kolovsky.modeler.Zone

/**
  * Created by kolovsky on 2.6.17.
  */
trait Database {
  def getEdges(): Array[(Int, Int, Boolean)]
  def getCost(): Array[Double]
  def getCapacity(): Array[Double]
  def getODM(): Array[(Zone, Zone, Double)]
}
