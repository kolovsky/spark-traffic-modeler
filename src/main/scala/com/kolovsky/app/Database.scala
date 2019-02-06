package com.kolovsky.app

import com.kolovsky.modeler.Zone

/**
  * Created by kolovsky on 2.6.17.
  */
trait Database {
  def getEdges(): Array[(Int, Int)]
  def getCost(): Array[Double]
  def getCapacity(): Array[Double]
  def getODM(): Array[(Zone, Zone, Double)]
  def getZones(): Array[(Zone, Double)]
  def getProfile(): Array[(Int, Double)]
  def getId(): Array[Int]
  def saveResult(modelName: String, cache_name: String,config: String, result: String): Unit
  def getTurnRestriction(): Array[Array[Int]]
}
