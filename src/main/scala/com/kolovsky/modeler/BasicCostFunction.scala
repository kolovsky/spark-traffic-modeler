package com.kolovsky.modeler

/**
  * Created by kolovsky on 29.5.17.
  */
class BasicCostFunction extends CostFunction with Serializable{
  override def cost(traffic: Double, capacity: Double, initCost: Double): Double = {
    initCost * (1 + 0.15 * math.pow(traffic/capacity, 4))
  }

  override def derivation(traffic: Double, capacity: Double, initCost: Double): Double = {
    (0.6 * initCost * math.pow(traffic, 3)) / math.pow(capacity, 4)
  }

  override def integral(traffic: Double, capacity: Double, initCost: Double): Double = {
    (0.03 * initCost * math.pow(traffic, 5)) / math.pow(capacity, 4) + initCost*traffic
  }
}
