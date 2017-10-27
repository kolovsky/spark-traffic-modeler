package com.kolovsky.modeler

/**
  * Created by kolovsky on 29.5.17.
  */
class BasicCostFunction(alpha: Double, beta: Double) extends CostFunction with Serializable{
  override def cost(traffic: Double, capacity: Double, initCost: Double): Double = {
    initCost * (1 + alpha * math.pow(traffic/capacity, beta))
  }

  override def derivation(traffic: Double, capacity: Double, initCost: Double): Double = {
    alpha*beta* initCost * math.pow(traffic, beta - 1) / math.pow(capacity,beta)
  }

  override def integral(traffic: Double, capacity: Double, initCost: Double): Double = {
    initCost*( (alpha*traffic*math.pow(traffic/capacity, beta)) / (beta + 1) + traffic)
  }
}
