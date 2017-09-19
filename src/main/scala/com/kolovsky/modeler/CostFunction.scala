package com.kolovsky.modeler

/**
  * Created by kolovsky on 26.5.17.
  */
trait CostFunction {
  /**
    * compute cost from flow (traffic)
    * @param traffic flow
    * @param capacity road capacity
    * @param initCost cost with 0 traffic c=c(0)
    * @return cost depends on traffic c=c(traffic)
    */
  def cost(traffic: Double, capacity: Double, initCost: Double): Double

  /**
    *
    * @param traffic flow
    * @param capacity road capacity
    * @param initCost cost with 0 traffic c=c(0)
    * @return integral form 0 to the traffic (pat og objective function)
    */
  def integral(traffic: Double, capacity: Double, initCost: Double): Double

  /**
    *
    * @param traffic flow
    * @param capacity road capacity
    * @param initCost cost with 0 traffic c=c(0)
    * @return derivation by traffic dc(traffic)/d(traffic)
    */
  def derivation(traffic: Double, capacity: Double, initCost: Double): Double
}
