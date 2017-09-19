package com.kolovsky.modeler

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kolovsky on 26.5.17.
  */
class PathSet(source: Zone, target: Zone, trips: Double) extends ODCell with Serializable{
  s = source
  t = target
  flow = trips
  var shortestPath: Path = _

  /**
    * Add shortest path to the set K_rs
    * @param path path Array[edge index]
    * @param cost distance (path length)
    * @return shortest path from K_rs (cost, path)
    */
  def addPathGetShortest(path: Array[Int], cost: Double): Path = {
    var i = 0
    var shortestIndex = -1
    for (p <- paths){
      if (cost == p.cost){
        shortestPath = p
        //shortestPath = new Path()
        //shortestPath.cost = p.cost
        //shortestPath.path = p.path
        shortestIndex = i
      }
      i += 1
    }
    if (shortestIndex != -1){
      paths.remove(shortestIndex)
    }
    else{
      shortestPath = new Path()
      shortestPath.path = path
      shortestPath.cost = cost
    }
    shortestPath
  }

  /**
    * Update cost (path length) in set K_rs according to cost edge map
    * @param costEdgeMap cost edge map
    */
  def updateCost(costEdgeMap: Array[Double]): Unit ={
    for(i <- paths.indices){
      var new_cost: Double = 0.0
      for (ei <- paths(i).path){
        new_cost += costEdgeMap(ei)
      }
      paths(i).cost = new_cost
    }
  }

  /**
    * Compute part of step size in GP (Gradient Projection); step = alpha/S
    * @param p - path (distance, flow, path)
    * @param capacityEdgeMap capacity edge map
    * @param initCostEdgeMap init cost edge map
    * @param trafficEdgeMap traffic edge map
    * @param cf cost function
    * @return S
    */
  def getS(p: Path, capacityEdgeMap: Array[Double], initCostEdgeMap: Array[Double], trafficEdgeMap: Array[Double], cf: CostFunction): Double ={
    var s: Double = 0
    for (ei <- p.path){
      if (!shortestPath.path.contains(ei)){
        s += cf.derivation(trafficEdgeMap(ei), capacityEdgeMap(ei), initCostEdgeMap(ei))
      }
    }
    for (ei <- shortestPath.path){
      if (!p.path.contains(ei)){
        s += cf.derivation(trafficEdgeMap(ei), capacityEdgeMap(ei), initCostEdgeMap(ei))
      }
    }
    s
  }

  /**
    * Move in descent direction.
    * @param alpha parameter for step  = alpha/S
    * @param capacityEdgeMap capacity edge map
    * @param initCostEdgeMap init cost edge map
    * @param trafficEdgeMap traffic edge map
    * @param cf cost function
    */
  def move(alpha: Double, capacityEdgeMap: Array[Double], initCostEdgeMap: Array[Double], trafficEdgeMap: Array[Double], cf: CostFunction): Unit = {
    var s: Double = 0
    var new_flow: Double = 0
    var flow_sum: Double = 0
    for (i <- paths.indices){
      s = getS(paths(i), capacityEdgeMap, initCostEdgeMap, trafficEdgeMap, cf)
      val nflow = paths(i).flow - (alpha/s)*(paths(i).cost - shortestPath.cost)
      new_flow = math.max(0, nflow)
      paths(i).flow = new_flow
      flow_sum += new_flow
    }
    shortestPath.flow = flow - flow_sum
    paths += shortestPath
    shortestPath = null
  }

  def toODCell(): ODCell = {
    this
  }
}
