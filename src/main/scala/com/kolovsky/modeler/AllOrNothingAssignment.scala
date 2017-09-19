package com.kolovsky.modeler

import com.kolovsky.graph.Graph
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by kolovsky on 24.5.17.
  */

class AllOrNothingAssignment(g: Broadcast[Graph],
                             initCost: Array[Double]) extends Assignment with Serializable{

  def run(odm: RDD[(Zone, Array[(Zone, Double)])]): Array[Double] ={
    val traffic = odm.flatMap(x => {
      val target = x._2.map(t => (t._1.n, t._2))
      g.value.getShortestPathsTrips(x._1.n, target, initCost)
    })
      .flatMap(x => x._4.map(e => (e.i, x._3)))
      .reduceByKey(_+_).collect()

    val trafficEdgeMap: Array[Double] = Array.ofDim(initCost.length)
    for (tr <- traffic){
      trafficEdgeMap(tr._1) = tr._2
    }
    trafficEdgeMap
  }

  override def getInfo(): String = {
    null
  }
}
