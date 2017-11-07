package com.kolovsky.modeler

import com.kolovsky.graph.Graph
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.ArrayBuffer

abstract class EquilibriumAssignment(g: Broadcast[Graph], debug: Boolean = false) extends Assignment with Serializable{
  var info: String = ""
  var startTime: Long = 0
  var lbs: ArrayBuffer[Double] = ArrayBuffer.empty
  /**
    * Get information about computing
    * @return
    */
  def getInfo(): String ={
    info
  }

  /**
    * Write record to the info
    * @param text text of message
    */
  def writeInfo(text: String): Unit = {
    val t = (System.currentTimeMillis() - startTime)/1000.0
    val m = f"[$t%.4f] $text%s \n"
    info += m
    if (debug){
      print(m)
    }
  }

  /**
    * Compute relative gap of the solution
    * @param OF value of the objetive function
    * @param trafficEdgeMap traffic edge map
    * @param costEdgeMap cost edge map
    * @param odm Origin-Destination matrix
    * @return relative gap
    */
  def relativeGap(OF: Double, trafficEdgeMap: Array[Double], costEdgeMap: Array[Double], odm: Types.ROWODM): Double = {
    val aon = new AllOrNothingAssignment(g, costEdgeMap)
    val aon_trafficEdgeMap = aon.run(odm)
    var gap: Double = 0
    for (i <- trafficEdgeMap.indices){
      gap += costEdgeMap(i) * (aon_trafficEdgeMap(i) - trafficEdgeMap(i))
    }
    lbs += (OF + gap)
    val blb = lbs.max
    val rg = - gap / math.abs(blb)
    rg
  }
}
