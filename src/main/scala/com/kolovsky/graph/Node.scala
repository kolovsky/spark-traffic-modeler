package com.kolovsky.graph

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kolovsky on 22.5.17.
  */
class Node() extends Serializable{
  var edges: ArrayBuffer[Edge] = ArrayBuffer()
  var income_edges: ArrayBuffer[Edge] = ArrayBuffer()
  var i: Int = -1

  def getOutComeEdges(in_edge: Edge = null): ArrayBuffer[Edge] = {
    if (in_edge == null || in_edge.tr == null){
      return edges
    }
    edges.filter(e => !in_edge.tr.contains(e.i))
  }

  override def toString: String = ""+i

}

