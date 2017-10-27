package com.kolovsky.graph

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kolovsky on 22.5.17.
  */
class Node() extends Serializable{
  var edges: ArrayBuffer[Edge] = ArrayBuffer()
  var edges_oposite: ArrayBuffer[Edge] = ArrayBuffer()
  var income_edges: ArrayBuffer[Edge] = ArrayBuffer()
  var i: Int = -1

  override def toString: String = ""+i

}
