package com.kolovsky.graph

/**
  * Represent edge in graph
  * @param source - index of source node
  * @param target - index of target node
  * @param index - index of edge
  */
class Edge(source: Int, target: Int, index: Int) extends Serializable {
  val s: Int = source
  val t: Int = target
  val i: Int = index
  // turning restriction (list of indexes)
  var tr: Array[Int] = _

  override def toString: String = ""+(i+1)
}
