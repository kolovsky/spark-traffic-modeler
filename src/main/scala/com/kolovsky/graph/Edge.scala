package com.kolovsky.graph

/**
  * Created by kolovsky on 22.5.17.
  */
class Edge(source: Int, target: Int, index: Int) extends Serializable{
  val s: Int = source
  val t: Int = target
  val i: Int = index

  override def toString: String = ""+(i+1)

  override def clone(): Edge = {
    new Edge(s, t, i)
  }
}
