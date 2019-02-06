package com.kolovsky.graph

object MinOrderEdge extends Ordering[(Double, Node, Edge)] {
  def compare(x:(Double, Node, Edge), y:(Double, Node, Edge)) = y._1 compare x._1
}
