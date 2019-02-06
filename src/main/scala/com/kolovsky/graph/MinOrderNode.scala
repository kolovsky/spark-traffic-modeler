package com.kolovsky.graph

/**
  * Created by kolovsky on 23.5.17.
  */
object MinOrderNode extends Ordering[(Double, Node)] {
  def compare(x:(Double, Node), y:(Double, Node)) = y._1 compare x._1
}
