package com.kolovsky.graph

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, PriorityQueue}

/**
  * Created by kolovsky on 22.5.17.
  */
class Graph() extends GraphBase with Serializable{
  var nodes: Array[Node] = null
  var edges: Array[Edge] = null
  val properties: mutable.Map[String, Any] = mutable.Map()
  val hm: mutable.HashMap[Int, Node] = mutable.HashMap()

  def idToNode(id: Int): Node ={
    val tmp = hm.get(id)
    if (tmp.isEmpty){
      throw new Exception("Node ID "+id+" does not exists!")
    }
    return tmp.get
  }

  override def addEdges(edges: Array[(Int, Int, Boolean)]): Array[Int] = {
    this.edges = Array.ofDim[Edge](edges.length)

    //construct HashMap and array of Nodes
    for (e <- edges){
      if (hm.get(e._1).isEmpty){
        hm += ((e._1, new Node()))
      }
      if (hm.get(e._2).isEmpty){
        hm += ((e._2, new Node()))
      }
    }
    val hmArr = hm.toArray
    this.nodes = hmArr.map(_._2)
    for (i <- 0 until this.nodes.length){
      nodes(i).i = i
    }

    //adding edges
    for (ei <- 0 until edges.length){
      val s_node = idToNode(edges(ei)._1)
      val t_node = idToNode(edges(ei)._2)
      this.edges(ei) = new Edge(s_node.i, t_node.i, ei)

      // true means one Direction
      if (edges(ei)._3){
        s_node.edges += this.edges(ei)
      }
      else{
        s_node.edges += this.edges(ei)
        t_node.edges_oposite += this.edges(ei)
      }
    }
    return hmArr.map(_._1)
  }

  override def numberEdges(): Int = edges.length

  override def numberNodes(): Int = nodes.length

  override def addEdge(e: (Int, Int, Boolean)): Unit = {
    //resize Array
    val new_edges = Array.ofDim[Edge](edges.length + 1)
    edges.copyToArray(new_edges,0)
    edges = new_edges

    val s_node = idToNode(e._1)
    val t_node = idToNode(e._2)

    val new_edge = new Edge(s_node.i, t_node.i, edges.length - 1)
    edges(edges.length - 1) = new_edge

    // true means one Direction
    if (e._3){
      s_node.edges += new_edge
    }
    else{
      s_node.edges += new_edge
      t_node.edges_oposite += new_edge
    }

  }

  override def addNode(id: Int): Node = {
    //resize array
    val new_nodes = Array.ofDim[Node](nodes.length + 1)
    nodes.copyToArray(new_nodes, 0)
    nodes = new_nodes

    // create new Node
    val n = new Node()
    n.i = nodes.length - 1
    // add node to array
    nodes(nodes.length - 1) = n
    // add to HashMap
    hm += ((id, n))
    return n
  }

  // mode: 1 - inDirection, 2 - opositeDirection, 3 - bothDirection
  override def disableEdge(e: Edge, mode: Int): Unit = {
    val s_node = nodes(e.s)
    val t_node = nodes(e.t)
    // inDirection
    if (mode == 1 || mode == 3){
      var j = -1
      for (i <- 0 until s_node.edges.length){
        if (s_node.edges(i).i == e.i){
          j = i
        }
      }
      if (j != -1){
        s_node.edges.remove(j)
      }
    }
    // opositeDirection
    if (mode == 2 || mode == 3){
      var j = -1
      for (i <- 0 until t_node.edges_oposite.length){
        if (t_node.edges_oposite(i).i == e.i){
          j = i
        }
      }
      if (j != -1){
        t_node.edges_oposite.remove(j)
      }
    }
  }

  override def enableEdge(e: Edge, oneDirection: Boolean): Unit = {
    val s_node = nodes(e.s)
    val t_node = nodes(e.t)

    if (oneDirection){
      s_node.edges += e
    }
    else{
      s_node.edges += e
      t_node.edges_oposite += e
    }
  }

  override def searchDijkstra(s: Int, cost: Array[Double]): (Array[Double], Array[Edge]) = {
    if (cost.length != edges.length){
      throw new Exception("cost have to same length as edges array")
    }
    val pq = PriorityQueue.empty[(Double, Node)](MinOrderNode)
    val s_node = idToNode(s)
    val dist: Array[Double] = Array.fill(nodes.length)(Double.PositiveInfinity)
    val prev: Array[Edge] = Array.ofDim(nodes.length)

    dist(s_node.i) = 0
    pq.enqueue((0,s_node))

    while (pq.nonEmpty){
      val n = pq.dequeue()._2
      for (e <- n.edges){
        if (dist(n.i) + cost(e.i) < dist(e.t)){
          dist(e.t) = dist(n.i) + cost(e.i)
          pq.enqueue((dist(e.t), nodes(e.t)))
          prev(e.t) = e
        }
      }
      for (e <- n.edges_oposite){
        if (dist(n.i) + cost(e.i) < dist(e.s)){
          dist(e.s) = dist(n.i) + cost(e.i)
          pq.enqueue((dist(e.s), nodes(e.s)))
          prev(e.s) = e
        }
      }
    }
    return (dist, prev)
  }

  override def getShortestPaths(s: Int, t: Array[Int], cost: Array[Double]): Array[(Int, Double, Array[Edge])] = {
    val (dist, prev) = searchDijkstra(s, cost)
    val paths: Array[(Int, Double, Array[Edge])] = Array.ofDim(t.length)
    for (i <- 0 until t.length){
      val t_node = idToNode(t(i))
      paths(i) = ( t(i), dist(t_node.i), getPath(prev, t(i)) )
    }
    return paths
  }

  private def getPath(prev: Array[Edge], t: Int): Array[Edge] ={
    val t_node = idToNode(t)
    val path: ArrayBuffer[Edge] = ArrayBuffer()
    var ae = prev(t_node.i)
    var ai = t_node.i
    while (ae != null){
      path += ae
      if (ae.s != ai){
        if (ae != null){
          ai = ae.s
        }
        ae = prev(ae.s)
      }
      else{
        if (ae != null){
          ai = ae.t
        }
        ae = prev(ae.t)
      }

    }
    return path.reverse.toArray
  }

  override def getShortestPathsTrips(s: Int, t: Array[(Int, Double)], cost: Array[Double]): Array[(Int, Double, Double, Array[Edge])] = {
    val (dist, prev) = searchDijkstra(s, cost)
    val paths: Array[(Int, Double, Double, Array[Edge])] = Array.ofDim(t.length)
    for (i <- 0 until t.length){
      val t_node = idToNode(t(i)._1)
      paths(i) = ( t(i)._1, dist(t_node.i),t(i)._2, getPath(prev, t(i)._1) )
    }
    return paths
  }

  def getShortestPathsT[T](s: Int, t: Array[(Int, T)], cost: Array[Double]): Array[(Int, Double, T, Array[Edge])] = {
    val (dist, prev) = searchDijkstra(s, cost)
    val paths: Array[(Int, Double, T, Array[Edge])] = Array.ofDim(t.length)
    for (i <- 0 until t.length){
      val t_node = idToNode(t(i)._1)
      paths(i) = ( t(i)._1, dist(t_node.i), t(i)._2, getPath(prev, t(i)._1) )
    }
    paths
  }

}
