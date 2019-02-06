package com.kolovsky.graph

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, PriorityQueue}

/**
  * Created by kolovsky on 22.5.17.
  */
class Graph() extends GraphBase with Serializable{
  var nodes: Array[Node] = _
  var edges: Array[Edge] = _
  val properties: mutable.Map[String, Any] = mutable.Map()
  val hm: mutable.HashMap[Int, Node] = mutable.HashMap()
  val hme: mutable.HashMap[Int, Edge] = mutable.HashMap()

  def idToNode(id: Int): Node ={
    val tmp = hm.get(id)
    if (tmp.isEmpty){
      throw new Exception("Node ID "+id+" does not exists!")
    }
    tmp.get
  }

  /**
    *
    * @param edge_id ID of edge
    * @return
    */
  def idToEdge(edge_id: Int): Edge = {
    if (hme.isEmpty){
      throw new Exception("Please add edge IDs (function addEdgeIDs)")
    }
    val tmp = hme.get(edge_id)
    if (tmp.isEmpty){
      throw new Exception("Node ID "+edge_id+" does not exists!")
    }
    tmp.get
  }

  override def addEdges(edges: Array[(Int, Int)]): Array[Int] = {
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
    for (i <- nodes.indices){
      nodes(i).i = i
    }

    //adding edges
    for (ei <- edges.indices){
      val s_node = idToNode(edges(ei)._1)
      val t_node = idToNode(edges(ei)._2)
      this.edges(ei) = new Edge(s_node.i, t_node.i, ei)

      // outcome edge
      s_node.edges += this.edges(ei)
      // incoming edges
      t_node.income_edges += this.edges(ei)
    }
    hmArr.map(_._1)
  }

  /**
    *
    * @param tr edge map turn restriction (edge ID)
    */
  def addTurnRestriction(tr: Array[Array[Int]]): Unit ={
    for (i <- tr.indices){
      if (tr(i) != null){
        edges(i).tr = tr(i).map(edge_id => idToEdge(edge_id).i)
      }
    }
  }

  /**
    *
    * @param edge_map_id edge map ID
    */
  def addEdgeIDs(edge_map_id: Array[Int]): Unit = {
    for (i <- edge_map_id.indices){
      hme += ((edge_map_id(i), edges(i)))
    }
  }

  override def numberEdges(): Int = edges.length

  override def numberNodes(): Int = nodes.length

  override def addEdge(e: (Int, Int)): Unit = {
    //resize Array
    val new_edges = Array.ofDim[Edge](edges.length + 1)
    edges.copyToArray(new_edges,0)
    edges = new_edges

    val s_node = idToNode(e._1)
    val t_node = idToNode(e._2)

    val new_edge = new Edge(s_node.i, t_node.i, edges.length - 1)
    edges(edges.length - 1) = new_edge

    s_node.edges += new_edge
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
    n
  }

  def searchDijkstraNTR(s: Int, cost: Array[Double]): (Array[Double], Array[Edge]) = {
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
    }
    (dist, prev)
  }

  def searchDijkstraTR(s: Int, cost: Array[Double]): (Array[Double], Array[Edge]) = {
    if (cost.length != edges.length){
      throw new Exception("cost have to same length as edges array")
    }
    val pq = PriorityQueue.empty[(Double, Node, Edge)](MinOrderEdge)
    val s_node = idToNode(s)
    val dist: Array[Double] = Array.fill(nodes.length)(Double.PositiveInfinity)
    val prev: Array[Edge] = Array.ofDim(nodes.length)

    dist(s_node.i) = 0
    pq.enqueue((0, s_node, null))

    while (pq.nonEmpty){
      val tmp = pq.dequeue()
      val n = tmp._2
      val in_e = tmp._3

      for (e <- n.getOutComeEdges(in_e)){
        if (dist(n.i) + cost(e.i) < dist(e.t)){
          dist(e.t) = dist(n.i) + cost(e.i)
          pq.enqueue((dist(e.t), nodes(e.t), e))
          prev(e.t) = e
        }
      }
    }
    (dist, prev)
  }

  override def searchDijkstra(s: Int, cost: Array[Double]): (Array[Double], Array[Edge]) ={
    searchDijkstraTR(s, cost)
  }

  override def getShortestPaths(s: Int, t: Array[Int], cost: Array[Double]): Array[(Int, Double, Array[Edge])] = {
    val (dist, prev) = searchDijkstra(s, cost)
    val paths: Array[(Int, Double, Array[Edge])] = Array.ofDim(t.length)
    for (i <- t.indices){
      val t_node = idToNode(t(i))
      paths(i) = ( t(i), dist(t_node.i), getPath(prev, t(i)) )
    }
    paths
  }

  def getPath(prev: Array[Edge], t: Int): Array[Edge] ={
    val t_node = idToNode(t)
    val path: ArrayBuffer[Edge] = ArrayBuffer()
    var ae = prev(t_node.i)
    while (ae != null){
      path += ae
      ae = prev(ae.s)
    }
    path.reverse.toArray
  }

  override def getShortestPathsTrips(s: Int, t: Array[(Int, Double)], cost: Array[Double]): Array[(Int, Double, Double, Array[Edge])] = {
    val (dist, prev) = searchDijkstra(s, cost)
    val paths: Array[(Int, Double, Double, Array[Edge])] = Array.ofDim(t.length)
    for (i <- t.indices){
      val t_node = idToNode(t(i)._1)
      paths(i) = ( t(i)._1, dist(t_node.i),t(i)._2, getPath(prev, t(i)._1) )
    }
    paths
  }

  def getShortestPathsT[T](s: Int, t: Array[(Int, T)], cost: Array[Double]): Array[(Int, Double, T, Array[Edge])] = {
    val (dist, prev) = searchDijkstra(s, cost)
    val paths: Array[(Int, Double, T, Array[Edge])] = Array.ofDim(t.length)
    for (i <- t.indices){
      val t_node = idToNode(t(i)._1)
      paths(i) = ( t(i)._1, dist(t_node.i), t(i)._2, getPath(prev, t(i)._1) )
    }
    paths
  }

  /**
    * Compute "bush" according to
    * Robert B. Dial, A path-based user-equilibrium traffic assignment algorithm that obviates path storage and enumeration,
    * In Transportation Research Part B: Methodological, Volume 40, Issue 10, 2006, Pages 917-936, ISSN 0191-2615,
    * https://doi.org/10.1016/j.trb.2006.02.008.
    * @param s source node ID
    * @param cost cost edge map
    * @return
    */
  def getBush(s: Int, cost: Array[Double]): Array[Boolean] = {
    val (dist, prev) = searchDijkstra(s, cost)
    val bush = Array.ofDim[Boolean](edges.length)
    for (i <- edges.indices){
      val e = edges(i)
      if (dist(e.t) > dist(e.s)){
        bush(i) = true
      }
      else {
        bush(i) = false
      }
    }
    bush
  }

  /**
    * Compute shortest and costest tree form source node "s". Mask must represent "bush" origins at "s".
    * @param s source node id
    * @param cost edge cost map
    * @param mask bush
    * @param bush_traffic traffic at bush form origin (used for max path constraints)
    * @return ((dist_min, prev_min),(dist_max, prev_max))
    */
  def getMinMaxTree(s: Int,
                    cost: Array[Double],
                    mask: Array[Boolean],
                    bush_traffic: Array[Double]): ((Array[Double], Array[Edge]), (Array[Double], Array[Edge])) = {

    val dist_min = Array.fill[Double](nodes.length)(Double.PositiveInfinity)
    val dist_max = Array.fill[Double](nodes.length)(Double.NegativeInfinity)
    val prev_min = Array.ofDim[Edge](nodes.length)
    val prev_max = Array.ofDim[Edge](nodes.length)

    val order_mask = mask.clone()
    val q = mutable.Queue.empty[(Node, Edge)]
    val s_n = idToNode(s)
    q += ((s_n, null))
    dist_min(s_n.i) = 0
    dist_max(s_n.i) = 0


    while (q.nonEmpty){
      val (n, in_e) = q.dequeue()
      for (e <- n.getOutComeEdges(in_e)){
        // is edge valid
        if (order_mask(e.i)){
          //maximum
          //max path can use only edges with non zero traffic (active edges)
          if (bush_traffic(e.i) != 0){
            val new_max_value = dist_max(n.i) + cost(e.i)
            if (dist_max(e.t) < new_max_value){
              dist_max(e.t) = new_max_value
              prev_max(e.t) = e
            }
          }

          //minimum
          val new_min_value = dist_min(n.i) + cost(e.i)
          if (dist_min(e.t) > new_min_value){
            dist_min(e.t) = new_min_value
            prev_min(e.t) = e
          }
          order_mask(e.i) = false

          //testing for incoming edges
          val m = nodes(e.t)
          if (!existsIncomeEdge(m, order_mask)){
            q += ((m, e))
          }
        }
      }
    }
    if (order_mask.count(x => x) != 0){
      //throw new Exception("mask do not represent acyclic graph")
    }
    ((dist_min, prev_min),(dist_max, prev_max))
  }

  /**
    * Return True if exists some incomeing edges, else False
    * @param n node
    * @param mask edge mask, false means disabled edge
    * @return
    */
  def existsIncomeEdge(n: Node, mask: Array[Boolean]): Boolean = {
    for (ep <- n.income_edges){
      if (mask(ep.i)){
        return true
      }
    }
    false
  }

}
