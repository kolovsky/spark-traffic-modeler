package com.kolovsky.graph

/**
  * Created by kolovsky on 22.5.17.
  */
trait GraphBase {
  /**
    * Method for construct graph.
    * @param edges list of edges (source_node_id, target_node_id, isOneDirection)
    */
  def addEdges(edges: Array[(Int, Int)]): Array[Int]

  /**
    * Add new Edge to the Graph. O(|E|) - linear complexity
    * @param e new edge (source_node_id, target_node_id, isOneDirection)
    */
  def addEdge(e: (Int, Int)): Unit

  /**
    * Add new Node to the Graph. O(|N|) - linear complexity
    * @param id edge ID
    * @return new Node
    */
  def addNode(id: Int): Node

  /**
    *
    * @return Number of nodes in graph
    */
  def numberNodes(): Int

  /**
    *
    * @return Number of edges in graph
    */
  def numberEdges(): Int

  /**
    * Dijkstra's search
    * @param s source node ID
    * @param cost cost properties map
    * @return (distance properties map, previous edge map)
    */
  def searchDijkstra(s: Int, cost: Array[Double]): (Array[Double], Array[Edge])

  /**
    *
    * @param s source node ID
    * @param t list of target node ID
    * @param cost cost properties edge map
    * @return shortest paths from source node to every target node Array[(target, distance, path)]
    */
  def getShortestPaths(s: Int, t: Array[Int], cost: Array[Double]): Array[(Int, Double, Array[Edge])]

  /**
    *
    * @param s source node ID
    * @param t list of target node ID with flow Array[(node ID, flow)]
    * @param cost cost properties edge map
    * @return shortest paths from source node to every target node Array[(target, distance, trips (flow), path)]
    */
  def getShortestPathsTrips(s: Int, t: Array[(Int, Double)], cost: Array[Double]): Array[(Int, Double, Double, Array[Edge])]
}
