package com.kolovsky.example

import com.kolovsky.graph.Graph
import com.kolovsky.modeler.{BAssignment, BasicCostFunction, PathBasedAssignment}
import org.apache.spark.SparkContext

/**
  * This example solves traffic user equilibrium problem (TAP) for two road graph.
  */
object TwoRoadAssignment {
  def main(args: Array[String]): Unit = {
    // graph object
    val g = new Graph()
    // add edges to the graph
    g.addEdges(ExampleData.edges)
    val sc = new SparkContext()

    // Origin-Destination matrix (RDD)
    val rowODM = sc.parallelize(ExampleData.odm)
    rowODM.persist()
    // cost function with parameters \alpha and \beta. initCost * (1 + alpha * (traffic/capacity)^beta)
    val cf = new BasicCostFunction(0.75, 4)

    // assignment objects, maximum number of iteration is 20 and maximum relative gap is 0.0001
    val a1 = new BAssignment(sc.broadcast(g), ExampleData.cost, cf, ExampleData.capacity, 20, 0.0001)
    val a2 = new PathBasedAssignment(sc.broadcast(g), ExampleData.cost, cf, ExampleData.capacity, 20, 0.0001, 0.8)

    val tr1 = a1.run(rowODM)
    val tr2 = a2.run(rowODM)

    println("Traffic on edges using B algorithm is          ("+tr1(0)+", "+tr1(1)+")")
    println("Traffic on edges using Path based algorithm is ("+tr2(0)+", "+tr2(1)+")")
  }
}
