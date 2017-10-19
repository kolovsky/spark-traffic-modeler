package com.kolovsky.modeler

import com.kolovsky.graph.Graph
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Object provides functionality for creting a target ODM using Gravity model.
  */
object TargetODMCreator extends Serializable{
  /**
    * Return ODM based on deterrence function "df".
    * @param zones set of zones RDD[(Zone, demand)], demand = number of trips
    * @param g graph
    * @param costEdgeMap edge cost map
    * @param max_re maximal relative error
    * @param df deterrence function, cost => Double
    * @return Origin-Destination matrix
    */
  def getODM(zones: RDD[(Zone, Double)],
             g: Broadcast[Graph],
             costEdgeMap: Array[Double],
             max_re: Double,
             df: Double => Double): Types.ROWODM = {

    zones.persist()
    // hash map of trips
    val hm = mutable.HashMap.empty[Zone, Double]
    hm ++= zones.collect()


    // cost matrix creation
    val c = getCostMatrix(zones.map(_._1), g, costEdgeMap)
    var before_ipf = c.flatMap(row => row._2.map(c => {
      (row._1, c._1, df(c._2))
    }))
    before_ipf.persist()
    before_ipf.localCheckpoint()

    // iterative proportial fitting (only one iteration)
    // relative error
    var re = Double.PositiveInfinity
    while (re > max_re){
      val row_k = before_ipf.map(c => (c._1, c._3))
        .reduceByKey(_+_)
        .map(sum => {(sum._1, hm(sum._1)/sum._2)})

      val after_row = before_ipf.map(c => (c._1,c))
        .join(row_k)
        .map(x => (x._2._1._1, x._2._1._2, x._2._1._3 * x._2._2))
      after_row.persist()

      val column_k = after_row.map(c => (c._2, c._3))
        .reduceByKey(_+_)
        .map(sum => (sum._1, hm(sum._1)/sum._2))
      column_k.persist()

      val balanced_odm = after_row.map(c => (c._2, c))
        .join(column_k)
        .map(x => (x._2._1._1, x._2._1._2, x._2._1._3 * x._2._2))
      balanced_odm.persist()
      balanced_odm.localCheckpoint()
      before_ipf = balanced_odm

      // pesnost vyvazeni
      val koef = balanced_odm.map(c => (c._1, c._3))
        .reduceByKey(_+_)
        .map(sum => hm(sum._1)/sum._2).mean()
      re = math.abs(1 - koef)

      println("average relative error is %.3f".formatLocal(java.util.Locale.US, re*100) + " %")
    }

    Types.ODMtoROWODM(before_ipf)
  }

  /**
    * Compure cost matrix
    * @param zones set of zones
    * @param g graph
    * @param cost edge cost map
    * @return cost matrix
    */
  def getCostMatrix(zones: RDD[Zone], g: Broadcast[Graph], cost: Array[Double]): Types.CostM = {
    val target = zones.map(z => (z.n, z)).collect()
    zones.map(z =>{
      val sp = g.value.getShortestPathsT(z.n, target, cost)
      (z, sp.map(x =>{(x._3, x._2)}).filter(_._2 != 0))
    })
  }
}
