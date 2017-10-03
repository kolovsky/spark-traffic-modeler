package com.kolovsky.modeler

import org.apache.spark.rdd.RDD

import scala.collection.mutable

object LineSearch {
  def updateODM(cells: RDD[ODCell], hm: mutable.HashMap[Int, Double], traffic: Array[Double]): RDD[ODCell] ={
    val vDa = cells.flatMap(c => {
      c.paths.flatMap(p => p.count_profiles.map(id => (id, - c.direction * c.flow * (p.flow / c.flow))))
    }).reduceByKey(_+_)
    vDa.persist()
    vDa.localCheckpoint()

    val citatel = vDa.map(x => {
      x._2 * (hm(x._1) - traffic(x._1))
    }).reduce(_+_)

    val jmenovatel = vDa.map(x => math.pow(x._2,2)).reduce(_+_)

    val lambda = citatel / jmenovatel

    cells.map(c => {
      var new_flow = c.flow*(1 - lambda * c.direction)
      if (new_flow < 0){
        new_flow = 0
      }
      c.flow = new_flow
      c
    })
  }
}
