package com.kolovsky.modeler

import com.kolovsky.graph.Graph
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
  * Created by kolovsky on 26.8.17.
  */
class EqilibriumDescentEstimator(odm: RDD[(Zone, Array[(Zone, Double)])],
                                 counts: Array[(Int, Double)],
                                 g: Broadcast[Graph],
                                 initCost: Array[Double],
                                 capacity: Array[Double],
                                 maxIter: Int,
                                 cf: CostFunction = new BasicCostFunction()
                                ) extends ODMEstimator with Serializable{

  override def estimate(): Constant.ROWODM = {
    val hm = HashMap.empty[Int, Double]
    hm ++= counts

    // assignment
    val assignmentor = new PathBasedAssignment(g, initCost, cf, capacity, 8, 0, 0.8)

    //main loap
    var iter = 0
    var odmt = odm
    var odmt_old: RDD[(Zone, Array[(Zone, Double)])] = null
    var vDa: RDD[(Int, Double)] = null
    var after_direction: RDD[Array[ODCell]] = null
    while (iter < maxIter){

      val traffic = assignmentor.run(odmt)
      val paths = assignmentor.getPaths()

      //OF
      val OF = counts.map(e => {
        math.pow(traffic(e._1) - e._2, 2)
      }).sum
      println("OF: " + OF)

      // count profiles and clear path
      val after_profiles = paths.map(_.map(c =>{
        c.paths = c.paths.map(p => {
          val count_profiles = ArrayBuffer.empty[Int]
          for(edgeIndex <- p.path){
            if(hm.contains(edgeIndex)){
              count_profiles += edgeIndex
            }
          }
          p.count_profiles = count_profiles.toArray
          p.path = null
          p
        })
        c
      }))

      // gradient
      val after_gradient = after_profiles.map(_.map(c =>{
        c.gradient = c.paths.map(p => {
          val pk = p.flow / c.flow
          var p_sum = 0.0
          for(edgeIndex <- p.count_profiles){
            if(hm.contains(edgeIndex)){
              val refVal = hm(edgeIndex)
              val modelVal = traffic(edgeIndex)
              p_sum += (modelVal - refVal)
            }
          }
          p_sum
        }).sum

        c
      }))

      //direction
      if (iter != 0){
        after_direction.unpersist(blocking = false)
      }
      after_direction = after_gradient.map(_.map(c => {
        c.direction = c.gradient
        c
      }))
      after_direction.persist()
      after_direction.localCheckpoint()

      // v derivation by a
      if (iter != 0){
        vDa.unpersist(blocking = false)
      }

      vDa = after_direction.flatMap(_.flatMap(c => {
        c.paths.flatMap(p => p.count_profiles.map(id => (id, - c.direction * c.flow * (p.flow / c.flow))))
      })).reduceByKey(_+_)
      vDa.persist()
      vDa.localCheckpoint()

      val citatel = vDa.map(x => {
        x._2 * (hm(x._1) - traffic(x._1))
      }).reduce(_+_)

      val jmenovatel = vDa.map(x => math.pow(x._2,2)).reduce(_+_)

      val lambda = citatel / jmenovatel

      //move in direction
      val after_move = after_direction.map(_.map(c => {
        var new_flow = c.flow*(1 - lambda * c.direction)
        if (new_flow < 0){
          new_flow = 0
        }
        c.flow = new_flow
        c
      }))
      // transform to RowODM
      odmt_old = odmt
      odmt = after_move.map(row => {
        val aa = row.map(c => {
          (c.t, c.flow)
        })

        (row(0).s, aa)
      })
      odmt.persist()
      odmt.localCheckpoint()

      odmt_old.unpersist(blocking = false)

      iter += 1
    }
    odmt
  }

  override def getInfo(): String = {
    null
  }
}
