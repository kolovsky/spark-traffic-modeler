package com.kolovsky.modeler
import com.kolovsky.graph.Graph
import com.kolovsky.modeler.Types.ROWODM
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Origin-Destinatin Matrix estimation for uncogested network
  * @param odm Origin-Destination matrix
  * @param g - graph
  * @param costEdgeMap cost edge map
  * @param counts - calibration data Array[(index, traffic)]
  * @param maxIter - maximum number of iteration
  * @param method - method for determing search direction
  *               "SD" - steepest descent (negative value of gradient)
  *               "PR" - conjugate gradient method - Polak-Ribier
  */

class AllOrNothingEstimator(odm: ROWODM,
                            g: Broadcast[Graph],
                            costEdgeMap: Array[Double],
                            counts: Array[(Int, Double)],
                            maxIter: Int,
                            method: String) extends ODMEstimator with Serializable{
  val hm = mutable.HashMap.empty[Int, Double]
  hm ++= counts

  override def estimate(): ROWODM = {
    val trafficEdgeMap = Array.ofDim[Double](g.value.edges.length)
    var cells = getODCells()
    cells.persist()
    cells.localCheckpoint()

    var n = 0
    while (n < maxIter){
      // assignment
      val traffic = cells.flatMap(c =>{
        c.paths(0).count_profiles.map(index => (index, c.flow))
      }).reduceByKey(_+_).collect()
      for (tr <- traffic){
        trafficEdgeMap(tr._1) = tr._2
      }

      val OF = counts.map(e => {
        math.pow(trafficEdgeMap(e._1) - e._2, 2)
      }).sum
      println("OF: " + OF)

      // compute gradient
      val after_gradient = cells.map(c =>{
        var sum = 0.0
        for(index <- c.paths(0).count_profiles){
          sum += (trafficEdgeMap(index) - hm(index))
        }
        c.old_gradient = c.gradient
        c.gradient = sum
        c
      })

      var after_direction: RDD[ODCell] = null
      if (method == "SD" || n == 0){
        after_direction = after_gradient.map(c => {
          c.old_direction = c.direction
          c.direction = c.gradient
          c
        })
      }
      else {
        var beta = 0.0
        if (method == "PR"){
          beta = after_gradient.map(c => (c.gradient - c.old_gradient) * c.gradient).reduce(_+_) / after_gradient.map(c => math.pow(c.old_gradient, 2)).reduce(_+_)
        }
        after_direction = after_gradient.map(c => {
          c.old_direction = c.direction
          c.direction = c.gradient + beta * c.old_direction
          c
        })
      }

      after_direction.persist()
      after_direction.localCheckpoint()

      cells = LineSearch.updateODM(after_direction, hm, trafficEdgeMap)
      cells.persist()
      cells.localCheckpoint()

      n += 1
    }
    Types.cellsToROWODM(cells)
  }

  override def getInfo(): String = {
    null
  }

  def getODCells(): RDD[ODCell] = {
    odm.flatMap(row => {
      val target = row._2.map(t => (t._1.n, t))
      val sp = g.value.getShortestPathsT(row._1.n, target, costEdgeMap)
      // from, to, flow, path
      sp.map(c => (row._1, c._3._1, c._3._2, c._4))
    }).map(c => {
      val count_profile = ArrayBuffer.empty[Int]
      // filter count profile
      for (e <- c._4){
        if(hm.contains(e.i)){
          count_profile += e.i
        }
      }
      // create path
      val path = new Path()
      path.count_profiles = count_profile.toArray
      path.flow = c._3
      // create cell
      val cell = new ODCell()
      cell.s = c._1
      cell.t = c._2
      cell.flow = c._3
      cell.paths += path
      cell
    })
  }

}
