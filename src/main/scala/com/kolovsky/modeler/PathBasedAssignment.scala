package com.kolovsky.modeler

import com.kolovsky.graph.Graph
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Solve user equlibrium problem using by Path based algorithm
  * @param g graph
  * @param initCost cost edge map without congestion effect
  * @param cf dependence between traffic volume and cost
  * @param capacityEdgeMap capacity edge map
  * @param maxIter maximum number of iteration
  * @param epsilon maximal relative gap
  * @param alpha step size for GP algorithm
  * @param callbackIter callback function, that is call after every iteration with traffic edge map
  */
class PathBasedAssignment(g: Broadcast[Graph],
                          initCost: Array[Double],
                          cf: CostFunction,
                          capacityEdgeMap: Array[Double],
                          maxIter: Int,
                          epsilon: Double,
                          alpha: Double,
                          callbackIter: Array[Double] => Unit = _ => ()) extends Assignment with Serializable{

  var ps_rdd: RDD[Array[PathSet]] = _
  var lbs: ArrayBuffer[Double] = ArrayBuffer.empty
  var info: String = ""

  /**
    * Perform the algorithm with Origin-Destinatin Matrix
    * @param odm - Origin-Destinatin Matrix
    * @return traffic edge map
    */
  def run(odm: Types.ROWODM): Array[Double] ={
    val time = System.currentTimeMillis()
    // INICIALIZATION
    val paths = odm.map(x => {
      val target = x._2.map(t => (t._1.n, t))
      g.value.getShortestPathsT(x._1.n, target, initCost).map(p => (x._1, p._3._1, p._2, p._3._2, p._4))
    })
    paths.persist()

    // all-or-nothing assignment
    val traffic = paths.flatMap(x => x).flatMap(x => x._5.map(e => (e.i, x._4)))
      .reduceByKey(_+_).collect()

    val trafficEdgeMap: Array[Double] = Array.ofDim(initCost.length)
    for (tr <- traffic){
      trafficEdgeMap(tr._1) = tr._2
    }
    // callback after all-or-nothing
    callbackIter(trafficEdgeMap)

    // compution init OF value
    val OF_init = g.value.edges.map(e => {
      cf.integral(trafficEdgeMap(e.i), capacityEdgeMap(e.i), initCost(e.i))
    }).sum
    println("OF init = "+math.floor(OF_init))

    // init PathSet
     ps_rdd = paths.map(row => row.map(x => {
         val ps = new PathSet(x._1, x._2, x._4)
         val np = new Path()
         np.path = x._5.map(_.i)
         np.cost = x._3
         np.flow = x._4
         ps.paths += np
         ps
       }))

    var cost: Array[Double] = null
    cost = g.value.edges.map(e => cf.cost(trafficEdgeMap(e.i), capacityEdgeMap(e.i), initCost(e.i)))
    var n = 0
    var OF = 0.0 // value of objective function
    var rg = 1.0 // value of relative gap
    while (n < maxIter &&  rg > epsilon){
      // UPDATE
      val after_update = ps_rdd.map(row => row.map(ps => {
        ps.updateCost(cost)
        ps
      }))
      // DIRECTION FINDING
      val after_direction = after_update.map(row => {
        val s = row(0).s.n
        val target = row.map(_.t.n)
        val paths = g.value.getShortestPaths(s, target, cost)
        for (i <- row.indices){
          row(i).addPathGetShortest(paths(i)._3.map(_.i), paths(i)._2)
        }
        row
      })
      //MOVE
      val after_move = after_direction.map(row => {
        for (i <- row.indices){
          row(i).move(alpha, capacityEdgeMap, initCost, trafficEdgeMap, cf)
        }
        row
      })
      // assignment
      after_move.persist()
      after_move.localCheckpoint()

      val traffic = after_move.flatMap(x => x)
        .flatMap(ps => ps.paths)
        .flatMap(p => p.path.map(id => (id, p.flow)))
        .reduceByKey(_+_).collect()
      for (tr <- traffic){
        trafficEdgeMap(tr._1) = tr._2
      }
      ps_rdd.unpersist(blocking = false)
      ps_rdd = after_move
      n += 1

      //callback
      callbackIter(trafficEdgeMap)
      // compute cost from traffic
      cost = g.value.edges.map(e => cf.cost(trafficEdgeMap(e.i), capacityEdgeMap(e.i), initCost(e.i)))

      // objective function
      OF = g.value.edges.map(e => {
        cf.integral(trafficEdgeMap(e.i), capacityEdgeMap(e.i), initCost(e.i))
      }).sum

      // relative gap
      rg = relativeGap(OF, trafficEdgeMap, cost, odm)
      //println("RG: "+rg)
      println((System.currentTimeMillis() - time)+", "+rg)
      info += (", "+rg)

    }
    trafficEdgeMap
  }

  /**
    * Get information about computing (e.g value of objective function)
    * @return
    */
  def getInfo(): String ={
    null
  }

  /**
    * Get all paths after assignment
    * @return
    */
  def getPaths(): RDD[Array[ODCell]] = {
    ps_rdd.map(_.map(ps => {
      ps.toODCell()
    }))
  }

  def relativeGap(OF: Double, trafficEdgeMap: Array[Double], costEdgeMap: Array[Double], odm: Types.ROWODM): Double ={
    val aon = new AllOrNothingAssignment(g, costEdgeMap)
    val aon_trafficEdgeMap = aon.run(odm)
    var gap: Double = 0
    for (i <- trafficEdgeMap.indices){
      gap += costEdgeMap(i) * (aon_trafficEdgeMap(i) - trafficEdgeMap(i))
    }
    lbs += (OF + gap)
    val blb = lbs.max
    val rg = - gap / math.abs(blb)
    rg
  }

}
