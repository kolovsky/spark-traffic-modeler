package com.kolovsky.modeler
import com.kolovsky.graph.{Edge, Graph}
import com.kolovsky.modeler.Types.ROWODM
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Compute user-equilibrium traffic assignment according B algorithm.
  * Robert B. Dial, A path-based user-equilibrium traffic assignment algorithm that obviates path storage and enumeration,
  * In Transportation Research Part B: Methodological, Volume 40, Issue 10, 2006, Pages 917-936, ISSN 0191-2615,
  * https://doi.org/10.1016/j.trb.2006.02.008.
  *
  * Running only with graph, where all edge are oneway.
  * @param g graph with oneway only
  * @param initCost init cost edge map
  * @param cf cost function
  * @param capacityEdgeMap capacity edge map
  * @param maxIter maximum number of iteration
  * @param epsilon maximum relative gap
  * @param callbackIter this function is call after every iteration with traffic argument
  */

class BAssignment(g: Broadcast[Graph],
                  initCost: Array[Double],
                  cf: CostFunction,
                  capacityEdgeMap: Array[Double],
                  maxIter: Int,
                  epsilon: Double,
                  callbackIter: Array[Double] => Unit = _ => ()) extends Assignment with Serializable {

  var lbs: ArrayBuffer[Double] = ArrayBuffer.empty

  override def run(odm: ROWODM): Array[Double] = {
    println("This algorithm can not be distributed. Serial algorithm are going to be performed.")
    runSerial(odm)
  }

  def runSerial(odm: ROWODM): Array[Double] = {
    // time
    val time = System.currentTimeMillis()

    // creating bush and loading bush
    val bushes = odm.map(row =>{
      val s = row._1.n
      // build bush
      val bush = g.value.getBush(s, initCost)
      // loading traffic on the bush using All-or-nothing
      val bush_traffic = loadBush(row, bush)

      (s, bush, bush_traffic)
    })
      .collect()

    var cost: Array[Double] = null
    val trafficEdgeMap = Array.fill[Double](g.value.edges.length)(0)

    // sum of all traffic_bush
    for (bb <- bushes){
      for (i <- bb._3.indices){
        trafficEdgeMap(i) += bb._3(i)
      }
    }

    cost = g.value.edges.map(e => cf.cost(trafficEdgeMap(e.i), capacityEdgeMap(e.i), initCost(e.i)))

    // init iterators
    var it = 0
    var it_g = 0
    var actual_bush_index = 0
    var rg = Double.PositiveInfinity

    // main loop
    while (it_g < maxIter && rg > epsilon){

      actual_bush_index = it % bushes.length

      var bush = bushes(actual_bush_index)._2
      val bush_traffic = bushes(actual_bush_index)._3
      val s = bushes(actual_bush_index)._1

      // compute min and max paths
      var ((dist_min, prev_min),(dist_max, prev_max)) = g.value.getMinMaxTree(s, cost, bush, bush_traffic)

      // shift traffic
      for(n <- g.value.nodes.map(_.i)) {
        if (prev_max(n)!= null && prev_min(n) != null){
          if(prev_max(n).i != prev_min(n).i){
            val (p_min, p_max) = shiftTraffic2(n, prev_min, prev_max)

            if (!p_min.isEmpty && !p_max.isEmpty){
              val traffic_addition = shiftTraffic(p_min, p_max, trafficEdgeMap, bush_traffic)
              for (i <- bush_traffic.indices){
                bush_traffic(i) += traffic_addition(i)
                trafficEdgeMap(i) += traffic_addition(i)
              }
            }
          }
        }
      }

      // compute new cost after shift
      cost = g.value.edges.map(e => cf.cost(trafficEdgeMap(e.i), capacityEdgeMap(e.i), initCost(e.i)))

      // impruve bush
      bush = impruveBush(s, cost, bush, bush_traffic)

      bushes(actual_bush_index) = (s, bush, bush_traffic)

      if (actual_bush_index == bushes.length - 1){
        // objective function
        val OF = g.value.edges.map(e => {
          cf.integral(trafficEdgeMap(e.i), capacityEdgeMap(e.i), initCost(e.i))
        }).sum

        // relative gap
        rg = relativeGap(OF, trafficEdgeMap, cost, odm)
        println((System.currentTimeMillis() - time)+", "+rg)

        // callback
        callbackIter(trafficEdgeMap)
        it_g += 1
      }
      it += 1
    }
    trafficEdgeMap
  }

  /**
    * Impruve bush [Dial2006]
    * @param s source node ID
    * @param cost cost edge map
    * @param bush bush edge map
    * @param bush_traffic bush traffic edge map
    * @return impruved bush
    */
  private def impruveBush(s: Int, cost: Array[Double], bush: Array[Boolean], bush_traffic: Array[Double]): Array[Boolean] = {
    val ((dist_min_tree, _),(_, _)) = g.value.getMinMaxTree(s, cost, bush, bush_traffic)

    for (e <- g.value.edges){
      val em_a = g.value.nodes(e.t).edges.filter(em => em.t == e.s)
      if (em_a.nonEmpty){
        val em = em_a(0)
        val i_cost = dist_min_tree(em.s)
        val j_cost = dist_min_tree(em.t)
        val e_cost = cost(em.i)
        if (i_cost + e_cost < j_cost){
          bush(e.i) = false
          bush(em.i) = true
        }
      }
    }
    bush
  }

  /**
    * Find min and max path form common node "m" to "n" given "n" [Dial2006]
    * @param node_index index of node "n"
    * @param pred_min min tree
    * @param pred_max max tree
    * @return (min path, max path)
    */
  private def shiftTraffic2(node_index: Int, pred_min: Array[Edge], pred_max: Array[Edge]): (Array[Edge], Array[Edge]) ={
    // node_index, index of edge in reverse path
    val hm = mutable.HashMap.empty[Int, Int]
    val t_node = g.value.nodes(node_index)
    var p_min: Array[Edge] = null
    var p_max: Array[Edge] = null


    // make hash map from min path
    val path_min: ArrayBuffer[Edge] = ArrayBuffer()
    var ae = pred_min(t_node.i)
    var ai = t_node.i
    hm += ((ai, ai))
    var i = 0
    while (ae != null){
      path_min += ae
      ai = ae.s
      hm += ((ai, i))
      ae = pred_min(ai)
      i += 1
    }

    // fins common "m"
    val path_max: ArrayBuffer[Edge] = ArrayBuffer()
    ae = pred_max(t_node.i)
    ai = t_node.i
    while (ae != null){
      path_max += ae
      ai = ae.s
      ae = pred_max(ai)
      if (hm.contains(ai)){
        ae = null
        p_max = path_max.reverse.toArray
        p_min = path_min.slice(0, hm(ai) + 1).reverse.toArray
      }
    }

    if (p_max(0).s != p_min(0).s || p_max.last.t != p_min.last.t){
      throw new Exception("problem in shiftTraffic 2 " + p_max.length + " "+p_min.length)
    }
    (p_min, p_max)
  }

  /**
    * Shift traffic from max path to min path.
    * @param p_min min path
    * @param p_max max path
    * @param traffic traffic edge map
    * @param bush_traffic bush traffic edge map
    * @return traffic addition (traffic difference)
    */
  private def shiftTraffic(p_min: Array[Edge],
                           p_max: Array[Edge],
                           traffic: Array[Double],
                           bush_traffic: Array[Double]): Array[Double] = {

    // initialization of traffic additions to zero
    val traffic_addition = Array.fill[Double](traffic.length)(0)

    // min value of the path traffic
    val max_dx_value = p_max.map(e => bush_traffic(e.i)).min
    if (max_dx_value <= 0){
      return traffic_addition
    }

    var it = 0
    var cost_path_min = 0.0
    var cost_path_max = 0.0
    var d_cost_p_min = 0.0
    var d_cost_p_max = 0.0
    var dx: Double = 0
    while (it < 2000){
      // compute path cost and derivation of the path cost
      cost_path_min = p_min.map(e => cf.cost(traffic(e.i) + traffic_addition(e.i), capacityEdgeMap(e.i), initCost(e.i))).sum
      cost_path_max = p_max.map(e => cf.cost(traffic(e.i) + traffic_addition(e.i), capacityEdgeMap(e.i), initCost(e.i))).sum
      d_cost_p_min = p_min.map(e => cf.derivation(traffic(e.i) + traffic_addition(e.i), capacityEdgeMap(e.i), initCost(e.i))).sum
      d_cost_p_max = p_max.map(e => cf.derivation(traffic(e.i) + traffic_addition(e.i), capacityEdgeMap(e.i), initCost(e.i))).sum

      // newton method
      dx = (cost_path_max - cost_path_min) / (d_cost_p_max + d_cost_p_min)

      if (dx > max_dx_value){
        it = 10000
        dx = max_dx_value
      }
      if (dx < 0){
        it = 10000
        dx = 0
      }

      //actualizate traffic addition
      for (e <- p_min){
        traffic_addition(e.i) = dx
      }
      for (e <- p_max){
        traffic_addition(e.i) = -dx
      }
      it += 1
    }
    traffic_addition
  }

  /**
    * Load traffic to the bush (All-or-nothing)
    * @param row one row from ROWODM
    * @param bush bush (bush edge map)
    * @return bush traffic (traffic, where origin at root of the bush)
    */
  private def loadBush(row: (Zone, Array[(Zone, Double)]), bush: Array[Boolean]): Array[Double] = {
    val s = row._1.n
    // get min tree
    val zeros = Array.fill[Double](bush.length)(0) // only for minMaxTree
    val ((_, prev_min),(_, _)) = g.value.getMinMaxTree(s, initCost, bush, zeros)

    // traffic bush array
    val bush_traffic = Array.fill[Double](bush.length)(0)
    for (tt <- row._2){
      val flow = tt._2
      val t = tt._1.n
      val path = g.value.getPath(prev_min, t)
      for (e <- path){
        bush_traffic(e.i) += flow
      }
    }
    bush_traffic
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

  override def getInfo(): String = {
    null
  }

}
