package com.kolovsky.example

import com.kolovsky.modeler.Zone

object ExampleData {
  // zones
  val zone1 = new Zone(1, 1)
  val zone2 = new Zone(2, 2)
  // set of edges
  val edges = Array((1, 2), (1, 2))
  // cost edge map
  val cost = Array(15.0, 20.0)
  // capacity edge map
  val capacity = Array(1000.0, 3000.0)
  // Origin-Destination matrix in Row form
  val odm = Array((zone1, Array((zone2, 8000.0))))
}
