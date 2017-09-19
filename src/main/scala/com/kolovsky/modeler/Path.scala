package com.kolovsky.modeler

/**
  * Created by kolovsky on 26.8.17.
  */
class Path {
  // array of edge index
  var path: Array[Int] = _
  // flow
  var flow: Double = 0
  // cost
  var cost: Double = 0
  // filtred path
  var count_profiles: Array[Int] = _
}
